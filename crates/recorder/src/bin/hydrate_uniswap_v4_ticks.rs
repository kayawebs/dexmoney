use std::{collections::BTreeMap, env, str::FromStr, time::Instant};

use alloy_primitives::{Address, U256};
use anyhow::{Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::{
    config::Settings,
    types::{PoolId, TickState},
};
use base_arb_storage::{
    postgres::{ensure_registry_schema, PostgresStore},
    redis::RedisStore,
    TickChangeStore, TickStateStore,
};
use serde_json::{json, Value};
use sqlx::Row;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const UNISWAP_V4_MODIFY_LIQUIDITY_TOPIC: &str =
    "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec";
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";
const DEFAULT_MANAGER_SCAN_FLUSH_POOL_BATCH_SIZE: usize = 256;

#[derive(Debug, Clone)]
struct Cli {
    from_block: Option<u64>,
    to_block: Option<u64>,
    max_lookback_blocks: Option<u64>,
    limit: i64,
    chunk_blocks: u64,
    manager_scan: bool,
    apply: bool,
    sync_redis: bool,
    refresh_existing: bool,
    flush_pool_batch_size: usize,
}

#[derive(Debug, Clone)]
struct V4Pool {
    manager: Address,
    pool_uid: String,
    pool_address: Address,
    first_block: u64,
}

#[derive(Debug, Default)]
struct TickAccumulator {
    liquidity_net: i128,
    liquidity_gross: i128,
}

#[derive(Debug, Default)]
struct Summary {
    pools: usize,
    hydrated_pools: usize,
    nonzero_tick_pools: usize,
    zero_tick_pools: usize,
    marked_changed_pools: usize,
    skipped_existing_pools: usize,
    skipped_lookback_pools: usize,
    ignored_logs: usize,
    invalid_logs: usize,
    negative_gross_ticks: usize,
    chunks: usize,
    logs_seen: usize,
    ticks_written: usize,
    failed: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = parse_args(env::args().skip(1))?;
    let settings = Settings::load()?;
    let store = PostgresStore::connect(&settings.postgres_url).await?;
    ensure_registry_schema(&store.pool).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;
    let provider = ChainProvider::from_settings(&settings);
    let latest_block = provider.get_block_number().await?;
    let to_block = cli.to_block.unwrap_or(latest_block);

    let pools = fetch_quoteable_v4_pools(&store, settings.chain_id, cli.limit).await?;
    println!("== Uniswap V4 Tick Hydration ==");
    println!(
        "mode={} pools={} to_block={} chunk_blocks={} sync_redis={}",
        if cli.apply { "apply" } else { "dry-run" },
        pools.len(),
        to_block,
        cli.chunk_blocks,
        cli.sync_redis
    );
    let run_from_block = cli.from_block.unwrap_or_else(|| {
        cli.max_lookback_blocks
            .map(|lookback| to_block.saturating_sub(lookback))
            .or_else(|| pools.iter().map(|pool| pool.first_block).min())
            .unwrap_or(to_block)
    });
    let hydration_run = if cli.apply {
        Some(
            store
                .start_pool_tick_hydration_run(
                    settings.chain_id,
                    "uniswap-v4",
                    settings.uniswap_v4_pool_manager,
                    run_from_block,
                    to_block,
                    pools.len(),
                )
                .await?,
        )
    } else {
        None
    };

    let summary = if cli.manager_scan {
        hydrate_ticks_by_manager_scan(
            &settings,
            &provider,
            &store,
            &redis,
            pools,
            cli.from_block,
            cli.max_lookback_blocks,
            to_block,
            cli.chunk_blocks,
            cli.apply,
            cli.sync_redis,
            cli.refresh_existing,
            hydration_run,
            cli.flush_pool_batch_size,
        )
        .await?
    } else {
        hydrate_ticks(
            &settings,
            &provider,
            &store,
            &redis,
            pools,
            cli.from_block,
            cli.max_lookback_blocks,
            to_block,
            cli.chunk_blocks,
            cli.apply,
            cli.sync_redis,
            cli.refresh_existing,
            hydration_run,
        )
        .await?
    };
    if let Some(run_id) = hydration_run {
        store
            .finish_pool_tick_hydration_run(
                run_id,
                summary.hydrated_pools,
                summary.ticks_written,
                if summary.failed == 0 {
                    "completed"
                } else {
                    "completed_with_failures"
                },
            )
            .await?;
    }

    println!();
    println!(
        "pools={} hydrated_pools={} chunks={} logs_seen={} ticks_written={} failed={}",
        summary.pools,
        summary.hydrated_pools,
        summary.chunks,
        summary.logs_seen,
        summary.ticks_written,
        summary.failed
    );
    println!(
        "nonzero_tick_pools={} zero_tick_pools={} marked_changed_pools={} skipped_existing_pools={} skipped_lookback_pools={} ignored_logs={}",
        summary.nonzero_tick_pools,
        summary.zero_tick_pools,
        summary.marked_changed_pools,
        summary.skipped_existing_pools,
        summary.skipped_lookback_pools,
        summary.ignored_logs
    );
    println!(
        "invalid_logs={} negative_gross_ticks={}",
        summary.invalid_logs, summary.negative_gross_ticks
    );
    Ok(())
}

async fn fetch_quoteable_v4_pools(
    store: &PostgresStore,
    chain_id: u64,
    limit: i64,
) -> Result<Vec<V4Pool>> {
    let limit_clause = if limit > 0 { "LIMIT $3" } else { "" };
    let query = format!(
        r#"
        SELECT manager_address, pool_uid, pool_address, first_block
        FROM protocol_pool_observations
        WHERE chain_id = $1
          AND protocol = 'uniswap-v4'
          AND pool_address IS NOT NULL
          AND token0 IS NOT NULL
          AND token1 IS NOT NULL
          AND fee_pips IS NOT NULL
          AND tick_spacing IS NOT NULL
          AND lower(COALESCE(hooks_address, $2)) = lower($2)
        ORDER BY logs_30d DESC, latest_block DESC, updated_at DESC
        {limit_clause}
        "#
    );
    let rows = sqlx::query(&query)
        .bind(i64::try_from(chain_id)?)
        .bind(ZERO_ADDRESS);
    let rows = if limit > 0 {
        rows.bind(limit).fetch_all(&store.pool).await?
    } else {
        rows.fetch_all(&store.pool).await?
    };

    rows.into_iter()
        .map(|row| {
            let manager = row.try_get::<String, _>("manager_address")?;
            let pool_uid = row.try_get::<String, _>("pool_uid")?;
            let pool_address = row.try_get::<String, _>("pool_address")?;
            let first_block = row.try_get::<i64, _>("first_block")?;
            Ok(V4Pool {
                manager: Address::from_str(&manager)
                    .with_context(|| format!("invalid manager address {manager}"))?,
                pool_uid: normalize_topic(&pool_uid)?,
                pool_address: Address::from_str(&pool_address)
                    .with_context(|| format!("invalid pool address {pool_address}"))?,
                first_block: u64::try_from(first_block)?,
            })
        })
        .collect()
}

async fn hydrate_ticks(
    settings: &Settings,
    provider: &ChainProvider,
    store: &PostgresStore,
    redis: &RedisStore,
    pools: Vec<V4Pool>,
    from_block_override: Option<u64>,
    max_lookback_blocks: Option<u64>,
    to_block: u64,
    chunk_blocks: u64,
    apply: bool,
    sync_redis: bool,
    refresh_existing: bool,
    hydration_run: Option<Uuid>,
) -> Result<Summary> {
    let mut summary = Summary {
        pools: pools.len(),
        ..Summary::default()
    };
    for pool in pools {
        if !refresh_existing && from_block_override.is_none() {
            let existing_ticks = redis.get_pool_ticks(pool.pool_address).await?;
            if !existing_ticks.is_empty() {
                println!(
                    "skip existing pool={} uid={} ticks={}",
                    pool.pool_address,
                    pool.pool_uid,
                    existing_ticks.len()
                );
                summary.skipped_existing_pools += 1;
                continue;
            }
        }
        let mut from_block = from_block_override.unwrap_or(pool.first_block);
        if let Some(max_lookback_blocks) = max_lookback_blocks {
            let min_from_block = to_block.saturating_sub(max_lookback_blocks);
            if from_block < min_from_block {
                println!(
                    "skip lookback pool={} uid={} from={} min_from={} to={} max_lookback_blocks={}",
                    pool.pool_address,
                    pool.pool_uid,
                    from_block,
                    min_from_block,
                    to_block,
                    max_lookback_blocks
                );
                summary.skipped_lookback_pools += 1;
                continue;
            }
            from_block = from_block.max(min_from_block);
        }
        let mut cursor = from_block;
        let started_at = Instant::now();
        let total_blocks = block_span(from_block, to_block);
        let mut ticks: BTreeMap<i32, TickAccumulator> = BTreeMap::new();
        while cursor <= to_block {
            let chunk_to = to_block.min(cursor.saturating_add(chunk_blocks).saturating_sub(1));
            summary.chunks += 1;
            let logs =
                match fetch_modify_liquidity_logs_split(provider, &pool, cursor, chunk_to).await {
                    Ok(logs) => logs,
                    Err(err) => {
                        summary.failed += 1;
                        println!(
                            "failed pool={} uid={} blocks={}..{} error={err:#}",
                            pool.pool_address, pool.pool_uid, cursor, chunk_to
                        );
                        break;
                    }
                };
            let chunk_logs = logs.len();
            summary.logs_seen += chunk_logs;
            let before_ticks = ticks.len();
            for raw in logs {
                let delta = match parse_modify_liquidity_delta(&raw) {
                    Ok(Some(delta)) => delta,
                    Ok(None) => continue,
                    Err(err) => {
                        summary.invalid_logs += 1;
                        println!("invalid modify liquidity log skipped error={err:#} raw={raw}");
                        continue;
                    }
                };
                if let Err(err) = apply_delta(&mut ticks, delta) {
                    summary.invalid_logs += 1;
                    println!("invalid modify liquidity log skipped error={err:#} raw={raw}");
                }
            }
            print_progress(
                "pool_scan",
                from_block,
                to_block,
                cursor,
                chunk_to,
                summary.chunks,
                started_at,
                total_blocks,
                &format!(
                    "pool={} uid={} chunk_logs={chunk_logs} tick_keys={} new_tick_keys={} logs_seen={} failed={}",
                    pool.pool_address,
                    pool.pool_uid,
                    ticks.len(),
                    ticks.len().saturating_sub(before_ticks),
                    summary.logs_seen,
                    summary.failed
                ),
            );
            if chunk_to == u64::MAX {
                break;
            }
            cursor = chunk_to + 1;
        }

        let tick_states = ticks
            .into_iter()
            .filter(|(_, acc)| acc.liquidity_gross > 0 || acc.liquidity_net != 0)
            .map(|(tick, acc)| {
                let liquidity_gross = nonnegative_liquidity_gross(acc.liquidity_gross);
                if liquidity_gross.is_zero() && acc.liquidity_gross < 0 {
                    summary.negative_gross_ticks += 1;
                }
                Ok(TickState {
                    pool_id: PoolId {
                        chain_id: settings.chain_id,
                        address: pool.pool_address,
                    },
                    tick,
                    liquidity_net: acc.liquidity_net,
                    liquidity_gross,
                    block_number: to_block,
                    updated_at: chrono::Utc::now(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        println!(
            "pool={} uid={} ticks={} from={} to={}",
            pool.pool_address,
            pool.pool_uid,
            tick_states.len(),
            from_block,
            to_block
        );
        if tick_states.is_empty() {
            summary.zero_tick_pools += 1;
        } else {
            summary.nonzero_tick_pools += 1;
        }
        if apply {
            store
                .replace_pool_ticks_current(
                    settings.chain_id,
                    pool.pool_address,
                    &tick_states,
                    "uniswap_v4_tick_hydrator",
                )
                .await?;
            if sync_redis {
                redis
                    .replace_pool_ticks(pool.pool_address, tick_states.clone())
                    .await?;
            }
            if sync_redis && !tick_states.is_empty() {
                redis
                    .mark_tick_changed_pools(vec![pool.pool_address])
                    .await?;
                summary.marked_changed_pools += 1;
            }
        }
        summary.ticks_written += tick_states.len();
        summary.hydrated_pools += 1;
        if let Some(run_id) = hydration_run {
            store
                .update_pool_tick_hydration_run_progress(
                    run_id,
                    summary.hydrated_pools,
                    summary.ticks_written,
                )
                .await?;
        }
    }
    Ok(summary)
}

async fn hydrate_ticks_by_manager_scan(
    settings: &Settings,
    provider: &ChainProvider,
    store: &PostgresStore,
    redis: &RedisStore,
    pools: Vec<V4Pool>,
    from_block_override: Option<u64>,
    max_lookback_blocks: Option<u64>,
    to_block: u64,
    chunk_blocks: u64,
    apply: bool,
    sync_redis: bool,
    refresh_existing: bool,
    hydration_run: Option<Uuid>,
    flush_pool_batch_size: usize,
) -> Result<Summary> {
    let mut summary = Summary {
        pools: pools.len(),
        ..Summary::default()
    };
    let min_from_block = max_lookback_blocks.map(|lookback| to_block.saturating_sub(lookback));
    let mut selected = BTreeMap::<String, V4Pool>::new();
    for pool in pools {
        if !refresh_existing && from_block_override.is_none() {
            let existing_ticks = redis.get_pool_ticks(pool.pool_address).await?;
            if !existing_ticks.is_empty() {
                println!(
                    "skip existing pool={} uid={} ticks={}",
                    pool.pool_address,
                    pool.pool_uid,
                    existing_ticks.len()
                );
                summary.skipped_existing_pools += 1;
                continue;
            }
        }
        if let Some(min_from_block) = min_from_block {
            if from_block_override.is_none() && pool.first_block < min_from_block {
                println!(
                    "skip lookback pool={} uid={} from={} min_from={} to={} max_lookback_blocks={}",
                    pool.pool_address,
                    pool.pool_uid,
                    pool.first_block,
                    min_from_block,
                    to_block,
                    max_lookback_blocks.unwrap_or_default()
                );
                summary.skipped_lookback_pools += 1;
                continue;
            }
        }
        selected.insert(pool.pool_uid.clone(), pool);
    }
    if selected.is_empty() {
        return Ok(summary);
    }

    let from_block = from_block_override.or(min_from_block).unwrap_or_else(|| {
        selected
            .values()
            .map(|pool| pool.first_block)
            .min()
            .unwrap_or(to_block)
    });

    let mut managers = selected
        .values()
        .map(|pool| pool.manager)
        .collect::<Vec<_>>();
    managers.sort();
    managers.dedup();
    let mut ticks_by_pool = selected
        .keys()
        .map(|pool_uid| (pool_uid.clone(), BTreeMap::<i32, TickAccumulator>::new()))
        .collect::<BTreeMap<_, _>>();

    for manager in managers {
        let started_at = Instant::now();
        let total_blocks = block_span(from_block, to_block);
        let mut cursor = from_block;
        while cursor <= to_block {
            let chunk_to = to_block.min(cursor.saturating_add(chunk_blocks).saturating_sub(1));
            summary.chunks += 1;
            let logs =
                match fetch_modify_liquidity_logs_for_manager(provider, manager, cursor, chunk_to)
                    .await
                {
                    Ok(logs) => logs,
                    Err(err) => {
                        summary.failed += selected.len();
                        println!(
                            "failed manager={} blocks={}..{} pending={} error={err:#}",
                            manager,
                            cursor,
                            chunk_to,
                            selected.len()
                        );
                        break;
                    }
                };
            let chunk_logs = logs.len();
            summary.logs_seen += chunk_logs;
            let ignored_before = summary.ignored_logs;
            let mut chunk_matched = 0usize;
            for raw in logs {
                let Some(pool_uid) = topic_at(&raw, 1) else {
                    summary.ignored_logs += 1;
                    continue;
                };
                let Some(ticks) = ticks_by_pool.get_mut(&pool_uid) else {
                    summary.ignored_logs += 1;
                    continue;
                };
                chunk_matched += 1;
                let delta = match parse_modify_liquidity_delta(&raw) {
                    Ok(Some(delta)) => delta,
                    Ok(None) => continue,
                    Err(err) => {
                        summary.invalid_logs += 1;
                        println!("invalid modify liquidity log skipped error={err:#} raw={raw}");
                        continue;
                    }
                };
                if let Err(err) = apply_delta(ticks, delta) {
                    summary.invalid_logs += 1;
                    println!("invalid modify liquidity log skipped error={err:#} raw={raw}");
                }
            }
            let chunk_ignored = summary.ignored_logs.saturating_sub(ignored_before);
            print_progress(
                "manager_tick_scan",
                from_block,
                to_block,
                cursor,
                chunk_to,
                summary.chunks,
                started_at,
                total_blocks,
                &format!(
                    "selected_pools={} manager={} chunk_logs={chunk_logs} chunk_matched={chunk_matched} chunk_ignored={chunk_ignored} logs_seen={} ignored_logs={} failed={}",
                    selected.len(),
                    manager,
                    summary.logs_seen,
                    summary.ignored_logs,
                    summary.failed
                ),
            );
            if chunk_to == u64::MAX {
                break;
            }
            cursor = chunk_to + 1;
        }
    }

    let flush_pool_batch_size = flush_pool_batch_size.max(1);
    let mut remaining_selected = selected;
    let mut batch_ticks = BTreeMap::new();
    let mut batch_index = 0usize;
    for (pool_uid, ticks) in ticks_by_pool {
        batch_ticks.insert(pool_uid, ticks);
        if batch_ticks.len() >= flush_pool_batch_size {
            batch_index += 1;
            flush_manager_scan_batch(
                settings,
                store,
                redis,
                &mut summary,
                &mut remaining_selected,
                std::mem::take(&mut batch_ticks),
                from_block,
                to_block,
                apply,
                sync_redis,
                hydration_run,
                batch_index,
            )
            .await?;
        }
    }
    if !batch_ticks.is_empty() {
        batch_index += 1;
        flush_manager_scan_batch(
            settings,
            store,
            redis,
            &mut summary,
            &mut remaining_selected,
            batch_ticks,
            from_block,
            to_block,
            apply,
            sync_redis,
            hydration_run,
            batch_index,
        )
        .await?;
    }
    Ok(summary)
}

#[allow(clippy::too_many_arguments)]
async fn flush_manager_scan_batch(
    settings: &Settings,
    store: &PostgresStore,
    redis: &RedisStore,
    summary: &mut Summary,
    selected: &mut BTreeMap<String, V4Pool>,
    ticks_by_pool: BTreeMap<String, BTreeMap<i32, TickAccumulator>>,
    from_block: u64,
    to_block: u64,
    apply: bool,
    sync_redis: bool,
    hydration_run: Option<Uuid>,
    batch_index: usize,
) -> Result<()> {
    let mut changed_pools = Vec::new();
    for (pool_uid, ticks) in ticks_by_pool {
        let pool = selected
            .remove(&pool_uid)
            .with_context(|| format!("selected pool disappeared {pool_uid}"))?;
        let tick_states = ticks
            .into_iter()
            .filter(|(_, acc)| acc.liquidity_gross > 0 || acc.liquidity_net != 0)
            .map(|(tick, acc)| {
                let liquidity_gross = nonnegative_liquidity_gross(acc.liquidity_gross);
                if liquidity_gross.is_zero() && acc.liquidity_gross < 0 {
                    summary.negative_gross_ticks += 1;
                }
                Ok(TickState {
                    pool_id: PoolId {
                        chain_id: settings.chain_id,
                        address: pool.pool_address,
                    },
                    tick,
                    liquidity_net: acc.liquidity_net,
                    liquidity_gross,
                    block_number: to_block,
                    updated_at: chrono::Utc::now(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        println!(
            "flush pool={} uid={} ticks={} from={} to={}",
            pool.pool_address,
            pool.pool_uid,
            tick_states.len(),
            from_block,
            to_block
        );
        if tick_states.is_empty() {
            summary.zero_tick_pools += 1;
        } else {
            summary.nonzero_tick_pools += 1;
        }
        if apply {
            store
                .replace_pool_ticks_current(
                    settings.chain_id,
                    pool.pool_address,
                    &tick_states,
                    "uniswap_v4_tick_hydrator",
                )
                .await?;
            if sync_redis {
                redis
                    .replace_pool_ticks(pool.pool_address, tick_states.clone())
                    .await?;
            }
            if sync_redis && !tick_states.is_empty() {
                changed_pools.push(pool.pool_address);
            }
        }
        summary.ticks_written += tick_states.len();
        summary.hydrated_pools += 1;
    }
    if sync_redis && !changed_pools.is_empty() {
        summary.marked_changed_pools += changed_pools.len();
        redis.mark_tick_changed_pools(changed_pools).await?;
    }
    if let Some(run_id) = hydration_run {
        store
            .update_pool_tick_hydration_run_progress(
                run_id,
                summary.hydrated_pools,
                summary.ticks_written,
            )
            .await?;
    }
    println!(
        "flush summary batch={} hydrated_pools={} ticks_written={} marked_changed_pools={}",
        batch_index, summary.hydrated_pools, summary.ticks_written, summary.marked_changed_pools
    );
    Ok(())
}

async fn fetch_modify_liquidity_logs_split(
    provider: &ChainProvider,
    pool: &V4Pool,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<Value>> {
    let mut ranges = vec![(from_block, to_block)];
    let mut out = Vec::new();

    while let Some((from, to)) = ranges.pop() {
        let params = json!([{
            "fromBlock": format!("0x{from:x}"),
            "toBlock": format!("0x{to:x}"),
            "address": format!("{:#x}", pool.manager),
            "topics": [
                [UNISWAP_V4_MODIFY_LIQUIDITY_TOPIC],
                [pool.pool_uid.clone()]
            ]
        }]);
        match provider.get_logs_raw(params).await {
            Ok(mut logs) => out.append(&mut logs),
            Err(_) if from < to => {
                let mid = from + (to - from) / 2;
                ranges.push((mid + 1, to));
                ranges.push((from, mid));
            }
            Err(err) => return Err(err),
        }
    }
    Ok(out)
}

async fn fetch_modify_liquidity_logs_for_manager(
    provider: &ChainProvider,
    manager: Address,
    from_block: u64,
    to_block: u64,
) -> Result<Vec<Value>> {
    let mut ranges = vec![(from_block, to_block)];
    let mut out = Vec::new();

    while let Some((from, to)) = ranges.pop() {
        let params = json!([{
            "fromBlock": format!("0x{from:x}"),
            "toBlock": format!("0x{to:x}"),
            "address": format!("{:#x}", manager),
            "topics": [
                [UNISWAP_V4_MODIFY_LIQUIDITY_TOPIC]
            ]
        }]);
        match provider.get_logs_raw(params).await {
            Ok(mut logs) => out.append(&mut logs),
            Err(_) if from < to => {
                let mid = from + (to - from) / 2;
                ranges.push((mid + 1, to));
                ranges.push((from, mid));
            }
            Err(err) => return Err(err),
        }
    }
    Ok(out)
}

fn parse_modify_liquidity_delta(raw: &Value) -> Result<Option<(i32, i32, i128)>> {
    let Some(topic0) = topic_at(raw, 0) else {
        return Ok(None);
    };
    if topic0 != UNISWAP_V4_MODIFY_LIQUIDITY_TOPIC {
        return Ok(None);
    }
    let words = data_words(raw);
    let tick_lower = words
        .first()
        .and_then(|word| parse_word_i24(word))
        .context("ModifyLiquidity missing tickLower")?;
    let tick_upper = words
        .get(1)
        .and_then(|word| parse_word_i24(word))
        .context("ModifyLiquidity missing tickUpper")?;
    let liquidity_delta = words
        .get(2)
        .map(|word| parse_word_i256_i128(word))
        .transpose()?
        .context("ModifyLiquidity missing liquidityDelta")?;
    Ok(Some((tick_lower, tick_upper, liquidity_delta)))
}

fn apply_delta(
    ticks: &mut BTreeMap<i32, TickAccumulator>,
    (tick_lower, tick_upper, liquidity_delta): (i32, i32, i128),
) -> Result<()> {
    if liquidity_delta == 0 {
        return Ok(());
    }
    let lower = ticks.entry(tick_lower).or_default();
    lower.liquidity_gross = lower
        .liquidity_gross
        .checked_add(liquidity_delta)
        .context("liquidity_gross overflow")?;
    lower.liquidity_net = lower
        .liquidity_net
        .checked_add(liquidity_delta)
        .context("liquidity_net overflow")?;

    let upper = ticks.entry(tick_upper).or_default();
    upper.liquidity_gross = upper
        .liquidity_gross
        .checked_add(liquidity_delta)
        .context("liquidity_gross overflow")?;
    upper.liquidity_net = upper
        .liquidity_net
        .checked_sub(liquidity_delta)
        .context("liquidity_net overflow")?;
    Ok(())
}

fn nonnegative_liquidity_gross(value: i128) -> U256 {
    if value <= 0 {
        U256::ZERO
    } else {
        U256::from(value as u128)
    }
}

fn parse_args<I>(mut args: I) -> Result<Cli>
where
    I: Iterator<Item = String>,
{
    let mut cli = Cli {
        from_block: None,
        to_block: None,
        max_lookback_blocks: None,
        limit: 200,
        chunk_blocks: 10_000,
        manager_scan: false,
        apply: false,
        sync_redis: true,
        refresh_existing: false,
        flush_pool_batch_size: DEFAULT_MANAGER_SCAN_FLUSH_POOL_BATCH_SIZE,
    };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--from-block" => cli.from_block = Some(parse_next(&mut args, "--from-block")?),
            "--to-block" => cli.to_block = Some(parse_next(&mut args, "--to-block")?),
            "--max-lookback-blocks" => {
                cli.max_lookback_blocks = Some(parse_next(&mut args, "--max-lookback-blocks")?)
            }
            "--limit" => cli.limit = parse_next(&mut args, "--limit")?,
            "--chunk-blocks" => cli.chunk_blocks = parse_next(&mut args, "--chunk-blocks")?,
            "--manager-scan" => cli.manager_scan = true,
            "--apply" => cli.apply = true,
            "--no-redis" => cli.sync_redis = false,
            "--refresh-existing" => cli.refresh_existing = true,
            "--flush-pool-batch-size" => {
                cli.flush_pool_batch_size = parse_next(&mut args, "--flush-pool-batch-size")?
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }
    if cli.chunk_blocks == 0 {
        anyhow::bail!("--chunk-blocks must be > 0");
    }
    if cli.flush_pool_batch_size == 0 {
        anyhow::bail!("--flush-pool-batch-size must be > 0");
    }
    Ok(cli)
}

fn parse_next<T, I>(args: &mut I, name: &str) -> Result<T>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
    I: Iterator<Item = String>,
{
    args.next()
        .with_context(|| format!("{name} requires a value"))?
        .parse::<T>()
        .with_context(|| format!("invalid value for {name}"))
}

fn print_help() {
    println!(
        "Usage: hydrate_uniswap_v4_ticks [--from-block N] [--to-block N] [--max-lookback-blocks N] [--limit 200] [--chunk-blocks 10000] [--manager-scan] [--flush-pool-batch-size 256] [--refresh-existing] [--apply] [--no-redis]\n\nUse --limit 0 to select all quoteable V4 pools. With --apply, ticks are written to Postgres pool_ticks_current and, unless --no-redis is set, synced to Redis for search."
    );
}

fn print_progress(
    label: &str,
    from_block: u64,
    to_block: u64,
    cursor: u64,
    chunk_to: u64,
    chunks: usize,
    started_at: Instant,
    total_blocks: u64,
    details: &str,
) {
    let scanned_blocks = block_span(from_block, chunk_to);
    let elapsed_secs = started_at.elapsed().as_secs_f64().max(0.001);
    let blocks_per_sec = scanned_blocks as f64 / elapsed_secs;
    let remaining_blocks = to_block.saturating_sub(chunk_to);
    let eta_secs = if blocks_per_sec > 0.0 {
        remaining_blocks as f64 / blocks_per_sec
    } else {
        0.0
    };
    println!(
        "{label} chunk={chunks} blocks={cursor}..{chunk_to} progress={scanned_blocks}/{total_blocks} elapsed_s={elapsed_secs:.1} blocks_per_s={blocks_per_sec:.1} eta_s={eta_secs:.1} {details}"
    );
}

fn block_span(from_block: u64, to_block: u64) -> u64 {
    to_block.saturating_sub(from_block).saturating_add(1)
}

fn topic_at(raw: &Value, index: usize) -> Option<String> {
    raw.get("topics")?
        .as_array()?
        .get(index)?
        .as_str()
        .map(str::to_ascii_lowercase)
}

fn data_words(raw: &Value) -> Vec<String> {
    let data = raw.get("data").and_then(Value::as_str).unwrap_or("0x");
    let hex = data.trim_start_matches("0x");
    hex.as_bytes()
        .chunks(64)
        .filter_map(|chunk| std::str::from_utf8(chunk).ok())
        .filter(|word| word.len() == 64)
        .map(|word| format!("0x{word}"))
        .collect()
}

fn normalize_topic(raw: &str) -> Result<String> {
    let hex = raw.trim_start_matches("0x").to_ascii_lowercase();
    if hex.len() != 64 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        anyhow::bail!("invalid topic {raw}");
    }
    Ok(format!("0x{hex}"))
}

fn parse_word_i24(word: &str) -> Option<i32> {
    let value = U256::from_str_radix(word.trim_start_matches("0x"), 16).ok()?;
    let low = (value & U256::from(0x00ff_ffffu64)).to::<u32>();
    let signed = if (low & 0x0080_0000) != 0 {
        i64::from(low) - (1_i64 << 24)
    } else {
        i64::from(low)
    };
    i32::try_from(signed).ok()
}

fn parse_word_i256_i128(word: &str) -> Result<i128> {
    let clean = word.trim_start_matches("0x");
    if clean.len() != 64 {
        anyhow::bail!("int256 ABI word must be 32 bytes");
    }
    let unsigned = U256::from_str_radix(clean, 16)?;
    let negative = clean
        .as_bytes()
        .first()
        .and_then(|byte| char::from(*byte).to_digit(16))
        .map(|nibble| nibble >= 8)
        .unwrap_or(false);
    if !negative {
        let value =
            u128::try_from(unsigned).map_err(|_| anyhow::anyhow!("int256 does not fit i128"))?;
        if value > i128::MAX as u128 {
            anyhow::bail!("positive int256 does not fit i128");
        }
        return Ok(value as i128);
    }

    let magnitude = U256::MAX
        .checked_sub(unsigned)
        .and_then(|value| value.checked_add(U256::from(1u64)))
        .ok_or_else(|| anyhow::anyhow!("negative int256 magnitude overflow"))?;
    let value =
        u128::try_from(magnitude).map_err(|_| anyhow::anyhow!("int256 does not fit i128"))?;
    if value > i128::MAX as u128 {
        anyhow::bail!("negative int256 does not fit i128");
    }
    Ok(-(value as i128))
}
