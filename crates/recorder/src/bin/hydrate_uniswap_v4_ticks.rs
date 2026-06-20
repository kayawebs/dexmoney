use std::{collections::BTreeMap, env, str::FromStr};

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

const UNISWAP_V4_MODIFY_LIQUIDITY_TOPIC: &str =
    "0xf208f4912782fd25c7f114ca3723a2d5dd6f3bcc3ac8db5af63baa85f711d5ec";
const V4_DYNAMIC_FEE_FLAG: i64 = 0x800000;
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

#[derive(Debug, Clone)]
struct Cli {
    from_block: Option<u64>,
    to_block: Option<u64>,
    max_lookback_blocks: Option<u64>,
    limit: i64,
    chunk_blocks: u64,
    apply: bool,
    refresh_existing: bool,
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
        "mode={} pools={} to_block={} chunk_blocks={}",
        if cli.apply { "apply" } else { "dry-run" },
        pools.len(),
        to_block,
        cli.chunk_blocks
    );

    let summary = hydrate_ticks(
        &settings,
        &provider,
        &redis,
        pools,
        cli.from_block,
        cli.max_lookback_blocks,
        to_block,
        cli.chunk_blocks,
        cli.apply,
        cli.refresh_existing,
    )
    .await?;

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
        "nonzero_tick_pools={} zero_tick_pools={} marked_changed_pools={} skipped_existing_pools={} skipped_lookback_pools={}",
        summary.nonzero_tick_pools,
        summary.zero_tick_pools,
        summary.marked_changed_pools,
        summary.skipped_existing_pools,
        summary.skipped_lookback_pools
    );
    Ok(())
}

async fn fetch_quoteable_v4_pools(
    store: &PostgresStore,
    chain_id: u64,
    limit: i64,
) -> Result<Vec<V4Pool>> {
    let rows = sqlx::query(
        r#"
        SELECT manager_address, pool_uid, pool_address, first_block
        FROM protocol_pool_observations
        WHERE chain_id = $1
          AND protocol = 'uniswap-v4'
          AND pool_address IS NOT NULL
          AND token0 IS NOT NULL
          AND token1 IS NOT NULL
          AND fee_pips IS NOT NULL
          AND fee_pips <> $2
          AND tick_spacing IS NOT NULL
          AND lower(COALESCE(hooks_address, $3)) = lower($3)
        ORDER BY logs_30d DESC, latest_block DESC, updated_at DESC
        LIMIT $4
        "#,
    )
    .bind(i64::try_from(chain_id)?)
    .bind(V4_DYNAMIC_FEE_FLAG)
    .bind(ZERO_ADDRESS)
    .bind(limit)
    .fetch_all(&store.pool)
    .await?;

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
    redis: &RedisStore,
    pools: Vec<V4Pool>,
    from_block_override: Option<u64>,
    max_lookback_blocks: Option<u64>,
    to_block: u64,
    chunk_blocks: u64,
    apply: bool,
    refresh_existing: bool,
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
            summary.logs_seen += logs.len();
            for raw in logs {
                let Some(delta) = parse_modify_liquidity_delta(&raw)? else {
                    continue;
                };
                apply_delta(&mut ticks, delta)?;
            }
            if chunk_to == u64::MAX {
                break;
            }
            cursor = chunk_to + 1;
        }

        let tick_states = ticks
            .into_iter()
            .filter(|(_, acc)| acc.liquidity_gross > 0 || acc.liquidity_net != 0)
            .map(|(tick, acc)| {
                Ok(TickState {
                    pool_id: PoolId {
                        chain_id: settings.chain_id,
                        address: pool.pool_address,
                    },
                    tick,
                    liquidity_net: acc.liquidity_net,
                    liquidity_gross: U256::from(u128::try_from(acc.liquidity_gross)?),
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
            redis
                .replace_pool_ticks(pool.pool_address, tick_states.clone())
                .await?;
            if !tick_states.is_empty() {
                redis
                    .mark_tick_changed_pools(vec![pool.pool_address])
                    .await?;
                summary.marked_changed_pools += 1;
            }
        }
        summary.ticks_written += tick_states.len();
        summary.hydrated_pools += 1;
    }
    Ok(summary)
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
        apply: false,
        refresh_existing: false,
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
            "--apply" => cli.apply = true,
            "--refresh-existing" => cli.refresh_existing = true,
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
        "Usage: hydrate_uniswap_v4_ticks [--from-block N] [--to-block N] [--max-lookback-blocks N] [--limit 200] [--chunk-blocks 10000] [--refresh-existing] [--apply]"
    );
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
