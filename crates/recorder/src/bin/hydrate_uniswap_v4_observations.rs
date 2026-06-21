use std::{
    collections::{HashMap, HashSet},
    env,
    str::FromStr,
    time::Instant,
};

use alloy_primitives::{Address, U256};
use anyhow::{Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_storage::postgres::{ensure_registry_schema, PostgresStore, ProtocolPoolObservation};
use serde_json::{json, Value};
use sqlx::Row;
use tokio::time::{sleep, Duration};
use tracing_subscriber::EnvFilter;

const UNISWAP_V4_INITIALIZE_TOPIC: &str =
    "0xdd466e674ea557f56295e2d0218a125ea4b4f0f6f3307b95f85e6110838d6438";

#[derive(Debug, Clone)]
struct Cli {
    from_block: Option<u64>,
    to_block: Option<u64>,
    lookback_blocks: u64,
    limit: i64,
    chunk_blocks: u64,
    manager_scan: bool,
    loop_mode: bool,
    interval_ms: u64,
    max_observation_age_blocks: Option<u64>,
    mark_not_found: bool,
    apply: bool,
}

#[derive(Debug, Clone)]
struct MissingObservation {
    manager_address: Address,
    pool_uid: String,
    first_block: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = parse_args(env::args().skip(1))?;
    let settings = Settings::load()?;
    let store = PostgresStore::connect(&settings.postgres_url).await?;
    ensure_registry_schema(&store.pool).await?;
    let provider = ChainProvider::from_settings(&settings);

    if cli.loop_mode {
        run_loop(settings, store, provider, cli).await?;
        return Ok(());
    }

    run_once(&settings, &store, &provider, &cli).await?;
    Ok(())
}

async fn run_loop(
    settings: Settings,
    store: PostgresStore,
    provider: ChainProvider,
    cli: Cli,
) -> Result<()> {
    if cli.manager_scan {
        anyhow::bail!("--loop is only supported for pending hydration, not --manager-scan");
    }
    if !cli.apply {
        anyhow::bail!("--loop requires --apply");
    }
    println!(
        "== Uniswap V4 Observation Hydrator Loop == interval_ms={} limit={} lookback_blocks={} max_observation_age_blocks={:?} chunk_blocks={}",
        cli.interval_ms, cli.limit, cli.lookback_blocks, cli.max_observation_age_blocks, cli.chunk_blocks
    );

    loop {
        if let Err(err) = run_once(&settings, &store, &provider, &cli).await {
            println!("loop iteration failed: {err:#}");
        }
        sleep(Duration::from_millis(cli.interval_ms.max(100))).await;
    }
}

async fn run_once(
    settings: &Settings,
    store: &PostgresStore,
    provider: &ChainProvider,
    cli: &Cli,
) -> Result<()> {
    let latest_block = provider.get_block_number().await?;
    let to_block = cli.to_block.unwrap_or(latest_block);
    let from_block = cli
        .from_block
        .unwrap_or_else(|| to_block.saturating_sub(cli.lookback_blocks));

    let missing = if cli.manager_scan {
        Vec::new()
    } else {
        let min_first_block = cli
            .max_observation_age_blocks
            .map(|age| latest_block.saturating_sub(age));
        fetch_missing_observations(store, settings.chain_id, cli.limit, min_first_block).await?
    };
    println!("== Uniswap V4 Observation Hydration ==");
    println!(
        "mode={} scan={} missing={} blocks={}..{} chunk_blocks={}",
        if cli.apply { "apply" } else { "dry-run" },
        if cli.manager_scan {
            "manager"
        } else {
            "pending"
        },
        missing.len(),
        from_block,
        to_block,
        cli.chunk_blocks
    );

    let HydrateSummary {
        hydrated,
        not_found,
        failed,
        chunks,
        logs_seen,
    } = if cli.manager_scan {
        hydrate_manager_scan(
            &settings,
            &store,
            &provider,
            from_block,
            to_block,
            cli.chunk_blocks,
            cli.apply,
        )
        .await?
    } else {
        hydrate_batch(
            &settings,
            &store,
            &provider,
            missing,
            from_block,
            to_block,
            cli.chunk_blocks,
            cli.apply,
            cli.mark_not_found,
        )
        .await?
    };

    println!();
    println!("hydrated={hydrated} not_found={not_found} failed={failed} chunks={chunks} logs_seen={logs_seen}");
    Ok(())
}

async fn hydrate_manager_scan(
    settings: &Settings,
    store: &PostgresStore,
    provider: &ChainProvider,
    from_block: u64,
    to_block: u64,
    chunk_blocks: u64,
    apply: bool,
) -> Result<HydrateSummary> {
    let manager = settings
        .uniswap_v4_pool_manager
        .context("UNISWAP_V4_POOL_MANAGER is required for --manager-scan")?;
    let mut summary = HydrateSummary::default();
    let started_at = Instant::now();
    let total_blocks = block_span(from_block, to_block);
    let mut cursor = from_block;
    while cursor <= to_block {
        let chunk_to = to_block.min(cursor.saturating_add(chunk_blocks).saturating_sub(1));
        summary.chunks += 1;
        let logs = match fetch_initialize_logs_split(provider, manager, cursor, chunk_to).await {
            Ok(logs) => logs,
            Err(err) => {
                summary.failed += 1;
                println!("failed manager={manager:#x} blocks={cursor}..{chunk_to} error={err:#}");
                if chunk_to == u64::MAX {
                    break;
                }
                cursor = chunk_to + 1;
                continue;
            }
        };
        let chunk_logs = logs.len();
        summary.logs_seen += chunk_logs;
        let mut chunk_hydrated = 0usize;
        for raw in logs {
            let Some(observation) = parse_initialize_observation(settings, manager, raw)? else {
                continue;
            };
            if apply {
                store.upsert_protocol_pool_observation(observation).await?;
            }
            summary.hydrated += 1;
            chunk_hydrated += 1;
        }
        print_progress(
            "manager_scan",
            from_block,
            to_block,
            cursor,
            chunk_to,
            summary.chunks,
            started_at,
            total_blocks,
            &format!(
                "manager={manager:#x} chunk_logs={chunk_logs} chunk_hydrated={chunk_hydrated} total_hydrated={} logs_seen={} failed={}",
                summary.hydrated, summary.logs_seen, summary.failed
            ),
        );
        if chunk_to == u64::MAX {
            break;
        }
        cursor = chunk_to + 1;
    }
    Ok(summary)
}

async fn fetch_missing_observations(
    store: &PostgresStore,
    chain_id: u64,
    limit: i64,
    min_first_block: Option<u64>,
) -> Result<Vec<MissingObservation>> {
    let rows = sqlx::query(
        r#"
        SELECT manager_address, pool_uid, first_block
        FROM protocol_pool_observations
        WHERE chain_id = $1
          AND protocol = 'uniswap-v4'
          AND (token0 IS NULL OR token1 IS NULL OR tick_spacing IS NULL OR hooks_address IS NULL)
          AND import_status != 'metadata_not_found'
          AND ($3::BIGINT IS NULL OR first_block >= $3)
        ORDER BY logs_30d DESC, latest_block DESC, updated_at DESC
        LIMIT $2
        "#,
    )
    .bind(i64::try_from(chain_id)?)
    .bind(limit)
    .bind(min_first_block.map(i64::try_from).transpose()?)
    .fetch_all(&store.pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let manager = row.try_get::<String, _>("manager_address")?;
            let pool_uid = row.try_get::<String, _>("pool_uid")?;
            let first_block = row.try_get::<i64, _>("first_block")?;
            Ok(MissingObservation {
                manager_address: Address::from_str(&manager)
                    .with_context(|| format!("invalid manager address {manager}"))?,
                pool_uid: pool_uid.to_ascii_lowercase(),
                first_block: u64::try_from(first_block)?,
            })
        })
        .collect()
}

#[derive(Debug, Default)]
struct HydrateSummary {
    hydrated: usize,
    not_found: usize,
    failed: usize,
    chunks: usize,
    logs_seen: usize,
}

async fn hydrate_batch(
    settings: &Settings,
    store: &PostgresStore,
    provider: &ChainProvider,
    missing: Vec<MissingObservation>,
    from_block: u64,
    to_block: u64,
    chunk_blocks: u64,
    apply: bool,
    mark_not_found: bool,
) -> Result<HydrateSummary> {
    let mut groups: HashMap<Address, HashMap<String, MissingObservation>> = HashMap::new();
    let mut search_from = from_block;
    for row in missing {
        search_from = search_from.min(row.first_block.saturating_sub(1));
        let uid = normalize_topic(&row.pool_uid)?;
        groups
            .entry(row.manager_address)
            .or_default()
            .insert(uid, row);
    }

    let mut summary = HydrateSummary::default();
    for (manager, rows) in groups {
        let mut pending = rows.keys().cloned().collect::<HashSet<_>>();
        let started_at = Instant::now();
        let total_blocks = block_span(search_from, to_block);
        let mut cursor = search_from;
        while cursor <= to_block && !pending.is_empty() {
            let chunk_to = to_block.min(cursor.saturating_add(chunk_blocks).saturating_sub(1));
            summary.chunks += 1;
            let logs = match fetch_initialize_logs_split(provider, manager, cursor, chunk_to).await
            {
                Ok(logs) => logs,
                Err(err) => {
                    summary.failed += pending.len();
                    println!(
                        "failed manager={manager:#x} blocks={cursor}..{chunk_to} pending={} error={err:#}",
                        pending.len()
                    );
                    pending.clear();
                    break;
                }
            };
            let chunk_logs = logs.len();
            let pending_before = pending.len();
            summary.logs_seen += chunk_logs;
            for raw in logs {
                let Some(pool_uid) = topic_at(&raw, 1) else {
                    continue;
                };
                if !pending.contains(&pool_uid) {
                    continue;
                }
                let Some(observation) = parse_initialize_observation(settings, manager, raw)?
                else {
                    continue;
                };
                println!(
                    "hydrate pool_uid={} token0={} token1={} fee_pips={:?} tick_spacing={:?} hooks={:?} block={}",
                    observation.pool_uid,
                    observation
                        .token0
                        .map(|address| format!("{address:#x}"))
                        .unwrap_or_else(|| "-".to_string()),
                    observation
                        .token1
                        .map(|address| format!("{address:#x}"))
                        .unwrap_or_else(|| "-".to_string()),
                    observation.fee_pips,
                    observation.tick_spacing,
                    observation.hooks_address.map(|address| format!("{address:#x}")),
                    observation.block_number,
                );
                if apply {
                    store.upsert_protocol_pool_observation(observation).await?;
                }
                pending.remove(&pool_uid);
                summary.hydrated += 1;
            }
            let chunk_hydrated = pending_before.saturating_sub(pending.len());
            print_progress(
                "pending_scan",
                search_from,
                to_block,
                cursor,
                chunk_to,
                summary.chunks,
                started_at,
                total_blocks,
                &format!(
                    "manager={manager:#x} chunk_logs={chunk_logs} chunk_hydrated={chunk_hydrated} total_hydrated={} pending={} logs_seen={} failed={}",
                    summary.hydrated,
                    pending.len(),
                    summary.logs_seen,
                    summary.failed
                ),
            );
            if chunk_to == u64::MAX {
                break;
            }
            cursor = chunk_to + 1;
        }
        summary.not_found += pending.len();
        if apply && mark_not_found && !pending.is_empty() {
            mark_metadata_not_found(store, settings.chain_id, manager, pending.iter()).await?;
        }
    }
    Ok(summary)
}

async fn mark_metadata_not_found<'a, I>(
    store: &PostgresStore,
    chain_id: u64,
    manager: Address,
    pool_uids: I,
) -> Result<()>
where
    I: IntoIterator<Item = &'a String>,
{
    for pool_uid in pool_uids {
        sqlx::query(
            r#"
            UPDATE protocol_pool_observations
            SET import_status = 'metadata_not_found',
                import_reason = 'Uniswap V4 Initialize not found by live metadata hydrator',
                updated_at = NOW()
            WHERE chain_id = $1
              AND protocol = 'uniswap-v4'
              AND lower(manager_address) = lower($2)
              AND lower(pool_uid) = lower($3)
            "#,
        )
        .bind(i64::try_from(chain_id)?)
        .bind(format!("{manager:#x}"))
        .bind(pool_uid)
        .execute(&store.pool)
        .await?;
    }
    Ok(())
}

async fn fetch_initialize_logs_split(
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
            "address": format!("{manager:#x}"),
            "topics": [[UNISWAP_V4_INITIALIZE_TOPIC]]
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

fn parse_initialize_observation(
    settings: &Settings,
    manager_address: Address,
    raw: Value,
) -> Result<Option<ProtocolPoolObservation>> {
    let Some(topic0) = topic_at(&raw, 0) else {
        return Ok(None);
    };
    if topic0 != UNISWAP_V4_INITIALIZE_TOPIC {
        return Ok(None);
    }
    let pool_uid = topic_at(&raw, 1).context("Initialize log missing PoolId topic")?;
    let token0 = topic_at(&raw, 2).and_then(|topic| address_from_topic(&topic));
    let token1 = topic_at(&raw, 3).and_then(|topic| address_from_topic(&topic));
    let data_words = data_words(&raw);
    let fee_pips = data_words
        .first()
        .and_then(|word| parse_word_u256(word))
        .and_then(|value| u32::try_from(value).ok());
    let fee_bps = fee_pips.map(|fee| fee / 100);
    let tick_spacing = data_words.get(1).and_then(|word| parse_word_i24(word));
    let hooks_address = data_words.get(2).and_then(|word| address_from_word(word));
    let sqrt_price_x96 = data_words.get(3).and_then(|word| parse_word_u256(word));
    let tick = data_words.get(4).and_then(|word| parse_word_i24(word));
    let block_number = raw
        .get("blockNumber")
        .and_then(Value::as_str)
        .and_then(parse_hex_u64)
        .unwrap_or(0);

    Ok(Some(ProtocolPoolObservation {
        chain_id: settings.chain_id,
        protocol: "uniswap-v4".to_string(),
        manager_address,
        pool_uid: pool_uid.clone(),
        pool_address: Some(synthetic_address_from_pool_uid(&pool_uid)?),
        topic0,
        event_type: "Initialize".to_string(),
        token0,
        token1,
        symbol: None,
        factory_address: Some(manager_address),
        dex: Some("UniswapV4".to_string()),
        variant: Some("UniswapV4".to_string()),
        fee_bps,
        fee_pips,
        pool_key_fee_pips: fee_pips,
        tick_spacing,
        hooks_address,
        sqrt_price_x96,
        liquidity: None,
        tick,
        block_number,
        discovery_source: "v4_initialize_hydrate".to_string(),
        import_status: "observed_only".to_string(),
        import_reason: Some(
            "Uniswap V4 PoolKey metadata hydrated; quote/state promotion pending".to_string(),
        ),
        raw_json: raw,
    }))
}

fn parse_args<I>(mut args: I) -> Result<Cli>
where
    I: Iterator<Item = String>,
{
    let mut cli = Cli {
        from_block: None,
        to_block: None,
        lookback_blocks: 2_000_000,
        limit: 500,
        chunk_blocks: 10_000,
        manager_scan: false,
        loop_mode: false,
        interval_ms: 30_000,
        max_observation_age_blocks: None,
        mark_not_found: false,
        apply: false,
    };

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--from-block" => cli.from_block = Some(parse_next(&mut args, "--from-block")?),
            "--to-block" => cli.to_block = Some(parse_next(&mut args, "--to-block")?),
            "--lookback-blocks" => {
                cli.lookback_blocks = parse_next(&mut args, "--lookback-blocks")?
            }
            "--limit" => cli.limit = parse_next(&mut args, "--limit")?,
            "--chunk-blocks" => cli.chunk_blocks = parse_next(&mut args, "--chunk-blocks")?,
            "--manager-scan" => cli.manager_scan = true,
            "--loop" => {
                cli.loop_mode = true;
                cli.mark_not_found = true;
            }
            "--interval-ms" => cli.interval_ms = parse_next(&mut args, "--interval-ms")?,
            "--max-observation-age-blocks" => {
                cli.max_observation_age_blocks =
                    Some(parse_next(&mut args, "--max-observation-age-blocks")?)
            }
            "--mark-not-found" => cli.mark_not_found = true,
            "--apply" => cli.apply = true,
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
        "Usage: hydrate_uniswap_v4_observations [--from-block N] [--to-block N] [--lookback-blocks 2000000] [--limit 500] [--chunk-blocks 10000] [--manager-scan] [--loop] [--interval-ms 30000] [--max-observation-age-blocks N] [--mark-not-found] [--apply]"
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

fn address_from_topic(topic: &str) -> Option<Address> {
    address_from_word(topic)
}

fn address_from_word(word: &str) -> Option<Address> {
    let hex = word.trim_start_matches("0x");
    if hex.len() != 64 {
        return None;
    }
    Address::from_str(&format!("0x{}", &hex[24..64])).ok()
}

fn parse_word_u256(word: &str) -> Option<U256> {
    U256::from_str_radix(word.trim_start_matches("0x"), 16).ok()
}

fn parse_word_i24(word: &str) -> Option<i32> {
    let value = parse_word_u256(word)?;
    let low = (value & U256::from(0x00ff_ffffu64)).to::<u32>();
    let signed = if (low & 0x0080_0000) != 0 {
        i64::from(low) - (1_i64 << 24)
    } else {
        i64::from(low)
    };
    i32::try_from(signed).ok()
}

fn parse_hex_u64(raw: &str) -> Option<u64> {
    u64::from_str_radix(raw.trim_start_matches("0x"), 16).ok()
}

fn synthetic_address_from_pool_uid(pool_uid: &str) -> Result<Address> {
    let topic = normalize_topic(pool_uid)?;
    Address::from_str(&format!("0x{}", &topic.trim_start_matches("0x")[24..64]))
        .context("invalid synthetic address")
}
