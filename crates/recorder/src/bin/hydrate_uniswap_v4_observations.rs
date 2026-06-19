use std::{env, str::FromStr};

use alloy_primitives::{Address, U256};
use anyhow::{Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_storage::postgres::{
    ensure_registry_schema, PostgresStore, ProtocolPoolObservation,
};
use serde_json::{json, Value};
use sqlx::Row;
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
    let latest_block = provider.get_block_number().await?;
    let to_block = cli.to_block.unwrap_or(latest_block);
    let from_block = cli
        .from_block
        .unwrap_or_else(|| to_block.saturating_sub(cli.lookback_blocks));

    let missing = fetch_missing_observations(&store, settings.chain_id, cli.limit).await?;
    println!("== Uniswap V4 Observation Hydration ==");
    println!(
        "mode={} missing={} blocks={}..{} chunk_blocks={}",
        if cli.apply { "apply" } else { "dry-run" },
        missing.len(),
        from_block,
        to_block,
        cli.chunk_blocks
    );

    let mut hydrated = 0usize;
    let mut not_found = 0usize;
    let mut failed = 0usize;

    for row in missing {
        let search_from = from_block.min(row.first_block.saturating_sub(1));
        match hydrate_one(
            &settings,
            &store,
            &provider,
            &row,
            search_from,
            to_block,
            cli.chunk_blocks,
            cli.apply,
        )
        .await
        {
            Ok(true) => hydrated += 1,
            Ok(false) => not_found += 1,
            Err(err) => {
                failed += 1;
                println!(
                    "failed pool_uid={} manager={:#x} error={err:#}",
                    row.pool_uid, row.manager_address
                );
            }
        }
    }

    println!();
    println!("hydrated={hydrated} not_found={not_found} failed={failed}");
    Ok(())
}

async fn fetch_missing_observations(
    store: &PostgresStore,
    chain_id: u64,
    limit: i64,
) -> Result<Vec<MissingObservation>> {
    let rows = sqlx::query(
        r#"
        SELECT manager_address, pool_uid, first_block
        FROM protocol_pool_observations
        WHERE chain_id = $1
          AND protocol = 'uniswap-v4'
          AND (token0 IS NULL OR token1 IS NULL OR tick_spacing IS NULL OR hooks_address IS NULL)
        ORDER BY logs_30d DESC, latest_block DESC, updated_at DESC
        LIMIT $2
        "#,
    )
    .bind(i64::try_from(chain_id)?)
    .bind(limit)
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

async fn hydrate_one(
    settings: &Settings,
    store: &PostgresStore,
    provider: &ChainProvider,
    missing: &MissingObservation,
    from_block: u64,
    to_block: u64,
    chunk_blocks: u64,
    apply: bool,
) -> Result<bool> {
    let pool_uid_topic = normalize_topic(&missing.pool_uid)?;
    let mut cursor = from_block;
    while cursor <= to_block {
        let chunk_to = to_block.min(cursor.saturating_add(chunk_blocks).saturating_sub(1));
        let params = json!([{
            "fromBlock": format!("0x{cursor:x}"),
            "toBlock": format!("0x{chunk_to:x}"),
            "address": format!("{:#x}", missing.manager_address),
            "topics": [[UNISWAP_V4_INITIALIZE_TOPIC], [pool_uid_topic]]
        }]);
        let logs = provider
            .get_logs_raw(params)
            .await
            .with_context(|| format!("eth_getLogs failed for blocks {cursor}..{chunk_to}"))?;
        for raw in logs {
            let Some(observation) =
                parse_initialize_observation(settings, missing.manager_address, raw)?
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
            return Ok(true);
        }
        if chunk_to == u64::MAX {
            break;
        }
        cursor = chunk_to + 1;
    }
    Ok(false)
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
        "Usage: hydrate_uniswap_v4_observations [--lookback-blocks 2000000] [--limit 500] [--chunk-blocks 10000] [--apply]"
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
