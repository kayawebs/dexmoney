use std::{env, str::FromStr};

use alloy_primitives::Address;
use anyhow::{bail, Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::{
    config::Settings,
    types::{DexKind, DiscoveredPool, PoolVariant},
};
use base_arb_storage::postgres::{FactoryRegistryRecord, PostgresStore};
use serde_json::{json, Value};
use tracing_subscriber::EnvFilter;

const APPROX_BASE_BLOCKS_PER_DAY: u64 = 43_200;
const DEFAULT_CHUNK_BLOCKS: u64 = 20_000;
const UNISWAP_V3_FACTORY: &str = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD";
const PANCAKE_V3_FACTORY: &str = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865";
const AERODROME_CLASSIC_FACTORY: &str = "0x420DD381b31aEf6683db6B902084cB0FFECe40Da";
const AERODROME_SLIPSTREAM_FACTORIES: [&str; 2] = [
    "0x5e7BB104d84c7CB9B682AaC2F3d509f5F406809A",
    "0xaDe65c38CD4849aDBA595a4323a8C7DdfE89716a",
];
const V3_POOL_CREATED_TOPIC: &str =
    "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118";
const CLASSIC_POOL_CREATED_TOPIC: &str =
    "0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e";
const CLASSIC_PAIR_CREATED_TOPIC: &str =
    "0xc4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9";
const SLIPSTREAM_POOL_CREATED_TOPIC: &str =
    "0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2";
const SLIPSTREAM_POOL_CREATED_WITH_INDEX_TOPIC: &str =
    "0xb4b64a6a7c41cd0232bfad78d5f845870be74762857744ff02be28578c5f4cb9";

#[derive(Debug, Clone)]
struct Cli {
    days: u64,
    from_block: Option<u64>,
    to_block: Option<u64>,
    chunk_blocks: u64,
    apply: bool,
    max_logs: Option<usize>,
}

#[derive(Debug, Clone)]
struct CreationLog {
    factory: Address,
    topic0: String,
    topics: Vec<String>,
    data: String,
    block_number: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = parse_args(env::args().skip(1))?;
    let settings = Settings::load()?;
    let provider = ChainProvider::from_settings(&settings);
    let store = PostgresStore::connect(&settings.postgres_url).await?;
    seed_default_factories(&store, &settings).await?;

    let trusted = store.trusted_factory_registry(settings.chain_id).await?;
    if trusted.is_empty() {
        bail!("no trusted factories found");
    }

    let latest = provider.get_block_number().await?;
    let to_block = cli.to_block.unwrap_or(latest);
    let from_block = cli.from_block.unwrap_or_else(|| {
        to_block.saturating_sub(cli.days.saturating_mul(APPROX_BASE_BLOCKS_PER_DAY))
    });
    if from_block > to_block {
        bail!("from block {from_block} is greater than to block {to_block}");
    }

    println!("== Trusted Factory Pool Backfill ==");
    println!("blocks: {from_block}..{to_block}");
    println!("chunk_blocks: {}", cli.chunk_blocks);
    println!("mode: {}", if cli.apply { "apply" } else { "dry-run" });
    println!("trusted_factories: {}", trusted.len());

    let mut scanned_logs = 0usize;
    let mut imported = 0usize;
    let mut observed_only = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    let mut cursor = from_block;
    while cursor <= to_block {
        let end = cursor
            .saturating_add(cli.chunk_blocks.saturating_sub(1))
            .min(to_block);
        let logs = fetch_creation_logs(&provider, &trusted, cursor, end).await?;
        for raw in logs {
            if cli.max_logs.is_some_and(|limit| scanned_logs >= limit) {
                break;
            }
            scanned_logs += 1;
            let log = match parse_creation_log(raw) {
                Ok(log) => log,
                Err(err) => {
                    skipped += 1;
                    eprintln!("skip creation log parse error: {err}");
                    continue;
                }
            };
            match process_creation_log(&provider, &store, &settings, &trusted, &log, cli.apply)
                .await
            {
                Ok(BackfillOutcome::Imported) => imported += 1,
                Ok(BackfillOutcome::ObservedOnly) => observed_only += 1,
                Ok(BackfillOutcome::Skipped) => skipped += 1,
                Err(err) => {
                    failed += 1;
                    eprintln!(
                        "failed pool-created factory={:#x} block={} topic0={}: {err:#}",
                        log.factory, log.block_number, log.topic0
                    );
                }
            }
        }
        if cli.max_logs.is_some_and(|limit| scanned_logs >= limit) {
            break;
        }
        cursor = end.saturating_add(1);
    }

    println!(
        "summary scanned_logs={scanned_logs} imported={imported} observed_only={observed_only} skipped={skipped} failed={failed}"
    );
    Ok(())
}

async fn fetch_creation_logs(
    provider: &ChainProvider,
    trusted: &[FactoryRegistryRecord],
    from_block: u64,
    to_block: u64,
) -> Result<Vec<Value>> {
    let addresses = trusted
        .iter()
        .map(|row| row.factory_address.to_ascii_lowercase())
        .collect::<Vec<_>>();
    let params = json!([{
        "address": addresses,
        "fromBlock": format!("0x{from_block:x}"),
        "toBlock": format!("0x{to_block:x}"),
        "topics": [[
            V3_POOL_CREATED_TOPIC,
            CLASSIC_POOL_CREATED_TOPIC,
            CLASSIC_PAIR_CREATED_TOPIC,
            SLIPSTREAM_POOL_CREATED_TOPIC,
            SLIPSTREAM_POOL_CREATED_WITH_INDEX_TOPIC
        ]]
    }]);
    provider
        .get_logs_raw(params)
        .await
        .with_context(|| format!("eth_getLogs factory creations {from_block}..{to_block}"))
}

async fn process_creation_log(
    provider: &ChainProvider,
    store: &PostgresStore,
    settings: &Settings,
    trusted: &[FactoryRegistryRecord],
    log: &CreationLog,
    apply: bool,
) -> Result<BackfillOutcome> {
    let Some(row) = trusted
        .iter()
        .find(|row| Address::from_str(&row.factory_address).ok() == Some(log.factory))
    else {
        return Ok(BackfillOutcome::Skipped);
    };
    let dex = parse_dex_kind(&row.dex)?;
    let variant = parse_pool_variant(&row.variant)?;
    let Some(pool) = find_created_pool_address(provider, log).await? else {
        return Ok(BackfillOutcome::Skipped);
    };

    let metadata = provider
        .resolve_observed_pool_metadata(pool, &log.topic0)
        .await?;
    let symbol = pair_symbol(provider, metadata.token0, metadata.token1).await;

    match provider
        .resolve_pool_for_trusted_factory(pool, log.factory, dex, variant)
        .await
    {
        Ok(discovered) => {
            if apply {
                import_discovered_pool(
                    store,
                    settings,
                    pool,
                    &log.topic0,
                    log.block_number,
                    &symbol,
                    dex,
                    variant,
                    log.factory,
                    discovered,
                )
                .await?;
            }
            println!(
                "importable pool={pool:#x} factory={:#x} symbol={} dex={} variant={} block={}",
                log.factory,
                symbol,
                dex_to_string(dex),
                variant_to_string(variant),
                log.block_number
            );
            Ok(BackfillOutcome::Imported)
        }
        Err(err) => {
            if apply {
                store
                    .upsert_observed_pool(
                        settings.chain_id,
                        pool,
                        &log.topic0,
                        "pool-created",
                        Some(metadata.token0),
                        Some(metadata.token1),
                        Some(&symbol),
                        Some(log.factory),
                        None,
                        None,
                        metadata.fee_bps,
                        metadata.fee_pips,
                        metadata.tick_spacing,
                        metadata.stable,
                        1,
                        1,
                        Some(i64::try_from(log.block_number)?),
                        Some(i64::try_from(log.block_number)?),
                        "observed_only",
                        Some(&format!("trusted factory pool resolve failed: {err}")),
                    )
                    .await?;
            }
            Ok(BackfillOutcome::ObservedOnly)
        }
    }
}

async fn find_created_pool_address(
    provider: &ChainProvider,
    log: &CreationLog,
) -> Result<Option<Address>> {
    for candidate in creation_log_candidate_addresses(log) {
        if candidate == Address::ZERO {
            continue;
        }
        let Ok(metadata) = provider
            .resolve_observed_pool_metadata(candidate, &log.topic0)
            .await
        else {
            continue;
        };
        if metadata.factory_address == Some(log.factory) {
            return Ok(Some(candidate));
        }
    }
    Ok(None)
}

#[allow(clippy::too_many_arguments)]
async fn import_discovered_pool(
    store: &PostgresStore,
    settings: &Settings,
    pool: Address,
    topic0: &str,
    block_number: u64,
    symbol: &str,
    dex: DexKind,
    variant: PoolVariant,
    factory: Address,
    discovered: DiscoveredPool,
) -> Result<()> {
    store
        .upsert_token_registry(
            settings.chain_id,
            discovered.state.token0,
            symbol.split('/').next().unwrap_or_default(),
        )
        .await?;
    store
        .upsert_token_registry(
            settings.chain_id,
            discovered.state.token1,
            symbol.split('/').nth(1).unwrap_or_default(),
        )
        .await?;
    let (token0, token1) = canonical_pair(discovered.state.token0, discovered.state.token1);
    let pair_id = store
        .upsert_token_pair(settings.chain_id, token0, token1, symbol)
        .await?;
    store.upsert_discovered_pool(pair_id, &discovered).await?;
    store
        .upsert_observed_pool(
            settings.chain_id,
            pool,
            topic0,
            "pool-created",
            Some(discovered.state.token0),
            Some(discovered.state.token1),
            Some(symbol),
            Some(factory),
            Some(dex_to_string(dex)),
            Some(variant_to_string(variant)),
            Some(discovered.state.fee_bps),
            discovered.state.fee_pips,
            discovered.tick_spacing,
            discovered.stable,
            1,
            1,
            Some(i64::try_from(block_number)?),
            Some(i64::try_from(block_number)?),
            "imported",
            None,
        )
        .await?;
    Ok(())
}

async fn seed_default_factories(store: &PostgresStore, settings: &Settings) -> Result<()> {
    let chain_id = settings.chain_id;
    let aerodrome_classic = settings
        .aerodrome_pool_factory
        .unwrap_or(AERODROME_CLASSIC_FACTORY.parse()?);
    store
        .upsert_factory_registry(
            chain_id,
            aerodrome_classic,
            "Aerodrome",
            "AerodromeVolatile",
            true,
            true,
            "default_config",
            Some("official/default Aerodrome Classic factory"),
            None,
            None,
            0,
        )
        .await?;

    let uniswap_v3 = settings
        .uniswap_v3_factory
        .unwrap_or(UNISWAP_V3_FACTORY.parse()?);
    store
        .upsert_factory_registry(
            chain_id,
            uniswap_v3,
            "UniswapV3",
            "UniswapV3",
            true,
            true,
            "default_config",
            Some("official/default Uniswap V3 factory"),
            None,
            None,
            0,
        )
        .await?;

    let pancake_v3 = settings
        .pancake_v3_factory
        .unwrap_or(PANCAKE_V3_FACTORY.parse()?);
    store
        .upsert_factory_registry(
            chain_id,
            pancake_v3,
            "PancakeSwap",
            "PancakeV3",
            true,
            true,
            "default_config",
            Some("official/default Pancake V3 factory"),
            None,
            None,
            0,
        )
        .await?;

    let mut slipstream_factories = Vec::new();
    if let Some(factory) = settings.aerodrome_slipstream_factory {
        slipstream_factories.push(factory);
    }
    for factory in AERODROME_SLIPSTREAM_FACTORIES {
        let factory = factory.parse()?;
        if !slipstream_factories.contains(&factory) {
            slipstream_factories.push(factory);
        }
    }
    for factory in slipstream_factories {
        store
            .upsert_factory_registry(
                chain_id,
                factory,
                "Aerodrome",
                "AerodromeSlipstream",
                true,
                true,
                "default_config",
                Some("official/default Aerodrome Slipstream factory"),
                None,
                None,
                0,
            )
            .await?;
    }
    Ok(())
}

fn parse_creation_log(raw: Value) -> Result<CreationLog> {
    let factory = raw
        .get("address")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("pool-created log missing address"))?
        .parse::<Address>()?;
    let topics = raw
        .get("topics")
        .and_then(Value::as_array)
        .map(|topics| {
            topics
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_ascii_lowercase)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let topic0 = topics
        .first()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("pool-created log missing topic0"))?;
    let data = raw
        .get("data")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("pool-created log missing data"))?
        .to_string();
    let block_number = raw
        .get("blockNumber")
        .and_then(Value::as_str)
        .and_then(parse_hex_u64)
        .ok_or_else(|| anyhow::anyhow!("pool-created log missing blockNumber"))?;
    Ok(CreationLog {
        factory,
        topic0,
        topics,
        data,
        block_number,
    })
}

fn creation_log_candidate_addresses(log: &CreationLog) -> Vec<Address> {
    let mut out = topic_word_addresses(&log.topics);
    out.extend(data_word_addresses(&log.data));
    out
}

fn topic_word_addresses(topics: &[String]) -> Vec<Address> {
    topics
        .iter()
        .skip(1)
        .filter_map(|topic| {
            let hex = topic.strip_prefix("0x").unwrap_or(topic);
            if hex.len() != 64 || !hex[..24].eq_ignore_ascii_case("000000000000000000000000") {
                return None;
            }
            format!("0x{}", &hex[24..64]).parse::<Address>().ok()
        })
        .collect()
}

fn data_word_addresses(data: &str) -> Vec<Address> {
    let hex = data.strip_prefix("0x").unwrap_or(data);
    if hex.len() < 64 {
        return Vec::new();
    }
    hex.as_bytes()
        .chunks(64)
        .filter_map(|word| std::str::from_utf8(word).ok())
        .filter(|word| word.len() == 64)
        .filter_map(|word| {
            let address_hex = &word[24..64];
            if !word[..24].eq_ignore_ascii_case("000000000000000000000000") {
                return None;
            }
            format!("0x{address_hex}").parse::<Address>().ok()
        })
        .collect()
}

async fn pair_symbol(provider: &ChainProvider, token0: Address, token1: Address) -> String {
    let token0_symbol = provider
        .fetch_erc20_symbol(token0)
        .await
        .unwrap_or_else(|_| short_address(token0));
    let token1_symbol = provider
        .fetch_erc20_symbol(token1)
        .await
        .unwrap_or_else(|_| short_address(token1));
    format!("{token0_symbol}/{token1_symbol}")
}

fn short_address(address: Address) -> String {
    let value = format!("{address:#x}");
    value.get(0..10).map(ToString::to_string).unwrap_or(value)
}

fn canonical_pair(a: Address, b: Address) -> (Address, Address) {
    if a < b {
        (a, b)
    } else {
        (b, a)
    }
}

fn parse_hex_u64(value: &str) -> Option<u64> {
    u64::from_str_radix(value.strip_prefix("0x").unwrap_or(value), 16).ok()
}

fn dex_to_string(dex: DexKind) -> &'static str {
    match dex {
        DexKind::Aerodrome => "Aerodrome",
        DexKind::UniswapV3 => "UniswapV3",
        DexKind::PancakeSwap => "PancakeSwap",
    }
}

fn variant_to_string(variant: PoolVariant) -> &'static str {
    match variant {
        PoolVariant::AerodromeVolatile => "AerodromeVolatile",
        PoolVariant::AerodromeSlipstream => "AerodromeSlipstream",
        PoolVariant::UniswapV3 => "UniswapV3",
        PoolVariant::PancakeV3 => "PancakeV3",
    }
}

fn parse_dex_kind(value: &str) -> Result<DexKind> {
    match value {
        "Aerodrome" => Ok(DexKind::Aerodrome),
        "UniswapV3" => Ok(DexKind::UniswapV3),
        "PancakeSwap" => Ok(DexKind::PancakeSwap),
        _ => bail!("unknown dex: {value}"),
    }
}

fn parse_pool_variant(value: &str) -> Result<PoolVariant> {
    match value {
        "AerodromeVolatile" => Ok(PoolVariant::AerodromeVolatile),
        "AerodromeSlipstream" => Ok(PoolVariant::AerodromeSlipstream),
        "UniswapV3" => Ok(PoolVariant::UniswapV3),
        "PancakeV3" => Ok(PoolVariant::PancakeV3),
        _ => bail!("unknown pool variant: {value}"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackfillOutcome {
    Imported,
    ObservedOnly,
    Skipped,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Cli> {
    let mut cli = Cli {
        days: 30,
        from_block: None,
        to_block: None,
        chunk_blocks: DEFAULT_CHUNK_BLOCKS,
        apply: false,
        max_logs: None,
    };
    let mut iter = args.peekable();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--days" => cli.days = parse_next(&mut iter, "--days")?,
            "--from-block" => cli.from_block = Some(parse_next(&mut iter, "--from-block")?),
            "--to-block" => cli.to_block = Some(parse_next(&mut iter, "--to-block")?),
            "--chunk-blocks" => cli.chunk_blocks = parse_next(&mut iter, "--chunk-blocks")?,
            "--max-logs" => cli.max_logs = Some(parse_next(&mut iter, "--max-logs")?),
            "--apply" => cli.apply = true,
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => bail!("unknown argument: {arg}"),
        }
    }
    if cli.chunk_blocks == 0 {
        bail!("--chunk-blocks must be greater than 0");
    }
    Ok(cli)
}

fn parse_next<T: FromStr>(
    iter: &mut std::iter::Peekable<impl Iterator<Item = String>>,
    name: &str,
) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    let value = iter
        .next()
        .with_context(|| format!("missing value for {name}"))?;
    value
        .parse::<T>()
        .map_err(|err| anyhow::anyhow!("invalid {name} value {value}: {err}"))
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin backfill_factory_pools -- [--days 30 | --from-block N --to-block N] [--chunk-blocks 20000] [--max-logs N] [--apply]"
    );
}
