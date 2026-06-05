use std::{collections::BTreeSet, env, str::FromStr};

use alloy_primitives::{Address, B256, U256};
use anyhow::{bail, Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_storage::postgres::{ensure_registry_schema, PostgresStore};
use serde_json::{json, Value};
use sqlx::{FromRow, PgPool, Row};
use tracing_subscriber::EnvFilter;

const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const UNI_V3_SWAP_TOPIC: &str =
    "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const PANCAKE_V3_SWAP_TOPIC: &str =
    "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83";
const CLASSIC_SWAP_TOPIC: &str =
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
const APPROX_BASE_BLOCKS_PER_DAY: u64 = 43_200;

#[derive(Debug, Clone)]
struct Cli {
    address: Address,
    days: i64,
    hydrate_limit: i64,
    hydrate_all: bool,
    hydrate_peer_blocks: i64,
    from_block: Option<u64>,
    to_block: Option<u64>,
    log_chunk_blocks: u64,
}

#[derive(Debug, FromRow)]
struct HitRow {
    block_number: i64,
    transaction_index: Option<i64>,
    tx_hash: String,
    from_address: Option<String>,
    to_address: Option<String>,
    effective_gas_price: Option<String>,
    max_priority_fee_per_gas: Option<String>,
    max_fee_per_gas: Option<String>,
    gas_used: Option<String>,
    base_fee_per_gas: Option<String>,
    paid_priority_fee_per_gas: Option<String>,
    pools: Option<String>,
    protocols: Option<String>,
}

#[derive(Debug, FromRow)]
struct RankRow {
    tx_hash: String,
    block_number: i64,
    transaction_index: Option<i64>,
    effective_gas_price: Option<String>,
    max_priority_fee_per_gas: Option<String>,
    paid_priority_fee_per_gas: Option<String>,
    observed_pool_txs_in_block: Option<i64>,
    effective_gas_rank: Option<i64>,
    priority_gas_rank: Option<i64>,
}

#[derive(Debug, FromRow)]
struct TransferSummaryRow {
    direction: String,
    token_address: String,
    counterparty: String,
    transfers: i64,
    txs: i64,
    first_block: i64,
    latest_block: i64,
}

#[derive(Debug, FromRow)]
struct TopicRow {
    topic0: Option<String>,
    logs: i64,
    txs: i64,
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

    let (from_block, to_block) = resolve_block_range(&provider, &cli).await?;
    print_scope(&store.pool, &cli, from_block, to_block).await?;
    collect_address_transfer_logs(&store.pool, &provider, &cli, from_block, to_block).await?;
    hydrate_missing_transactions(&store.pool, &provider, &cli).await?;
    hydrate_peer_block_transactions(&store.pool, &provider, &cli).await?;
    print_report(&store.pool, &cli, from_block, to_block).await?;
    Ok(())
}

fn parse_args<I>(args: I) -> Result<Cli>
where
    I: IntoIterator<Item = String>,
{
    let mut address = None;
    let mut days = 30_i64;
    let mut hydrate_limit = 5_000_i64;
    let mut hydrate_all = false;
    let mut hydrate_peer_blocks = 0_i64;
    let mut from_block = None;
    let mut to_block = None;
    let mut log_chunk_blocks = 2_000_u64;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--address" => {
                address = Some(
                    Address::from_str(&iter.next().context("missing value for --address")?)
                        .context("invalid --address")?,
                );
            }
            "--days" => {
                days = iter
                    .next()
                    .context("missing value for --days")?
                    .parse()
                    .context("invalid --days")?;
            }
            "--hydrate-limit" => {
                hydrate_limit = iter
                    .next()
                    .context("missing value for --hydrate-limit")?
                    .parse()
                    .context("invalid --hydrate-limit")?;
            }
            "--hydrate-all" => {
                hydrate_all = true;
            }
            "--hydrate-peer-blocks" => {
                hydrate_peer_blocks = iter
                    .next()
                    .context("missing value for --hydrate-peer-blocks")?
                    .parse()
                    .context("invalid --hydrate-peer-blocks")?;
            }
            "--from-block" => {
                from_block = Some(
                    iter.next()
                        .context("missing value for --from-block")?
                        .parse()
                        .context("invalid --from-block")?,
                );
            }
            "--to-block" => {
                to_block = Some(
                    iter.next()
                        .context("missing value for --to-block")?
                        .parse()
                        .context("invalid --to-block")?,
                );
            }
            "--log-chunk-blocks" => {
                log_chunk_blocks = iter
                    .next()
                    .context("missing value for --log-chunk-blocks")?
                    .parse()
                    .context("invalid --log-chunk-blocks")?;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    if hydrate_limit <= 0 {
        bail!("--hydrate-limit must be positive");
    }
    if hydrate_peer_blocks < 0 {
        bail!("--hydrate-peer-blocks must be non-negative");
    }

    Ok(Cli {
        address: address.context("--address is required")?,
        days,
        hydrate_limit,
        hydrate_all,
        hydrate_peer_blocks,
        from_block,
        to_block,
        log_chunk_blocks,
    })
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin competitor_report -- --address <addr> [--days 30] [--from-block N] [--to-block N] [--hydrate-limit 5000] [--hydrate-all] [--hydrate-peer-blocks 50] [--log-chunk-blocks 2000]"
    );
}

async fn resolve_block_range(provider: &ChainProvider, cli: &Cli) -> Result<(u64, u64)> {
    let latest = provider.get_block_number().await?;
    let to_block = cli.to_block.unwrap_or(latest);
    let from_block = cli.from_block.unwrap_or_else(|| {
        to_block.saturating_sub((cli.days.max(0) as u64).saturating_mul(APPROX_BASE_BLOCKS_PER_DAY))
    });
    if from_block > to_block {
        bail!("from block {from_block} is greater than to block {to_block}");
    }
    Ok((from_block, to_block))
}

async fn print_scope(pool: &PgPool, cli: &Cli, from_block: u64, to_block: u64) -> Result<()> {
    let row = sqlx::query(
        r#"
        SELECT
            COUNT(*)::bigint AS transfers,
            COUNT(DISTINCT tx_hash)::bigint AS txs,
            COUNT(DISTINCT token_address)::bigint AS tokens,
            COUNT(DISTINCT counterparty)::bigint AS counterparties
        FROM observed_address_transfers
        WHERE lower(seed_address) = lower($1)
          AND block_number BETWEEN $2 AND $3
        "#,
    )
    .bind(format!("{:#x}", cli.address))
    .bind(i64::try_from(from_block)?)
    .bind(i64::try_from(to_block)?)
    .fetch_one(pool)
    .await?;

    println!("== Scope ==");
    println!("address: {:#x}", cli.address);
    println!("days: {}", cli.days);
    println!("blocks: {from_block}..{to_block}");
    println!(
        "cached_address_transfers: {} txs: {} tokens: {} counterparties: {}",
        cell_i64(&row, "transfers").unwrap_or_default(),
        cell_i64(&row, "txs").unwrap_or_default(),
        cell_i64(&row, "tokens").unwrap_or_default(),
        cell_i64(&row, "counterparties").unwrap_or_default(),
    );
    println!();
    Ok(())
}

async fn hydrate_missing_transactions(
    pool: &PgPool,
    provider: &ChainProvider,
    cli: &Cli,
) -> Result<()> {
    let address = format!("{:#x}", cli.address).to_ascii_lowercase();
    println!("== Hydrate ==");

    let mut total_inserted = 0usize;
    let mut total_skipped = 0usize;
    let mut total_blocks = BTreeSet::new();
    let mut batches = 0usize;

    loop {
        let hashes = fetch_missing_transaction_hashes(pool, &address, cli.hydrate_limit).await?;
        if hashes.is_empty() {
            if batches == 0 {
                println!("no missing observed transactions");
            }
            break;
        }

        println!(
            "batch {} fetching {} tx/receipt rows from RPC",
            batches + 1,
            hashes.len()
        );
        let mut inserted = 0usize;
        let mut skipped = 0usize;
        let mut batch_blocks = BTreeSet::new();
        for raw_hash in hashes {
            let tx_hash =
                B256::from_str(&raw_hash).with_context(|| format!("invalid tx hash {raw_hash}"))?;
            let Some(tx_json) = provider.get_transaction_by_hash(tx_hash).await? else {
                skipped += 1;
                continue;
            };
            let Some(receipt) = provider.get_transaction_receipt(tx_hash).await? else {
                skipped += 1;
                continue;
            };
            if let Some(block_number) =
                upsert_observed_transaction(pool, &tx_json, &receipt.raw).await?
            {
                batch_blocks.insert(block_number);
                total_blocks.insert(block_number);
            }
            inserted += 1;
        }

        hydrate_observed_blocks(pool, provider, batch_blocks).await?;
        total_inserted += inserted;
        total_skipped += skipped;
        batches += 1;

        if !cli.hydrate_all || inserted == 0 {
            break;
        }
    }

    hydrate_missing_blocks_for_address(pool, provider, &address).await?;
    println!(
        "batches: {batches} upserted: {total_inserted} skipped_missing_rpc: {total_skipped} blocks_touched: {}",
        total_blocks.len()
    );
    println!();
    Ok(())
}

async fn fetch_missing_transaction_hashes(
    pool: &PgPool,
    address: &str,
    limit: i64,
) -> Result<Vec<String>> {
    sqlx::query_scalar(
        r#"
        SELECT t.tx_hash
        FROM observed_address_transfers t
        LEFT JOIN observed_transactions ot ON lower(ot.tx_hash) = lower(t.tx_hash)
        WHERE lower(t.seed_address) = lower($1)
          AND ot.tx_hash IS NULL
        GROUP BY t.tx_hash
        ORDER BY MAX(t.block_number) DESC
        LIMIT $2
        "#,
    )
    .bind(address)
    .bind(limit)
    .fetch_all(pool)
    .await
    .context("failed to fetch missing observed transaction hashes")
}

async fn hydrate_peer_block_transactions(
    pool: &PgPool,
    provider: &ChainProvider,
    cli: &Cli,
) -> Result<()> {
    if cli.hydrate_peer_blocks <= 0 {
        return Ok(());
    }

    let address = format!("{:#x}", cli.address).to_ascii_lowercase();
    let blocks: Vec<i64> = sqlx::query_scalar(
        r#"
        SELECT DISTINCT ot.block_number
        FROM observed_address_transfers t
        JOIN observed_transactions ot ON lower(ot.tx_hash) = lower(t.tx_hash)
        WHERE lower(t.seed_address) = lower($1)
        ORDER BY ot.block_number DESC
        LIMIT $2
        "#,
    )
    .bind(address)
    .bind(cli.hydrate_peer_blocks)
    .fetch_all(pool)
    .await?;

    println!("== Hydrate Peer Blocks ==");
    if blocks.is_empty() {
        println!("no hydrated related tx blocks available");
        println!();
        return Ok(());
    }

    let mut block_count = 0usize;
    let mut tx_seen = 0usize;
    let mut tx_fetched = 0usize;
    let mut tx_skipped = 0usize;

    for block_number in blocks {
        if block_number < 0 {
            continue;
        }
        let Some(block_json) = provider
            .get_block_by_number_raw(block_number as u64, false)
            .await
            .with_context(|| format!("failed to fetch peer block {block_number}"))?
        else {
            continue;
        };
        upsert_observed_block(pool, block_number, &block_json).await?;
        block_count += 1;

        let txs = block_json
            .get("transactions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        tx_seen += txs.len();

        for tx_value in txs {
            let Some(raw_hash) = tx_value.as_str() else {
                continue;
            };
            if observed_transaction_exists(pool, raw_hash).await? {
                continue;
            }
            let tx_hash =
                B256::from_str(raw_hash).with_context(|| format!("invalid tx hash {raw_hash}"))?;
            let Some(tx_json) = provider.get_transaction_by_hash(tx_hash).await? else {
                tx_skipped += 1;
                continue;
            };
            let Some(receipt) = provider.get_transaction_receipt(tx_hash).await? else {
                tx_skipped += 1;
                continue;
            };
            upsert_observed_transaction(pool, &tx_json, &receipt.raw).await?;
            tx_fetched += 1;
        }
    }

    println!(
        "blocks: {block_count} block_txs_seen: {tx_seen} newly_hydrated_peer_txs: {tx_fetched} skipped_missing_rpc: {tx_skipped}"
    );
    println!();
    Ok(())
}

async fn observed_transaction_exists(pool: &PgPool, tx_hash: &str) -> Result<bool> {
    let exists: bool = sqlx::query_scalar(
        r#"
        SELECT EXISTS (
            SELECT 1
            FROM observed_transactions
            WHERE lower(tx_hash) = lower($1)
        )
        "#,
    )
    .bind(tx_hash)
    .fetch_one(pool)
    .await?;
    Ok(exists)
}

async fn collect_address_transfer_logs(
    pool: &PgPool,
    provider: &ChainProvider,
    cli: &Cli,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
    println!("== Collect Address Transfer Logs ==");
    let seed = format!("{:#x}", cli.address).to_ascii_lowercase();
    let topic = address_topic(cli.address);
    let mut cursor = from_block;
    let mut inserted = 0usize;
    let mut chunks = 0usize;

    while cursor <= to_block {
        let chunk_to = cursor
            .saturating_add(cli.log_chunk_blocks.saturating_sub(1))
            .min(to_block);
        for direction in [TransferDirection::Out, TransferDirection::In] {
            let topics = match direction {
                TransferDirection::Out => json!([TRANSFER_TOPIC, topic]),
                TransferDirection::In => json!([TRANSFER_TOPIC, Value::Null, topic]),
            };
            let logs = provider
                .get_logs_raw(json!([{
                    "fromBlock": format!("0x{cursor:x}"),
                    "toBlock": format!("0x{chunk_to:x}"),
                    "topics": topics,
                }]))
                .await
                .with_context(|| {
                    format!(
                        "eth_getLogs failed for {direction:?} transfers at blocks {cursor}..{chunk_to}"
                    )
                })?;
            for log in logs {
                if upsert_address_transfer(pool, &seed, direction, &log).await? {
                    inserted += 1;
                }
            }
        }
        chunks += 1;
        cursor = chunk_to.saturating_add(1);
    }

    println!(
        "scanned_chunks: {chunks} new_or_updated_transfer_rows: {inserted} block_range: {from_block}..{to_block}"
    );
    println!();
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum TransferDirection {
    In,
    Out,
}

impl TransferDirection {
    fn as_str(self) -> &'static str {
        match self {
            TransferDirection::In => "in",
            TransferDirection::Out => "out",
        }
    }
}

async fn upsert_address_transfer(
    pool: &PgPool,
    seed: &str,
    direction: TransferDirection,
    log: &Value,
) -> Result<bool> {
    let tx_hash = required_str(log, "transactionHash")?.to_ascii_lowercase();
    let token_address = required_str(log, "address")?.to_ascii_lowercase();
    let block_number = hex_i64(log.get("blockNumber").and_then(Value::as_str))
        .context("transfer log missing blockNumber")?;
    let log_index = hex_i64(log.get("logIndex").and_then(Value::as_str))
        .context("transfer log missing logIndex")?;
    let topics = log
        .get("topics")
        .and_then(Value::as_array)
        .context("transfer log missing topics")?;
    if topics.len() < 3 {
        bail!("transfer log has fewer than 3 topics");
    }
    let from_address = topic_address(topics[1].as_str().context("missing transfer from topic")?)?;
    let to_address = topic_address(topics[2].as_str().context("missing transfer to topic")?)?;
    let amount = hex_decimal(log.get("data").and_then(Value::as_str)).unwrap_or_else(|| "0".into());
    let counterparty = match direction {
        TransferDirection::In => from_address.clone(),
        TransferDirection::Out => to_address.clone(),
    };

    let result = sqlx::query(
        r#"
        INSERT INTO observed_address_transfers (
            seed_address, direction, tx_hash, block_number, log_index, token_address,
            from_address, to_address, counterparty, amount, raw_log_json, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW())
        ON CONFLICT (seed_address, direction, tx_hash, log_index)
        DO UPDATE SET
            block_number = EXCLUDED.block_number,
            token_address = EXCLUDED.token_address,
            from_address = EXCLUDED.from_address,
            to_address = EXCLUDED.to_address,
            counterparty = EXCLUDED.counterparty,
            amount = EXCLUDED.amount,
            raw_log_json = EXCLUDED.raw_log_json,
            updated_at = NOW()
        "#,
    )
    .bind(seed)
    .bind(direction.as_str())
    .bind(tx_hash)
    .bind(block_number)
    .bind(log_index)
    .bind(token_address)
    .bind(from_address)
    .bind(to_address)
    .bind(counterparty)
    .bind(amount)
    .bind(log)
    .execute(pool)
    .await?;

    Ok(result.rows_affected() > 0)
}

async fn upsert_observed_transaction(
    pool: &PgPool,
    tx: &Value,
    receipt: &Value,
) -> Result<Option<i64>> {
    let tx_hash = required_str(tx, "hash").or_else(|_| required_str(receipt, "transactionHash"))?;
    let block_number = hex_i64(receipt.get("blockNumber").and_then(Value::as_str))
        .or_else(|| hex_i64(tx.get("blockNumber").and_then(Value::as_str)))
        .context("missing blockNumber")?;
    let transaction_index = hex_i64(receipt.get("transactionIndex").and_then(Value::as_str))
        .or_else(|| hex_i64(tx.get("transactionIndex").and_then(Value::as_str)));
    let nonce = hex_i64(tx.get("nonce").and_then(Value::as_str));
    let status = receipt
        .get("status")
        .and_then(Value::as_str)
        .map(|value| value == "0x1");

    sqlx::query(
        r#"
        INSERT INTO observed_transactions (
            tx_hash, block_number, transaction_index, from_address, to_address, nonce,
            status, gas_limit, gas_used, effective_gas_price, max_fee_per_gas,
            max_priority_fee_per_gas, tx_json, receipt_json, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,NOW())
        ON CONFLICT (tx_hash)
        DO UPDATE SET
            block_number = EXCLUDED.block_number,
            transaction_index = EXCLUDED.transaction_index,
            from_address = EXCLUDED.from_address,
            to_address = EXCLUDED.to_address,
            nonce = EXCLUDED.nonce,
            status = EXCLUDED.status,
            gas_limit = EXCLUDED.gas_limit,
            gas_used = EXCLUDED.gas_used,
            effective_gas_price = EXCLUDED.effective_gas_price,
            max_fee_per_gas = EXCLUDED.max_fee_per_gas,
            max_priority_fee_per_gas = EXCLUDED.max_priority_fee_per_gas,
            tx_json = EXCLUDED.tx_json,
            receipt_json = EXCLUDED.receipt_json,
            updated_at = NOW()
        "#,
    )
    .bind(tx_hash.to_ascii_lowercase())
    .bind(block_number)
    .bind(transaction_index)
    .bind(optional_lower_str(tx, "from"))
    .bind(optional_lower_str(tx, "to"))
    .bind(nonce)
    .bind(status)
    .bind(hex_decimal(tx.get("gas").and_then(Value::as_str)))
    .bind(hex_decimal(receipt.get("gasUsed").and_then(Value::as_str)))
    .bind(hex_decimal(
        receipt.get("effectiveGasPrice").and_then(Value::as_str),
    ))
    .bind(hex_decimal(tx.get("maxFeePerGas").and_then(Value::as_str)))
    .bind(hex_decimal(
        tx.get("maxPriorityFeePerGas").and_then(Value::as_str),
    ))
    .bind(tx)
    .bind(receipt)
    .execute(pool)
    .await?;
    Ok(Some(block_number))
}

async fn hydrate_missing_blocks_for_address(
    pool: &PgPool,
    provider: &ChainProvider,
    address: &str,
) -> Result<()> {
    let block_numbers: Vec<i64> = sqlx::query_scalar(
        r#"
        SELECT DISTINCT ot.block_number
        FROM observed_address_transfers t
        JOIN observed_transactions ot ON lower(ot.tx_hash) = lower(t.tx_hash)
        LEFT JOIN observed_blocks b ON b.block_number = ot.block_number
        WHERE lower(t.seed_address) = lower($1)
          AND b.block_number IS NULL
        ORDER BY ot.block_number DESC
        LIMIT 10000
        "#,
    )
    .bind(address)
    .fetch_all(pool)
    .await?;

    hydrate_observed_blocks(pool, provider, block_numbers.into_iter().collect()).await
}

async fn hydrate_observed_blocks(
    pool: &PgPool,
    provider: &ChainProvider,
    block_numbers: BTreeSet<i64>,
) -> Result<()> {
    if block_numbers.is_empty() {
        return Ok(());
    }

    let mut upserted = 0usize;
    for block_number in block_numbers {
        if block_number < 0 {
            continue;
        }
        let Some(block_json) = provider
            .get_block_by_number_raw(block_number as u64, false)
            .await
            .with_context(|| format!("failed to fetch block {block_number}"))?
        else {
            continue;
        };
        upsert_observed_block(pool, block_number, &block_json).await?;
        upserted += 1;
    }

    println!("hydrated_blocks: {upserted}");
    Ok(())
}

async fn upsert_observed_block(pool: &PgPool, block_number: i64, block: &Value) -> Result<()> {
    let tx_count = block
        .get("transactions")
        .and_then(Value::as_array)
        .map(|items| i64::try_from(items.len()).unwrap_or(i64::MAX));

    sqlx::query(
        r#"
        INSERT INTO observed_blocks (
            block_number, block_hash, base_fee_per_gas, gas_used, gas_limit,
            block_timestamp, tx_count, block_json, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW())
        ON CONFLICT (block_number)
        DO UPDATE SET
            block_hash = EXCLUDED.block_hash,
            base_fee_per_gas = EXCLUDED.base_fee_per_gas,
            gas_used = EXCLUDED.gas_used,
            gas_limit = EXCLUDED.gas_limit,
            block_timestamp = EXCLUDED.block_timestamp,
            tx_count = EXCLUDED.tx_count,
            block_json = EXCLUDED.block_json,
            updated_at = NOW()
        "#,
    )
    .bind(block_number)
    .bind(optional_str(block, "hash"))
    .bind(hex_decimal(
        block.get("baseFeePerGas").and_then(Value::as_str),
    ))
    .bind(hex_decimal(block.get("gasUsed").and_then(Value::as_str)))
    .bind(hex_decimal(block.get("gasLimit").and_then(Value::as_str)))
    .bind(hex_i64(block.get("timestamp").and_then(Value::as_str)))
    .bind(tx_count)
    .bind(block)
    .execute(pool)
    .await?;
    Ok(())
}

async fn print_report(pool: &PgPool, cli: &Cli, from_block: u64, to_block: u64) -> Result<()> {
    let address = format!("{:#x}", cli.address).to_ascii_lowercase();
    print_transfer_counterparties(pool, &address).await?;
    print_profit_pair_distribution(pool, &address, from_block, to_block).await?;
    print_profit_token_distribution(pool, &address, from_block, to_block).await?;
    print_address_gas_summary(pool, &address).await?;
    print_execution_lanes(pool, &address).await?;
    print_gas_strategy_recommendation(pool, &address).await?;
    print_address_hits(pool, &address, cli.days).await?;
    print_address_gas_ranks(pool, &address, cli.days).await?;
    print_receipt_topic_summary(pool, &address).await?;
    print_competitor_swap_pool_summary(pool, &address).await?;
    print_related_watched_pool_swaps(pool, &address).await?;
    Ok(())
}

async fn print_address_gas_summary(pool: &PgPool, address: &str) -> Result<()> {
    let row = sqlx::query(
        r#"
        WITH related AS (
            SELECT DISTINCT tx_hash
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
        ),
        gas AS (
            SELECT
                ot.effective_gas_price::numeric AS effective_gas_price,
                ot.gas_used::numeric AS gas_used,
                CASE
                    WHEN ot.effective_gas_price IS NOT NULL
                     AND ob.base_fee_per_gas IS NOT NULL
                     AND ot.effective_gas_price::numeric >= ob.base_fee_per_gas::numeric
                    THEN ot.effective_gas_price::numeric - ob.base_fee_per_gas::numeric
                    ELSE NULL
                END AS paid_priority_fee_per_gas
            FROM observed_transactions ot
            JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
            LEFT JOIN observed_blocks ob ON ob.block_number = ot.block_number
            WHERE ot.effective_gas_price IS NOT NULL
        )
        SELECT
            COUNT(*)::bigint AS n,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY effective_gas_price) AS p50_effective,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY effective_gas_price) AS p90_effective,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY effective_gas_price) AS p99_effective,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY paid_priority_fee_per_gas) AS p50_paid_priority,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY paid_priority_fee_per_gas) AS p90_paid_priority,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY paid_priority_fee_per_gas) AS p99_paid_priority,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY gas_used) AS p50_gas_used,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY gas_used) AS p90_gas_used,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY gas_used) AS p99_gas_used
        FROM gas
        "#,
    )
    .bind(address)
    .fetch_one(pool)
    .await?;

    println!("== Address Gas Summary ==");
    println!("txs: {}", cell_i64(&row, "n").unwrap_or_default());
    println!(
        "effective_gas_price wei p50={} p90={} p99={}",
        cell_f64(&row, "p50_effective").unwrap_or_default(),
        cell_f64(&row, "p90_effective").unwrap_or_default(),
        cell_f64(&row, "p99_effective").unwrap_or_default(),
    );
    println!(
        "paid_priority_fee wei p50={} p90={} p99={}",
        cell_f64(&row, "p50_paid_priority").unwrap_or_default(),
        cell_f64(&row, "p90_paid_priority").unwrap_or_default(),
        cell_f64(&row, "p99_paid_priority").unwrap_or_default(),
    );
    println!(
        "gas_used p50={} p90={} p99={}",
        cell_f64(&row, "p50_gas_used").unwrap_or_default(),
        cell_f64(&row, "p90_gas_used").unwrap_or_default(),
        cell_f64(&row, "p99_gas_used").unwrap_or_default(),
    );
    println!();
    Ok(())
}

async fn print_execution_lanes(pool: &PgPool, address: &str) -> Result<()> {
    let rows = sqlx::query(
        r#"
        WITH related AS (
            SELECT DISTINCT tx_hash
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
        ),
        gas AS (
            SELECT
                COALESCE(lower(ot.from_address), '-') AS from_address,
                COALESCE(lower(ot.to_address), '-') AS to_address,
                ot.block_number,
                ot.effective_gas_price::numeric AS effective_gas_price,
                ot.gas_used::numeric AS gas_used,
                CASE
                    WHEN ot.effective_gas_price IS NOT NULL
                     AND ob.base_fee_per_gas IS NOT NULL
                     AND ot.effective_gas_price::numeric >= ob.base_fee_per_gas::numeric
                    THEN ot.effective_gas_price::numeric - ob.base_fee_per_gas::numeric
                    ELSE NULL
                END AS paid_priority_fee_per_gas
            FROM observed_transactions ot
            JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
            LEFT JOIN observed_blocks ob ON ob.block_number = ot.block_number
            WHERE ot.effective_gas_price IS NOT NULL
        )
        SELECT
            from_address,
            to_address,
            COUNT(*)::bigint AS txs,
            MIN(block_number)::bigint AS first_block,
            MAX(block_number)::bigint AS latest_block,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY effective_gas_price) AS p50_effective,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY effective_gas_price) AS p90_effective,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY paid_priority_fee_per_gas) AS p50_paid_priority,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY paid_priority_fee_per_gas) AS p90_paid_priority,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY gas_used) AS p50_gas_used,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY gas_used) AS p90_gas_used
        FROM gas
        GROUP BY from_address, to_address
        ORDER BY txs DESC, latest_block DESC
        LIMIT 20
        "#,
    )
    .bind(address)
    .fetch_all(pool)
    .await?;

    println!("== Execution Lanes By From -> To ==");
    if rows.is_empty() {
        println!("no hydrated transaction lanes");
        println!();
        return Ok(());
    }
    for row in rows {
        println!(
            "from={} to={} txs={} blocks={}..{} effective_p50={} effective_p90={} paid_priority_p50={} paid_priority_p90={} gas_used_p50={} gas_used_p90={}",
            cell_string(&row, "from_address").unwrap_or_else(|| "-".into()),
            cell_string(&row, "to_address").unwrap_or_else(|| "-".into()),
            cell_i64(&row, "txs").unwrap_or_default(),
            cell_i64(&row, "first_block").unwrap_or_default(),
            cell_i64(&row, "latest_block").unwrap_or_default(),
            cell_f64(&row, "p50_effective").unwrap_or_default(),
            cell_f64(&row, "p90_effective").unwrap_or_default(),
            cell_f64(&row, "p50_paid_priority").unwrap_or_default(),
            cell_f64(&row, "p90_paid_priority").unwrap_or_default(),
            cell_f64(&row, "p50_gas_used").unwrap_or_default(),
            cell_f64(&row, "p90_gas_used").unwrap_or_default(),
        );
    }
    println!();
    Ok(())
}

async fn print_gas_strategy_recommendation(pool: &PgPool, address: &str) -> Result<()> {
    let row = sqlx::query(
        r#"
        WITH related AS (
            SELECT DISTINCT tx_hash
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
        ),
        gas AS (
            SELECT
                ob.base_fee_per_gas::numeric AS base_fee_per_gas,
                ot.effective_gas_price::numeric AS effective_gas_price,
                ot.gas_used::numeric AS gas_used,
                CASE
                    WHEN ot.effective_gas_price IS NOT NULL
                     AND ob.base_fee_per_gas IS NOT NULL
                     AND ot.effective_gas_price::numeric >= ob.base_fee_per_gas::numeric
                    THEN ot.effective_gas_price::numeric - ob.base_fee_per_gas::numeric
                    ELSE NULL
                END AS paid_priority_fee_per_gas
            FROM observed_transactions ot
            JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
            JOIN observed_blocks ob ON ob.block_number = ot.block_number
            WHERE ot.effective_gas_price IS NOT NULL
              AND ob.base_fee_per_gas IS NOT NULL
        )
        SELECT
            COUNT(*)::bigint AS n,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY base_fee_per_gas) AS p50_base_fee,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY base_fee_per_gas) AS p90_base_fee,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY base_fee_per_gas) AS p99_base_fee,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY paid_priority_fee_per_gas) AS p50_paid_priority,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY paid_priority_fee_per_gas) AS p90_paid_priority,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY paid_priority_fee_per_gas) AS p99_paid_priority,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY gas_used) AS p90_gas_used,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY gas_used) AS p99_gas_used
        FROM gas
        "#,
    )
    .bind(address)
    .fetch_one(pool)
    .await?;

    let n = cell_i64(&row, "n").unwrap_or_default();
    let p50_priority = cell_f64(&row, "p50_paid_priority").unwrap_or_default();
    let p90_priority = cell_f64(&row, "p90_paid_priority").unwrap_or_default();
    let p99_priority = cell_f64(&row, "p99_paid_priority").unwrap_or_default();
    let p90_base = cell_f64(&row, "p90_base_fee").unwrap_or_default();
    let p99_base = cell_f64(&row, "p99_base_fee").unwrap_or_default();
    let p90_gas = cell_f64(&row, "p90_gas_used").unwrap_or_default();
    let p99_gas = cell_f64(&row, "p99_gas_used").unwrap_or_default();

    let normal_priority = p90_priority.max(p50_priority * 1.10).ceil();
    let aggressive_priority = p99_priority.max(normal_priority * 2.0).ceil();
    let normal_max_fee = (p90_base + normal_priority).ceil();
    let aggressive_max_fee = (p99_base + aggressive_priority).ceil();
    let normal_eth_budget = normal_max_fee * p90_gas / 1e18;
    let aggressive_eth_budget = aggressive_max_fee * p99_gas / 1e18;

    println!("== Gas Strategy Recommendation ==");
    if n == 0 {
        println!("not enough hydrated block gas data");
        println!();
        return Ok(());
    }
    println!(
        "samples: {n} base_fee_p90={} base_fee_p99={} paid_priority_p50={} paid_priority_p90={} paid_priority_p99={}",
        fmt_wei(p90_base),
        fmt_wei(p99_base),
        fmt_wei(p50_priority),
        fmt_wei(p90_priority),
        fmt_wei(p99_priority),
    );
    println!(
        "normal: priority_fee={} max_fee={} gas_limit_floor≈{} eth_budget≈{:.8}",
        fmt_wei(normal_priority),
        fmt_wei(normal_max_fee),
        p90_gas.ceil() as u64,
        normal_eth_budget,
    );
    println!(
        "aggressive: priority_fee={} max_fee={} gas_limit_floor≈{} eth_budget≈{:.8}",
        fmt_wei(aggressive_priority),
        fmt_wei(aggressive_max_fee),
        p99_gas.ceil() as u64,
        aggressive_eth_budget,
    );
    println!();
    Ok(())
}

async fn print_transfer_counterparties(pool: &PgPool, address: &str) -> Result<()> {
    let rows: Vec<TransferSummaryRow> = sqlx::query_as(
        r#"
        SELECT
            direction,
            token_address,
            counterparty,
            COUNT(*)::bigint AS transfers,
            COUNT(DISTINCT tx_hash)::bigint AS txs,
            MIN(block_number)::bigint AS first_block,
            MAX(block_number)::bigint AS latest_block
        FROM observed_address_transfers
        WHERE lower(seed_address) = lower($1)
        GROUP BY direction, token_address, counterparty
        ORDER BY txs DESC, transfers DESC
        LIMIT 50
        "#,
    )
    .bind(address)
    .fetch_all(pool)
    .await?;

    println!("== Top Transfer Counterparties ==");
    if rows.is_empty() {
        println!("no ERC20 Transfer logs involving seed address in cached range");
        println!();
        return Ok(());
    }
    for row in rows {
        println!(
            "{} token={} counterparty={} txs={} transfers={} blocks={}..{}",
            row.direction,
            row.token_address,
            row.counterparty,
            row.txs,
            row.transfers,
            row.first_block,
            row.latest_block,
        );
    }
    println!();
    Ok(())
}

async fn print_profit_pair_distribution(
    pool: &PgPool,
    address: &str,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
    let rows = sqlx::query(
        r#"
        WITH deltas AS (
            SELECT
                lower(tx_hash) AS tx_hash,
                lower(token_address) AS token_address,
                SUM(CASE WHEN direction = 'in' THEN amount::numeric ELSE -amount::numeric END) AS net_amount
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
              AND block_number BETWEEN $2 AND $3
            GROUP BY lower(tx_hash), lower(token_address)
            HAVING SUM(CASE WHEN direction = 'in' THEN amount::numeric ELSE -amount::numeric END) <> 0
        ),
        labeled AS (
            SELECT
                d.tx_hash,
                d.token_address,
                COALESCE(t.symbol, left(d.token_address, 10)) AS token_symbol,
                d.net_amount
            FROM deltas d
            LEFT JOIN tokens t ON lower(t.token_address) = d.token_address
        ),
        tx_shapes AS (
            SELECT
                l.tx_hash,
                COALESCE(
                    string_agg(l.token_symbol, '+' ORDER BY l.token_symbol) FILTER (WHERE l.net_amount < 0),
                    'none'
                ) AS input_tokens,
                string_agg(l.token_symbol, '+' ORDER BY l.token_symbol) FILTER (WHERE l.net_amount > 0) AS profit_tokens,
                COUNT(*) FILTER (WHERE l.net_amount < 0)::bigint AS input_token_count,
                COUNT(*) FILTER (WHERE l.net_amount > 0)::bigint AS profit_token_count
            FROM labeled l
            GROUP BY l.tx_hash
            HAVING COUNT(*) FILTER (WHERE l.net_amount > 0) > 0
        ),
        swap_context AS (
            SELECT
                lower(ot.tx_hash) AS tx_hash,
                string_agg(DISTINCT COALESCE(tp.symbol, lower(log->>'address')), ', ' ORDER BY COALESCE(tp.symbol, lower(log->>'address'))) FILTER (WHERE log->>'address' IS NOT NULL) AS swap_pairs,
                string_agg(DISTINCT COALESCE(p.dex || ':' || p.variant, 'unknown'), ', ' ORDER BY COALESCE(p.dex || ':' || p.variant, 'unknown')) FILTER (WHERE log->>'address' IS NOT NULL) AS protocols
            FROM observed_transactions ot
            CROSS JOIN LATERAL jsonb_array_elements(ot.receipt_json->'logs') AS log
            LEFT JOIN pools p ON lower(p.pool_address) = lower(log->>'address')
            LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
            WHERE lower(log->'topics'->>0) = ANY($4)
            GROUP BY lower(ot.tx_hash)
        )
        SELECT
            s.input_tokens,
            s.profit_tokens,
            COUNT(*)::bigint AS txs,
            MIN(ot.block_number)::bigint AS first_block,
            MAX(ot.block_number)::bigint AS latest_block,
            COUNT(*) FILTER (WHERE s.input_token_count = 0)::bigint AS pure_inflow_txs,
            COUNT(*) FILTER (WHERE s.input_token_count > 0)::bigint AS swap_like_txs,
            string_agg(DISTINCT sc.swap_pairs, ' | ' ORDER BY sc.swap_pairs) FILTER (WHERE sc.swap_pairs IS NOT NULL) AS swap_pairs,
            string_agg(DISTINCT sc.protocols, ' | ' ORDER BY sc.protocols) FILTER (WHERE sc.protocols IS NOT NULL) AS protocols
        FROM tx_shapes s
        LEFT JOIN observed_transactions ot ON lower(ot.tx_hash) = s.tx_hash
        LEFT JOIN swap_context sc ON sc.tx_hash = s.tx_hash
        WHERE ot.status IS DISTINCT FROM FALSE
        GROUP BY s.input_tokens, s.profit_tokens
        ORDER BY txs DESC, latest_block DESC
        LIMIT 50
        "#,
    )
    .bind(address)
    .bind(i64::try_from(from_block)?)
    .bind(i64::try_from(to_block)?)
    .bind(vec![
        UNI_V3_SWAP_TOPIC.to_string(),
        PANCAKE_V3_SWAP_TOPIC.to_string(),
        CLASSIC_SWAP_TOPIC.to_string(),
    ])
    .fetch_all(pool)
    .await?;

    println!("== Profit Pair Distribution By Net ERC20 Delta ==");
    if rows.is_empty() {
        println!("no profitable net-inflow transaction shapes from cached transfer data");
        println!();
        return Ok(());
    }
    for row in rows {
        println!(
            "{} -> {} txs={} swap_like={} pure_inflow={} blocks={}..{} swaps=[{}] protocols=[{}]",
            cell_string(&row, "input_tokens").unwrap_or_else(|| "-".into()),
            cell_string(&row, "profit_tokens").unwrap_or_else(|| "-".into()),
            cell_i64(&row, "txs").unwrap_or_default(),
            cell_i64(&row, "swap_like_txs").unwrap_or_default(),
            cell_i64(&row, "pure_inflow_txs").unwrap_or_default(),
            cell_i64(&row, "first_block").unwrap_or_default(),
            cell_i64(&row, "latest_block").unwrap_or_default(),
            cell_string(&row, "swap_pairs").unwrap_or_else(|| "-".into()),
            cell_string(&row, "protocols").unwrap_or_else(|| "-".into()),
        );
    }
    println!();
    Ok(())
}

async fn print_profit_token_distribution(
    pool: &PgPool,
    address: &str,
    from_block: u64,
    to_block: u64,
) -> Result<()> {
    let rows = sqlx::query(
        r#"
        WITH deltas AS (
            SELECT
                lower(tx_hash) AS tx_hash,
                lower(token_address) AS token_address,
                SUM(CASE WHEN direction = 'in' THEN amount::numeric ELSE -amount::numeric END) AS net_amount
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
              AND block_number BETWEEN $2 AND $3
            GROUP BY lower(tx_hash), lower(token_address)
            HAVING SUM(CASE WHEN direction = 'in' THEN amount::numeric ELSE -amount::numeric END) > 0
        )
        SELECT
            d.token_address,
            COALESCE(t.symbol, left(d.token_address, 10)) AS token_symbol,
            COUNT(DISTINCT d.tx_hash)::bigint AS txs,
            SUM(d.net_amount)::text AS total_profit_raw,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY d.net_amount)::text AS p50_profit_raw,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY d.net_amount)::text AS p90_profit_raw,
            MIN(ot.block_number)::bigint AS first_block,
            MAX(ot.block_number)::bigint AS latest_block
        FROM deltas d
        LEFT JOIN tokens t ON lower(t.token_address) = d.token_address
        LEFT JOIN observed_transactions ot ON lower(ot.tx_hash) = d.tx_hash
        WHERE ot.status IS DISTINCT FROM FALSE
        GROUP BY d.token_address, t.symbol
        ORDER BY txs DESC, latest_block DESC
        LIMIT 50
        "#,
    )
    .bind(address)
    .bind(i64::try_from(from_block)?)
    .bind(i64::try_from(to_block)?)
    .fetch_all(pool)
    .await?;

    println!("== Profit Token Distribution By Net ERC20 Delta ==");
    if rows.is_empty() {
        println!("no positive net token deltas from cached transfer data");
        println!();
        return Ok(());
    }
    for row in rows {
        println!(
            "token={} ({}) txs={} total_raw={} p50_raw={} p90_raw={} blocks={}..{}",
            cell_string(&row, "token_symbol").unwrap_or_else(|| "-".into()),
            cell_string(&row, "token_address").unwrap_or_else(|| "-".into()),
            cell_i64(&row, "txs").unwrap_or_default(),
            cell_string(&row, "total_profit_raw").unwrap_or_else(|| "-".into()),
            cell_string(&row, "p50_profit_raw").unwrap_or_else(|| "-".into()),
            cell_string(&row, "p90_profit_raw").unwrap_or_else(|| "-".into()),
            cell_i64(&row, "first_block").unwrap_or_default(),
            cell_i64(&row, "latest_block").unwrap_or_default(),
        );
    }
    println!();
    Ok(())
}

async fn print_address_hits(pool: &PgPool, address: &str, days: i64) -> Result<()> {
    let rows: Vec<HitRow> = sqlx::query_as(
        r#"
        WITH related AS (
            SELECT DISTINCT tx_hash
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
        )
        SELECT
            ot.block_number,
            ot.transaction_index,
            ot.tx_hash,
            ot.from_address,
            ot.to_address,
            ot.effective_gas_price,
            ot.max_priority_fee_per_gas,
            ot.max_fee_per_gas,
            ot.gas_used,
            ob.base_fee_per_gas,
            CASE
                WHEN ot.effective_gas_price IS NOT NULL
                 AND ob.base_fee_per_gas IS NOT NULL
                 AND ot.effective_gas_price::numeric >= ob.base_fee_per_gas::numeric
                THEN (ot.effective_gas_price::numeric - ob.base_fee_per_gas::numeric)::text
                ELSE NULL
            END AS paid_priority_fee_per_gas,
            string_agg(DISTINCT lower(e.pool_address), ', ' ORDER BY lower(e.pool_address)) FILTER (WHERE e.pool_address IS NOT NULL) AS pools,
            string_agg(DISTINCT e.dex || ':' || e.event_type, ', ' ORDER BY e.dex || ':' || e.event_type) FILTER (WHERE e.dex IS NOT NULL) AS protocols
        FROM observed_transactions ot
        JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
        LEFT JOIN observed_blocks ob ON ob.block_number = ot.block_number
        LEFT JOIN dex_events e ON lower(e.tx_hash) = lower(ot.tx_hash)
        WHERE ot.block_number >= (
            SELECT COALESCE(MAX(block_number), 0) - ($2::bigint * 43200)
            FROM observed_transactions
        )
        GROUP BY
            ot.block_number, ot.transaction_index, ot.tx_hash, ot.from_address,
            ot.to_address, ot.effective_gas_price, ot.max_priority_fee_per_gas,
            ot.max_fee_per_gas, ot.gas_used, ob.base_fee_per_gas
        ORDER BY ot.block_number DESC, ot.transaction_index DESC
        LIMIT 50
        "#,
    )
    .bind(address)
    .bind(days)
    .fetch_all(pool)
    .await?;

    println!("== Address Hits On Watched Pools ==");
    if rows.is_empty() {
        println!("no hydrated transactions for address transfer set yet");
        println!();
        return Ok(());
    }
    for row in rows {
        println!(
            "block={} idx={} tx={} from={} to={} effective={} base_fee={} paid_priority={} tx_priority={} max_fee={} gas_used={} pools=[{}] proto=[{}]",
            row.block_number,
            row.transaction_index.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
            row.tx_hash,
            row.from_address.unwrap_or_else(|| "-".into()),
            row.to_address.unwrap_or_else(|| "-".into()),
            row.effective_gas_price.unwrap_or_else(|| "-".into()),
            row.base_fee_per_gas.unwrap_or_else(|| "-".into()),
            row.paid_priority_fee_per_gas.unwrap_or_else(|| "-".into()),
            row.max_priority_fee_per_gas.unwrap_or_else(|| "-".into()),
            row.max_fee_per_gas.unwrap_or_else(|| "-".into()),
            row.gas_used.unwrap_or_else(|| "-".into()),
            row.pools.unwrap_or_else(|| "-".into()),
            row.protocols.unwrap_or_else(|| "-".into()),
        );
    }
    println!();
    Ok(())
}

async fn print_address_gas_ranks(pool: &PgPool, address: &str, days: i64) -> Result<()> {
    let rows: Vec<RankRow> = sqlx::query_as(
        r#"
        WITH related AS (
            SELECT DISTINCT tx_hash
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
        ),
        hits AS (
            SELECT DISTINCT ot.*
            FROM observed_transactions ot
            JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
            WHERE ot.block_number >= (
                SELECT COALESCE(MAX(block_number), 0) - ($2::bigint * 43200)
                FROM observed_transactions
            )
            ORDER BY ot.block_number DESC, ot.transaction_index DESC
            LIMIT 50
        )
        SELECT
            h.tx_hash,
            h.block_number,
            h.transaction_index,
            h.effective_gas_price,
            h.max_priority_fee_per_gas,
            CASE
                WHEN h.effective_gas_price IS NOT NULL
                 AND ob.base_fee_per_gas IS NOT NULL
                 AND h.effective_gas_price::numeric >= ob.base_fee_per_gas::numeric
                THEN (h.effective_gas_price::numeric - ob.base_fee_per_gas::numeric)::text
                ELSE NULL
            END AS paid_priority_fee_per_gas,
            (
                SELECT COUNT(*)::bigint
                FROM observed_transactions p
                WHERE p.block_number = h.block_number
                  AND p.effective_gas_price IS NOT NULL
            ) AS observed_pool_txs_in_block,
            (
                SELECT COUNT(*)::bigint + 1
                FROM observed_transactions p
                WHERE p.block_number = h.block_number
                  AND p.effective_gas_price IS NOT NULL
                  AND h.effective_gas_price IS NOT NULL
                  AND p.effective_gas_price::numeric > h.effective_gas_price::numeric
            ) AS effective_gas_rank,
            (
                SELECT COUNT(*)::bigint + 1
                FROM observed_transactions p
                LEFT JOIN observed_blocks pob ON pob.block_number = p.block_number
                WHERE p.block_number = h.block_number
                  AND p.effective_gas_price IS NOT NULL
                  AND pob.base_fee_per_gas IS NOT NULL
                  AND h.effective_gas_price IS NOT NULL
                  AND ob.base_fee_per_gas IS NOT NULL
                  AND (p.effective_gas_price::numeric - pob.base_fee_per_gas::numeric) >
                      (h.effective_gas_price::numeric - ob.base_fee_per_gas::numeric)
            ) AS priority_gas_rank
        FROM hits h
        LEFT JOIN observed_blocks ob ON ob.block_number = h.block_number
        ORDER BY h.block_number DESC, h.transaction_index DESC
        "#,
    )
    .bind(address)
    .bind(days)
    .fetch_all(pool)
    .await?;

    println!("== Gas Rank Within Observed Watched-Pool Txs In Same Block ==");
    if rows.is_empty() {
        println!("no rank rows");
        println!();
        return Ok(());
    }
    for row in rows {
        println!(
            "block={} idx={} tx={} effective={} rank={} peers={} paid_priority={} tx_priority={} paid_priority_rank={}",
            row.block_number,
            row.transaction_index
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            row.tx_hash,
            row.effective_gas_price.unwrap_or_else(|| "-".into()),
            row.effective_gas_rank
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            row.observed_pool_txs_in_block
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            row.paid_priority_fee_per_gas.unwrap_or_else(|| "-".into()),
            row.max_priority_fee_per_gas.unwrap_or_else(|| "-".into()),
            row.priority_gas_rank
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
        );
    }
    println!();
    Ok(())
}

async fn print_receipt_topic_summary(pool: &PgPool, address: &str) -> Result<()> {
    let rows: Vec<TopicRow> = sqlx::query_as(
        r#"
        WITH related AS (
            SELECT DISTINCT tx_hash
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
        ),
        logs AS (
            SELECT
                lower(log->'topics'->>0) AS topic0,
                ot.tx_hash
            FROM observed_transactions ot
            JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
            CROSS JOIN LATERAL jsonb_array_elements(ot.receipt_json->'logs') AS log
        )
        SELECT topic0, COUNT(*)::bigint AS logs, COUNT(DISTINCT tx_hash)::bigint AS txs
        FROM logs
        GROUP BY topic0
        ORDER BY txs DESC, logs DESC
        LIMIT 30
        "#,
    )
    .bind(address)
    .fetch_all(pool)
    .await?;

    println!("== Receipt Event Topic Summary For Related Txs ==");
    for row in rows {
        println!(
            "topic={} txs={} logs={}",
            row.topic0.unwrap_or_else(|| "-".into()),
            row.txs,
            row.logs
        );
    }
    println!();
    Ok(())
}

async fn print_competitor_swap_pool_summary(pool: &PgPool, address: &str) -> Result<()> {
    let rows = sqlx::query(
        r#"
        WITH related AS (
            SELECT DISTINCT tx_hash
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
        ),
        swap_logs AS (
            SELECT
                lower(log->>'address') AS pool_address,
                lower(log->'topics'->>0) AS topic0,
                ot.tx_hash,
                ot.block_number
            FROM observed_transactions ot
            JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
            CROSS JOIN LATERAL jsonb_array_elements(ot.receipt_json->'logs') AS log
            WHERE lower(log->'topics'->>0) = ANY($2)
        )
        SELECT
            sl.pool_address,
            CASE sl.topic0
                WHEN $3 THEN 'v3/slipstream'
                WHEN $4 THEN 'pancake-v3'
                WHEN $5 THEN 'classic-v2'
                ELSE sl.topic0
            END AS swap_family,
            COALESCE(p.dex, '-') AS dex,
            COALESCE(p.variant, '-') AS variant,
            COALESCE(tp.symbol, '-') AS symbol,
            COUNT(*)::bigint AS swap_logs,
            COUNT(DISTINCT sl.tx_hash)::bigint AS txs,
            MIN(sl.block_number)::bigint AS first_block,
            MAX(sl.block_number)::bigint AS latest_block,
            BOOL_OR(p.pool_address IS NOT NULL) AS registered
        FROM swap_logs sl
        LEFT JOIN pools p ON lower(p.pool_address) = sl.pool_address
        LEFT JOIN token_pairs tp ON tp.id = p.token_pair_id
        GROUP BY sl.pool_address, sl.topic0, p.dex, p.variant, tp.symbol
        ORDER BY txs DESC, swap_logs DESC
        LIMIT 50
        "#,
    )
    .bind(address)
    .bind(vec![
        UNI_V3_SWAP_TOPIC.to_string(),
        PANCAKE_V3_SWAP_TOPIC.to_string(),
        CLASSIC_SWAP_TOPIC.to_string(),
    ])
    .bind(UNI_V3_SWAP_TOPIC)
    .bind(PANCAKE_V3_SWAP_TOPIC)
    .bind(CLASSIC_SWAP_TOPIC)
    .fetch_all(pool)
    .await?;

    println!("== Competitor Swap Pools / Protocols ==");
    if rows.is_empty() {
        println!("no swap logs found in hydrated related receipts");
        println!();
        return Ok(());
    }
    for row in rows {
        println!(
            "pool={} family={} registered={} symbol={} dex={} variant={} txs={} logs={} blocks={}..{}",
            cell_string(&row, "pool_address").unwrap_or_else(|| "-".into()),
            cell_string(&row, "swap_family").unwrap_or_else(|| "-".into()),
            cell_bool(&row, "registered").unwrap_or(false),
            cell_string(&row, "symbol").unwrap_or_else(|| "-".into()),
            cell_string(&row, "dex").unwrap_or_else(|| "-".into()),
            cell_string(&row, "variant").unwrap_or_else(|| "-".into()),
            cell_i64(&row, "txs").unwrap_or_default(),
            cell_i64(&row, "swap_logs").unwrap_or_default(),
            cell_i64(&row, "first_block").unwrap_or_default(),
            cell_i64(&row, "latest_block").unwrap_or_default(),
        );
    }
    println!();
    Ok(())
}

async fn print_related_watched_pool_swaps(pool: &PgPool, address: &str) -> Result<()> {
    let rows: Vec<HitRow> = sqlx::query_as(
        r#"
        WITH related AS (
            SELECT DISTINCT tx_hash
            FROM observed_address_transfers
            WHERE lower(seed_address) = lower($1)
        )
        SELECT
            ot.block_number,
            ot.transaction_index,
            ot.tx_hash,
            ot.from_address,
            ot.to_address,
            ot.effective_gas_price,
            ot.max_priority_fee_per_gas,
            ot.max_fee_per_gas,
            ot.gas_used,
            ob.base_fee_per_gas,
            CASE
                WHEN ot.effective_gas_price IS NOT NULL
                 AND ob.base_fee_per_gas IS NOT NULL
                 AND ot.effective_gas_price::numeric >= ob.base_fee_per_gas::numeric
                THEN (ot.effective_gas_price::numeric - ob.base_fee_per_gas::numeric)::text
                ELSE NULL
            END AS paid_priority_fee_per_gas,
            string_agg(DISTINCT lower(e.pool_address), ', ' ORDER BY lower(e.pool_address)) AS pools,
            string_agg(DISTINCT e.dex || ':' || e.event_type, ', ' ORDER BY e.dex || ':' || e.event_type) AS protocols
        FROM observed_transactions ot
        JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
        LEFT JOIN observed_blocks ob ON ob.block_number = ot.block_number
        JOIN dex_events e ON lower(e.tx_hash) = lower(ot.tx_hash)
        WHERE e.event_type = 'Swap'
        GROUP BY
            ot.block_number, ot.transaction_index, ot.tx_hash, ot.from_address,
            ot.to_address, ot.effective_gas_price, ot.max_priority_fee_per_gas,
            ot.max_fee_per_gas, ot.gas_used, ob.base_fee_per_gas
        ORDER BY ot.block_number DESC, ot.transaction_index DESC
        LIMIT 50
        "#,
    )
    .bind(address)
    .fetch_all(pool)
    .await?;

    println!("== Related Txs Touching Our Watched Pools ==");
    if rows.is_empty() {
        println!("no related txs joined to current dex_events watched pools");
        println!();
        return Ok(());
    }
    for row in rows {
        println!(
            "block={} idx={} tx={} from={} to={} effective={} base_fee={} paid_priority={} tx_priority={} pools=[{}] proto=[{}]",
            row.block_number,
            row.transaction_index
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            row.tx_hash,
            row.from_address.unwrap_or_else(|| "-".into()),
            row.to_address.unwrap_or_else(|| "-".into()),
            row.effective_gas_price.unwrap_or_else(|| "-".into()),
            row.base_fee_per_gas.unwrap_or_else(|| "-".into()),
            row.paid_priority_fee_per_gas.unwrap_or_else(|| "-".into()),
            row.max_priority_fee_per_gas.unwrap_or_else(|| "-".into()),
            row.pools.unwrap_or_else(|| "-".into()),
            row.protocols.unwrap_or_else(|| "-".into()),
        );
    }
    println!();
    Ok(())
}

#[allow(dead_code)]
async fn print_watched_pool_gas_summary(pool: &PgPool, days: i64) -> Result<()> {
    let row = sqlx::query(
        r#"
        SELECT
            COUNT(DISTINCT ot.tx_hash)::bigint AS n,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY ot.effective_gas_price::numeric) AS p50_effective,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY ot.effective_gas_price::numeric) AS p90_effective,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY ot.effective_gas_price::numeric) AS p99_effective,
            percentile_cont(0.50) WITHIN GROUP (ORDER BY ot.max_priority_fee_per_gas::numeric) AS p50_priority,
            percentile_cont(0.90) WITHIN GROUP (ORDER BY ot.max_priority_fee_per_gas::numeric) AS p90_priority,
            percentile_cont(0.99) WITHIN GROUP (ORDER BY ot.max_priority_fee_per_gas::numeric) AS p99_priority
        FROM observed_transactions ot
        JOIN dex_events e ON lower(e.tx_hash) = lower(ot.tx_hash)
        WHERE e.created_at >= NOW() - ($1::text || ' days')::interval
          AND ot.effective_gas_price IS NOT NULL
          AND ot.max_priority_fee_per_gas IS NOT NULL
        "#,
    )
    .bind(days)
    .fetch_one(pool)
    .await?;

    println!("== Watched-Pool Gas Summary ==");
    println!("n: {}", cell_i64(&row, "n").unwrap_or_default());
    println!(
        "effective_gas_price wei p50={} p90={} p99={}",
        cell_f64(&row, "p50_effective").unwrap_or_default(),
        cell_f64(&row, "p90_effective").unwrap_or_default(),
        cell_f64(&row, "p99_effective").unwrap_or_default(),
    );
    println!(
        "priority_fee wei p50={} p90={} p99={}",
        cell_f64(&row, "p50_priority").unwrap_or_default(),
        cell_f64(&row, "p90_priority").unwrap_or_default(),
        cell_f64(&row, "p99_priority").unwrap_or_default(),
    );
    println!();
    Ok(())
}

fn required_str<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .with_context(|| format!("missing {key}"))
}

fn optional_lower_str(value: &Value, key: &str) -> Option<String> {
    value.get(key)?.as_str().map(|raw| raw.to_ascii_lowercase())
}

fn optional_str(value: &Value, key: &str) -> Option<String> {
    value.get(key)?.as_str().map(str::to_string)
}

fn address_topic(address: Address) -> String {
    format!("0x{:0>64}", hex::encode(address.as_slice()))
}

fn topic_address(topic: &str) -> Result<String> {
    let raw = topic.trim_start_matches("0x");
    if raw.len() != 64 {
        bail!("invalid address topic length: {topic}");
    }
    Ok(format!("0x{}", &raw[24..]).to_ascii_lowercase())
}

fn hex_i64(raw: Option<&str>) -> Option<i64> {
    let raw = raw?;
    i64::from_str_radix(raw.trim_start_matches("0x"), 16).ok()
}

fn hex_decimal(raw: Option<&str>) -> Option<String> {
    let raw = raw?;
    U256::from_str_radix(raw.trim_start_matches("0x"), 16)
        .ok()
        .map(|value| value.to_string())
}

fn cell_i64(row: &sqlx::postgres::PgRow, key: &str) -> Option<i64> {
    row.try_get::<Option<i64>, _>(key).ok().flatten()
}

fn cell_f64(row: &sqlx::postgres::PgRow, key: &str) -> Option<f64> {
    row.try_get::<Option<f64>, _>(key).ok().flatten()
}

fn cell_string(row: &sqlx::postgres::PgRow, key: &str) -> Option<String> {
    row.try_get::<Option<String>, _>(key).ok().flatten()
}

fn cell_bool(row: &sqlx::postgres::PgRow, key: &str) -> Option<bool> {
    row.try_get::<Option<bool>, _>(key).ok().flatten()
}

fn fmt_wei(value: f64) -> String {
    format!("{:.0}", value.max(0.0))
}
