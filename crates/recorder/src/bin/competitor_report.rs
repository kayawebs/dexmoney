use std::{env, str::FromStr};

use alloy_primitives::{Address, B256, U256};
use anyhow::{bail, Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_storage::postgres::{ensure_registry_schema, PostgresStore};
use serde_json::{json, Value};
use sqlx::{FromRow, PgPool, Row};
use tracing_subscriber::EnvFilter;

const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const APPROX_BASE_BLOCKS_PER_DAY: u64 = 43_200;

#[derive(Debug, Clone)]
struct Cli {
    address: Address,
    days: i64,
    hydrate_limit: i64,
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
    print_report(&store.pool, &cli).await?;
    Ok(())
}

fn parse_args<I>(args: I) -> Result<Cli>
where
    I: IntoIterator<Item = String>,
{
    let mut address = None;
    let mut days = 30_i64;
    let mut hydrate_limit = 5_000_i64;
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

    Ok(Cli {
        address: address.context("--address is required")?,
        days,
        hydrate_limit,
        from_block,
        to_block,
        log_chunk_blocks,
    })
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin competitor_report -- --address <addr> [--days 30] [--from-block N] [--to-block N] [--hydrate-limit 5000] [--log-chunk-blocks 2000]"
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
    let hashes: Vec<String> = sqlx::query_scalar(
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
    .bind(cli.hydrate_limit)
    .fetch_all(pool)
    .await?;

    if hashes.is_empty() {
        println!("== Hydrate ==");
        println!("no missing observed transactions");
        println!();
        return Ok(());
    }

    println!("== Hydrate ==");
    println!("fetching {} tx/receipt rows from RPC", hashes.len());
    let mut inserted = 0usize;
    let mut skipped = 0usize;
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
        upsert_observed_transaction(pool, &tx_json, &receipt.raw).await?;
        inserted += 1;
    }
    println!("upserted: {inserted} skipped_missing_rpc: {skipped}");
    println!();
    Ok(())
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

async fn upsert_observed_transaction(pool: &PgPool, tx: &Value, receipt: &Value) -> Result<()> {
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
    Ok(())
}

async fn print_report(pool: &PgPool, cli: &Cli) -> Result<()> {
    let address = format!("{:#x}", cli.address).to_ascii_lowercase();
    print_transfer_counterparties(pool, &address).await?;
    print_address_hits(pool, &address, cli.days).await?;
    print_address_gas_ranks(pool, &address, cli.days).await?;
    print_receipt_topic_summary(pool, &address).await?;
    print_related_watched_pool_swaps(pool, &address).await?;
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
            string_agg(DISTINCT lower(e.pool_address), ', ' ORDER BY lower(e.pool_address)) FILTER (WHERE e.pool_address IS NOT NULL) AS pools,
            string_agg(DISTINCT e.dex || ':' || e.event_type, ', ' ORDER BY e.dex || ':' || e.event_type) FILTER (WHERE e.dex IS NOT NULL) AS protocols
        FROM observed_transactions ot
        JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
        LEFT JOIN dex_events e ON lower(e.tx_hash) = lower(ot.tx_hash)
        WHERE ot.block_number >= (
            SELECT COALESCE(MAX(block_number), 0) - ($2::bigint * 43200)
            FROM observed_transactions
        )
        GROUP BY
            ot.block_number, ot.transaction_index, ot.tx_hash, ot.from_address,
            ot.to_address, ot.effective_gas_price, ot.max_priority_fee_per_gas,
            ot.max_fee_per_gas, ot.gas_used
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
            "block={} idx={} tx={} from={} to={} effective={} priority={} max_fee={} gas_used={} pools=[{}] proto=[{}]",
            row.block_number,
            row.transaction_index.map(|v| v.to_string()).unwrap_or_else(|| "-".into()),
            row.tx_hash,
            row.from_address.unwrap_or_else(|| "-".into()),
            row.to_address.unwrap_or_else(|| "-".into()),
            row.effective_gas_price.unwrap_or_else(|| "-".into()),
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
                WHERE p.block_number = h.block_number
                  AND p.max_priority_fee_per_gas IS NOT NULL
                  AND h.max_priority_fee_per_gas IS NOT NULL
                  AND p.max_priority_fee_per_gas::numeric > h.max_priority_fee_per_gas::numeric
            ) AS priority_gas_rank
        FROM hits h
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
            "block={} idx={} tx={} effective={} rank={} peers={} priority={} priority_rank={}",
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
            string_agg(DISTINCT lower(e.pool_address), ', ' ORDER BY lower(e.pool_address)) AS pools,
            string_agg(DISTINCT e.dex || ':' || e.event_type, ', ' ORDER BY e.dex || ':' || e.event_type) AS protocols
        FROM observed_transactions ot
        JOIN related r ON lower(r.tx_hash) = lower(ot.tx_hash)
        JOIN dex_events e ON lower(e.tx_hash) = lower(ot.tx_hash)
        WHERE e.event_type = 'Swap'
        GROUP BY
            ot.block_number, ot.transaction_index, ot.tx_hash, ot.from_address,
            ot.to_address, ot.effective_gas_price, ot.max_priority_fee_per_gas,
            ot.max_fee_per_gas, ot.gas_used
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
            "block={} idx={} tx={} from={} to={} effective={} priority={} pools=[{}] proto=[{}]",
            row.block_number,
            row.transaction_index
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            row.tx_hash,
            row.from_address.unwrap_or_else(|| "-".into()),
            row.to_address.unwrap_or_else(|| "-".into()),
            row.effective_gas_price.unwrap_or_else(|| "-".into()),
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
