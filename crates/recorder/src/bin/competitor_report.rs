use std::{env, str::FromStr};

use alloy_primitives::{Address, B256, U256};
use anyhow::{bail, Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_storage::postgres::{ensure_registry_schema, PostgresStore};
use serde_json::Value;
use sqlx::{FromRow, PgPool, Row};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
struct Cli {
    address: Address,
    days: i64,
    hydrate_limit: i64,
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

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = parse_args(env::args().skip(1))?;
    let settings = Settings::load()?;
    let store = PostgresStore::connect(&settings.postgres_url).await?;
    ensure_registry_schema(&store.pool).await?;
    let provider = ChainProvider::from_settings(&settings);

    print_scope(&store.pool, &cli).await?;
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
    })
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin competitor_report -- --address <addr> [--days 30] [--hydrate-limit 5000]"
    );
}

async fn print_scope(pool: &PgPool, cli: &Cli) -> Result<()> {
    let row = sqlx::query(
        r#"
        SELECT
            COUNT(DISTINCT e.tx_hash) AS dex_event_txs,
            COUNT(DISTINCT ot.tx_hash) AS observed_txs,
            MIN(e.block_number) AS min_block,
            MAX(e.block_number) AS max_block
        FROM dex_events e
        LEFT JOIN observed_transactions ot ON lower(ot.tx_hash) = lower(e.tx_hash)
        WHERE e.created_at >= NOW() - ($1::text || ' days')::interval
          AND e.event_type = 'Swap'
        "#,
    )
    .bind(cli.days)
    .fetch_one(pool)
    .await?;

    println!("== Scope ==");
    println!("address: {:#x}", cli.address);
    println!("days: {}", cli.days);
    println!(
        "watched_pool_swap_txs: {} observed_cached: {} blocks: {}..{}",
        cell_i64(&row, "dex_event_txs").unwrap_or_default(),
        cell_i64(&row, "observed_txs").unwrap_or_default(),
        cell_i64(&row, "min_block").unwrap_or_default(),
        cell_i64(&row, "max_block").unwrap_or_default(),
    );
    println!();
    Ok(())
}

async fn hydrate_missing_transactions(
    pool: &PgPool,
    provider: &ChainProvider,
    cli: &Cli,
) -> Result<()> {
    let hashes: Vec<String> = sqlx::query_scalar(
        r#"
        SELECT e.tx_hash
        FROM dex_events e
        LEFT JOIN observed_transactions ot ON lower(ot.tx_hash) = lower(e.tx_hash)
        WHERE e.created_at >= NOW() - ($1::text || ' days')::interval
          AND e.event_type = 'Swap'
          AND ot.tx_hash IS NULL
        GROUP BY e.tx_hash
        ORDER BY MAX(e.block_number) DESC
        LIMIT $2
        "#,
    )
    .bind(cli.days)
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
    print_address_hits(pool, &address, cli.days).await?;
    print_address_gas_ranks(pool, &address, cli.days).await?;
    print_watched_pool_gas_summary(pool, cli.days).await?;
    Ok(())
}

async fn print_address_hits(pool: &PgPool, address: &str, days: i64) -> Result<()> {
    let rows: Vec<HitRow> = sqlx::query_as(
        r#"
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
        JOIN dex_events e ON lower(e.tx_hash) = lower(ot.tx_hash)
        WHERE e.created_at >= NOW() - ($2::text || ' days')::interval
          AND (lower(ot.from_address) = lower($1) OR lower(ot.to_address) = lower($1))
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
        println!("no watched-pool txs matched this address as tx.from or tx.to");
        println!("if this is a collection address, add an address-index source or seed executor tx hashes");
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
        WITH hits AS (
            SELECT DISTINCT ot.*
            FROM observed_transactions ot
            JOIN dex_events e ON lower(e.tx_hash) = lower(ot.tx_hash)
            WHERE e.created_at >= NOW() - ($2::text || ' days')::interval
              AND (lower(ot.from_address) = lower($1) OR lower(ot.to_address) = lower($1))
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
