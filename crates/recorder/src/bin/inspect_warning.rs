use std::env;

use anyhow::{bail, Context, Result};
use base_arb_common::config::Settings;
use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
struct Cli {
    pool: Option<String>,
    block: Option<i64>,
    window: i64,
}

#[derive(Debug, FromRow)]
struct WarningRow {
    created_at: DateTime<Utc>,
    pool_address: String,
    block_number: i64,
    drift_bps: i64,
    local_tick: Option<i64>,
    onchain_tick: Option<i64>,
    local_sqrt: Option<String>,
    onchain_sqrt: Option<String>,
    local_liquidity: Option<String>,
    onchain_liquidity: Option<String>,
}

#[derive(Debug, FromRow)]
struct LiquidityUpdateRow {
    created_at: DateTime<Utc>,
    block_number: i64,
    tx_hash: String,
    log_index: i64,
    event_type: String,
    current_tick: i64,
    tick_lower: i64,
    tick_upper: i64,
    amount: String,
    previous_liquidity: String,
    next_liquidity: String,
}

#[derive(Debug, FromRow)]
struct EventRow {
    created_at: DateTime<Utc>,
    block_number: i64,
    log_index: i64,
    event_type: String,
    tx_hash: String,
    topic0: Option<String>,
    topic1: Option<String>,
    topic2: Option<String>,
    topic3: Option<String>,
    data: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = parse_args(env::args().skip(1))?;
    let settings = Settings::load()?;
    let pool = PgPool::connect(&settings.postgres_url).await?;

    let warning = load_warning(&pool, &cli).await?;
    print_warning(&warning);

    let updates = load_liquidity_updates(
        &pool,
        &warning.pool_address,
        warning.block_number,
        cli.window,
    )
    .await?;
    print_liquidity_updates(&updates);

    let events = load_events(
        &pool,
        &warning.pool_address,
        warning.block_number,
        cli.window,
    )
    .await?;
    print_events(&events);

    Ok(())
}

fn parse_args<I>(args: I) -> Result<Cli>
where
    I: IntoIterator<Item = String>,
{
    let mut pool = None;
    let mut block = None;
    let mut window = 50_i64;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--pool" => {
                pool = Some(iter.next().context("missing value for --pool")?);
            }
            "--block" => {
                block = Some(
                    iter.next()
                        .context("missing value for --block")?
                        .parse()
                        .context("invalid --block")?,
                );
            }
            "--window" => {
                window = iter
                    .next()
                    .context("missing value for --window")?
                    .parse()
                    .context("invalid --window")?;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Cli {
        pool,
        block,
        window,
    })
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin inspect-warning -- [--pool <address>] [--block <number>] [--window <blocks>]"
    );
}

async fn load_warning(pool: &PgPool, cli: &Cli) -> Result<WarningRow> {
    match (&cli.pool, cli.block) {
        (Some(pool_address), Some(block_number)) => sqlx::query_as::<_, WarningRow>(
            r#"
                SELECT
                  created_at,
                  pool_address,
                  block_number,
                  drift_bps,
                  local_state_json->>'tick' AS local_tick,
                  onchain_state_json->>'tick' AS onchain_tick,
                  local_state_json->>'sqrt_price_x96' AS local_sqrt,
                  onchain_state_json->>'sqrt_price_x96' AS onchain_sqrt,
                  local_state_json->>'liquidity' AS local_liquidity,
                  onchain_state_json->>'liquidity' AS onchain_liquidity
                FROM pool_state_warnings
                WHERE lower(pool_address) = lower($1)
                  AND block_number = $2
                ORDER BY created_at DESC
                LIMIT 1
                "#,
        )
        .bind(pool_address)
        .bind(block_number)
        .fetch_one(pool)
        .await
        .with_context(|| format!("warning not found for pool={pool_address} block={block_number}")),
        (Some(pool_address), None) => sqlx::query_as::<_, WarningRow>(
            r#"
                SELECT
                  created_at,
                  pool_address,
                  block_number,
                  drift_bps,
                  local_state_json->>'tick' AS local_tick,
                  onchain_state_json->>'tick' AS onchain_tick,
                  local_state_json->>'sqrt_price_x96' AS local_sqrt,
                  onchain_state_json->>'sqrt_price_x96' AS onchain_sqrt,
                  local_state_json->>'liquidity' AS local_liquidity,
                  onchain_state_json->>'liquidity' AS onchain_liquidity
                FROM pool_state_warnings
                WHERE lower(pool_address) = lower($1)
                ORDER BY created_at DESC
                LIMIT 1
                "#,
        )
        .bind(pool_address)
        .fetch_one(pool)
        .await
        .with_context(|| format!("no warnings found for pool={pool_address}")),
        (None, None) => sqlx::query_as::<_, WarningRow>(
            r#"
                SELECT
                  created_at,
                  pool_address,
                  block_number,
                  drift_bps,
                  local_state_json->>'tick' AS local_tick,
                  onchain_state_json->>'tick' AS onchain_tick,
                  local_state_json->>'sqrt_price_x96' AS local_sqrt,
                  onchain_state_json->>'sqrt_price_x96' AS onchain_sqrt,
                  local_state_json->>'liquidity' AS local_liquidity,
                  onchain_state_json->>'liquidity' AS onchain_liquidity
                FROM pool_state_warnings
                ORDER BY created_at DESC
                LIMIT 1
                "#,
        )
        .fetch_one(pool)
        .await
        .context("no warnings found"),
        (None, Some(_)) => bail!("--block requires --pool"),
    }
}

async fn load_liquidity_updates(
    pool: &PgPool,
    pool_address: &str,
    block_number: i64,
    window: i64,
) -> Result<Vec<LiquidityUpdateRow>> {
    sqlx::query_as::<_, LiquidityUpdateRow>(
        r#"
        SELECT
          created_at,
          block_number,
          tx_hash,
          log_index,
          event_type,
          current_tick,
          tick_lower,
          tick_upper,
          amount,
          previous_liquidity,
          next_liquidity
        FROM v3_liquidity_updates
        WHERE lower(pool_address) = lower($1)
          AND block_number BETWEEN $2 AND $3
        ORDER BY block_number, log_index
        "#,
    )
    .bind(pool_address)
    .bind(block_number - window)
    .bind(block_number + 5)
    .fetch_all(pool)
    .await
    .context("failed to load v3_liquidity_updates")
}

async fn load_events(
    pool: &PgPool,
    pool_address: &str,
    block_number: i64,
    window: i64,
) -> Result<Vec<EventRow>> {
    sqlx::query_as::<_, EventRow>(
        r#"
        SELECT
          created_at,
          block_number,
          log_index,
          event_type,
          tx_hash,
          raw_data_json->'topics'->>0 AS topic0,
          raw_data_json->'topics'->>1 AS topic1,
          raw_data_json->'topics'->>2 AS topic2,
          raw_data_json->'topics'->>3 AS topic3,
          raw_data_json->>'data' AS data
        FROM dex_events
        WHERE lower(pool_address) = lower($1)
          AND block_number BETWEEN $2 AND $3
        ORDER BY block_number, log_index
        "#,
    )
    .bind(pool_address)
    .bind(block_number - window)
    .bind(block_number + 5)
    .fetch_all(pool)
    .await
    .context("failed to load dex_events")
}

fn print_warning(row: &WarningRow) {
    println!("== Warning ==");
    println!("created_at: {}", row.created_at);
    println!("pool_address: {}", row.pool_address);
    println!("block_number: {}", row.block_number);
    println!("drift_bps: {}", row.drift_bps);
    println!("local_tick: {:?}", row.local_tick);
    println!("onchain_tick: {:?}", row.onchain_tick);
    println!("local_sqrt: {:?}", row.local_sqrt);
    println!("onchain_sqrt: {:?}", row.onchain_sqrt);
    println!("local_liquidity: {:?}", row.local_liquidity);
    println!("onchain_liquidity: {:?}", row.onchain_liquidity);
    println!();
}

fn print_liquidity_updates(rows: &[LiquidityUpdateRow]) {
    println!("== V3 Liquidity Updates ==");
    if rows.is_empty() {
        println!("(none)");
        println!();
        return;
    }
    for row in rows {
        println!(
            "{} block={} log_index={} event={} tx={} tick={} range=[{}, {}) amount={} prev={} next={}",
            row.created_at,
            row.block_number,
            row.log_index,
            row.event_type,
            row.tx_hash,
            row.current_tick,
            row.tick_lower,
            row.tick_upper,
            row.amount,
            row.previous_liquidity,
            row.next_liquidity
        );
    }
    println!();
}

fn print_events(rows: &[EventRow]) {
    println!("== Raw Events ==");
    if rows.is_empty() {
        println!("(none)");
        return;
    }
    for row in rows {
        println!(
            "{} block={} log_index={} event={} tx={} topic0={:?} topic1={:?} topic2={:?} topic3={:?} data={:?}",
            row.created_at,
            row.block_number,
            row.log_index,
            row.event_type,
            row.tx_hash,
            row.topic0,
            row.topic1,
            row.topic2,
            row.topic3,
            row.data
        );
    }
}
