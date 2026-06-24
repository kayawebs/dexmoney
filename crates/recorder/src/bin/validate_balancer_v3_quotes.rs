use std::{collections::HashSet, env, str::FromStr};

use alloy_primitives::{Address, U256};
use anyhow::{anyhow, Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::{
    config::Settings,
    types::{DexKind, PoolVariant},
};
use base_arb_storage::postgres::{ensure_registry_schema, PostgresStore};
use sqlx::Row;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
struct Args {
    apply: bool,
    limit: i64,
    amount_denom: u64,
    pools: HashSet<Address>,
}

#[derive(Debug, Clone)]
struct BalancerPair {
    chain_id: u64,
    pool: Address,
    token0: Address,
    token1: Address,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = parse_args()?;
    let settings = Settings::load()?;
    let router = settings
        .balancer_v3_router
        .context("BALANCER_V3_ROUTER is required")?;
    let sender = settings
        .executor_contract_multihop
        .or(settings.executor_contract_2hop)
        .or(settings.executor_contract)
        .unwrap_or(Address::ZERO);
    let store = PostgresStore::connect(&settings.postgres_url).await?;
    ensure_registry_schema(&store.pool).await?;
    let provider = ChainProvider::from_settings(&settings);
    let pairs = load_balancer_pairs(&store, &settings, &args).await?;

    println!("== Balancer V3 Quote Validation ==");
    println!(
        "mode={} pools={} limit={} amount_denom={} router={router:#x} sender={sender:#x}",
        if args.apply { "apply" } else { "dry-run" },
        pairs.len(),
        args.limit,
        args.amount_denom,
    );

    let mut checked = 0usize;
    let mut ready = 0usize;
    let mut zero_output = 0usize;
    let mut failed = 0usize;

    for pair in pairs {
        for (token_in, token_out) in [(pair.token0, pair.token1), (pair.token1, pair.token0)] {
            checked += 1;
            let amount_in = match validation_amount(&provider, token_in, args.amount_denom).await {
                Ok(amount) => amount,
                Err(err) => {
                    failed += 1;
                    let error = format!("token decimals failed: {err:#}");
                    println!(
                        "failed pool={:#x} token_in={token_in:#x} token_out={token_out:#x} error={error}",
                        pair.pool
                    );
                    if args.apply {
                        store
                            .upsert_pool_quote_coverage(
                                pair.chain_id,
                                pair.pool,
                                token_in,
                                token_out,
                                Some(DexKind::Balancer),
                                Some(PoolVariant::BalancerV3),
                                "query_failed",
                                None,
                                None,
                                "balancer_v3_quote_validation",
                                Some(&error),
                            )
                            .await?;
                    }
                    continue;
                }
            };
            let quote = provider
                .query_balancer_v3_swap_exact_in(
                    router, pair.pool, token_in, token_out, amount_in, sender,
                )
                .await;
            match quote {
                Ok(amount_out) if amount_out > U256::ZERO => {
                    ready += 1;
                    println!(
                        "ready pool={:#x} token_in={token_in:#x} token_out={token_out:#x} amount_in={} amount_out={}",
                        pair.pool, amount_in, amount_out
                    );
                    if args.apply {
                        store
                            .upsert_pool_quote_coverage(
                                pair.chain_id,
                                pair.pool,
                                token_in,
                                token_out,
                                Some(DexKind::Balancer),
                                Some(PoolVariant::BalancerV3),
                                "ready",
                                Some(amount_in),
                                Some(amount_out),
                                "balancer_v3_quote_validation",
                                None,
                            )
                            .await?;
                    }
                }
                Ok(amount_out) => {
                    zero_output += 1;
                    println!(
                        "zero_output pool={:#x} token_in={token_in:#x} token_out={token_out:#x} amount_in={} amount_out={}",
                        pair.pool, amount_in, amount_out
                    );
                    if args.apply {
                        store
                            .upsert_pool_quote_coverage(
                                pair.chain_id,
                                pair.pool,
                                token_in,
                                token_out,
                                Some(DexKind::Balancer),
                                Some(PoolVariant::BalancerV3),
                                "zero_output",
                                Some(amount_in),
                                Some(amount_out),
                                "balancer_v3_quote_validation",
                                None,
                            )
                            .await?;
                    }
                }
                Err(err) => {
                    failed += 1;
                    let error = err.to_string();
                    println!(
                        "failed pool={:#x} token_in={token_in:#x} token_out={token_out:#x} amount_in={} error={error}",
                        pair.pool, amount_in
                    );
                    if args.apply {
                        store
                            .upsert_pool_quote_coverage(
                                pair.chain_id,
                                pair.pool,
                                token_in,
                                token_out,
                                Some(DexKind::Balancer),
                                Some(PoolVariant::BalancerV3),
                                "query_failed",
                                Some(amount_in),
                                None,
                                "balancer_v3_quote_validation",
                                Some(&error),
                            )
                            .await?;
                    }
                }
            }
        }
    }

    println!("checked={checked} ready={ready} zero_output={zero_output} failed={failed}");
    Ok(())
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        apply: false,
        limit: 100,
        amount_denom: 100,
        pools: HashSet::new(),
    };
    let mut iter = env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--apply" => args.apply = true,
            "--limit" => {
                args.limit = iter
                    .next()
                    .ok_or_else(|| anyhow!("--limit requires a value"))?
                    .parse()
                    .context("invalid --limit")?;
            }
            "--amount-denom" => {
                args.amount_denom = iter
                    .next()
                    .ok_or_else(|| anyhow!("--amount-denom requires a value"))?
                    .parse()
                    .context("invalid --amount-denom")?;
            }
            "--pool" => {
                let pool = iter
                    .next()
                    .ok_or_else(|| anyhow!("--pool requires an address"))?
                    .parse::<Address>()
                    .context("invalid --pool address")?;
                args.pools.insert(pool);
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            _ => anyhow::bail!("unknown argument: {arg}"),
        }
    }
    if args.limit <= 0 {
        anyhow::bail!("--limit must be positive");
    }
    if args.amount_denom == 0 {
        anyhow::bail!("--amount-denom must be positive");
    }
    Ok(args)
}

fn print_usage() {
    eprintln!(
        "Usage: validate_balancer_v3_quotes [--apply] [--limit 100] [--amount-denom 100] [--pool 0x...]"
    );
}

async fn load_balancer_pairs(
    store: &PostgresStore,
    settings: &Settings,
    args: &Args,
) -> Result<Vec<BalancerPair>> {
    let pool_filter = args
        .pools
        .iter()
        .map(|pool| format!("{pool:#x}"))
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (lower(p.pool_address), lower(p.token0), lower(p.token1))
          p.chain_id,
          p.pool_address,
          p.token0,
          p.token1
        FROM pools p
        WHERE p.chain_id = $1
          AND p.enabled
          AND p.dex = 'Balancer'
          AND p.variant = 'BalancerV3'
          AND p.token0 IS NOT NULL
          AND p.token1 IS NOT NULL
          AND (
            cardinality($3::TEXT[]) = 0
            OR EXISTS (
              SELECT 1
              FROM unnest($3::TEXT[]) AS filter(pool_address)
              WHERE lower(filter.pool_address) = lower(p.pool_address)
            )
          )
        ORDER BY lower(p.pool_address), lower(p.token0), lower(p.token1), p.updated_at DESC
        LIMIT $2
        "#,
    )
    .bind(i64::try_from(settings.chain_id)?)
    .bind(args.limit)
    .bind(&pool_filter)
    .fetch_all(&store.pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let chain_id = row.try_get::<i64, _>("chain_id")?;
            let pool = row.try_get::<String, _>("pool_address")?;
            let token0 = row.try_get::<String, _>("token0")?;
            let token1 = row.try_get::<String, _>("token1")?;
            Ok(BalancerPair {
                chain_id: u64::try_from(chain_id)?,
                pool: Address::from_str(&pool).context("invalid pool address")?,
                token0: Address::from_str(&token0).context("invalid token0 address")?,
                token1: Address::from_str(&token1).context("invalid token1 address")?,
            })
        })
        .collect()
}

async fn validation_amount(
    provider: &ChainProvider,
    token: Address,
    amount_denom: u64,
) -> Result<U256> {
    let decimals = provider.fetch_token_decimals(token).await?;
    let unit = pow10(decimals);
    Ok((unit / U256::from(amount_denom)).max(U256::from(1u64)))
}

fn pow10(decimals: u8) -> U256 {
    let mut out = U256::from(1u64);
    for _ in 0..decimals {
        out *= U256::from(10u64);
    }
    out
}
