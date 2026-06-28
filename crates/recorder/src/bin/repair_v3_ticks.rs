use std::{collections::HashSet, env, time::Duration};

use alloy_primitives::{Address, U256};
use anyhow::{anyhow, Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::types::{DexKind, PoolId, PoolState, PoolVariant};
use base_arb_storage::postgres::PostgresStore;
use base_arb_storage::{redis::RedisStore, TickChangeStore, TickRepairStore, TickStateStore};
use chrono::{DateTime, Utc};
use sqlx::Row;

#[derive(Debug, Clone)]
struct Args {
    apply: bool,
    force: bool,
    limit: i64,
    max_age_hours: i64,
    word_radius: i32,
    queued_word_radius: i32,
    gaps_only: bool,
    pools: HashSet<Address>,
    loop_enabled: bool,
    interval_secs: u64,
}

#[derive(Debug, Clone)]
struct RepairPool {
    state: PoolState,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();
    let args = parse_args()?;
    let settings = Settings::load()?;
    let provider = ChainProvider::from_settings(&settings);
    let postgres = PostgresStore::connect(&settings.postgres_url).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;

    if args.loop_enabled {
        println!(
            "== V3-style Tick Repair Daemon == mode={} interval_secs={} limit={} max_age_hours={} word_radius={} queued_word_radius={} force={} gaps_only={}",
            if args.apply { "apply" } else { "dry-run" },
            args.interval_secs,
            args.limit,
            args.max_age_hours,
            args.word_radius,
            args.queued_word_radius,
            args.force,
            args.gaps_only,
        );
        let mut pass = 0u64;
        loop {
            pass = pass.saturating_add(1);
            if let Err(err) = run_repair_once(&provider, &postgres, &redis, &args, Some(pass)).await
            {
                eprintln!("repair pass failed pass={pass} error={err:#}");
            }
            tokio::time::sleep(Duration::from_secs(args.interval_secs)).await;
        }
    }

    run_repair_once(&provider, &postgres, &redis, &args, None).await
}

async fn run_repair_once(
    provider: &ChainProvider,
    postgres: &PostgresStore,
    redis: &RedisStore,
    args: &Args,
    pass: Option<u64>,
) -> Result<()> {
    let queued_repair_pools = redis.drain_tick_repair_pools(args.limit as usize).await?;
    let queued_repair_set = queued_repair_pools.iter().copied().collect::<HashSet<_>>();
    let pools = load_repair_pools(postgres, args, &queued_repair_pools).await?;
    let queued_selected = pools
        .iter()
        .filter(|pool| queued_repair_set.contains(&pool.state.pool_id.address))
        .count();
    println!("== V3-style Tick Repair ==");
    println!(
        "mode={} pass={} pools={} queued={} queued_selected={} queued_unselected={} limit={} max_age_hours={} word_radius={} queued_word_radius={} force={} gaps_only={}",
        if args.apply { "apply" } else { "dry-run" },
        pass.map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        pools.len(),
        queued_repair_pools.len(),
        queued_selected,
        queued_repair_pools.len().saturating_sub(queued_selected),
        args.limit,
        args.max_age_hours,
        args.word_radius,
        args.queued_word_radius,
        args.force,
        args.gaps_only,
    );

    let mut checked = 0usize;
    let mut persisted_existing = 0usize;
    let mut repaired = 0usize;
    let mut zero_ticks = 0usize;
    let mut failed = 0usize;
    let mut ticks_written = 0usize;

    for pool in pools {
        checked += 1;
        let address = pool.state.pool_id.address;
        let force_refresh = args.force || queued_repair_set.contains(&address);
        let repair_word_radius = if queued_repair_set.contains(&address) {
            args.queued_word_radius
        } else {
            args.word_radius
        };
        let existing_ticks = redis.get_pool_ticks(address).await?;
        if !force_refresh && !existing_ticks.is_empty() {
            persisted_existing += 1;
            ticks_written += existing_ticks.len();
            if args.apply {
                postgres
                    .replace_pool_ticks_current(
                        pool.state.pool_id.chain_id,
                        address,
                        &existing_ticks,
                        "v3_tick_repair_redis_cache",
                    )
                    .await?;
                postgres
                    .upsert_pool_tick_coverage(
                        pool.state.pool_id.chain_id,
                        address,
                        Some(pool.state.dex),
                        Some(pool.state.variant),
                        protocol_for_pool(&pool.state),
                        "ready",
                        existing_ticks.len(),
                        Some(pool.state.block_number),
                        "v3_tick_repair_redis_cache",
                        Some(args.word_radius),
                        None,
                        None,
                    )
                    .await?;
            }
            println!(
                "persist existing redis ticks pool={address:#x} variant={:?} ticks={}",
                pool.state.variant,
                existing_ticks.len()
            );
            continue;
        }

        match provider
            .fetch_initialized_ticks_around_state(&pool.state, repair_word_radius)
            .await
        {
            Ok(ticks) => {
                if ticks.is_empty() {
                    zero_ticks += 1;
                    if args.apply {
                        postgres
                            .replace_pool_ticks_current(
                                pool.state.pool_id.chain_id,
                                address,
                                &[],
                                "v3_tick_repair",
                            )
                            .await?;
                        postgres
                            .upsert_pool_tick_coverage(
                                pool.state.pool_id.chain_id,
                                address,
                                Some(pool.state.dex),
                                Some(pool.state.variant),
                                protocol_for_pool(&pool.state),
                                "zero_ticks",
                                0,
                                Some(pool.state.block_number),
                                "v3_tick_repair",
                                Some(repair_word_radius),
                                None,
                                None,
                            )
                            .await?;
                    }
                    println!(
                        "zero ticks pool={address:#x} variant={:?} block={} tick={:?}",
                        pool.state.variant, pool.state.block_number, pool.state.tick
                    );
                    continue;
                }
                ticks_written += ticks.len();
                if args.apply {
                    postgres
                        .replace_pool_ticks_current(
                            pool.state.pool_id.chain_id,
                            address,
                            &ticks,
                            "v3_tick_repair",
                        )
                        .await?;
                    postgres
                        .upsert_pool_tick_coverage(
                            pool.state.pool_id.chain_id,
                            address,
                            Some(pool.state.dex),
                            Some(pool.state.variant),
                            protocol_for_pool(&pool.state),
                            "ready",
                            ticks.len(),
                            Some(pool.state.block_number),
                            "v3_tick_repair",
                            Some(repair_word_radius),
                            None,
                            None,
                        )
                        .await?;
                    redis.replace_pool_ticks(address, ticks.clone()).await?;
                    redis.mark_tick_changed_pools(vec![address]).await?;
                }
                repaired += 1;
                println!(
                    "repaired pool={address:#x} variant={:?} ticks={} block={} tick={:?}",
                    pool.state.variant,
                    ticks.len(),
                    pool.state.block_number,
                    pool.state.tick
                );
            }
            Err(err) => {
                failed += 1;
                if args.apply {
                    postgres
                        .upsert_pool_tick_coverage(
                            pool.state.pool_id.chain_id,
                            address,
                            Some(pool.state.dex),
                            Some(pool.state.variant),
                            protocol_for_pool(&pool.state),
                            "refresh_failed",
                            0,
                            Some(pool.state.block_number),
                            "v3_tick_repair",
                            Some(repair_word_radius),
                            None,
                            None,
                        )
                        .await?;
                }
                println!(
                    "failed pool={address:#x} variant={:?} block={} error={err:#}",
                    pool.state.variant, pool.state.block_number
                );
            }
        }
    }

    println!(
        "checked={checked} repaired={repaired} persisted_existing={persisted_existing} zero_ticks={zero_ticks} ticks_written={ticks_written} failed={failed}"
    );
    Ok(())
}

fn protocol_for_pool(state: &PoolState) -> Option<&'static str> {
    match (state.dex, state.variant) {
        (DexKind::Aerodrome, PoolVariant::AerodromeSlipstream) => Some("aerodrome-slipstream"),
        (DexKind::UniswapV3, PoolVariant::UniswapV3) => Some("uniswap-v3"),
        (DexKind::PancakeSwap, PoolVariant::PancakeV3) => Some("pancake-v3"),
        _ => None,
    }
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        apply: false,
        force: false,
        limit: 100,
        max_age_hours: 24,
        word_radius: 8,
        queued_word_radius: 256,
        gaps_only: false,
        pools: HashSet::new(),
        loop_enabled: false,
        interval_secs: 30,
    };
    let mut iter = env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--apply" => args.apply = true,
            "--force" => args.force = true,
            "--gaps-only" => args.gaps_only = true,
            "--loop" => args.loop_enabled = true,
            "--limit" => {
                args.limit = iter
                    .next()
                    .ok_or_else(|| anyhow!("--limit requires a value"))?
                    .parse()
                    .context("invalid --limit")?;
            }
            "--max-age-hours" => {
                args.max_age_hours = iter
                    .next()
                    .ok_or_else(|| anyhow!("--max-age-hours requires a value"))?
                    .parse()
                    .context("invalid --max-age-hours")?;
            }
            "--word-radius" => {
                args.word_radius = iter
                    .next()
                    .ok_or_else(|| anyhow!("--word-radius requires a value"))?
                    .parse()
                    .context("invalid --word-radius")?;
            }
            "--queued-word-radius" => {
                args.queued_word_radius = iter
                    .next()
                    .ok_or_else(|| anyhow!("--queued-word-radius requires a value"))?
                    .parse()
                    .context("invalid --queued-word-radius")?;
            }
            "--interval-secs" => {
                args.interval_secs = iter
                    .next()
                    .ok_or_else(|| anyhow!("--interval-secs requires a value"))?
                    .parse()
                    .context("invalid --interval-secs")?;
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
    if args.max_age_hours <= 0 {
        anyhow::bail!("--max-age-hours must be positive");
    }
    if args.word_radius < 0 {
        anyhow::bail!("--word-radius must be non-negative");
    }
    if args.queued_word_radius < args.word_radius {
        anyhow::bail!("--queued-word-radius must be greater than or equal to --word-radius");
    }
    if args.loop_enabled && args.interval_secs == 0 {
        anyhow::bail!("--interval-secs must be positive");
    }
    Ok(args)
}

fn print_usage() {
    eprintln!(
        "Usage: repair_v3_ticks [--apply] [--force] [--gaps-only] [--loop] [--interval-secs 30] [--limit 100] [--max-age-hours 24] [--word-radius 8] [--queued-word-radius 256] [--pool 0x...]"
    );
}

async fn load_repair_pools(
    postgres: &PostgresStore,
    args: &Args,
    queued_repair_pools: &[Address],
) -> Result<Vec<RepairPool>> {
    let mut selected_pools = args.pools.clone();
    selected_pools.extend(queued_repair_pools.iter().copied());
    let pool_filter = selected_pools
        .iter()
        .map(|pool| format!("{pool:#x}"))
        .collect::<Vec<_>>();

    let rows = if !pool_filter.is_empty() {
        sqlx::query(
            r#"
            WITH wanted AS (
              SELECT lower(pool_address) AS pool_address
              FROM unnest($2::TEXT[]) AS filter(pool_address)
            )
            SELECT
              p.chain_id,
              p.pool_address,
              p.dex,
              p.variant,
              p.factory_address,
              p.token0,
              p.token1,
              p.fee_bps,
              p.tick_spacing,
              p.stable,
              ls.reserve0,
              ls.reserve1,
              ls.sqrt_price_x96,
              ls.liquidity,
              ls.tick,
              ls.block_number,
              ls.updated_at
            FROM wanted w
            JOIN pools p
              ON lower(p.pool_address) = w.pool_address
            JOIN LATERAL (
              SELECT
                reserve0,
                reserve1,
                sqrt_price_x96,
                liquidity,
                tick,
                block_number,
                updated_at
              FROM pool_states ps
              WHERE lower(ps.pool_address) = lower(p.pool_address)
              ORDER BY ps.block_number DESC, ps.updated_at DESC
              LIMIT 1
            ) ls ON TRUE
            WHERE p.enabled
              AND p.variant IN ('AerodromeSlipstream', 'UniswapV3', 'PancakeV3')
              AND ls.sqrt_price_x96 IS NOT NULL
              AND ls.liquidity IS NOT NULL
              AND ls.tick IS NOT NULL
            ORDER BY ls.updated_at DESC
            LIMIT $1
            "#,
        )
        .bind(args.limit)
        .bind(&pool_filter)
        .fetch_all(&postgres.pool)
        .await?
    } else if args.gaps_only {
        sqlx::query(
            r#"
            WITH candidates AS (
              SELECT
                tc.chain_id,
                tc.pool_address,
                tc.status,
                tc.updated_at
              FROM pool_tick_coverage tc
              WHERE tc.variant IN ('AerodromeSlipstream', 'UniswapV3', 'PancakeV3')
                AND (
                  tc.status IN ('refresh_failed', 'zero_ticks')
                  OR (tc.status = 'ready' AND tc.tick_count = 0)
                )
              ORDER BY
                CASE
                  WHEN tc.status = 'refresh_failed' THEN 0
                  WHEN tc.status = 'zero_ticks' THEN 1
                  WHEN tc.status = 'ready' AND tc.tick_count = 0 THEN 2
                  ELSE 3
                END,
                tc.updated_at DESC
              LIMIT $1
            )
            SELECT
              p.chain_id,
              p.pool_address,
              p.dex,
              p.variant,
              p.factory_address,
              p.token0,
              p.token1,
              p.fee_bps,
              p.tick_spacing,
              p.stable,
              ls.reserve0,
              ls.reserve1,
              ls.sqrt_price_x96,
              ls.liquidity,
              ls.tick,
              ls.block_number,
              ls.updated_at
            FROM candidates c
            JOIN pools p
              ON p.chain_id = c.chain_id
             AND lower(p.pool_address) = lower(c.pool_address)
            JOIN LATERAL (
              SELECT
                reserve0,
                reserve1,
                sqrt_price_x96,
                liquidity,
                tick,
                block_number,
                updated_at
              FROM pool_states ps
              WHERE lower(ps.pool_address) = lower(p.pool_address)
                AND ps.updated_at >= NOW() - ($2::BIGINT * INTERVAL '1 hour')
              ORDER BY ps.block_number DESC, ps.updated_at DESC
              LIMIT 1
            ) ls ON TRUE
            WHERE p.enabled
              AND p.variant IN ('AerodromeSlipstream', 'UniswapV3', 'PancakeV3')
              AND ls.sqrt_price_x96 IS NOT NULL
              AND ls.liquidity IS NOT NULL
              AND ls.tick IS NOT NULL
            ORDER BY c.updated_at DESC
            "#,
        )
        .bind(args.limit)
        .bind(args.max_age_hours)
        .fetch_all(&postgres.pool)
        .await?
    } else {
        sqlx::query(
            r#"
        SELECT
          p.chain_id,
          p.pool_address,
          p.dex,
          p.variant,
          p.factory_address,
          p.token0,
          p.token1,
          p.fee_bps,
          p.tick_spacing,
          p.stable,
          ls.reserve0,
          ls.reserve1,
          ls.sqrt_price_x96,
          ls.liquidity,
          ls.tick,
          ls.block_number,
          ls.updated_at
        FROM pools p
        JOIN LATERAL (
          SELECT
            reserve0,
            reserve1,
            sqrt_price_x96,
            liquidity,
            tick,
            block_number,
            updated_at
          FROM pool_states ps
          WHERE lower(ps.pool_address) = lower(p.pool_address)
            AND ps.updated_at >= NOW() - ($2::BIGINT * INTERVAL '1 hour')
          ORDER BY ps.block_number DESC, ps.updated_at DESC
          LIMIT 1
        ) ls ON TRUE
        LEFT JOIN pool_tick_coverage tc
          ON tc.chain_id = p.chain_id
         AND lower(tc.pool_address) = lower(p.pool_address)
        LEFT JOIN LATERAL (
          SELECT EXISTS (
            SELECT 1
            FROM pool_ticks_current pt
            WHERE pt.chain_id = p.chain_id
              AND lower(pt.pool_address) = lower(p.pool_address)
            LIMIT 1
          ) AS has_ticks
        ) tr ON TRUE
        WHERE p.enabled
          AND p.variant IN ('AerodromeSlipstream', 'UniswapV3', 'PancakeV3')
          AND ls.sqrt_price_x96 IS NOT NULL
          AND ls.liquidity IS NOT NULL
          AND ls.tick IS NOT NULL
          AND (
            cardinality($3::TEXT[]) = 0
            OR EXISTS (
              SELECT 1
              FROM unnest($3::TEXT[]) AS filter(pool_address)
              WHERE lower(filter.pool_address) = lower(p.pool_address)
            )
          )
          AND (
            NOT $4::BOOLEAN
            OR tc.status IS NULL
            OR tc.status = 'refresh_failed'
            OR (tc.status = 'ready' AND NOT COALESCE(tr.has_ticks, FALSE))
          )
        ORDER BY
          CASE
            WHEN tc.status IS NULL THEN 0
            WHEN tc.status = 'refresh_failed' THEN 1
            WHEN tc.status = 'ready' AND NOT COALESCE(tr.has_ticks, FALSE) THEN 2
            ELSE 3
          END,
          ls.updated_at DESC
        LIMIT $1
        "#,
        )
        .bind(args.limit)
        .bind(args.max_age_hours)
        .bind(&pool_filter)
        .bind(args.gaps_only)
        .fetch_all(&postgres.pool)
        .await?
    };

    rows.into_iter()
        .map(|row| {
            let chain_id: i64 = row.try_get("chain_id")?;
            let pool_address: String = row.try_get("pool_address")?;
            let dex: String = row.try_get("dex")?;
            let variant: String = row.try_get("variant")?;
            let factory_address: Option<String> = row.try_get("factory_address")?;
            let token0: String = row.try_get("token0")?;
            let token1: String = row.try_get("token1")?;
            let fee_bps: Option<i64> = row.try_get("fee_bps")?;
            let tick_spacing: Option<i64> = row.try_get("tick_spacing")?;
            let stable: Option<bool> = row.try_get("stable")?;
            let reserve0: Option<String> = row.try_get("reserve0")?;
            let reserve1: Option<String> = row.try_get("reserve1")?;
            let sqrt_price_x96: Option<String> = row.try_get("sqrt_price_x96")?;
            let liquidity: Option<String> = row.try_get("liquidity")?;
            let tick: Option<i64> = row.try_get("tick")?;
            let block_number: i64 = row.try_get("block_number")?;
            let updated_at: DateTime<Utc> = row.try_get("updated_at")?;

            let state = PoolState {
                pool_id: PoolId {
                    chain_id: u64::try_from(chain_id)?,
                    address: parse_address(&pool_address)?,
                },
                dex: parse_dex(&dex)?,
                variant: parse_variant(&variant)?,
                factory_address: parse_optional_address(factory_address.as_deref())?,
                token0: parse_address(&token0)?,
                token1: parse_address(&token1)?,
                token0_decimals: None,
                token1_decimals: None,
                fee_bps: u32::try_from(fee_bps.unwrap_or_default())?,
                fee_pips: None,
                pool_key_fee_pips: None,
                hooks_address: None,
                stable,
                reserve0: parse_optional_u256(reserve0.as_deref())?,
                reserve1: parse_optional_u256(reserve1.as_deref())?,
                balancer_model: None,
                balancer_weight0: None,
                balancer_weight1: None,
                balancer_scaling_factor0: None,
                balancer_scaling_factor1: None,
                balancer_token_rate0: None,
                balancer_token_rate1: None,
                balancer_swap_fee_percentage: None,
                sqrt_price_x96: parse_optional_u256(sqrt_price_x96.as_deref())?,
                liquidity: parse_optional_u256(liquidity.as_deref())?,
                tick: tick.map(i32::try_from).transpose()?,
                tick_spacing: tick_spacing.map(i32::try_from).transpose()?,
                block_number: u64::try_from(block_number)?,
                valid_through_block: u64::try_from(block_number)?,
                updated_at,
            };
            Ok(RepairPool { state })
        })
        .collect()
}

fn parse_address(value: &str) -> Result<Address> {
    value
        .parse::<Address>()
        .with_context(|| format!("invalid address {value}"))
}

fn parse_optional_address(value: Option<&str>) -> Result<Option<Address>> {
    value.map(parse_address).transpose()
}

fn parse_optional_u256(value: Option<&str>) -> Result<Option<U256>> {
    value
        .filter(|value| !value.is_empty())
        .map(|value| {
            value
                .parse::<U256>()
                .with_context(|| format!("invalid U256 {value}"))
        })
        .transpose()
}

fn parse_dex(value: &str) -> Result<DexKind> {
    match value {
        "Aerodrome" => Ok(DexKind::Aerodrome),
        "UniswapV3" => Ok(DexKind::UniswapV3),
        "PancakeSwap" => Ok(DexKind::PancakeSwap),
        other => anyhow::bail!("unsupported dex {other}"),
    }
}

fn parse_variant(value: &str) -> Result<PoolVariant> {
    match value {
        "AerodromeSlipstream" => Ok(PoolVariant::AerodromeSlipstream),
        "UniswapV3" => Ok(PoolVariant::UniswapV3),
        "PancakeV3" => Ok(PoolVariant::PancakeV3),
        other => anyhow::bail!("unsupported variant {other}"),
    }
}
