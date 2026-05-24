use std::{env, str::FromStr};

use alloy_primitives::{keccak256, Address, U256};
use anyhow::{bail, Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::{
    config::Settings,
    constants::{AERODROME_SLIPSTREAM_ROUTER, PANCAKE_V3_FACTORY, PANCAKE_V3_ROUTER},
    types::{ArbPath, DexKind, PoolRegistryEntry, PoolState, PoolVariant, SwapStep},
};
use base_arb_dex::{
    aerodrome::{AerodromeStableQuoter, AerodromeVolatileQuoter},
    quoter::DexQuoter,
    uniswap_v3::{quote_exact_in_with_ticks_diagnostics, UniswapV3CurrentTickQuoter},
};
use base_arb_storage::{redis::RedisStore, TickStateStore};
use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const DEFAULT_AERODROME_FACTORY: &str = "0x420DD381b31aEf6683db6B902084cB0FFECe40Da";
const DEFAULT_UNISWAP_V3_FACTORY: &str = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD";
const EXECUTOR_DEADLINE_SECS: i64 = 60;

#[derive(Debug, Clone)]
struct Cli {
    opportunity: Option<Uuid>,
    path: Option<String>,
    amount: Option<U256>,
    min_profit: Option<U256>,
    skip_executor_call: bool,
}

#[derive(Debug, FromRow)]
struct OpportunityRow {
    id: Uuid,
    created_at: DateTime<Utc>,
    block_number: i64,
    token_in: String,
    amount_in: String,
    expected_profit: String,
    min_profit: String,
    path_json: serde_json::Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = parse_args(env::args().skip(1))?;
    let settings = Settings::load()?;
    let pool = PgPool::connect(&settings.postgres_url).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;
    let provider = ChainProvider::from_settings(&settings);

    let row = load_opportunity(&pool, &cli).await?;
    let path: ArbPath = serde_json::from_value(row.path_json.clone())
        .with_context(|| format!("failed to decode path_json for opportunity {}", row.id))?;
    let amount_in = cli
        .amount
        .unwrap_or(parse_u256_decimal(&row.amount_in).context("invalid opportunity amount_in")?);
    let min_profit = cli
        .min_profit
        .unwrap_or(parse_u256_decimal(&row.min_profit).context("invalid opportunity min_profit")?);
    let token_in = Address::from_str(&row.token_in).context("invalid opportunity token_in")?;

    println!("== Route ==");
    println!("opportunity: {}", row.id);
    println!("created_at: {}", row.created_at);
    println!("block_number: {}", row.block_number);
    println!("path: {}", path.name);
    println!("token_in: {token_in:#x}");
    println!("amount_in: {amount_in}");
    println!("expected_profit: {}", row.expected_profit);
    println!("min_profit: {min_profit}");

    let block_number = provider.get_block_number().await?;
    let block_hash = provider.get_block_hash(block_number).await?;
    println!("\n== Latest Block ==");
    println!("block_number: {block_number}");
    println!("block_hash: {block_hash}");

    for (idx, step) in path.steps.iter().enumerate() {
        validate_step(
            idx + 1,
            step,
            &settings,
            &provider,
            &block_hash,
            block_number,
        )
        .await?;
    }
    let expected_profit =
        parse_u256_decimal(&row.expected_profit).context("invalid opportunity expected_profit")?;
    validate_step_quotes(
        &path,
        amount_in,
        expected_profit,
        &provider,
        &redis,
        &block_hash,
        block_number,
    )
    .await?;

    if !cli.skip_executor_call {
        validate_executor_call(&path, token_in, amount_in, min_profit, &settings, &provider)
            .await?;
    }

    Ok(())
}

async fn validate_step_quotes(
    path: &ArbPath,
    amount_in: U256,
    opportunity_expected_profit: U256,
    provider: &ChainProvider,
    tick_store: &RedisStore,
    block_hash: &str,
    block_number: u64,
) -> Result<()> {
    println!("\n== Step Quote Check ==");
    let mut local_amount = amount_in;
    let mut direct_amount = Some(amount_in);
    for (idx, step) in path.steps.iter().enumerate() {
        let step_no = idx + 1;
        let entry = PoolRegistryEntry {
            pool_address: step.pool,
            dex: step.dex,
            variant: step.variant.unwrap_or(default_variant(step.dex)),
            factory_address: step.factory_address,
            token0: step.token_in,
            token1: step.token_out,
            fee_bps: step.fee_bps.unwrap_or_default(),
            tick_spacing: step.tick_spacing,
            stable: Some(classic_stable_flag(step)),
            enabled: true,
        };
        let state = provider
            .fetch_pool_state_from_registry_at_block_hash(&entry, block_hash, block_number)
            .await
            .with_context(|| format!("failed to fetch state for step {step_no} quote check"))?;
        let local = quote_local_step(step, &state, local_amount, tick_store)
            .await
            .with_context(|| format!("failed local quote for step {step_no}"))?;

        if matches!(
            (step.dex, step.variant),
            (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None)
        ) {
            if let Some(amount) = direct_amount {
                let onchain =
                    pool_get_amount_out(provider, step.pool, amount, step.token_in).await?;
                println!(
                    "step {step_no}: amount_in={amount} local_amount_in={local_amount} local_quote={local} pool_getAmountOut={onchain} diff_bps={}",
                    diff_bps(local, onchain)
                );
                direct_amount = Some(onchain);
            } else {
                println!(
                    "step {step_no}: local_amount_in={local_amount} local_quote={local} pool_getAmountOut=skipped_after_v3"
                );
            }
        }
        local_amount = local;
    }
    let direct_profit = direct_amount.map(|amount| amount.saturating_sub(amount_in));
    println!(
        "final: opportunity_expected_profit={} latest_local_profit={} latest_pool_getAmountOut_profit={}",
        opportunity_expected_profit,
        local_amount.saturating_sub(amount_in),
        direct_profit
            .map(|profit| profit.to_string())
            .unwrap_or_else(|| "unavailable".into())
    );
    Ok(())
}

async fn quote_local_step(
    step: &SwapStep,
    state: &PoolState,
    amount_in: U256,
    tick_store: &RedisStore,
) -> Result<U256> {
    match state.variant {
        PoolVariant::AerodromeVolatile => {
            let quote = if state.stable.unwrap_or(false) {
                AerodromeStableQuoter
                    .quote_exact_in(state, step.token_in, amount_in)
                    .await?
            } else {
                AerodromeVolatileQuoter
                    .quote_exact_in(state, step.token_in, amount_in)
                    .await?
            };
            Ok(quote.amount_out)
        }
        PoolVariant::AerodromeSlipstream | PoolVariant::UniswapV3 | PoolVariant::PancakeV3 => {
            let ticks = tick_store.get_pool_ticks(state.pool_id.address).await?;
            if ticks.is_empty() {
                let quote = UniswapV3CurrentTickQuoter
                    .quote_exact_in(state, step.token_in, amount_in)
                    .await?;
                println!(
                    "v3_local_quote: pool={:#x} mode=current_tick_fallback fee_pips={:?} tick_count=0 amount_in={} amount_out={}",
                    step.pool, state.fee_pips, amount_in, quote.amount_out
                );
                Ok(quote.amount_out)
            } else {
                let (quote, diagnostics) =
                    quote_exact_in_with_ticks_diagnostics(state, &ticks, step.token_in, amount_in)?;
                println!(
                    "v3_local_quote: pool={:#x} mode=cross_tick fee_pips={:?} tick_count={} amount_in={} amount_out={} ticks_used={} crossed_ticks={} exhausted={}",
                    step.pool,
                    state.fee_pips,
                    ticks.len(),
                    amount_in,
                    quote.amount_out,
                    diagnostics.ticks_used,
                    diagnostics.crossed_ticks,
                    diagnostics.tick_range_exhausted
                );
                Ok(quote.amount_out)
            }
        }
    }
}

fn parse_args<I>(args: I) -> Result<Cli>
where
    I: IntoIterator<Item = String>,
{
    let mut opportunity = None;
    let mut path = None;
    let mut amount = None;
    let mut min_profit = None;
    let mut skip_executor_call = false;
    let mut iter = args.into_iter();

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--opportunity" => {
                let raw = iter.next().context("missing value for --opportunity")?;
                opportunity = Some(Uuid::parse_str(&raw).context("invalid --opportunity UUID")?);
            }
            "--path" => {
                path = Some(iter.next().context("missing value for --path")?);
            }
            "--amount" => {
                amount = Some(parse_u256_decimal(
                    &iter.next().context("missing value for --amount")?,
                )?);
            }
            "--min-profit" => {
                min_profit = Some(parse_u256_decimal(
                    &iter.next().context("missing value for --min-profit")?,
                )?);
            }
            "--skip-executor-call" => {
                skip_executor_call = true;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }

    Ok(Cli {
        opportunity,
        path,
        amount,
        min_profit,
        skip_executor_call,
    })
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin validate_route -- [--opportunity <uuid> | --path <path_name>] [--amount <raw>] [--min-profit <raw>] [--skip-executor-call]"
    );
}

async fn load_opportunity(pool: &PgPool, cli: &Cli) -> Result<OpportunityRow> {
    match (&cli.opportunity, &cli.path) {
        (Some(id), _) => sqlx::query_as::<_, OpportunityRow>(
            r#"
            SELECT id, created_at, block_number, token_in, amount_in, expected_profit, min_profit, path_json
            FROM opportunities
            WHERE id = $1
            "#,
        )
        .bind(id)
        .fetch_one(pool)
        .await
        .with_context(|| format!("opportunity not found: {id}")),
        (None, Some(path)) => sqlx::query_as::<_, OpportunityRow>(
            r#"
            SELECT id, created_at, block_number, token_in, amount_in, expected_profit, min_profit, path_json
            FROM opportunities
            WHERE path_json->>'name' = $1
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(path)
        .fetch_one(pool)
        .await
        .with_context(|| format!("no opportunity found for path: {path}")),
        (None, None) => sqlx::query_as::<_, OpportunityRow>(
            r#"
            SELECT o.id, o.created_at, o.block_number, o.token_in, o.amount_in,
                   o.expected_profit, o.min_profit, o.path_json
            FROM opportunities o
            JOIN simulations s ON s.opportunity_id = o.id
            WHERE s.success = false
              AND COALESCE(s.revert_reason, '') <> 'candidate expired'
            ORDER BY s.created_at DESC
            LIMIT 1
            "#,
        )
        .fetch_one(pool)
        .await
        .context("no failed non-expired simulation found; pass --opportunity or --path"),
    }
}

async fn validate_step(
    step_no: usize,
    step: &SwapStep,
    settings: &Settings,
    provider: &ChainProvider,
    block_hash: &str,
    block_number: u64,
) -> Result<()> {
    println!("\n== Step {step_no} ==");
    println!("dex: {:?} variant: {:?}", step.dex, step.variant);
    println!("pool: {:#x}", step.pool);
    println!("factory: {:?}", step.factory_address);
    println!("token_in: {:#x}", step.token_in);
    println!("token_out: {:#x}", step.token_out);
    println!(
        "fee_bps: {:?} stable: {:?} tick_spacing: {:?}",
        step.fee_bps, step.stable, step.tick_spacing
    );

    let entry = PoolRegistryEntry {
        pool_address: step.pool,
        dex: step.dex,
        variant: step.variant.unwrap_or(default_variant(step.dex)),
        factory_address: step.factory_address,
        token0: step.token_in,
        token1: step.token_out,
        fee_bps: step.fee_bps.unwrap_or_default(),
        tick_spacing: step.tick_spacing,
        stable: Some(classic_stable_flag(step)),
        enabled: true,
    };

    match provider
        .fetch_pool_state_from_registry_at_block_hash(&entry, block_hash, block_number)
        .await
    {
        Ok(state) => {
            println!("onchain_state: ok source_block={}", state.block_number);
            println!(
                "reserve0={:?} reserve1={:?} sqrt_price_x96={:?} liquidity={:?} tick={:?}",
                state.reserve0, state.reserve1, state.sqrt_price_x96, state.liquidity, state.tick
            );
        }
        Err(err) => {
            println!("onchain_state: FAILED: {err:#}");
        }
    }

    match factory_for_step(step, settings) {
        Ok(Some(factory)) => match factory_pool_for_step(step, factory, provider).await {
            Ok(pool) if pool == step.pool => {
                println!("factory_check: ok factory={factory:#x}");
            }
            Ok(pool) => {
                println!(
                    "factory_check: MISMATCH factory={factory:#x} returned={pool:#x} expected={:#x}",
                    step.pool
                );
            }
            Err(err) => {
                println!("factory_check: FAILED factory={factory:#x}: {err:#}");
            }
        },
        Ok(None) => println!("factory_check: skipped (factory not configured)"),
        Err(err) => println!("factory_check: skipped ({err:#})"),
    }

    if matches!(
        (step.dex, step.variant),
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None)
    ) {
        validate_classic_router_quote(step, settings, provider).await?;
    }

    Ok(())
}

async fn validate_classic_router_quote(
    step: &SwapStep,
    settings: &Settings,
    provider: &ChainProvider,
) -> Result<()> {
    let Some(router) = settings.aerodrome_router else {
        println!("classic_router_quote: skipped (AERODROME_ROUTER not configured)");
        return Ok(());
    };
    let factory = step
        .factory_address
        .or(settings.aerodrome_pool_factory)
        .or_else(|| DEFAULT_AERODROME_FACTORY.parse().ok())
        .unwrap_or(Address::ZERO);
    let data = encode_get_amounts_out(
        U256::from(1_000_000u64),
        step.token_in,
        step.token_out,
        classic_stable_flag(step),
        factory,
    );
    let data_hex = format!("0x{}", hex::encode(data));
    match provider
        .eth_call_from(None, router, &data_hex, "Aerodrome getAmountsOut")
        .await
    {
        Ok(raw) => {
            let amounts = decode_uint_array(&raw);
            println!("classic_router_quote: ok router={router:#x} amounts={amounts:?}");
        }
        Err(err) => {
            println!("classic_router_quote: FAILED router={router:#x}: {err:#}");
        }
    }
    Ok(())
}

async fn pool_get_amount_out(
    provider: &ChainProvider,
    pool: Address,
    amount_in: U256,
    token_in: Address,
) -> Result<U256> {
    let data = encode_pool_get_amount_out(amount_in, token_in);
    let data_hex = format!("0x{}", hex::encode(data));
    let raw = provider
        .eth_call_from(None, pool, &data_hex, "Aerodrome pool getAmountOut")
        .await?;
    decode_single_uint(&raw).context("failed to decode pool getAmountOut")
}

fn diff_bps(a: U256, b: U256) -> U256 {
    let denominator = a.max(b);
    if denominator.is_zero() {
        return U256::ZERO;
    }
    let diff = if a > b { a - b } else { b - a };
    diff.saturating_mul(U256::from(10_000u64)) / denominator
}

async fn validate_executor_call(
    path: &ArbPath,
    token_in: Address,
    amount_in: U256,
    min_profit: U256,
    settings: &Settings,
    provider: &ChainProvider,
) -> Result<()> {
    let Some(executor) = settings
        .executor_contract
        .filter(|address| *address != Address::ZERO)
    else {
        println!("\n== Executor Call ==\nskipped: EXECUTOR_CONTRACT not configured");
        return Ok(());
    };
    let Some(operator) = settings
        .eoa_address_1
        .filter(|address| *address != Address::ZERO)
    else {
        println!("\n== Executor Call ==\nskipped: EOA_ADDRESS_1 not configured");
        return Ok(());
    };

    println!("\n== Executor Call ==");
    let deadline = U256::from((Utc::now().timestamp() + EXECUTOR_DEADLINE_SECS) as u64);
    let calldata =
        build_execute_calldata(path, token_in, amount_in, min_profit, deadline, settings)?;
    let data_hex = format!("0x{}", hex::encode(&calldata));
    println!("executor: {executor:#x}");
    println!("operator: {operator:#x}");
    println!("calldata_bytes: {}", calldata.len());

    match provider
        .eth_call_from(
            Some(operator),
            executor,
            &data_hex,
            "Executor executeWithOwnFunds",
        )
        .await
    {
        Ok(raw) => println!("executor_call: ok result={raw}"),
        Err(err) => println!("executor_call: FAILED: {err:#}"),
    }

    Ok(())
}

fn factory_for_step(step: &SwapStep, settings: &Settings) -> Result<Option<Address>> {
    if let Some(factory) = step.factory_address {
        return Ok(Some(factory));
    }

    match (step.dex, step.variant) {
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None) => {
            Ok(Some(
                settings
                    .aerodrome_pool_factory
                    .or_else(|| DEFAULT_AERODROME_FACTORY.parse().ok())
                    .context("AERODROME_POOL_FACTORY is not configured")?,
            ))
        }
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream)) => {
            Ok(settings.aerodrome_slipstream_factory)
        }
        (DexKind::UniswapV3, Some(PoolVariant::UniswapV3)) | (DexKind::UniswapV3, None) => {
            Ok(Some(
                settings
                    .uniswap_v3_factory
                    .or_else(|| DEFAULT_UNISWAP_V3_FACTORY.parse().ok())
                    .context("UNISWAP_V3_FACTORY is not configured")?,
            ))
        }
        (DexKind::PancakeSwap, Some(PoolVariant::PancakeV3)) | (DexKind::PancakeSwap, None) => {
            Ok(Some(
                settings
                    .pancake_v3_factory
                    .or_else(|| PANCAKE_V3_FACTORY.parse().ok())
                    .context("PANCAKE_V3_FACTORY is not configured")?,
            ))
        }
        _ => Ok(None),
    }
}

async fn factory_pool_for_step(
    step: &SwapStep,
    factory: Address,
    provider: &ChainProvider,
) -> Result<Address> {
    let data = match (step.dex, step.variant) {
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None) => {
            encode_factory_get_pool_bool(step.token_in, step.token_out, classic_stable_flag(step))
        }
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream)) => {
            let spacing = step
                .tick_spacing
                .context("tick_spacing is required for Slipstream factory check")?;
            encode_factory_get_pool_int24(step.token_in, step.token_out, spacing)
        }
        (DexKind::UniswapV3, _) | (DexKind::PancakeSwap, _) => {
            let fee = step
                .fee_bps
                .unwrap_or_default()
                .checked_mul(100)
                .context("fee overflow")?;
            encode_factory_get_pool_uint24(step.token_in, step.token_out, fee)
        }
        _ => bail!("unsupported dex/variant for factory check"),
    };
    let raw = provider
        .eth_call_from(
            None,
            factory,
            &format!("0x{}", hex::encode(data)),
            "factory getPool",
        )
        .await?;
    decode_address_result(&raw).context("factory getPool returned non-address")
}

fn build_execute_calldata(
    path: &ArbPath,
    token_in: Address,
    amount_in: U256,
    min_profit: U256,
    deadline: U256,
    settings: &Settings,
) -> Result<Vec<u8>> {
    let selector = &keccak256(
        b"executeWithOwnFunds(address,uint256,(uint8,address,address,address,address,uint24,bool,address)[],uint256,uint256)",
    )[..4];
    let mut out = Vec::new();
    out.extend_from_slice(selector);
    out.extend(encode_address(token_in));
    out.extend(encode_u256(amount_in));
    out.extend(encode_u256(U256::from(160u64)));
    out.extend(encode_u256(min_profit));
    out.extend(encode_u256(deadline));
    out.extend(encode_executor_steps(path, settings)?);
    Ok(out)
}

fn encode_executor_steps(path: &ArbPath, settings: &Settings) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(encode_u256(U256::from(path.steps.len())));
    for step in &path.steps {
        out.extend(encode_u256(U256::from(executor_dex_kind(step)?)));
        out.extend(encode_address(
            router_for_step(step, settings)
                .with_context(|| format!("router missing for {:?} {:?}", step.dex, step.variant))?,
        ));
        out.extend(encode_address(step.pool));
        out.extend(encode_address(step.token_in));
        out.extend(encode_address(step.token_out));
        out.extend(encode_u256(U256::from(router_fee_for_step(step)?)));
        out.extend(encode_bool(classic_stable_flag(step)));
        out.extend(encode_address(
            factory_for_step(step, settings)?.unwrap_or(Address::ZERO),
        ));
    }
    Ok(out)
}

fn router_for_step(step: &SwapStep, settings: &Settings) -> Option<Address> {
    match (step.dex, step.variant) {
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream)) => settings
            .aerodrome_slipstream_router
            .or_else(|| AERODROME_SLIPSTREAM_ROUTER.parse().ok()),
        (DexKind::Aerodrome, _) => settings.aerodrome_router,
        (DexKind::UniswapV3, _) => settings.uniswap_v3_router,
        (DexKind::PancakeSwap, _) => settings
            .pancake_v3_router
            .or_else(|| PANCAKE_V3_ROUTER.parse().ok()),
    }
}

fn router_fee_for_step(step: &SwapStep) -> Result<u32> {
    let fee_bps = step.fee_bps.unwrap_or_default();
    Ok(match (step.dex, step.variant) {
        (DexKind::UniswapV3, _) | (DexKind::PancakeSwap, _) => {
            fee_bps.checked_mul(100).context("V3 fee bps overflow")?
        }
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream)) => {
            let spacing = step
                .tick_spacing
                .context("tick_spacing is required for Aerodrome Slipstream")?;
            if spacing <= 0 {
                bail!("invalid Aerodrome Slipstream tick_spacing {spacing}");
            }
            u32::try_from(spacing)?
        }
        _ => fee_bps,
    })
}

fn executor_dex_kind(step: &SwapStep) -> Result<u8> {
    match (step.dex, step.variant) {
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None) => {
            Ok(0)
        }
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream)) => Ok(1),
        (DexKind::UniswapV3, Some(PoolVariant::UniswapV3)) | (DexKind::UniswapV3, None) => Ok(2),
        (DexKind::PancakeSwap, Some(PoolVariant::PancakeV3)) | (DexKind::PancakeSwap, None) => {
            Ok(2)
        }
        _ => bail!("dex and pool variant mismatch"),
    }
}

fn default_variant(dex: DexKind) -> PoolVariant {
    match dex {
        DexKind::Aerodrome => PoolVariant::AerodromeVolatile,
        DexKind::UniswapV3 => PoolVariant::UniswapV3,
        DexKind::PancakeSwap => PoolVariant::PancakeV3,
    }
}

fn classic_stable_flag(step: &SwapStep) -> bool {
    matches!(
        (step.dex, step.variant),
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None)
    ) && step.stable.unwrap_or(false)
}

fn encode_factory_get_pool_bool(token_in: Address, token_out: Address, stable: bool) -> Vec<u8> {
    let mut out = keccak256(b"getPool(address,address,bool)")[..4].to_vec();
    out.extend(encode_address(token_in));
    out.extend(encode_address(token_out));
    out.extend(encode_bool(stable));
    out
}

fn encode_factory_get_pool_uint24(token_in: Address, token_out: Address, fee: u32) -> Vec<u8> {
    let mut out = keccak256(b"getPool(address,address,uint24)")[..4].to_vec();
    out.extend(encode_address(token_in));
    out.extend(encode_address(token_out));
    out.extend(encode_u256(U256::from(fee)));
    out
}

fn encode_factory_get_pool_int24(token_in: Address, token_out: Address, spacing: i32) -> Vec<u8> {
    let mut out = keccak256(b"getPool(address,address,int24)")[..4].to_vec();
    out.extend(encode_address(token_in));
    out.extend(encode_address(token_out));
    out.extend(encode_i24(spacing));
    out
}

fn encode_get_amounts_out(
    amount_in: U256,
    token_in: Address,
    token_out: Address,
    stable: bool,
    factory: Address,
) -> Vec<u8> {
    let mut out =
        keccak256(b"getAmountsOut(uint256,(address,address,bool,address)[])")[..4].to_vec();
    out.extend(encode_u256(amount_in));
    out.extend(encode_u256(U256::from(64u64)));
    out.extend(encode_u256(U256::from(1u64)));
    out.extend(encode_address(token_in));
    out.extend(encode_address(token_out));
    out.extend(encode_bool(stable));
    out.extend(encode_address(factory));
    out
}

fn encode_pool_get_amount_out(amount_in: U256, token_in: Address) -> Vec<u8> {
    let mut out = keccak256(b"getAmountOut(uint256,address)")[..4].to_vec();
    out.extend(encode_u256(amount_in));
    out.extend(encode_address(token_in));
    out
}

fn encode_address(address: Address) -> Vec<u8> {
    let mut out = vec![0u8; 12];
    out.extend_from_slice(address.as_slice());
    out
}

fn encode_bool(value: bool) -> Vec<u8> {
    encode_u256(U256::from(u8::from(value)))
}

fn encode_i24(value: i32) -> Vec<u8> {
    if value >= 0 {
        encode_u256(U256::from(value as u32))
    } else {
        let mut out = [0xffu8; 32];
        let raw = (value as i64) & 0x00ff_ffff;
        out[29] = ((raw >> 16) & 0xff) as u8;
        out[30] = ((raw >> 8) & 0xff) as u8;
        out[31] = (raw & 0xff) as u8;
        out.to_vec()
    }
}

fn encode_u256(value: U256) -> Vec<u8> {
    value.to_be_bytes::<32>().to_vec()
}

fn decode_address_result(raw: &str) -> Option<Address> {
    let clean = raw.trim_start_matches("0x");
    if clean.len() < 64 {
        return None;
    }
    Address::from_str(&format!("0x{}", &clean[24..64])).ok()
}

fn decode_uint_array(raw: &str) -> Vec<U256> {
    let words = decode_words(raw);
    if words.len() < 2 {
        return Vec::new();
    }
    let len = words[1].to::<usize>();
    words.into_iter().skip(2).take(len).collect()
}

fn decode_single_uint(raw: &str) -> Option<U256> {
    decode_words(raw).into_iter().next()
}

fn decode_words(raw: &str) -> Vec<U256> {
    let clean = raw.trim_start_matches("0x");
    clean
        .as_bytes()
        .chunks(64)
        .filter_map(|chunk| {
            if chunk.len() != 64 {
                return None;
            }
            let word = std::str::from_utf8(chunk).ok()?;
            U256::from_str_radix(word, 16).ok()
        })
        .collect()
}

fn parse_u256_decimal(value: &str) -> Result<U256> {
    U256::from_str_radix(value.trim(), 10).context("invalid decimal U256")
}
