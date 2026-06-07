use std::{
    collections::BTreeMap,
    env,
    fs::{self, File},
    io::{BufWriter, Write},
    path::PathBuf,
    str::FromStr,
};

use alloy_primitives::{keccak256, Address, U256};
use anyhow::{bail, Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::{
    config::Settings,
    constants::{AERODROME_SLIPSTREAM_ROUTER, PANCAKE_V3_FACTORY, PANCAKE_V3_ROUTER},
    types::{ArbPath, DexKind, PoolVariant, SwapStep},
};
use chrono::{DateTime, Utc};
use sqlx::{FromRow, PgPool};
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

const DEFAULT_AERODROME_FACTORY: &str = "0x420DD381b31aEf6683db6B902084cB0FFECe40Da";
const DEFAULT_UNISWAP_V3_FACTORY: &str = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD";
const REPLAY_DEADLINE_SECS: u64 = 30 * 24 * 60 * 60;

#[derive(Debug)]
struct Cli {
    hours: i64,
    limit: i64,
    reason: Option<String>,
    out: Option<PathBuf>,
}

#[derive(Debug, FromRow)]
struct ReplayRow {
    opportunity_id: Uuid,
    opportunity_at: DateTime<Utc>,
    opportunity_block: i64,
    token_in: String,
    amount_in: String,
    expected_profit: String,
    min_profit: String,
    path_json: serde_json::Value,
    simulation_id: Uuid,
    simulation_at: DateTime<Utc>,
    simulation_block: Option<i64>,
    simulation_success: bool,
    simulation_revert_reason: Option<String>,
}

#[derive(Debug)]
struct ReplayResult {
    original_reason: String,
    historical_result: String,
    historical_zero_min_result: Option<String>,
    classification: String,
    profit: Option<U256>,
    structural_notes: Vec<String>,
    router_probe_notes: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = parse_args(env::args().skip(1))?;
    let settings = Settings::load()?;
    let pool = PgPool::connect(&settings.postgres_url).await?;
    let provider = ChainProvider::from_settings(&settings);
    let rows = load_rows(&pool, &cli).await?;

    let out_path = cli.out.unwrap_or_else(default_report_path);
    if let Some(parent) = out_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
    }
    let mut writer = BufWriter::new(
        File::create(&out_path)
            .with_context(|| format!("failed to create {}", out_path.display()))?,
    );

    writeln!(writer, "simulation replay report")?;
    writeln!(writer, "generated_at_utc: {}", Utc::now())?;
    writeln!(writer, "hours: {}", cli.hours)?;
    writeln!(writer, "limit: {}", cli.limit)?;
    writeln!(writer, "reason: {}", cli.reason.as_deref().unwrap_or("all"))?;
    writeln!(writer, "rows: {}", rows.len())?;
    writeln!(writer)?;

    let mut summary = BTreeMap::<String, usize>::new();
    for (idx, row) in rows.iter().enumerate() {
        let result = replay_row(row, &settings, &provider).await;
        write_row(&mut writer, idx + 1, row, &result)?;
        let key = result
            .as_ref()
            .map(|value| value.classification.clone())
            .unwrap_or_else(|err| format!("replay_error: {}", compact_err(err)));
        *summary.entry(key).or_default() += 1;
    }

    writeln!(writer)?;
    writeln!(writer, "== Summary ==")?;
    for (classification, n) in summary {
        writeln!(writer, "{classification}\t{n}")?;
    }
    writer.flush()?;

    println!("wrote {}", out_path.display());
    Ok(())
}

async fn replay_row(
    row: &ReplayRow,
    settings: &Settings,
    provider: &ChainProvider,
) -> Result<ReplayResult> {
    let path: ArbPath = serde_json::from_value(row.path_json.clone())
        .with_context(|| format!("failed to decode path_json for {}", row.opportunity_id))?;
    let token_in = Address::from_str(&row.token_in).context("invalid token_in")?;
    let amount_in = parse_u256_decimal(&row.amount_in).context("invalid amount_in")?;
    let min_profit = parse_u256_decimal(&row.min_profit).context("invalid min_profit")?;
    let original_reason = row.simulation_revert_reason.clone().unwrap_or_else(|| {
        if row.simulation_success {
            "success".into()
        } else {
            "-".into()
        }
    });

    let structural_notes = structural_checks(&path, settings, provider, row.opportunity_block)
        .await
        .unwrap_or_else(|err| vec![format!("structural_check_error: {err:#}")]);

    let executor = settings
        .executor_contract
        .filter(|address| *address != Address::ZERO)
        .context("EXECUTOR_CONTRACT is not configured")?;
    let operator = settings
        .eoa_address_1
        .filter(|address| *address != Address::ZERO)
        .context("EOA_ADDRESS_1 is not configured")?;
    let deadline = U256::from((Utc::now().timestamp() as u64).saturating_add(REPLAY_DEADLINE_SECS));
    let calldata =
        build_execute_calldata(&path, token_in, amount_in, min_profit, deadline, settings)?;
    let data = format!("0x{}", hex::encode(calldata));
    let call = provider
        .eth_call_from_at_block(
            Some(operator),
            executor,
            &data,
            "historical Executor executeWithOwnFunds",
            Some(u64::try_from(row.opportunity_block).context("negative opportunity block")?),
        )
        .await;

    let (historical_result, profit) = match call {
        Ok(raw) => {
            let profit = decode_uint256_result(&raw);
            (format!("success raw={raw}"), profit)
        }
        Err(err) => (format_revert_reason(&format!("{err:#}")), None),
    };
    let historical_zero_min_result = if historical_result.contains("MinProfitNotMet")
        || historical_result.contains("router/no-revert-data")
    {
        let zero_min_calldata =
            build_execute_calldata(&path, token_in, amount_in, U256::ZERO, deadline, settings)?;
        let zero_min_data = format!("0x{}", hex::encode(zero_min_calldata));
        let zero_min_call = provider
            .eth_call_from_at_block(
                Some(operator),
                executor,
                &zero_min_data,
                "historical Executor executeWithOwnFunds minProfit=0",
                Some(u64::try_from(row.opportunity_block).context("negative opportunity block")?),
            )
            .await;
        Some(match zero_min_call {
            Ok(raw) => format!(
                "success profit={}",
                decode_uint256_result(&raw)
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| format!("raw={raw}"))
            ),
            Err(err) => format_revert_reason(&format!("{err:#}")),
        })
    } else {
        None
    };
    let router_probe_notes = if historical_result.contains("router/no-revert-data") {
        router_step1_probe(&path, amount_in, settings, provider, row.opportunity_block)
            .await
            .unwrap_or_else(|err| vec![format!("router_probe_error: {err:#}")])
    } else {
        Vec::new()
    };
    let classification = classify(
        &original_reason,
        &historical_result,
        historical_zero_min_result.as_deref(),
        &structural_notes,
    );

    Ok(ReplayResult {
        original_reason,
        historical_result,
        historical_zero_min_result,
        classification,
        profit,
        structural_notes,
        router_probe_notes,
    })
}

async fn structural_checks(
    path: &ArbPath,
    settings: &Settings,
    provider: &ChainProvider,
    block_number: i64,
) -> Result<Vec<String>> {
    let mut notes = Vec::new();
    for (idx, step) in path.steps.iter().enumerate() {
        let step_no = idx + 1;
        let router = router_for_step(step, settings);
        if router.is_none() {
            notes.push(format!(
                "step {step_no}: router missing for {:?} {:?}",
                step.dex, step.variant
            ));
        }
        if let Err(err) = executor_dex_kind(step) {
            notes.push(format!(
                "step {step_no}: executor dex kind invalid: {err:#}"
            ));
        }
        if let Err(err) = router_fee_for_step(step) {
            notes.push(format!("step {step_no}: router fee invalid: {err:#}"));
        }
        let Some(factory) = factory_for_step(step, settings)? else {
            notes.push(format!("step {step_no}: factory missing/skipped"));
            continue;
        };
        if factory == Address::ZERO {
            notes.push(format!("step {step_no}: factory zero/skipped"));
            continue;
        }
        match factory_pool_for_step_at_block(step, factory, provider, block_number).await {
            Ok(pool) if pool == step.pool => {
                notes.push(format!("step {step_no}: factory ok {factory:#x}"));
            }
            Ok(pool) => {
                notes.push(format!(
                    "step {step_no}: factory mismatch factory={factory:#x} returned={pool:#x} expected={:#x}",
                    step.pool
                ));
            }
            Err(err) => {
                notes.push(format!(
                    "step {step_no}: factory check failed factory={factory:#x}: {err:#}"
                ));
            }
        }
    }
    Ok(notes)
}

async fn router_step1_probe(
    path: &ArbPath,
    amount_in: U256,
    settings: &Settings,
    provider: &ChainProvider,
    block_number: i64,
) -> Result<Vec<String>> {
    let mut notes = Vec::new();
    let Some(step) = path.steps.first() else {
        return Ok(vec!["no step1".into()]);
    };
    let Some(router) = router_for_step(step, settings) else {
        return Ok(vec!["step1 router missing".into()]);
    };
    let Some(executor) = settings
        .executor_contract
        .filter(|address| *address != Address::ZERO)
    else {
        return Ok(vec!["executor missing".into()]);
    };
    let deadline = U256::from((Utc::now().timestamp() as u64).saturating_add(REPLAY_DEADLINE_SECS));
    let calldata = build_router_step_calldata(step, amount_in, executor, deadline, settings)?;
    let raw = provider
        .eth_call_from_at_block(
            Some(executor),
            router,
            &format!("0x{}", hex::encode(calldata)),
            "router step1 probe",
            Some(u64::try_from(block_number).context("negative block number")?),
        )
        .await;
    match raw {
        Ok(raw) => notes.push(format!(
            "step1 router probe ok output={}",
            decode_router_amount_out(&raw)
                .map(|value| value.to_string())
                .unwrap_or_else(|| format!("raw={raw}"))
        )),
        Err(err) => notes.push(format!(
            "step1 router probe failed: {}",
            format_revert_reason(&format!("{err:#}"))
        )),
    }
    Ok(notes)
}

fn classify(
    original: &str,
    historical: &str,
    historical_zero_min: Option<&str>,
    structural_notes: &[String],
) -> String {
    if structural_notes.iter().any(|note| {
        note.contains("factory mismatch")
            || note.contains("router missing")
            || note.contains("router fee invalid")
            || note.contains("executor dex kind invalid")
    }) {
        return "structural".into();
    }
    if historical.starts_with("success") {
        return if original.contains("MinProfitNotMet") {
            "original_failed_but_historical_success".into()
        } else {
            "historical_success".into()
        };
    }
    if let Some(zero_min) = historical_zero_min {
        if zero_min.starts_with("success") {
            return "historical_positive_below_min_profit".into();
        }
    }
    if historical.contains("MinProfitNotMet") {
        return "historical_min_profit_not_met".into();
    }
    if historical.contains("InsufficientAllowance") {
        return "historical_insufficient_allowance".into();
    }
    if historical.contains("router/no-revert-data") {
        return "historical_router_no_revert_data".into();
    }
    if historical.contains("PoolMismatch") {
        return "structural_pool_mismatch".into();
    }
    format!("historical_other: {}", short_reason(historical))
}

fn write_row(
    writer: &mut BufWriter<File>,
    idx: usize,
    row: &ReplayRow,
    result: &Result<ReplayResult>,
) -> Result<()> {
    writeln!(writer, "== Replay {idx} ==")?;
    writeln!(writer, "opportunity_id: {}", row.opportunity_id)?;
    writeln!(writer, "simulation_id: {}", row.simulation_id)?;
    writeln!(writer, "opportunity_at: {}", row.opportunity_at)?;
    writeln!(writer, "simulation_at: {}", row.simulation_at)?;
    writeln!(writer, "opportunity_block: {}", row.opportunity_block)?;
    writeln!(
        writer,
        "simulation_block: {}",
        row.simulation_block
            .map(|block| block.to_string())
            .unwrap_or_else(|| "-".into())
    )?;
    writeln!(
        writer,
        "simulation_block_delta: {}",
        row.simulation_block
            .map(|block| (block - row.opportunity_block).to_string())
            .unwrap_or_else(|| "-".into())
    )?;
    writeln!(writer, "expected_profit: {}", row.expected_profit)?;
    writeln!(writer, "min_profit: {}", row.min_profit)?;
    match result {
        Ok(result) => {
            writeln!(writer, "original_reason: {}", result.original_reason)?;
            writeln!(writer, "historical_result: {}", result.historical_result)?;
            if let Some(zero_min) = &result.historical_zero_min_result {
                writeln!(writer, "historical_zero_min_result: {zero_min}")?;
            }
            writeln!(
                writer,
                "historical_profit: {}",
                result
                    .profit
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".into())
            )?;
            writeln!(writer, "classification: {}", result.classification)?;
            writeln!(writer, "structural_notes:")?;
            for note in &result.structural_notes {
                writeln!(writer, "  - {note}")?;
            }
            if !result.router_probe_notes.is_empty() {
                writeln!(writer, "router_probe_notes:")?;
                for note in &result.router_probe_notes {
                    writeln!(writer, "  - {note}")?;
                }
            }
        }
        Err(err) => {
            writeln!(writer, "replay_error: {err:#}")?;
        }
    }
    writeln!(writer)?;
    Ok(())
}

async fn load_rows(pool: &PgPool, cli: &Cli) -> Result<Vec<ReplayRow>> {
    let reason = cli.reason.as_deref().unwrap_or("%");
    sqlx::query_as::<_, ReplayRow>(
        r#"
        SELECT
            o.id AS opportunity_id,
            o.created_at AS opportunity_at,
            o.block_number AS opportunity_block,
            o.token_in,
            o.amount_in,
            o.expected_profit,
            o.min_profit,
            o.path_json,
            s.id AS simulation_id,
            s.created_at AS simulation_at,
            s.block_number AS simulation_block,
            s.success AS simulation_success,
            s.revert_reason AS simulation_revert_reason
        FROM simulations s
        JOIN opportunities o ON o.id = s.opportunity_id
        WHERE s.created_at >= now() - ($1::bigint * interval '1 hour')
          AND s.success = false
          AND COALESCE(s.revert_reason, '') ILIKE $2
        ORDER BY s.created_at DESC
        LIMIT $3
        "#,
    )
    .bind(cli.hours)
    .bind(reason)
    .bind(cli.limit)
    .fetch_all(pool)
    .await
    .context("failed to load simulations")
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Cli> {
    let mut hours = 12;
    let mut limit = 50;
    let mut reason = None;
    let mut out = None;
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--hours" => {
                hours = args
                    .next()
                    .context("--hours requires a value")?
                    .parse()
                    .context("invalid --hours")?;
            }
            "--limit" => {
                limit = args
                    .next()
                    .context("--limit requires a value")?
                    .parse()
                    .context("invalid --limit")?;
            }
            "--reason" => {
                reason = Some(args.next().context("--reason requires a value")?);
            }
            "--out" => {
                out = Some(PathBuf::from(
                    args.next().context("--out requires a value")?,
                ));
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => bail!("unknown argument: {other}"),
        }
    }
    Ok(Cli {
        hours,
        limit,
        reason,
        out,
    })
}

fn print_usage() {
    println!(
        "Usage: cargo run -p base-arb-recorder --bin replay_simulations -- [--hours 12] [--limit 50] [--reason '%MinProfitNotMet%'] [--out reports/replay.txt]"
    );
}

fn default_report_path() -> PathBuf {
    PathBuf::from(format!(
        "reports/replay-{}.txt",
        Utc::now().format("%Y%m%dT%H%M%SZ")
    ))
}

async fn factory_pool_for_step_at_block(
    step: &SwapStep,
    factory: Address,
    provider: &ChainProvider,
    block_number: i64,
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
        .eth_call_from_at_block(
            None,
            factory,
            &format!("0x{}", hex::encode(data)),
            "factory getPool",
            Some(u64::try_from(block_number).context("negative block number")?),
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

fn build_router_step_calldata(
    step: &SwapStep,
    amount_in: U256,
    recipient: Address,
    deadline: U256,
    settings: &Settings,
) -> Result<Vec<u8>> {
    match (step.dex, step.variant) {
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None) => {
            let factory = factory_for_step(step, settings)?.unwrap_or(Address::ZERO);
            Ok(encode_classic_swap_exact_tokens_for_tokens(
                amount_in,
                step.token_in,
                step.token_out,
                classic_stable_flag(step),
                factory,
                recipient,
                deadline,
            ))
        }
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream)) => {
            let spacing = step
                .tick_spacing
                .context("tick_spacing is required for Aerodrome Slipstream router probe")?;
            Ok(encode_exact_input_single_int24(
                step.token_in,
                step.token_out,
                spacing,
                recipient,
                deadline,
                amount_in,
            ))
        }
        (DexKind::UniswapV3, _) | (DexKind::PancakeSwap, _) => {
            let fee = step
                .fee_bps
                .unwrap_or_default()
                .checked_mul(100)
                .context("fee overflow")?;
            Ok(encode_exact_input_single_uint24(
                step.token_in,
                step.token_out,
                fee,
                recipient,
                deadline,
                amount_in,
            ))
        }
        _ => bail!("unsupported dex/variant for router probe"),
    }
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

fn encode_exact_input_single_uint24(
    token_in: Address,
    token_out: Address,
    fee: u32,
    recipient: Address,
    deadline: U256,
    amount_in: U256,
) -> Vec<u8> {
    let mut out = keccak256(
        b"exactInputSingle((address,address,uint24,address,uint256,uint256,uint256,uint160))",
    )[..4]
        .to_vec();
    out.extend(encode_address(token_in));
    out.extend(encode_address(token_out));
    out.extend(encode_u256(U256::from(fee)));
    out.extend(encode_address(recipient));
    out.extend(encode_u256(deadline));
    out.extend(encode_u256(amount_in));
    out.extend(encode_u256(U256::ZERO));
    out.extend(encode_u256(U256::ZERO));
    out
}

fn encode_exact_input_single_int24(
    token_in: Address,
    token_out: Address,
    tick_spacing: i32,
    recipient: Address,
    deadline: U256,
    amount_in: U256,
) -> Vec<u8> {
    let mut out = keccak256(
        b"exactInputSingle((address,address,int24,address,uint256,uint256,uint256,uint160))",
    )[..4]
        .to_vec();
    out.extend(encode_address(token_in));
    out.extend(encode_address(token_out));
    out.extend(encode_i24(tick_spacing));
    out.extend(encode_address(recipient));
    out.extend(encode_u256(deadline));
    out.extend(encode_u256(amount_in));
    out.extend(encode_u256(U256::ZERO));
    out.extend(encode_u256(U256::ZERO));
    out
}

fn encode_classic_swap_exact_tokens_for_tokens(
    amount_in: U256,
    token_in: Address,
    token_out: Address,
    stable: bool,
    factory: Address,
    recipient: Address,
    deadline: U256,
) -> Vec<u8> {
    let mut out =
        keccak256(b"swapExactTokensForTokens(uint256,uint256,(address,address,bool,address)[],address,uint256)")
            [..4]
            .to_vec();
    out.extend(encode_u256(amount_in));
    out.extend(encode_u256(U256::ZERO));
    out.extend(encode_u256(U256::from(160u64)));
    out.extend(encode_address(recipient));
    out.extend(encode_u256(deadline));
    out.extend(encode_u256(U256::from(1u64)));
    out.extend(encode_address(token_in));
    out.extend(encode_address(token_out));
    out.extend(encode_bool(stable));
    out.extend(encode_address(factory));
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
    let mut out = [0u8; 32];
    value
        .to_be_bytes::<32>()
        .iter()
        .enumerate()
        .for_each(|(idx, byte)| out[idx] = *byte);
    out.to_vec()
}

fn decode_address_result(raw: &str) -> Option<Address> {
    let clean = raw.trim_start_matches("0x");
    if clean.len() < 64 {
        return None;
    }
    Address::from_str(&format!("0x{}", &clean[24..64])).ok()
}

fn decode_uint256_result(raw: &str) -> Option<U256> {
    let clean = raw.trim_start_matches("0x");
    if clean.len() < 64 {
        return None;
    }
    U256::from_str_radix(&clean[0..64], 16).ok()
}

fn decode_router_amount_out(raw: &str) -> Option<U256> {
    let clean = raw.trim_start_matches("0x");
    if clean.len() < 64 {
        return None;
    }
    let first = U256::from_str_radix(&clean[0..64], 16).ok()?;
    if clean.len() >= 128 && first == U256::from(32u64) {
        let len = U256::from_str_radix(&clean[64..128], 16).ok()?;
        if len >= U256::from(1u64) && clean.len() >= 192 {
            return U256::from_str_radix(&clean[128..192], 16).ok();
        }
    }
    Some(first)
}

fn parse_u256_decimal(value: &str) -> Result<U256> {
    U256::from_str_radix(value, 10).with_context(|| format!("invalid U256 decimal: {value}"))
}

fn format_revert_reason(raw: &str) -> String {
    match decode_executor_error(raw) {
        Some(error) => error.to_string(),
        None => compact_revert_reason(raw),
    }
}

fn decode_executor_error(raw: &str) -> Option<&'static str> {
    for selector in hex_selectors(raw) {
        if let Some(error) = executor_error_name(&selector) {
            return Some(error);
        }
    }
    None
}

fn executor_error_name(selector: &str) -> Option<&'static str> {
    match selector {
        "0x5fc483c5" => Some("Executor revert: OnlyOwner"),
        "0x27e1f1e5" => Some("Executor revert: OnlyOperator"),
        "0xeced32bc" => Some("Executor revert: PausedError"),
        "0x1ab7da6b" => Some("Executor revert: DeadlineExpired"),
        "0xf84835a0" => Some("Executor revert: TokenNotWhitelisted"),
        "0xb76b08ae" => Some("Executor revert: RouterNotWhitelisted"),
        "0x1b4c7fdf" => Some("Executor revert: PoolNotWhitelisted"),
        "0x8de0e0da" => Some("Executor revert: FactoryNotWhitelisted"),
        "0x20db8267" => Some("Executor revert: InvalidPath"),
        "0x1ae9030e" => Some("Executor revert: InvalidStepCount"),
        "0xf4d678b8" => Some("Executor revert: InsufficientBalance"),
        "0x13be252b" => Some("Executor revert: InsufficientAllowance"),
        "0xa5d3ca34" => Some("Executor revert: UnsupportedDex"),
        "0x270815a0" => Some("Executor revert: InvalidTickSpacing"),
        "0xeab28cb4" => Some("Executor revert: PoolMismatch"),
        "0xd433008b" => Some("Executor revert: MinProfitNotMet"),
        "0x90b8ec18" => Some("Executor revert: TransferFailed"),
        "0x8164f842" => Some("Executor revert: ApprovalFailed"),
        _ => None,
    }
}

fn hex_selectors(raw: &str) -> Vec<String> {
    let mut selectors = Vec::new();
    for part in raw.split(|c: char| {
        c.is_whitespace() || matches!(c, '"' | '\'' | ',' | ':' | '{' | '}' | '[' | ']')
    }) {
        let clean = part.trim_matches(|c: char| !c.is_ascii_hexdigit() && c != 'x');
        if clean.len() >= 10
            && clean.starts_with("0x")
            && clean[2..10].chars().all(|c| c.is_ascii_hexdigit())
        {
            selectors.push(clean[..10].to_ascii_lowercase());
        }
    }
    selectors
}

fn compact_revert_reason(raw: &str) -> String {
    if raw.contains("execution reverted data=-")
        || raw.contains("execution reverted data=null")
        || raw.contains("execution reverted data=\"0x\"")
    {
        return "router/no-revert-data".to_string();
    }
    short_reason(raw)
}

fn short_reason(raw: &str) -> String {
    let line = raw
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or(raw);
    if line.len() > 220 {
        format!("{}...", &line[..220])
    } else {
        line.to_string()
    }
}

fn compact_err(err: &anyhow::Error) -> String {
    short_reason(&format!("{err:#}"))
}
