use alloy_primitives::{keccak256, Address, U256};
use anyhow::{Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::constants::{
    AERODROME_CLASSIC_FACTORY, AERODROME_SLIPSTREAM_ROUTER, PANCAKE_V3_FACTORY, PANCAKE_V3_ROUTER,
};
use base_arb_common::types::{Candidate, DexKind, PoolVariant, SimulationResult};
use chrono::Utc;

const EXECUTOR_DEADLINE_SECS: i64 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ApprovalRequirement {
    pub token: Address,
    pub spender: Address,
}

pub async fn simulate(
    provider: &ChainProvider,
    settings: &Settings,
    operator: Address,
    candidate: &Candidate,
    min_simulated_profit_usdc: f64,
) -> SimulationResult {
    let observed_block = provider.get_block_number().await.ok();
    match simulate_inner(
        provider,
        settings,
        operator,
        candidate,
        min_simulated_profit_usdc,
        observed_block,
    )
    .await
    {
        Ok(result) => result,
        Err(err) => {
            let raw_error = format!("{err:#}");
            let calldata = build_simulation_calldata(settings, candidate, candidate.min_profit)
                .unwrap_or_default();
            SimulationResult {
                id: uuid::Uuid::new_v4(),
                opportunity_id: candidate.id,
                success: false,
                path_name: Some(candidate.path.name.clone()),
                token_in: Some(candidate.token_in),
                amount_in: Some(candidate.amount_in),
                expected_profit: Some(candidate.expected_profit),
                min_profit: Some(candidate.min_profit),
                simulated_profit: U256::ZERO,
                gas_estimate: None,
                block_number: observed_block,
                base_fee_per_gas: None,
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
                gas_cost_cap: None,
                gas_cost_expected: None,
                net_simulated_profit: None,
                revert_reason: Some(format_revert_reason(&raw_error)),
                calldata,
            }
        }
    }
}

async fn simulate_inner(
    provider: &ChainProvider,
    settings: &Settings,
    operator: Address,
    candidate: &Candidate,
    min_simulated_profit_usdc: f64,
    observed_block: Option<u64>,
) -> Result<SimulationResult> {
    if candidate.is_expired(Utc::now()) {
        anyhow::bail!("candidate expired");
    }

    let _ = min_simulated_profit_usdc;
    let min_profit_units = candidate.min_profit;
    if candidate.expected_profit < min_profit_units {
        anyhow::bail!("profit below simulated threshold");
    }

    let executor = executor_for_candidate(settings, candidate)?;
    let calldata = build_simulation_calldata(settings, candidate, min_profit_units)?;
    let data = format!("0x{}", hex::encode(&calldata));

    let raw_result = provider
        .eth_call_from(
            Some(operator),
            executor,
            &data,
            "Executor executeWithOwnFunds",
        )
        .await?;
    let simulated_profit = decode_uint256_result(&raw_result).unwrap_or(candidate.expected_profit);
    let gas_estimate = provider
        .estimate_gas(operator, executor, &data)
        .await
        .context("failed to estimate executor tx gas after successful eth_call")?;
    let fees = simulation_fee_suggestion(provider, settings).await?;
    let gas_cost_cap = gas_estimate.saturating_mul(fees.max_fee_per_gas);
    let gas_cost_expected = gas_estimate.saturating_mul(fees.expected_fee_per_gas);
    let net_simulated_profit = if candidate.token_in == settings.weth_address {
        Some(simulated_profit.saturating_sub(gas_cost_expected))
    } else {
        None
    };
    let profit_meets_threshold = simulated_profit >= min_profit_units;
    let block_number = provider.get_block_number().await.ok().or(observed_block);

    Ok(SimulationResult {
        id: uuid::Uuid::new_v4(),
        opportunity_id: candidate.id,
        success: profit_meets_threshold,
        path_name: Some(candidate.path.name.clone()),
        token_in: Some(candidate.token_in),
        amount_in: Some(candidate.amount_in),
        expected_profit: Some(candidate.expected_profit),
        min_profit: Some(candidate.min_profit),
        simulated_profit,
        gas_estimate: Some(gas_estimate),
        block_number,
        base_fee_per_gas: Some(fees.base_fee_per_gas),
        max_fee_per_gas: Some(fees.max_fee_per_gas),
        max_priority_fee_per_gas: Some(fees.max_priority_fee_per_gas),
        gas_cost_cap: Some(gas_cost_cap),
        gas_cost_expected: Some(gas_cost_expected),
        net_simulated_profit,
        revert_reason: if !profit_meets_threshold {
            Some("simulated profit below threshold".into())
        } else {
            None
        },
        calldata,
    })
}

fn build_simulation_calldata(
    settings: &Settings,
    candidate: &Candidate,
    min_profit: U256,
) -> Result<Vec<u8>> {
    let deadline = U256::from((Utc::now().timestamp() + EXECUTOR_DEADLINE_SECS) as u64);
    build_execute_calldata(candidate, min_profit, deadline, settings)
}

pub fn build_live_execution_calldata(
    settings: &Settings,
    candidate: &Candidate,
) -> Result<Vec<u8>> {
    build_simulation_calldata(settings, candidate, candidate.min_profit)
}

pub fn executor_for_candidate(settings: &Settings, candidate: &Candidate) -> Result<Address> {
    let requires_hub = candidate_requires_direct_execution(candidate, settings);
    let selected = if requires_hub {
        settings
            .executor_contract_multihop
            .or(settings.executor_contract)
    } else if candidate.path.steps.len() == 2 {
        settings
            .executor_contract_2hop
            .or(settings.executor_contract)
    } else {
        settings
            .executor_contract_multihop
            .or(settings.executor_contract)
    };
    selected
        .filter(|address| *address != Address::ZERO)
        .with_context(|| {
            if requires_hub {
                "EXECUTOR_CONTRACT_MULTIHOP or EXECUTOR_CONTRACT is required for direct pool execution"
            } else if candidate.path.steps.len() == 2 {
                "EXECUTOR_CONTRACT_2HOP or EXECUTOR_CONTRACT is not configured"
            } else {
                "EXECUTOR_CONTRACT_MULTIHOP or EXECUTOR_CONTRACT is not configured"
            }
        })
}

struct SimulationFeeSuggestion {
    base_fee_per_gas: U256,
    expected_fee_per_gas: U256,
    max_fee_per_gas: U256,
    max_priority_fee_per_gas: U256,
}

async fn simulation_fee_suggestion(
    provider: &ChainProvider,
    settings: &Settings,
) -> Result<SimulationFeeSuggestion> {
    let suggested = provider.suggested_eip1559_fee_details().await?;
    let min_priority = parse_optional_u256(settings.execution_min_priority_fee_wei.as_deref())?
        .unwrap_or(U256::ZERO);
    let priority = apply_bps(
        suggested.max_priority_fee_per_gas,
        settings.execution_priority_fee_multiplier_bps,
    )
    .max(min_priority);
    let base_component = apply_bps(
        suggested.base_fee_per_gas,
        settings.execution_max_fee_multiplier_bps,
    );
    let max_fee = base_component
        .saturating_add(priority)
        .max(suggested.max_fee_per_gas);

    Ok(SimulationFeeSuggestion {
        base_fee_per_gas: suggested.base_fee_per_gas,
        expected_fee_per_gas: suggested.base_fee_per_gas.saturating_add(priority),
        max_fee_per_gas: max_fee,
        max_priority_fee_per_gas: priority,
    })
}

pub fn build_execute_calldata(
    candidate: &Candidate,
    min_profit: U256,
    deadline: U256,
    settings: &Settings,
) -> Result<Vec<u8>> {
    let selector = &keccak256(
        b"executeWithOwnFunds(address,uint256,(uint8,address,address,address,address,uint24,bool,address,bytes)[],uint256,uint256)",
    )[..4];
    let mut out = Vec::new();
    out.extend_from_slice(selector);
    out.extend(encode_address(candidate.token_in));
    out.extend(encode_u256(candidate.amount_in));
    out.extend(encode_u256(U256::from(160u64)));
    out.extend(encode_u256(min_profit));
    out.extend(encode_u256(deadline));
    out.extend(encode_steps(candidate, settings)?);
    Ok(out)
}

pub fn required_token_approvals(
    candidate: &Candidate,
    settings: &Settings,
) -> Result<Vec<ApprovalRequirement>> {
    let mut approvals = Vec::new();
    for step in &candidate.path.steps {
        if step_execution_kind(step, settings)?.is_direct() {
            continue;
        }
        let spender = router_for_step(step.dex, step.variant, settings)
            .with_context(|| format!("router missing for {:?} {:?}", step.dex, step.variant))?;
        approvals.push(ApprovalRequirement {
            token: step.token_in,
            spender,
        });
    }
    approvals.sort();
    approvals.dedup();
    Ok(approvals)
}

fn encode_steps(candidate: &Candidate, settings: &Settings) -> Result<Vec<u8>> {
    encode_swap_steps(candidate.path.steps.iter(), |step| {
        let execution_kind = step_execution_kind(step, settings)?;
        let router = if execution_kind.is_direct() {
            Address::ZERO
        } else {
            router_for_step(step.dex, step.variant, settings)
                .with_context(|| format!("router missing for {:?} {:?}", step.dex, step.variant))?
        };
        Ok(EncodedSwapStep {
            dex: execution_kind.abi_value(),
            router,
            pool: step.pool,
            token_in: step.token_in,
            token_out: step.token_out,
            fee: router_fee_for_step(step.dex, step.variant, step.fee_bps, step.tick_spacing)?,
            stable: classic_stable_flag(step),
            factory: factory_for_step(step, settings)?,
            data: Vec::new(),
        })
    })
}

struct EncodedSwapStep {
    dex: u8,
    router: Address,
    pool: Address,
    token_in: Address,
    token_out: Address,
    fee: u32,
    stable: bool,
    factory: Address,
    data: Vec<u8>,
}

fn encode_swap_steps<'a, I, F>(steps: I, mut encode_step: F) -> Result<Vec<u8>>
where
    I: IntoIterator<Item = &'a base_arb_common::types::SwapStep>,
    F: FnMut(&'a base_arb_common::types::SwapStep) -> Result<EncodedSwapStep>,
{
    let encoded_steps = steps
        .into_iter()
        .map(|step| encode_step(step).map(encode_swap_step_tuple))
        .collect::<Result<Vec<_>>>()?;

    let mut out = Vec::new();
    out.extend(encode_u256(U256::from(encoded_steps.len())));
    let mut offset = 32usize
        .checked_mul(encoded_steps.len())
        .context("swap step offset overflow")?;
    for step in &encoded_steps {
        out.extend(encode_u256(U256::from(offset)));
        offset = offset
            .checked_add(step.len())
            .context("swap step offset overflow")?;
    }
    for step in encoded_steps {
        out.extend(step);
    }
    Ok(out)
}

fn encode_swap_step_tuple(step: EncodedSwapStep) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend(encode_u256(U256::from(step.dex)));
    out.extend(encode_address(step.router));
    out.extend(encode_address(step.pool));
    out.extend(encode_address(step.token_in));
    out.extend(encode_address(step.token_out));
    out.extend(encode_u256(U256::from(step.fee)));
    out.extend(encode_bool(step.stable));
    out.extend(encode_address(step.factory));
    out.extend(encode_u256(U256::from(9 * 32)));
    out.extend(encode_bytes(&step.data));
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecutorStepKind {
    AerodromeClassic,
    AerodromeSlipstream,
    UniswapV3Router,
    PancakeV3Router,
    DirectV2,
    DirectV3,
}

impl ExecutorStepKind {
    fn abi_value(self) -> u8 {
        match self {
            Self::AerodromeClassic => 0,
            Self::AerodromeSlipstream => 1,
            Self::UniswapV3Router => 2,
            Self::PancakeV3Router => 3,
            Self::DirectV2 => 4,
            Self::DirectV3 => 5,
        }
    }

    fn is_direct(self) -> bool {
        matches!(self, Self::DirectV2 | Self::DirectV3)
    }
}

fn candidate_requires_direct_execution(candidate: &Candidate, settings: &Settings) -> bool {
    candidate
        .path
        .steps
        .iter()
        .any(|step| step_execution_kind(step, settings).is_ok_and(|kind| kind.is_direct()))
}

fn step_execution_kind(
    step: &base_arb_common::types::SwapStep,
    settings: &Settings,
) -> Result<ExecutorStepKind> {
    match (step.dex, step.variant) {
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None) => {
            let default_factory = settings
                .aerodrome_pool_factory
                .or_else(|| AERODROME_CLASSIC_FACTORY.parse().ok());
            if !step.stable.unwrap_or(false)
                && is_unknown_router_factory(step.factory_address, default_factory)
            {
                Ok(ExecutorStepKind::DirectV2)
            } else {
                Ok(ExecutorStepKind::AerodromeClassic)
            }
        }
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream)) => {
            Ok(ExecutorStepKind::AerodromeSlipstream)
        }
        (DexKind::UniswapV3, Some(PoolVariant::UniswapV3)) | (DexKind::UniswapV3, None) => {
            if is_unknown_router_factory(step.factory_address, settings.uniswap_v3_factory) {
                Ok(ExecutorStepKind::DirectV3)
            } else {
                Ok(ExecutorStepKind::UniswapV3Router)
            }
        }
        (DexKind::PancakeSwap, Some(PoolVariant::PancakeV3)) | (DexKind::PancakeSwap, None) => {
            let pancake_factory = settings
                .pancake_v3_factory
                .or_else(|| PANCAKE_V3_FACTORY.parse().ok());
            if is_unknown_router_factory(step.factory_address, pancake_factory) {
                Ok(ExecutorStepKind::DirectV3)
            } else {
                Ok(ExecutorStepKind::PancakeV3Router)
            }
        }
        _ => anyhow::bail!("dex and pool variant mismatch"),
    }
}

fn is_unknown_router_factory(
    step_factory: Option<Address>,
    configured_factory: Option<Address>,
) -> bool {
    let Some(factory) = step_factory else {
        return false;
    };
    let Some(configured) = configured_factory else {
        return true;
    };
    factory != configured
}

fn router_fee_for_step(
    dex: DexKind,
    variant: Option<PoolVariant>,
    fee_bps: Option<u32>,
    tick_spacing: Option<i32>,
) -> Result<u32> {
    let fee_bps = fee_bps.unwrap_or_default();
    Ok(match (dex, variant) {
        (DexKind::UniswapV3, Some(PoolVariant::UniswapV3))
        | (DexKind::UniswapV3, None)
        | (DexKind::PancakeSwap, Some(PoolVariant::PancakeV3))
        | (DexKind::PancakeSwap, None) => fee_bps
            .checked_mul(100)
            .ok_or_else(|| anyhow::anyhow!("V3 fee bps overflow"))?,
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream)) => {
            let tick_spacing =
                tick_spacing.context("tick_spacing is required for Aerodrome Slipstream")?;
            if tick_spacing <= 0 {
                anyhow::bail!("invalid Aerodrome Slipstream tick_spacing {tick_spacing}");
            }
            u32::try_from(tick_spacing)?
        }
        _ => fee_bps,
    })
}

fn classic_stable_flag(step: &base_arb_common::types::SwapStep) -> bool {
    matches!(
        (step.dex, step.variant),
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None)
    ) && step.stable.unwrap_or(false)
}

fn factory_for_step(
    step: &base_arb_common::types::SwapStep,
    settings: &Settings,
) -> Result<Address> {
    if let Some(factory) = step.factory_address {
        return Ok(factory);
    }

    let dex = step.dex;
    let variant = step.variant;
    if matches!(
        (dex, variant),
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None)
    ) {
        settings
            .aerodrome_pool_factory
            .context("AERODROME_POOL_FACTORY is required for Aerodrome Classic execution")
    } else if matches!(
        (dex, variant),
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream))
    ) {
        Ok(settings
            .aerodrome_slipstream_factory
            .unwrap_or(Address::ZERO))
    } else if matches!(
        (dex, variant),
        (DexKind::UniswapV3, Some(PoolVariant::UniswapV3)) | (DexKind::UniswapV3, None)
    ) {
        Ok(settings.uniswap_v3_factory.unwrap_or(Address::ZERO))
    } else if matches!(
        (dex, variant),
        (DexKind::PancakeSwap, Some(PoolVariant::PancakeV3)) | (DexKind::PancakeSwap, None)
    ) {
        Ok(settings
            .pancake_v3_factory
            .or_else(|| PANCAKE_V3_FACTORY.parse().ok())
            .unwrap_or(Address::ZERO))
    } else {
        Ok(Address::ZERO)
    }
}

fn router_for_step(
    dex: DexKind,
    variant: Option<PoolVariant>,
    settings: &Settings,
) -> Option<Address> {
    match (dex, variant) {
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

fn encode_address(address: Address) -> Vec<u8> {
    let mut out = vec![0u8; 12];
    out.extend_from_slice(address.as_slice());
    out
}

fn encode_u256(value: U256) -> Vec<u8> {
    let mut out = [0u8; 32];
    value
        .to_be_bytes::<32>()
        .iter()
        .enumerate()
        .for_each(|(i, b)| {
            out[i] = *b;
        });
    out.to_vec()
}

fn encode_bool(value: bool) -> Vec<u8> {
    encode_u256(U256::from(u8::from(value)))
}

fn encode_bytes(value: &[u8]) -> Vec<u8> {
    let mut out = encode_u256(U256::from(value.len()));
    out.extend_from_slice(value);
    let padding = (32 - (value.len() % 32)) % 32;
    out.extend(std::iter::repeat_n(0u8, padding));
    out
}

fn decode_uint256_result(raw: &str) -> Option<U256> {
    let clean = raw.trim_start_matches("0x");
    if clean.len() < 64 {
        return None;
    }
    U256::from_str_radix(&clean[0..64], 16).ok()
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
    if raw.contains("candidate expired") {
        return "candidate expired".to_string();
    }
    if raw.contains("execution reverted data=-")
        || raw.contains("execution reverted data=null")
        || raw.contains("execution reverted data=\"0x\"")
    {
        return "router/no-revert-data".to_string();
    }
    let mut lines = raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return tail_chars(raw, 240);
    }
    if lines.len() > 3 {
        lines.truncate(3);
    }
    let joined = lines.join(" | ");
    if joined.len() > 240 {
        tail_chars(&joined, 240)
    } else {
        joined
    }
}

fn tail_chars(value: &str, max_chars: usize) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() <= max_chars {
        return value.to_string();
    }
    let start = chars.len().saturating_sub(max_chars);
    format!("...{}", chars[start..].iter().collect::<String>())
}

fn apply_bps(value: U256, bps: u64) -> U256 {
    value
        .saturating_mul(U256::from(bps))
        .checked_div(U256::from(10_000u64))
        .unwrap_or(value)
}

fn parse_optional_u256(raw: Option<&str>) -> Result<Option<U256>> {
    raw.map(|value| U256::from_str_radix(value.trim_start_matches("0x"), 10))
        .transpose()
        .context("invalid U256 decimal config value")
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, U256};
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    use base_arb_common::types::{ArbPath, OpportunityStatus, SwapStep};

    use super::{
        build_execute_calldata, decode_executor_error, decode_uint256_result,
        executor_for_candidate, required_token_approvals, router_fee_for_step,
    };

    fn settings() -> base_arb_common::config::Settings {
        base_arb_common::config::Settings {
            base_rpc_http: "http://127.0.0.1:8545".into(),
            base_rpc_ws: "ws://127.0.0.1:8546".into(),
            base_rpc_flashblocks_ws: None,
            postgres_url: "postgres://user:password@localhost:5632/base_arb".into(),
            redis_url: "redis://127.0.0.1:6779".into(),
            chain_id: 8453,
            usdc_address: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            weth_address: address!("4200000000000000000000000000000000000006"),
            aerodrome_router: Some(address!("1111111111111111111111111111111111111111")),
            aerodrome_pool_factory: None,
            aerodrome_slipstream_router: Some(address!("4444444444444444444444444444444444444444")),
            aerodrome_slipstream_factory: None,
            aerodrome_usdc_weth_pool: None,
            uniswap_v3_factory: None,
            uniswap_v3_router: Some(address!("2222222222222222222222222222222222222222")),
            uniswap_v3_quoter: None,
            uniswap_v3_usdc_weth_500_pool: None,
            uniswap_v3_usdc_weth_3000_pool: None,
            pancake_v3_factory: None,
            pancake_v3_router: None,
            executor_contract: Some(address!("3333333333333333333333333333333333333333")),
            executor_contract_2hop: None,
            executor_contract_multihop: None,
            executor_owner_private_key: None,
            deployer_private_key: None,
            eoa_address_1: Some(address!("4444444444444444444444444444444444444444")),
            eoa_private_key_1: None,
            search_amount_usdc: Some("10,30,50,100".into()),
            min_expected_profit_usdc: 0.01,
            min_simulated_profit_usdc: 0.005,
            candidate_ttl_ms: 500,
            max_pool_state_age_ms: 300_000,
            max_price_impact_bps: 50,
            pool_active_refresh_interval_secs: 60,
            pool_active_refresh_batch_size: 25,
            market_data_flashblocks_enabled: true,
            market_data_global_pool_discovery_enabled: true,
            competitor_pool_discovery_enabled: true,
            competitor_collector_address: Some(address!(
                "0629da86af5a4ae1ba5e1589b13702558d0fb056"
            )),
            competitor_pool_discovery_interval_ms: 1000,
            competitor_pool_discovery_lookback_blocks: 100,
            competitor_pool_discovery_max_block_span: 25,
            searcher_multihop_enabled: true,
            aerodrome_fee_refresh_interval_secs: 15,
            v3_tick_refresh_interval_secs: 60,
            v3_tick_bitmap_word_radius: 8,
            v3_quote_safety_bps: 2,
            quote_max_state_block_lag: 0,
            min_profit_failure_ttl_secs: 21_600,
            execution_min_priority_fee_wei: None,
            execution_priority_fee_multiplier_bps: 20_000,
            execution_max_fee_multiplier_bps: 30_000,
            execution_pending_replacement_blocks: 2,
            execution_replacement_fee_bump_bps: 12_500,
            execution_max_replacements: 3,
            execution_gas_profit_buffer_bps: 15_000,
            execution_max_candidate_lag_blocks: 1,
            execution_submit_enabled: false,
            execution_eoa_pool_size: 5,
            execution_worker_min_balance_wei: Some("200000000000000".into()),
            execution_worker_target_balance_wei: Some("500000000000000".into()),
            execution_failure_rate_min_txs: 10,
            execution_min_success_rate_bps: 2_000,
            monitor_web_password: None,
        }
    }

    fn candidate_with_step_count(step_count: usize) -> base_arb_common::types::Candidate {
        let step = SwapStep {
            dex: base_arb_common::types::DexKind::UniswapV3,
            variant: Some(base_arb_common::types::PoolVariant::UniswapV3),
            factory_address: None,
            pool: address!("5555555555555555555555555555555555555555"),
            token_in: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            token_out: address!("4200000000000000000000000000000000000006"),
            fee_bps: Some(5),
            stable: None,
            tick_spacing: None,
        };
        base_arb_common::types::Candidate {
            id: Uuid::new_v4(),
            created_at: Utc::now(),
            expires_at: Utc::now() + Duration::seconds(1),
            block_number: 1,
            strategy: "demo".into(),
            token_in: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            amount_in: U256::from(1_000_000u64),
            expected_amount_out: U256::from(1_010_000u64),
            expected_profit: U256::from(10_000u64),
            min_profit: U256::from(5_000u64),
            price_impact_bps: 1,
            path: ArbPath {
                name: "demo".into(),
                steps: vec![step; step_count],
                diagnostics: None,
            },
            status: OpportunityStatus::Created,
        }
    }

    #[test]
    fn selects_executor_by_step_count() {
        let mut settings = settings();
        settings.executor_contract = Some(address!("3333333333333333333333333333333333333333"));
        settings.executor_contract_2hop =
            Some(address!("2222222222222222222222222222222222222222"));
        settings.executor_contract_multihop =
            Some(address!("4444444444444444444444444444444444444444"));

        assert_eq!(
            executor_for_candidate(&settings, &candidate_with_step_count(2)).unwrap(),
            address!("2222222222222222222222222222222222222222")
        );
        assert_eq!(
            executor_for_candidate(&settings, &candidate_with_step_count(3)).unwrap(),
            address!("4444444444444444444444444444444444444444")
        );
        assert_eq!(
            executor_for_candidate(&settings, &candidate_with_step_count(4)).unwrap(),
            address!("4444444444444444444444444444444444444444")
        );
    }

    #[test]
    fn direct_v3_uses_multihop_executor_and_needs_no_router_approval() {
        let mut settings = settings();
        settings.uniswap_v3_factory = Some(address!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        settings.executor_contract = Some(address!("3333333333333333333333333333333333333333"));
        settings.executor_contract_2hop =
            Some(address!("2222222222222222222222222222222222222222"));
        settings.executor_contract_multihop =
            Some(address!("4444444444444444444444444444444444444444"));

        let mut candidate = candidate_with_step_count(2);
        candidate.path.steps[0].factory_address =
            Some(address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));
        candidate.path.steps[1].factory_address =
            Some(address!("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"));

        assert_eq!(
            executor_for_candidate(&settings, &candidate).unwrap(),
            address!("4444444444444444444444444444444444444444")
        );
        assert!(required_token_approvals(&candidate, &settings)
            .unwrap()
            .is_empty());

        let calldata =
            build_execute_calldata(&candidate, U256::from(5_000u64), U256::from(123), &settings)
                .unwrap();
        let array_start = 4 + 5 * 32;
        let first_element_offset =
            U256::from_be_slice(&calldata[array_start + 32..array_start + 64]).to::<usize>();
        let first_step_kind_offset = array_start + 32 + first_element_offset;
        assert_eq!(
            U256::from_be_slice(&calldata[first_step_kind_offset..first_step_kind_offset + 32]),
            U256::from(5u64)
        );
    }

    #[test]
    fn builds_executor_calldata() {
        let candidate = base_arb_common::types::Candidate {
            id: Uuid::new_v4(),
            created_at: Utc::now(),
            expires_at: Utc::now() + Duration::seconds(1),
            block_number: 1,
            strategy: "demo".into(),
            token_in: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
            amount_in: U256::from(1_000_000u64),
            expected_amount_out: U256::from(1_010_000u64),
            expected_profit: U256::from(10_000u64),
            min_profit: U256::from(5_000u64),
            price_impact_bps: 1,
            path: ArbPath {
                name: "demo".into(),
                steps: vec![SwapStep {
                    dex: base_arb_common::types::DexKind::UniswapV3,
                    variant: Some(base_arb_common::types::PoolVariant::UniswapV3),
                    factory_address: None,
                    pool: address!("5555555555555555555555555555555555555555"),
                    token_in: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
                    token_out: address!("4200000000000000000000000000000000000006"),
                    fee_bps: Some(5),
                    stable: None,
                    tick_spacing: None,
                }],
                diagnostics: None,
            },
            status: OpportunityStatus::Created,
        };

        let calldata = build_execute_calldata(
            &candidate,
            U256::from(5_000u64),
            U256::from(123),
            &settings(),
        )
        .unwrap();

        assert_eq!(calldata.len() % 32, 4);
        assert!(calldata.len() > 4 + 5 * 32);
    }

    #[test]
    fn decodes_uint256_result() {
        assert_eq!(
            decode_uint256_result(
                "0x000000000000000000000000000000000000000000000000000000000000002a"
            ),
            Some(U256::from(42u64))
        );
    }

    #[test]
    fn converts_v3_fee_bps_to_router_fee_tier() {
        assert_eq!(
            router_fee_for_step(
                base_arb_common::types::DexKind::UniswapV3,
                Some(base_arb_common::types::PoolVariant::UniswapV3),
                Some(5),
                None
            )
            .unwrap(),
            500
        );
        assert_eq!(
            router_fee_for_step(
                base_arb_common::types::DexKind::PancakeSwap,
                Some(base_arb_common::types::PoolVariant::PancakeV3),
                Some(25),
                None
            )
            .unwrap(),
            2500
        );
    }

    #[test]
    fn encodes_slipstream_tick_spacing_as_router_fee_field() {
        assert_eq!(
            router_fee_for_step(
                base_arb_common::types::DexKind::Aerodrome,
                Some(base_arb_common::types::PoolVariant::AerodromeSlipstream),
                Some(30),
                Some(100)
            )
            .unwrap(),
            100
        );
    }

    #[test]
    fn decodes_executor_revert_selector_from_rpc_error() {
        let raw = r#"eth_call Executor executeWithOwnFunds to=0x63dfe526981eae8688b6bfaf5cfec575d8e89a43 data=0x51589239 caused by rpc eth_call failed: code=3 message=execution reverted data="0xf4d678b8""#;
        assert_eq!(
            decode_executor_error(raw),
            Some("Executor revert: InsufficientBalance")
        );
    }

    #[test]
    fn formats_decoded_revert_without_calldata_blob() {
        let raw = r#"eth_call Executor executeWithOwnFunds to=0x63dfe526981eae8688b6bfaf5cfec575d8e89a43 data=0x51589239 caused by rpc eth_call failed: code=3 message=execution reverted data="0xd433008b""#;
        assert_eq!(
            super::format_revert_reason(raw),
            "Executor revert: MinProfitNotMet"
        );
    }

    #[test]
    fn formats_router_revert_without_data() {
        let raw = r#"eth_call Executor executeWithOwnFunds to=0x63dfe526981eae8688b6bfaf5cfec575d8e89a43 data=0x51589239 caused by rpc eth_call failed: code=3 message=execution reverted data=-"#;
        assert_eq!(super::format_revert_reason(raw), "router/no-revert-data");
    }

    #[test]
    fn unknown_revert_keeps_tail_instead_of_calldata_prefix() {
        let raw = format!(
            "eth_call Executor executeWithOwnFunds to=0x63dfe526981eae8688b6bfaf5cfec575d8e89a43 data=0x{}: rpc eth_call failed: code=3 message=execution reverted with unknown selector",
            "51".repeat(300)
        );
        let formatted = super::format_revert_reason(&raw);
        assert!(formatted.starts_with("..."));
        assert!(formatted.contains("execution reverted"));
        assert!(!formatted.contains("executeWithOwnFunds to="));
    }
}
