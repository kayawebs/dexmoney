use alloy_primitives::{keccak256, Address, U256};
use anyhow::{Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::constants::{
    AERODROME_SLIPSTREAM_ROUTER, PANCAKE_V3_FACTORY, PANCAKE_V3_ROUTER,
};
use base_arb_common::types::{Candidate, DexKind, PoolVariant, SimulationResult};
use chrono::Utc;

const EXECUTOR_DEADLINE_SECS: i64 = 30;

pub async fn simulate(
    provider: &ChainProvider,
    settings: &Settings,
    operator: Address,
    candidate: &Candidate,
    min_simulated_profit_usdc: f64,
) -> SimulationResult {
    match simulate_inner(
        provider,
        settings,
        operator,
        candidate,
        min_simulated_profit_usdc,
    )
    .await
    {
        Ok(result) => result,
        Err(err) => {
            let raw_error = format!("{err:#}");
            SimulationResult {
                id: uuid::Uuid::new_v4(),
                opportunity_id: candidate.id,
                success: false,
                simulated_profit: U256::ZERO,
                gas_estimate: None,
                revert_reason: Some(format_revert_reason(&raw_error)),
                calldata: Vec::new(),
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
) -> Result<SimulationResult> {
    if candidate.is_expired(Utc::now()) {
        anyhow::bail!("candidate expired");
    }

    let _ = min_simulated_profit_usdc;
    let min_profit_units = candidate.min_profit;
    if candidate.expected_profit < min_profit_units {
        anyhow::bail!("profit below simulated threshold");
    }

    let executor = settings
        .executor_contract
        .filter(|address| *address != Address::ZERO)
        .context("EXECUTOR_CONTRACT is not configured")?;
    let deadline = U256::from((Utc::now().timestamp() + EXECUTOR_DEADLINE_SECS) as u64);
    let calldata = build_execute_calldata(candidate, min_profit_units, deadline, settings)?;
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

    Ok(SimulationResult {
        id: uuid::Uuid::new_v4(),
        opportunity_id: candidate.id,
        success: simulated_profit >= min_profit_units,
        simulated_profit,
        gas_estimate: Some(U256::from(350_000u64)),
        revert_reason: if simulated_profit < min_profit_units {
            Some("simulated profit below threshold".into())
        } else {
            None
        },
        calldata,
    })
}

pub fn build_execute_calldata(
    candidate: &Candidate,
    min_profit: U256,
    deadline: U256,
    settings: &Settings,
) -> Result<Vec<u8>> {
    let selector = &keccak256(
        b"executeWithOwnFunds(address,uint256,(uint8,address,address,address,address,uint24,bool,address)[],uint256,uint256)",
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

pub fn min_profit_failure_key(candidate: &Candidate) -> String {
    let mut raw = format!(
        "{}|{:#x}|{}|{}|{}",
        candidate.path.name,
        candidate.token_in,
        candidate.amount_in,
        candidate.min_profit,
        candidate.expected_profit
    );
    for step in &candidate.path.steps {
        raw.push_str(&format!(
            "|{:?}|{:?}|{:?}|{:#x}|{:#x}|{:#x}|{:?}|{:?}|{:?}",
            step.dex,
            step.variant,
            step.factory_address,
            step.pool,
            step.token_in,
            step.token_out,
            step.fee_bps,
            step.stable,
            step.tick_spacing
        ));
    }
    format!("{:#x}", keccak256(raw.as_bytes()))
}

pub fn route_failure_key(candidate: &Candidate) -> String {
    let mut raw = format!(
        "{}|{:#x}|{}",
        candidate.path.name, candidate.token_in, candidate.amount_in
    );
    append_step_fingerprint(&mut raw, candidate);
    format!("{:#x}", keccak256(raw.as_bytes()))
}

pub fn is_min_profit_not_met(simulation: &SimulationResult) -> bool {
    simulation.revert_reason.as_deref() == Some("Executor revert: MinProfitNotMet")
}

pub fn is_structural_route_failure(simulation: &SimulationResult) -> bool {
    matches!(
        simulation.revert_reason.as_deref(),
        Some("router/no-revert-data")
            | Some("Executor revert: PoolMismatch")
            | Some("Executor revert: UnsupportedDex")
            | Some("Executor revert: InvalidPath")
            | Some("Executor revert: InvalidTickSpacing")
    )
}

fn append_step_fingerprint(raw: &mut String, candidate: &Candidate) {
    for step in &candidate.path.steps {
        raw.push_str(&format!(
            "|{:?}|{:?}|{:?}|{:#x}|{:#x}|{:#x}|{:?}|{:?}|{:?}",
            step.dex,
            step.variant,
            step.factory_address,
            step.pool,
            step.token_in,
            step.token_out,
            step.fee_bps,
            step.stable,
            step.tick_spacing
        ));
    }
}

fn encode_steps(candidate: &Candidate, settings: &Settings) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(encode_u256(U256::from(candidate.path.steps.len())));

    for step in &candidate.path.steps {
        let router = router_for_step(step.dex, step.variant, settings)
            .with_context(|| format!("router missing for {:?} {:?}", step.dex, step.variant))?;
        out.extend(encode_u256(U256::from(executor_dex_kind(
            step.dex,
            step.variant,
        )?)));
        out.extend(encode_address(router));
        out.extend(encode_address(step.pool));
        out.extend(encode_address(step.token_in));
        out.extend(encode_address(step.token_out));
        out.extend(encode_u256(U256::from(router_fee_for_step(
            step.dex,
            step.variant,
            step.fee_bps,
            step.tick_spacing,
        )?)));
        out.extend(encode_bool(classic_stable_flag(step)));
        out.extend(encode_address(factory_for_step(step, settings)?));
    }

    Ok(out)
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

fn executor_dex_kind(dex: DexKind, variant: Option<PoolVariant>) -> Result<u8> {
    match (dex, variant) {
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeVolatile)) | (DexKind::Aerodrome, None) => {
            Ok(0)
        }
        (DexKind::Aerodrome, Some(PoolVariant::AerodromeSlipstream)) => Ok(1),
        (DexKind::UniswapV3, Some(PoolVariant::UniswapV3)) | (DexKind::UniswapV3, None) => Ok(2),
        (DexKind::PancakeSwap, Some(PoolVariant::PancakeV3)) | (DexKind::PancakeSwap, None) => {
            Ok(2)
        }
        _ => anyhow::bail!("dex and pool variant mismatch"),
    }
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

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, U256};
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    use base_arb_common::types::{ArbPath, OpportunityStatus, SwapStep};

    use super::{
        build_execute_calldata, decode_executor_error, decode_uint256_result, router_fee_for_step,
    };

    fn settings() -> base_arb_common::config::Settings {
        base_arb_common::config::Settings {
            base_rpc_http: "http://127.0.0.1:8545".into(),
            base_rpc_ws: "ws://127.0.0.1:8546".into(),
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
            v3_tick_refresh_interval_secs: 60,
            v3_tick_bitmap_word_radius: 8,
            v3_quote_safety_bps: 2,
            min_profit_failure_ttl_secs: 21_600,
            execution_min_priority_fee_wei: None,
            execution_priority_fee_multiplier_bps: 20_000,
            execution_max_fee_multiplier_bps: 30_000,
            execution_pending_replacement_blocks: 2,
            execution_replacement_fee_bump_bps: 12_500,
            execution_max_replacements: 3,
            execution_gas_profit_buffer_bps: 15_000,
            searcher_onchain_validate: true,
            monitor_web_password: None,
        }
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
