use alloy_primitives::{keccak256, Address, U256};
use anyhow::{Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::types::{Candidate, DexKind, SimulationResult};
use chrono::Utc;

const EXECUTOR_DEADLINE_SECS: i64 = 30;

pub async fn simulate(
    provider: &ChainProvider,
    settings: &Settings,
    candidate: &Candidate,
    min_simulated_profit_usdc: f64,
) -> SimulationResult {
    match simulate_inner(provider, settings, candidate, min_simulated_profit_usdc).await {
        Ok(result) => result,
        Err(err) => SimulationResult {
            opportunity_id: candidate.id,
            success: false,
            simulated_profit: U256::ZERO,
            gas_estimate: None,
            revert_reason: Some(err.to_string()),
            calldata: Vec::new(),
        },
    }
}

async fn simulate_inner(
    provider: &ChainProvider,
    settings: &Settings,
    candidate: &Candidate,
    min_simulated_profit_usdc: f64,
) -> Result<SimulationResult> {
    if candidate.is_expired(Utc::now()) {
        anyhow::bail!("candidate expired");
    }

    let min_profit_units = U256::from((min_simulated_profit_usdc * 1_000_000.0) as u64);
    if candidate.expected_profit < min_profit_units {
        anyhow::bail!("profit below simulated threshold");
    }

    let executor = settings
        .executor_contract
        .filter(|address| *address != Address::ZERO)
        .context("EXECUTOR_CONTRACT is not configured")?;
    let operator = settings
        .eoa_address_1
        .filter(|address| *address != Address::ZERO)
        .context("EOA_ADDRESS_1 is required for eth_call operator simulation")?;

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
        b"executeWithOwnFunds(address,uint256,(uint8,address,address,address,address,uint24,bytes)[],uint256,uint256)",
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

fn encode_steps(candidate: &Candidate, settings: &Settings) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend(encode_u256(U256::from(candidate.path.steps.len())));

    let heads_len = candidate
        .path
        .steps
        .len()
        .checked_mul(7 * 32)
        .context("steps head length overflow")?;
    let mut tails = Vec::new();

    for step in &candidate.path.steps {
        let router = router_for_step(step.dex, settings)
            .with_context(|| format!("router missing for {:?}", step.dex))?;
        out.extend(encode_u256(U256::from(match step.dex {
            DexKind::Aerodrome => 0u8,
            DexKind::UniswapV3 => 1u8,
        })));
        out.extend(encode_address(router));
        out.extend(encode_address(step.pool));
        out.extend(encode_address(step.token_in));
        out.extend(encode_address(step.token_out));
        out.extend(encode_u256(U256::from(step.fee_bps.unwrap_or_default())));
        out.extend(encode_u256(U256::from(heads_len + tails.len())));
        tails.extend(encode_bytes(&[]));
    }

    out.extend(tails);
    Ok(out)
}

fn router_for_step(dex: DexKind, settings: &Settings) -> Option<Address> {
    match dex {
        DexKind::Aerodrome => settings.aerodrome_router,
        DexKind::UniswapV3 => settings.uniswap_v3_router,
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

fn encode_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend(encode_u256(U256::from(bytes.len())));
    out.extend_from_slice(bytes);
    let padding = (32 - (bytes.len() % 32)) % 32;
    out.extend(vec![0u8; padding]);
    out
}

fn decode_uint256_result(raw: &str) -> Option<U256> {
    let clean = raw.trim_start_matches("0x");
    if clean.len() < 64 {
        return None;
    }
    U256::from_str_radix(&clean[0..64], 16).ok()
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, U256};
    use chrono::{Duration, Utc};
    use uuid::Uuid;

    use base_arb_common::types::{ArbPath, OpportunityStatus, SwapStep};

    use super::{build_execute_calldata, decode_uint256_result};

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
            aerodrome_slipstream_factory: None,
            aerodrome_usdc_weth_pool: None,
            uniswap_v3_factory: None,
            uniswap_v3_router: Some(address!("2222222222222222222222222222222222222222")),
            uniswap_v3_quoter: None,
            uniswap_v3_usdc_weth_500_pool: None,
            uniswap_v3_usdc_weth_3000_pool: None,
            executor_contract: Some(address!("3333333333333333333333333333333333333333")),
            eoa_address_1: Some(address!("4444444444444444444444444444444444444444")),
            eoa_private_key_1: None,
            min_expected_profit_usdc: 0.01,
            min_simulated_profit_usdc: 0.005,
            candidate_ttl_ms: 500,
            max_price_impact_bps: 50,
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
                    pool: address!("5555555555555555555555555555555555555555"),
                    token_in: address!("833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
                    token_out: address!("4200000000000000000000000000000000000006"),
                    fee_bps: Some(5),
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
}
