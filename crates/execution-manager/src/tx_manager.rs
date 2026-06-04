use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, Result};
use base_arb_chain::provider::{ChainProvider, Eip1559FeeSuggestion, TxReceipt};
use base_arb_common::config::Settings;
use base_arb_common::types::{Candidate, SimulationResult, TxResult, TxStatus};
use chrono::Utc;
use ethers_core::types::{
    transaction::eip2718::TypedTransaction, Bytes, Eip1559TransactionRequest, NameOrAddress,
    U256 as EthersU256, U64,
};
use ethers_signers::{LocalWallet, Signer};

const GAS_LIMIT_MULTIPLIER_BPS: u64 = 12_000;
const EXECUTED_EVENT_TOPIC: &str =
    "0xe953ae62f4f69be1c6d943cb68d93d288f23ffae7332b84196d46e9e778b23b2";

#[derive(Debug, Clone)]
pub struct ExecutionWallet {
    wallet: LocalWallet,
    address: Address,
}

#[derive(Debug, Clone)]
pub struct Submission {
    pub tx_hash: B256,
    pub nonce: u64,
    pub simulation_id: uuid::Uuid,
    pub submitted_block: u64,
    pub gas_limit: U256,
    pub max_fee_per_gas: U256,
    pub max_priority_fee_per_gas: U256,
}

impl ExecutionWallet {
    pub fn from_private_key(private_key: &str, chain_id: u64) -> Result<Self> {
        let wallet = private_key
            .parse::<LocalWallet>()
            .context("failed to parse EOA_PRIVATE_KEY_1")?
            .with_chain_id(chain_id);
        let address = ethers_address_to_alloy(wallet.address());
        Ok(Self { wallet, address })
    }

    pub fn address(&self) -> Address {
        self.address
    }
}

pub async fn submit_candidate(
    provider: &ChainProvider,
    wallet: &ExecutionWallet,
    settings: &Settings,
    candidate: &Candidate,
    simulation: &SimulationResult,
    nonce: u64,
) -> Result<Submission> {
    let executor = settings
        .executor_contract
        .filter(|address| *address != Address::ZERO)
        .context("EXECUTOR_CONTRACT is not configured")?;
    if simulation.calldata.is_empty() {
        anyhow::bail!("simulation calldata is empty");
    }

    let data_hex = format!("0x{}", hex::encode(&simulation.calldata));
    let estimated_gas = provider
        .estimate_gas(wallet.address, executor, &data_hex)
        .await
        .context("failed to estimate executor tx gas")?;
    let gas_limit = bump_gas_limit(estimated_gas);
    let fees = aggressive_fee_suggestion(provider, settings).await?;
    ensure_expected_profit_covers_gas(
        settings,
        candidate,
        gas_limit,
        fees.base_fee_per_gas
            .saturating_add(fees.max_priority_fee_per_gas),
    )?;
    if candidate.is_expired(Utc::now()) {
        anyhow::bail!("candidate expired before tx broadcast");
    }

    let submitted_block = provider.get_block_number().await?;
    let tx_hash = sign_and_send(
        provider,
        wallet,
        settings,
        executor,
        &simulation.calldata,
        nonce,
        gas_limit,
        fees.max_fee_per_gas,
        fees.max_priority_fee_per_gas,
    )
    .await?;

    tracing::info!(
        candidate_id = %candidate.id,
        tx_hash = %tx_hash,
        nonce,
        gas_limit = %gas_limit,
        max_fee_per_gas = %fees.max_fee_per_gas,
        max_priority_fee_per_gas = %fees.max_priority_fee_per_gas,
        submitted_block,
        "tx submitted"
    );

    Ok(Submission {
        tx_hash,
        nonce,
        simulation_id: simulation.id,
        submitted_block,
        gas_limit,
        max_fee_per_gas: fees.max_fee_per_gas,
        max_priority_fee_per_gas: fees.max_priority_fee_per_gas,
    })
}

pub async fn replace_pending_transaction(
    provider: &ChainProvider,
    wallet: &ExecutionWallet,
    settings: &Settings,
    calldata: &[u8],
    nonce: u64,
    gas_limit: U256,
    previous_max_fee_per_gas: U256,
    previous_max_priority_fee_per_gas: U256,
) -> Result<Submission> {
    let executor = settings
        .executor_contract
        .filter(|address| *address != Address::ZERO)
        .context("EXECUTOR_CONTRACT is not configured")?;
    if calldata.is_empty() {
        anyhow::bail!("replacement calldata is empty");
    }

    let suggested = aggressive_fee_suggestion(provider, settings).await?;
    let max_fee_per_gas = suggested.max_fee_per_gas.max(bump_fee(
        previous_max_fee_per_gas,
        settings.execution_replacement_fee_bump_bps,
    ));
    let max_priority_fee_per_gas = suggested.max_priority_fee_per_gas.max(bump_fee(
        previous_max_priority_fee_per_gas,
        settings.execution_replacement_fee_bump_bps,
    ));
    let submitted_block = provider.get_block_number().await?;
    let tx_hash = sign_and_send(
        provider,
        wallet,
        settings,
        executor,
        calldata,
        nonce,
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas,
    )
    .await?;

    tracing::info!(
        tx_hash = %tx_hash,
        nonce,
        gas_limit = %gas_limit,
        max_fee_per_gas = %max_fee_per_gas,
        max_priority_fee_per_gas = %max_priority_fee_per_gas,
        submitted_block,
        "replacement tx submitted"
    );

    Ok(Submission {
        tx_hash,
        nonce,
        simulation_id: uuid::Uuid::nil(),
        submitted_block,
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas,
    })
}

async fn sign_and_send(
    provider: &ChainProvider,
    wallet: &ExecutionWallet,
    settings: &Settings,
    executor: Address,
    calldata: &[u8],
    nonce: u64,
    gas_limit: U256,
    max_fee_per_gas: U256,
    max_priority_fee_per_gas: U256,
) -> Result<B256> {
    let tx = Eip1559TransactionRequest::new()
        .from(alloy_address_to_ethers(wallet.address))
        .to(NameOrAddress::Address(alloy_address_to_ethers(executor)))
        .nonce(EthersU256::from(nonce))
        .chain_id(U64::from(settings.chain_id))
        .gas(alloy_u256_to_ethers(gas_limit)?)
        .max_fee_per_gas(alloy_u256_to_ethers(max_fee_per_gas)?)
        .max_priority_fee_per_gas(alloy_u256_to_ethers(max_priority_fee_per_gas)?)
        .value(EthersU256::zero())
        .data(Bytes::from(calldata.to_vec()));
    let typed = TypedTransaction::Eip1559(tx);
    let signature = wallet.wallet.sign_transaction(&typed).await?;
    let raw = typed.rlp_signed(&signature);
    let raw_hex = format!("0x{}", hex::encode(raw.as_ref()));
    provider.send_raw_transaction(&raw_hex).await
}

pub fn pending_tx_result(candidate: &Candidate, eoa: Address, submission: &Submission) -> TxResult {
    TxResult {
        opportunity_id: candidate.id,
        simulation_id: Some(submission.simulation_id),
        eoa,
        tx_hash: Some(submission.tx_hash),
        nonce: submission.nonce,
        status: TxStatus::Pending,
        realized_profit: None,
        gas_used: None,
        effective_gas_price: None,
        revert_reason: None,
        receipt_json: None,
    }
}

pub fn pending_replacement_tx_result(
    opportunity_id: uuid::Uuid,
    simulation_id: Option<uuid::Uuid>,
    eoa: Address,
    submission: &Submission,
) -> TxResult {
    TxResult {
        opportunity_id,
        simulation_id,
        eoa,
        tx_hash: Some(submission.tx_hash),
        nonce: submission.nonce,
        status: TxStatus::Pending,
        realized_profit: None,
        gas_used: None,
        effective_gas_price: None,
        revert_reason: None,
        receipt_json: None,
    }
}

pub fn dropped_replaced_tx_result(
    opportunity_id: uuid::Uuid,
    simulation_id: Option<uuid::Uuid>,
    eoa: Address,
    old_tx_hash: B256,
    nonce: u64,
    new_tx_hash: B256,
) -> TxResult {
    TxResult {
        opportunity_id,
        simulation_id,
        eoa,
        tx_hash: Some(old_tx_hash),
        nonce,
        status: TxStatus::Dropped,
        realized_profit: None,
        gas_used: None,
        effective_gas_price: None,
        revert_reason: Some(format!("replaced by {new_tx_hash:#x}")),
        receipt_json: None,
    }
}

pub fn dropped_consumed_nonce_tx_result(
    opportunity_id: uuid::Uuid,
    simulation_id: Option<uuid::Uuid>,
    eoa: Address,
    tx_hash: B256,
    nonce: u64,
    confirmed_nonce: u64,
) -> TxResult {
    TxResult {
        opportunity_id,
        simulation_id,
        eoa,
        tx_hash: Some(tx_hash),
        nonce,
        status: TxStatus::Dropped,
        realized_profit: None,
        gas_used: None,
        effective_gas_price: None,
        revert_reason: Some(format!(
            "pending nonce {nonce} already consumed by chain confirmed nonce {confirmed_nonce}"
        )),
        receipt_json: None,
    }
}

pub fn receipt_tx_result(
    opportunity_id: uuid::Uuid,
    simulation_id: Option<uuid::Uuid>,
    eoa: Address,
    nonce: u64,
    receipt: &TxReceipt,
) -> TxResult {
    let realized_profit = if receipt.success {
        extract_executed_profit(&receipt.raw)
    } else {
        None
    };
    TxResult {
        opportunity_id,
        simulation_id,
        eoa,
        tx_hash: Some(receipt.tx_hash),
        nonce,
        status: if receipt.success {
            TxStatus::Confirmed
        } else {
            TxStatus::Reverted
        },
        realized_profit,
        gas_used: receipt.gas_used,
        effective_gas_price: receipt.effective_gas_price,
        revert_reason: if receipt.success {
            None
        } else {
            Some("transaction reverted".to_string())
        },
        receipt_json: Some(receipt.raw.clone()),
    }
}

fn extract_executed_profit(receipt: &serde_json::Value) -> Option<U256> {
    let logs = receipt.get("logs")?.as_array()?;
    for log in logs {
        let topic0 = log
            .get("topics")?
            .as_array()?
            .first()?
            .as_str()?
            .to_ascii_lowercase();
        if topic0 != EXECUTED_EVENT_TOPIC {
            continue;
        }
        let data = log.get("data")?.as_str()?.trim_start_matches("0x");
        if data.len() < 128 {
            continue;
        }
        if let Ok(profit) = U256::from_str_radix(&data[64..128], 16) {
            return Some(profit);
        }
    }
    None
}

fn bump_gas_limit(value: U256) -> U256 {
    value
        .saturating_mul(U256::from(GAS_LIMIT_MULTIPLIER_BPS))
        .checked_div(U256::from(10_000u64))
        .unwrap_or(value)
        .saturating_add(U256::from(25_000u64))
}

async fn aggressive_fee_suggestion(
    provider: &ChainProvider,
    settings: &Settings,
) -> Result<Eip1559FeeSuggestion> {
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

    Ok(Eip1559FeeSuggestion {
        base_fee_per_gas: suggested.base_fee_per_gas,
        max_fee_per_gas: max_fee,
        max_priority_fee_per_gas: priority,
    })
}

fn ensure_expected_profit_covers_gas(
    settings: &Settings,
    candidate: &Candidate,
    gas_limit: U256,
    expected_fee_per_gas: U256,
) -> Result<()> {
    if candidate.token_in != settings.weth_address {
        return Ok(());
    }
    let expected_gas_cost = gas_limit.saturating_mul(expected_fee_per_gas);
    let required_profit = apply_bps(expected_gas_cost, settings.execution_gas_profit_buffer_bps)
        .max(candidate.min_profit);
    if candidate.expected_profit < required_profit {
        anyhow::bail!(
            "expected WETH profit {} below gas-adjusted threshold {} (gas_limit={}, expected_fee_per_gas={})",
            candidate.expected_profit,
            required_profit,
            gas_limit,
            expected_fee_per_gas
        );
    }
    Ok(())
}

fn bump_fee(value: U256, bump_bps: u64) -> U256 {
    apply_bps(value, bump_bps).saturating_add(U256::from(1u64))
}

fn apply_bps(value: U256, bps: u64) -> U256 {
    value
        .saturating_mul(U256::from(bps))
        .checked_div(U256::from(10_000u64))
        .unwrap_or(value)
}

fn parse_optional_u256(raw: Option<&str>) -> Result<Option<U256>> {
    let Some(raw) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    Ok(Some(U256::from_str_radix(raw, 10)?))
}

fn alloy_address_to_ethers(address: Address) -> ethers_core::types::Address {
    ethers_core::types::Address::from_slice(address.as_slice())
}

fn ethers_address_to_alloy(address: ethers_core::types::Address) -> Address {
    Address::from_slice(address.as_bytes())
}

fn alloy_u256_to_ethers(value: U256) -> Result<EthersU256> {
    EthersU256::from_dec_str(&value.to_string()).context("failed to convert U256")
}
