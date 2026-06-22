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
use std::collections::HashSet;

use crate::simulator::{executor_for_candidate, ApprovalRequirement};

const GAS_LIMIT_MULTIPLIER_BPS: u64 = 12_000;
const UNCHECKED_ARB_FALLBACK_GAS_LIMIT: u64 = 900_000;
const APPROVE_TOKEN_SELECTOR: [u8; 4] = [0xda, 0x3e, 0x33, 0x97];
const ERC20_ALLOWANCE_SELECTOR: [u8; 4] = [0xdd, 0x62, 0xed, 0x3e];
const EXECUTOR_OPERATORS_SELECTOR: [u8; 4] = [0x13, 0xe7, 0xc9, 0xd8];
const SET_OPERATOR_SELECTOR: [u8; 4] = [0x55, 0x8a, 0x72, 0x97];
const EXECUTED_EVENT_TOPIC: &str =
    "0xe953ae62f4f69be1c6d943cb68d93d288f23ffae7332b84196d46e9e778b23b2";
const MAX_UINT: U256 = U256::MAX;
const MIN_EXISTING_ALLOWANCE: u128 = 1_000_000_000_000_000_000u128;

#[derive(Debug, Clone)]
pub struct ExecutionWallet {
    wallet: LocalWallet,
    address: Address,
}

#[derive(Debug, Clone)]
pub struct Submission {
    pub tx_hash: B256,
    pub nonce: u64,
    pub simulation_id: Option<uuid::Uuid>,
    pub executor_contract: Address,
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
    let executor = executor_for_candidate(settings, candidate)?;
    if simulation.calldata.is_empty() {
        anyhow::bail!("simulation calldata is empty");
    }

    let gas_limit = simulation
        .gas_estimate
        .map(bump_gas_limit)
        .unwrap_or_else(|| U256::from(UNCHECKED_ARB_FALLBACK_GAS_LIMIT));
    let (max_fee_per_gas, max_priority_fee_per_gas) = match (
        simulation.max_fee_per_gas,
        simulation.max_priority_fee_per_gas,
    ) {
        (Some(max_fee_per_gas), Some(max_priority_fee_per_gas)) => {
            (max_fee_per_gas, max_priority_fee_per_gas)
        }
        _ => {
            let fees = aggressive_fee_suggestion(provider, settings).await?;
            (fees.max_fee_per_gas, fees.max_priority_fee_per_gas)
        }
    };
    if candidate.is_expired(Utc::now()) {
        anyhow::bail!("candidate expired before tx broadcast");
    }

    let submitted_block = simulation.block_number.unwrap_or(candidate.block_number);
    let tx_hash = sign_and_send(
        provider,
        wallet,
        settings,
        executor,
        &simulation.calldata,
        nonce,
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas,
    )
    .await?;

    tracing::info!(
        candidate_id = %candidate.id,
        tx_hash = %tx_hash,
        nonce,
        gas_limit = %gas_limit,
        max_fee_per_gas = %max_fee_per_gas,
        max_priority_fee_per_gas = %max_priority_fee_per_gas,
        submitted_block,
        "tx submitted"
    );

    Ok(Submission {
        tx_hash,
        nonce,
        simulation_id: Some(simulation.id),
        executor_contract: executor,
        submitted_block,
        gas_limit,
        max_fee_per_gas,
        max_priority_fee_per_gas,
    })
}

pub async fn submit_candidate_unchecked(
    provider: &ChainProvider,
    wallet: &ExecutionWallet,
    settings: &Settings,
    candidate: &Candidate,
    simulation_id: Option<uuid::Uuid>,
    calldata: &[u8],
    nonce: u64,
) -> Result<Submission> {
    let executor = executor_for_candidate(settings, candidate)?;
    if calldata.is_empty() {
        anyhow::bail!("unchecked candidate calldata is empty");
    }

    let gas_limit = U256::from(UNCHECKED_ARB_FALLBACK_GAS_LIMIT);
    let fees = aggressive_fee_suggestion(provider, settings).await?;
    if candidate.is_expired(Utc::now()) {
        anyhow::bail!("candidate expired before unchecked tx broadcast");
    }

    let submitted_block = candidate.block_number;
    let tx_hash = sign_and_send(
        provider,
        wallet,
        settings,
        executor,
        calldata,
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
        "unchecked tx submitted after lazy approval"
    );

    Ok(Submission {
        tx_hash,
        nonce,
        simulation_id,
        executor_contract: executor,
        submitted_block,
        gas_limit,
        max_fee_per_gas: fees.max_fee_per_gas,
        max_priority_fee_per_gas: fees.max_priority_fee_per_gas,
    })
}

pub async fn operator_enabled(
    provider: &ChainProvider,
    executor: Address,
    operator: Address,
) -> Result<bool> {
    let data = encode_address_call(EXECUTOR_OPERATORS_SELECTOR, operator);
    let raw = provider
        .eth_call_from(None, executor, &hex_data(&data), "Executor operators")
        .await?;
    Ok(decode_u256_result(&raw)? != U256::ZERO)
}

pub async fn submit_set_operator(
    provider: &ChainProvider,
    wallet: &ExecutionWallet,
    settings: &Settings,
    executor: Address,
    operator: Address,
    nonce: u64,
) -> Result<Submission> {
    let calldata = encode_address_bool_call(SET_OPERATOR_SELECTOR, operator, true);
    submit_contract_call(provider, wallet, settings, executor, calldata, nonce).await
}

pub async fn submit_executor_approval(
    provider: &ChainProvider,
    wallet: &ExecutionWallet,
    settings: &Settings,
    executor: Address,
    approval: ApprovalRequirement,
    nonce: u64,
) -> Result<Submission> {
    let calldata = encode_approve_token_call(approval.token, approval.spender, MAX_UINT);
    submit_contract_call(provider, wallet, settings, executor, calldata, nonce).await
}

pub async fn submit_value_transfer(
    provider: &ChainProvider,
    wallet: &ExecutionWallet,
    settings: &Settings,
    to: Address,
    value: U256,
    nonce: u64,
) -> Result<Submission> {
    let fees = aggressive_fee_suggestion(provider, settings).await?;
    let gas_limit = U256::from(21_000u64);
    let submitted_block = provider.get_block_number().await?;
    let tx_hash = sign_and_send_value(
        provider,
        wallet,
        settings,
        to,
        &[],
        value,
        nonce,
        gas_limit,
        fees.max_fee_per_gas,
        fees.max_priority_fee_per_gas,
    )
    .await?;

    tracing::info!(
        tx_hash = %tx_hash,
        nonce,
        to = %to,
        value = %value,
        gas_limit = %gas_limit,
        max_fee_per_gas = %fees.max_fee_per_gas,
        max_priority_fee_per_gas = %fees.max_priority_fee_per_gas,
        submitted_block,
        "worker funding tx submitted"
    );

    Ok(Submission {
        tx_hash,
        nonce,
        simulation_id: None,
        executor_contract: Address::ZERO,
        submitted_block,
        gas_limit,
        max_fee_per_gas: fees.max_fee_per_gas,
        max_priority_fee_per_gas: fees.max_priority_fee_per_gas,
    })
}

async fn submit_contract_call(
    provider: &ChainProvider,
    wallet: &ExecutionWallet,
    settings: &Settings,
    contract: Address,
    calldata: Vec<u8>,
    nonce: u64,
) -> Result<Submission> {
    let data_hex = hex_data(&calldata);
    let estimated_gas = provider
        .estimate_gas(wallet.address, contract, &data_hex)
        .await
        .context("failed to estimate admin tx gas")?;
    let gas_limit = bump_gas_limit(estimated_gas);
    let fees = aggressive_fee_suggestion(provider, settings).await?;
    let submitted_block = provider.get_block_number().await?;
    let tx_hash = sign_and_send(
        provider,
        wallet,
        settings,
        contract,
        &calldata,
        nonce,
        gas_limit,
        fees.max_fee_per_gas,
        fees.max_priority_fee_per_gas,
    )
    .await?;

    tracing::info!(
        tx_hash = %tx_hash,
        nonce,
        contract = %contract,
        gas_limit = %gas_limit,
        max_fee_per_gas = %fees.max_fee_per_gas,
        max_priority_fee_per_gas = %fees.max_priority_fee_per_gas,
        submitted_block,
        "executor admin tx submitted"
    );

    Ok(Submission {
        tx_hash,
        nonce,
        simulation_id: None,
        executor_contract: contract,
        submitted_block,
        gas_limit,
        max_fee_per_gas: fees.max_fee_per_gas,
        max_priority_fee_per_gas: fees.max_priority_fee_per_gas,
    })
}

pub async fn missing_approvals(
    provider: &ChainProvider,
    settings: &Settings,
    candidate: &Candidate,
    requirements: &[ApprovalRequirement],
) -> Result<Vec<ApprovalRequirement>> {
    let executor = executor_for_candidate(settings, candidate)?;
    let mut missing = Vec::new();
    for requirement in requirements.iter().copied() {
        if requirement.token == Address::ZERO || requirement.spender == Address::ZERO {
            continue;
        }
        let data = encode_two_address_call(ERC20_ALLOWANCE_SELECTOR, executor, requirement.spender);
        let raw = provider
            .eth_call_from(
                None,
                requirement.token,
                &hex_data(&data),
                "ERC20 allowance for lazy approval",
            )
            .await?;
        let allowance = decode_u256_result(&raw)?;
        if allowance < U256::from(MIN_EXISTING_ALLOWANCE) {
            missing.push(requirement);
        }
    }
    Ok(missing)
}

pub async fn missing_approvals_cached(
    provider: &ChainProvider,
    settings: &Settings,
    candidate: &Candidate,
    requirements: &[ApprovalRequirement],
    approved_cache: &mut HashSet<(Address, Address, Address)>,
) -> Result<Vec<ApprovalRequirement>> {
    let executor = executor_for_candidate(settings, candidate)?;
    let mut missing = Vec::new();
    for requirement in requirements.iter().copied() {
        if requirement.token == Address::ZERO || requirement.spender == Address::ZERO {
            continue;
        }
        let cache_key = (executor, requirement.token, requirement.spender);
        if approved_cache.contains(&cache_key) {
            continue;
        }
        let data = encode_two_address_call(ERC20_ALLOWANCE_SELECTOR, executor, requirement.spender);
        let raw = provider
            .eth_call_from(
                None,
                requirement.token,
                &hex_data(&data),
                "ERC20 allowance for lazy approval",
            )
            .await?;
        let allowance = decode_u256_result(&raw)?;
        if allowance < U256::from(MIN_EXISTING_ALLOWANCE) {
            missing.push(requirement);
        } else {
            approved_cache.insert(cache_key);
        }
    }
    Ok(missing)
}

pub async fn replace_pending_transaction(
    provider: &ChainProvider,
    wallet: &ExecutionWallet,
    settings: &Settings,
    executor: Address,
    calldata: &[u8],
    nonce: u64,
    gas_limit: U256,
    previous_max_fee_per_gas: U256,
    previous_max_priority_fee_per_gas: U256,
) -> Result<Submission> {
    if executor == Address::ZERO {
        anyhow::bail!("pending executor contract is not configured");
    }
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
        simulation_id: None,
        executor_contract: executor,
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

async fn sign_and_send_value(
    provider: &ChainProvider,
    wallet: &ExecutionWallet,
    settings: &Settings,
    to: Address,
    calldata: &[u8],
    value: U256,
    nonce: u64,
    gas_limit: U256,
    max_fee_per_gas: U256,
    max_priority_fee_per_gas: U256,
) -> Result<B256> {
    let tx = Eip1559TransactionRequest::new()
        .from(alloy_address_to_ethers(wallet.address))
        .to(NameOrAddress::Address(alloy_address_to_ethers(to)))
        .nonce(EthersU256::from(nonce))
        .chain_id(U64::from(settings.chain_id))
        .gas(alloy_u256_to_ethers(gas_limit)?)
        .max_fee_per_gas(alloy_u256_to_ethers(max_fee_per_gas)?)
        .max_priority_fee_per_gas(alloy_u256_to_ethers(max_priority_fee_per_gas)?)
        .value(alloy_u256_to_ethers(value)?)
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
        simulation_id: submission.simulation_id,
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

fn encode_approve_token_call(token: Address, spender: Address, amount: U256) -> Vec<u8> {
    let mut out = Vec::with_capacity(100);
    out.extend(APPROVE_TOKEN_SELECTOR);
    out.extend(encode_address(token));
    out.extend(encode_address(spender));
    out.extend(encode_u256(amount));
    out
}

fn encode_two_address_call(selector: [u8; 4], first: Address, second: Address) -> Vec<u8> {
    let mut out = Vec::with_capacity(68);
    out.extend(selector);
    out.extend(encode_address(first));
    out.extend(encode_address(second));
    out
}

fn encode_address_call(selector: [u8; 4], address: Address) -> Vec<u8> {
    let mut out = Vec::with_capacity(36);
    out.extend(selector);
    out.extend(encode_address(address));
    out
}

fn encode_address_bool_call(selector: [u8; 4], address: Address, value: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(68);
    out.extend(selector);
    out.extend(encode_address(address));
    out.extend(encode_u256(U256::from(value as u8)));
    out
}

fn encode_address(address: Address) -> [u8; 32] {
    let mut out = [0_u8; 32];
    out[12..].copy_from_slice(address.as_slice());
    out
}

fn encode_u256(value: U256) -> [u8; 32] {
    value.to_be_bytes()
}

fn hex_data(data: &[u8]) -> String {
    format!("0x{}", hex::encode(data))
}

fn decode_u256_result(raw: &str) -> Result<U256> {
    let clean = raw.trim_start_matches("0x");
    if clean.len() < 64 {
        anyhow::bail!("eth_call returned short u256 result");
    }
    U256::from_str_radix(&clean[clean.len() - 64..], 16).map_err(Into::into)
}
