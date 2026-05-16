use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, Result};
use base_arb_chain::provider::{ChainProvider, TxReceipt};
use base_arb_common::types::{Candidate, SimulationResult, TxResult, TxStatus};
use ethers_core::types::{
    transaction::eip2718::TypedTransaction, Bytes, Eip1559TransactionRequest, NameOrAddress,
    U256 as EthersU256, U64,
};
use ethers_signers::{LocalWallet, Signer};

const GAS_LIMIT_MULTIPLIER_BPS: u64 = 12_000;

#[derive(Debug, Clone)]
pub struct ExecutionWallet {
    wallet: LocalWallet,
    address: Address,
}

#[derive(Debug, Clone)]
pub struct Submission {
    pub tx_hash: B256,
    pub nonce: u64,
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
    settings: &base_arb_common::config::Settings,
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
    let (max_fee_per_gas, max_priority_fee_per_gas) = provider.suggested_eip1559_fees().await?;

    let tx = Eip1559TransactionRequest::new()
        .from(alloy_address_to_ethers(wallet.address))
        .to(NameOrAddress::Address(alloy_address_to_ethers(executor)))
        .nonce(EthersU256::from(nonce))
        .chain_id(U64::from(settings.chain_id))
        .gas(alloy_u256_to_ethers(gas_limit)?)
        .max_fee_per_gas(alloy_u256_to_ethers(max_fee_per_gas)?)
        .max_priority_fee_per_gas(alloy_u256_to_ethers(max_priority_fee_per_gas)?)
        .value(EthersU256::zero())
        .data(Bytes::from(simulation.calldata.clone()));
    let typed = TypedTransaction::Eip1559(tx);
    let signature = wallet.wallet.sign_transaction(&typed).await?;
    let raw = typed.rlp_signed(&signature);
    let raw_hex = format!("0x{}", hex::encode(raw.as_ref()));
    let tx_hash = provider.send_raw_transaction(&raw_hex).await?;

    tracing::info!(
        candidate_id = %candidate.id,
        tx_hash = %tx_hash,
        nonce,
        gas_limit = %gas_limit,
        "tx submitted"
    );

    Ok(Submission { tx_hash, nonce })
}

pub fn pending_tx_result(candidate: &Candidate, eoa: Address, submission: &Submission) -> TxResult {
    TxResult {
        opportunity_id: candidate.id,
        simulation_id: None,
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

pub fn receipt_tx_result(
    opportunity_id: uuid::Uuid,
    eoa: Address,
    nonce: u64,
    receipt: &TxReceipt,
) -> TxResult {
    TxResult {
        opportunity_id,
        simulation_id: None,
        eoa,
        tx_hash: Some(receipt.tx_hash),
        nonce,
        status: if receipt.success {
            TxStatus::Confirmed
        } else {
            TxStatus::Reverted
        },
        realized_profit: None,
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

fn bump_gas_limit(value: U256) -> U256 {
    value
        .saturating_mul(U256::from(GAS_LIMIT_MULTIPLIER_BPS))
        .checked_div(U256::from(10_000u64))
        .unwrap_or(value)
        .saturating_add(U256::from(25_000u64))
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
