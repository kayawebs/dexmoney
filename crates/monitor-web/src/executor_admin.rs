use alloy_primitives::{Address, B256, U256};
use anyhow::{Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::{
    config::Settings,
    constants::{AERODROME_SLIPSTREAM_ROUTER, PANCAKE_V3_ROUTER},
    types::{DiscoveredPool, PoolVariant},
};
use ethers_core::types::{
    transaction::eip2718::TypedTransaction, Bytes, Eip1559TransactionRequest, NameOrAddress,
    U256 as EthersU256, U64,
};
use ethers_signers::{LocalWallet, Signer};

const MAX_UINT: U256 = U256::MAX;
const GAS_LIMIT_MULTIPLIER_BPS: u64 = 12_000;

const TOKEN_WHITELIST_SELECTOR: [u8; 4] = [0x75, 0x3d, 0x75, 0x63];
const POOL_WHITELIST_SELECTOR: [u8; 4] = [0x4c, 0x70, 0x17, 0x9d];
const ROUTER_WHITELIST_SELECTOR: [u8; 4] = [0x63, 0xcb, 0xb1, 0x45];
const FACTORY_WHITELIST_SELECTOR: [u8; 4] = [0xf2, 0xb9, 0xc6, 0xe3];
const SET_TOKEN_WHITELIST_SELECTOR: [u8; 4] = [0xc9, 0xbc, 0xc9, 0x7e];
const SET_POOL_WHITELIST_SELECTOR: [u8; 4] = [0x2d, 0xab, 0x1f, 0x36];
const SET_ROUTER_WHITELIST_SELECTOR: [u8; 4] = [0x7b, 0x22, 0x43, 0x39];
const SET_FACTORY_WHITELIST_SELECTOR: [u8; 4] = [0x44, 0x7f, 0xd7, 0xfd];
const APPROVE_TOKEN_SELECTOR: [u8; 4] = [0xda, 0x3e, 0x33, 0x97];
const ERC20_ALLOWANCE_SELECTOR: [u8; 4] = [0xdd, 0x62, 0xed, 0x3e];

#[derive(Debug, Clone, Default)]
pub struct ExecutorAdminReport {
    pub submitted: usize,
    pub skipped: usize,
    pub skipped_reason: Option<String>,
    pub tx_hashes: Vec<B256>,
}

impl ExecutorAdminReport {
    pub fn summary(&self) -> String {
        if let Some(reason) = &self.skipped_reason {
            return format!("executor auto-config skipped: {reason}");
        }
        format!(
            "executor auto-config submitted {} txs, skipped {} existing settings",
            self.submitted, self.skipped
        )
    }
}

struct AdminWallet {
    wallet: LocalWallet,
    address: Address,
}

pub async fn configure_executor_for_pair(
    provider: &ChainProvider,
    settings: &Settings,
    discovered: &[DiscoveredPool],
    token0: Address,
    token1: Address,
) -> Result<ExecutorAdminReport> {
    if !settings.execution_submit_enabled {
        return Ok(ExecutorAdminReport {
            skipped_reason: Some(
                "EXECUTION_SUBMIT_ENABLED=false; registry updated without executor transactions"
                    .to_string(),
            ),
            ..ExecutorAdminReport::default()
        });
    }

    let Some(executor) = settings
        .executor_contract
        .filter(|address| *address != Address::ZERO)
    else {
        return Ok(ExecutorAdminReport {
            skipped_reason: Some("EXECUTOR_CONTRACT is not configured".to_string()),
            ..ExecutorAdminReport::default()
        });
    };
    let Some(private_key) = settings
        .executor_owner_private_key
        .as_deref()
        .or(settings.deployer_private_key.as_deref())
    else {
        return Ok(ExecutorAdminReport {
            skipped_reason: Some(
                "EXECUTOR_OWNER_PRIVATE_KEY or DEPLOYER_PRIVATE_KEY is not configured".to_string(),
            ),
            ..ExecutorAdminReport::default()
        });
    };

    let wallet = AdminWallet::from_private_key(private_key, settings.chain_id)?;
    let mut nonce = provider.get_transaction_count(wallet.address, true).await?;
    let mut report = ExecutorAdminReport::default();

    let routers = executor_routers(settings, discovered);
    let whitelist_supported = executor_whitelists_supported(provider, executor).await;

    if whitelist_supported {
        for router in routers.iter().copied() {
            ensure_mapping_enabled(
                provider,
                &wallet,
                settings.chain_id,
                executor,
                &mut nonce,
                &mut report,
                ROUTER_WHITELIST_SELECTOR,
                SET_ROUTER_WHITELIST_SELECTOR,
                router,
                "router whitelist",
            )
            .await?;
        }
    }

    if whitelist_supported
        && discovered.iter().any(|pool| {
            pool.state.dex == base_arb_common::types::DexKind::Aerodrome
                && pool.state.variant == PoolVariant::AerodromeVolatile
        })
    {
        if let Some(factory) = settings
            .aerodrome_pool_factory
            .filter(|address| *address != Address::ZERO)
        {
            ensure_mapping_enabled(
                provider,
                &wallet,
                settings.chain_id,
                executor,
                &mut nonce,
                &mut report,
                FACTORY_WHITELIST_SELECTOR,
                SET_FACTORY_WHITELIST_SELECTOR,
                factory,
                "factory whitelist",
            )
            .await?;
        }
    }

    for token in [token0, token1] {
        if whitelist_supported {
            ensure_mapping_enabled(
                provider,
                &wallet,
                settings.chain_id,
                executor,
                &mut nonce,
                &mut report,
                TOKEN_WHITELIST_SELECTOR,
                SET_TOKEN_WHITELIST_SELECTOR,
                token,
                "token whitelist",
            )
            .await?;
        }
        for router in routers.iter().copied() {
            ensure_token_approval(
                provider,
                &wallet,
                settings.chain_id,
                executor,
                token,
                router,
                &mut nonce,
                &mut report,
            )
            .await?;
        }
    }

    if whitelist_supported {
        for pool in discovered {
            ensure_mapping_enabled(
                provider,
                &wallet,
                settings.chain_id,
                executor,
                &mut nonce,
                &mut report,
                POOL_WHITELIST_SELECTOR,
                SET_POOL_WHITELIST_SELECTOR,
                pool.state.pool_id.address,
                "pool whitelist",
            )
            .await?;
        }
    }

    Ok(report)
}

fn executor_routers(settings: &Settings, discovered: &[DiscoveredPool]) -> Vec<Address> {
    let mut routers = [settings.aerodrome_router, settings.uniswap_v3_router]
        .into_iter()
        .flatten()
        .filter(|address| *address != Address::ZERO)
        .collect::<Vec<_>>();

    if discovered.iter().any(|pool| {
        pool.state.dex == base_arb_common::types::DexKind::PancakeSwap
            && pool.state.variant == PoolVariant::PancakeV3
    }) {
        if let Some(router) = settings
            .pancake_v3_router
            .or_else(|| PANCAKE_V3_ROUTER.parse().ok())
            .filter(|address| *address != Address::ZERO)
        {
            routers.push(router);
        }
    }

    if discovered.iter().any(|pool| {
        pool.state.dex == base_arb_common::types::DexKind::Aerodrome
            && pool.state.variant == PoolVariant::AerodromeSlipstream
    }) {
        if let Some(router) = settings
            .aerodrome_slipstream_router
            .or_else(|| AERODROME_SLIPSTREAM_ROUTER.parse().ok())
            .filter(|address| *address != Address::ZERO)
        {
            routers.push(router);
        }
    }

    routers.sort();
    routers.dedup();
    routers
}

async fn executor_whitelists_supported(provider: &ChainProvider, executor: Address) -> bool {
    call_bool(
        provider,
        executor,
        &encode_address_call(TOKEN_WHITELIST_SELECTOR, Address::ZERO),
        "token whitelist support probe",
    )
    .await
    .is_ok()
}

impl AdminWallet {
    fn from_private_key(private_key: &str, chain_id: u64) -> Result<Self> {
        let wallet = private_key
            .parse::<LocalWallet>()
            .context("failed to parse executor owner private key")?
            .with_chain_id(chain_id);
        let address = Address::from_slice(wallet.address().as_bytes());
        Ok(Self { wallet, address })
    }
}

async fn ensure_mapping_enabled(
    provider: &ChainProvider,
    wallet: &AdminWallet,
    chain_id: u64,
    executor: Address,
    nonce: &mut u64,
    report: &mut ExecutorAdminReport,
    view_selector: [u8; 4],
    set_selector: [u8; 4],
    value: Address,
    label: &str,
) -> Result<()> {
    let view_data = encode_address_call(view_selector, value);
    if call_bool(provider, executor, &view_data, label).await? {
        report.skipped += 1;
        return Ok(());
    }

    let data = encode_address_bool_call(set_selector, value, true);
    let tx_hash = send_admin_tx(provider, wallet, chain_id, executor, data, *nonce).await?;
    *nonce = nonce.saturating_add(1);
    report.submitted += 1;
    report.tx_hashes.push(tx_hash);
    Ok(())
}

async fn ensure_token_approval(
    provider: &ChainProvider,
    wallet: &AdminWallet,
    chain_id: u64,
    executor: Address,
    token: Address,
    spender: Address,
    nonce: &mut u64,
    report: &mut ExecutorAdminReport,
) -> Result<()> {
    let allowance_data = encode_two_address_call(ERC20_ALLOWANCE_SELECTOR, executor, spender);
    let allowance = call_u256(provider, token, &allowance_data, "ERC20 allowance").await?;
    if allowance > U256::from(1_000_000_000_000_000_000u128) {
        report.skipped += 1;
        return Ok(());
    }

    let data = encode_approve_token_call(token, spender, MAX_UINT);
    let tx_hash = send_admin_tx(provider, wallet, chain_id, executor, data, *nonce).await?;
    *nonce = nonce.saturating_add(1);
    report.submitted += 1;
    report.tx_hashes.push(tx_hash);
    Ok(())
}

async fn call_bool(
    provider: &ChainProvider,
    to: Address,
    calldata: &[u8],
    label: &str,
) -> Result<bool> {
    let raw = provider
        .eth_call_from(None, to, &hex_data(calldata), label)
        .await?;
    Ok(call_u256_from_raw(&raw)? != U256::ZERO)
}

async fn call_u256(
    provider: &ChainProvider,
    to: Address,
    calldata: &[u8],
    label: &str,
) -> Result<U256> {
    let raw = provider
        .eth_call_from(None, to, &hex_data(calldata), label)
        .await?;
    call_u256_from_raw(&raw)
}

fn call_u256_from_raw(raw: &str) -> Result<U256> {
    let clean = raw.trim_start_matches("0x");
    if clean.len() < 64 {
        anyhow::bail!("eth_call returned short u256 result");
    }
    U256::from_str_radix(&clean[clean.len() - 64..], 16).map_err(Into::into)
}

async fn send_admin_tx(
    provider: &ChainProvider,
    wallet: &AdminWallet,
    chain_id: u64,
    to: Address,
    calldata: Vec<u8>,
    nonce: u64,
) -> Result<B256> {
    let data_hex = hex_data(&calldata);
    let estimated_gas = provider
        .estimate_gas(wallet.address, to, &data_hex)
        .await
        .context("failed to estimate executor admin tx gas")?;
    let gas_limit = bump_gas_limit(estimated_gas);
    let (max_fee_per_gas, max_priority_fee_per_gas) = provider.suggested_eip1559_fees().await?;

    let tx = Eip1559TransactionRequest::new()
        .from(ethers_core::types::Address::from_slice(
            wallet.address.as_slice(),
        ))
        .to(NameOrAddress::Address(
            ethers_core::types::Address::from_slice(to.as_slice()),
        ))
        .nonce(EthersU256::from(nonce))
        .chain_id(U64::from(chain_id))
        .gas(alloy_u256_to_ethers(gas_limit)?)
        .max_fee_per_gas(alloy_u256_to_ethers(max_fee_per_gas)?)
        .max_priority_fee_per_gas(alloy_u256_to_ethers(max_priority_fee_per_gas)?)
        .value(EthersU256::zero())
        .data(Bytes::from(calldata));
    let typed = TypedTransaction::Eip1559(tx);
    let signature = wallet.wallet.sign_transaction(&typed).await?;
    let raw = typed.rlp_signed(&signature);
    let raw_hex = format!("0x{}", hex::encode(raw.as_ref()));
    provider.send_raw_transaction(&raw_hex).await
}

fn bump_gas_limit(value: U256) -> U256 {
    value
        .saturating_mul(U256::from(GAS_LIMIT_MULTIPLIER_BPS))
        .checked_div(U256::from(10_000u64))
        .unwrap_or(value)
        .saturating_add(U256::from(25_000u64))
}

fn encode_address_call(selector: [u8; 4], address: Address) -> Vec<u8> {
    let mut out = Vec::with_capacity(36);
    out.extend(selector);
    out.extend(encode_address(address));
    out
}

fn encode_two_address_call(selector: [u8; 4], first: Address, second: Address) -> Vec<u8> {
    let mut out = Vec::with_capacity(68);
    out.extend(selector);
    out.extend(encode_address(first));
    out.extend(encode_address(second));
    out
}

fn encode_address_bool_call(selector: [u8; 4], address: Address, value: bool) -> Vec<u8> {
    let mut out = Vec::with_capacity(68);
    out.extend(selector);
    out.extend(encode_address(address));
    out.extend(encode_bool(value));
    out
}

fn encode_approve_token_call(token: Address, spender: Address, amount: U256) -> Vec<u8> {
    let mut out = Vec::with_capacity(100);
    out.extend(APPROVE_TOKEN_SELECTOR);
    out.extend(encode_address(token));
    out.extend(encode_address(spender));
    out.extend(encode_u256(amount));
    out
}

fn encode_address(address: Address) -> [u8; 32] {
    let mut out = [0_u8; 32];
    out[12..].copy_from_slice(address.as_slice());
    out
}

fn encode_bool(value: bool) -> [u8; 32] {
    let mut out = [0_u8; 32];
    if value {
        out[31] = 1;
    }
    out
}

fn encode_u256(value: U256) -> [u8; 32] {
    value.to_be_bytes()
}

fn hex_data(data: &[u8]) -> String {
    format!("0x{}", hex::encode(data))
}

fn alloy_u256_to_ethers(value: U256) -> Result<EthersU256> {
    EthersU256::from_dec_str(&value.to_string()).context("failed to convert U256")
}
