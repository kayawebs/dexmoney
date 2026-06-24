use std::{collections::HashSet, env, str::FromStr};

use alloy_primitives::{keccak256, Address, U256};
use anyhow::{anyhow, bail, Context, Result};
use base_arb_chain::provider::ChainProvider;
use base_arb_common::{
    config::Settings,
    types::{DexKind, PoolVariant},
};
use base_arb_storage::postgres::{ensure_registry_schema, PostgresStore};
use serde_json::{json, Value};
use sqlx::Row;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone)]
struct Args {
    apply: bool,
    limit: i64,
    refresh_existing: bool,
    pools: HashSet<Address>,
}

#[derive(Debug, Clone)]
struct BalancerPool {
    chain_id: u64,
    pool: Address,
    latest_block: Option<u64>,
}

#[derive(Debug, Clone)]
struct BalancerModel {
    family: String,
    status: String,
    raw_json: Value,
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let args = parse_args()?;
    let settings = Settings::load()?;
    let vault = settings
        .balancer_v3_vault
        .context("BALANCER_V3_VAULT is required")?;
    let store = PostgresStore::connect(&settings.postgres_url).await?;
    ensure_registry_schema(&store.pool).await?;
    let provider = ChainProvider::from_settings(&settings);
    let pools = load_balancer_pools(&store, &settings, &args).await?;

    println!("== Balancer V3 Model Classification ==");
    println!(
        "mode={} pools={} limit={} refresh_existing={} vault={vault:#x}",
        if args.apply { "apply" } else { "dry-run" },
        pools.len(),
        args.limit,
        args.refresh_existing,
    );

    let mut weighted = 0usize;
    let mut stable = 0usize;
    let mut unsupported = 0usize;
    let mut failed = 0usize;

    for pool in pools {
        let classification = classify_pool(&provider, vault, pool.pool).await;
        match classification {
            Ok(model) => {
                match model.family.as_str() {
                    "weighted" => weighted += 1,
                    "stable" => stable += 1,
                    _ => unsupported += 1,
                }
                println!(
                    "{} pool={:#x} family={} status={}",
                    if args.apply {
                        "classified"
                    } else {
                        "dry_classified"
                    },
                    pool.pool,
                    model.family,
                    model.status
                );
                if args.apply {
                    store
                        .upsert_pool_model_coverage(
                            pool.chain_id,
                            pool.pool,
                            Some(DexKind::Balancer),
                            Some(PoolVariant::BalancerV3),
                            Some(&model.family),
                            &model.status,
                            "balancer_v3_model_classification",
                            pool.latest_block,
                            model.raw_json,
                            None,
                        )
                        .await?;
                }
            }
            Err(err) => {
                failed += 1;
                let error = err.to_string();
                println!("failed pool={:#x} error={error}", pool.pool);
                if args.apply {
                    store
                        .upsert_pool_model_coverage(
                            pool.chain_id,
                            pool.pool,
                            Some(DexKind::Balancer),
                            Some(PoolVariant::BalancerV3),
                            None,
                            "classification_failed",
                            "balancer_v3_model_classification",
                            pool.latest_block,
                            json!({}),
                            Some(&error),
                        )
                        .await?;
                }
            }
        }
    }

    println!("weighted={weighted} stable={stable} unsupported={unsupported} failed={failed}");
    Ok(())
}

async fn classify_pool(
    provider: &ChainProvider,
    vault: Address,
    pool: Address,
) -> Result<BalancerModel> {
    let tokens = call_address_array(provider, vault, "getPoolTokens(address)", pool)
        .await
        .with_context(|| format!("Vault getPoolTokens failed for {pool:#x}"))?;
    let balances = call_uint_array(provider, vault, "getCurrentLiveBalances(address)", pool)
        .await
        .with_context(|| format!("Vault getCurrentLiveBalances failed for {pool:#x}"))?;
    let (decimal_scaling_factors, token_rates) =
        call_two_uint_arrays(provider, vault, "getPoolTokenRates(address)", pool)
            .await
            .with_context(|| format!("Vault getPoolTokenRates failed for {pool:#x}"))?;
    let static_swap_fee = call_uint(
        provider,
        vault,
        "getStaticSwapFeePercentage(address)",
        Some(pool),
    )
    .await
    .with_context(|| format!("Vault getStaticSwapFeePercentage failed for {pool:#x}"))?;

    if tokens.len() < 2 {
        bail!("Balancer pool has fewer than 2 tokens");
    }
    if balances.len() != tokens.len() {
        bail!(
            "Balancer token/balance length mismatch tokens={} balances={}",
            tokens.len(),
            balances.len()
        );
    }
    if decimal_scaling_factors.len() != tokens.len() || token_rates.len() != tokens.len() {
        bail!(
            "Balancer token/rate length mismatch tokens={} scaling={} rates={}",
            tokens.len(),
            decimal_scaling_factors.len(),
            token_rates.len()
        );
    }

    match call_uint_array(provider, pool, "getNormalizedWeights()", Address::ZERO).await {
        Ok(weights) if weights.len() == tokens.len() => {
            let status = if tokens.len() == 2 {
                "weighted_inputs_ready"
            } else {
                "weighted_multi_token_unsupported"
            };
            return Ok(BalancerModel {
                family: "weighted".to_string(),
                status: status.to_string(),
                raw_json: json!({
                    "tokens": address_strings(&tokens),
                    "balances_live_scaled18": u256_strings(&balances),
                    "static_swap_fee_percentage": static_swap_fee.to_string(),
                    "normalized_weights": u256_strings(&weights),
                    "decimal_scaling_factors": u256_strings(&decimal_scaling_factors),
                    "token_rates": u256_strings(&token_rates),
                }),
            });
        }
        Ok(weights) => {
            return Ok(BalancerModel {
                family: "weighted".to_string(),
                status: "weighted_input_mismatch".to_string(),
                raw_json: json!({
                    "tokens": address_strings(&tokens),
                    "balances_live_scaled18": u256_strings(&balances),
                    "static_swap_fee_percentage": static_swap_fee.to_string(),
                    "normalized_weights": u256_strings(&weights),
                    "decimal_scaling_factors": u256_strings(&decimal_scaling_factors),
                    "token_rates": u256_strings(&token_rates),
                    "error": format!("weights length {} != token length {}", weights.len(), tokens.len()),
                }),
            });
        }
        Err(weight_err) => match call_amplification_parameter(provider, pool).await {
            Ok((amp, is_updating, precision)) => {
                return Ok(BalancerModel {
                    family: "stable".to_string(),
                    status: "stable_inputs_ready".to_string(),
                    raw_json: json!({
                        "tokens": address_strings(&tokens),
                        "balances_live_scaled18": u256_strings(&balances),
                        "static_swap_fee_percentage": static_swap_fee.to_string(),
                        "amplification_parameter": amp.to_string(),
                        "amplification_is_updating": is_updating,
                        "amplification_precision": precision.to_string(),
                        "decimal_scaling_factors": u256_strings(&decimal_scaling_factors),
                        "token_rates": u256_strings(&token_rates),
                    }),
                });
            }
            Err(stable_err) => {
                return Ok(BalancerModel {
                    family: "unsupported".to_string(),
                    status: "unsupported_pool_type".to_string(),
                    raw_json: json!({
                        "tokens": address_strings(&tokens),
                        "balances_live_scaled18": u256_strings(&balances),
                        "static_swap_fee_percentage": static_swap_fee.to_string(),
                        "decimal_scaling_factors": u256_strings(&decimal_scaling_factors),
                        "token_rates": u256_strings(&token_rates),
                        "weighted_probe_error": weight_err.to_string(),
                        "stable_probe_error": stable_err.to_string(),
                    }),
                });
            }
        },
    }
}

async fn load_balancer_pools(
    store: &PostgresStore,
    settings: &Settings,
    args: &Args,
) -> Result<Vec<BalancerPool>> {
    let pool_filter = args
        .pools
        .iter()
        .map(|pool| format!("{pool:#x}"))
        .collect::<Vec<_>>();
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ON (lower(p.pool_address))
          p.chain_id,
          p.pool_address,
          po.latest_block
        FROM pools p
        LEFT JOIN protocol_pool_observations po
          ON po.chain_id = p.chain_id
         AND po.protocol = 'balancer-v3'
         AND lower(po.pool_address) = lower(p.pool_address)
        LEFT JOIN pool_model_coverage mc
          ON mc.chain_id = p.chain_id
         AND lower(mc.pool_address) = lower(p.pool_address)
        WHERE p.chain_id = $1
          AND p.enabled
          AND p.dex = 'Balancer'
          AND p.variant = 'BalancerV3'
          AND (
            cardinality($3::TEXT[]) = 0
            OR EXISTS (
              SELECT 1
              FROM unnest($3::TEXT[]) AS filter(pool_address)
              WHERE lower(filter.pool_address) = lower(p.pool_address)
            )
          )
          AND (
            $4::BOOLEAN
            OR cardinality($3::TEXT[]) > 0
            OR mc.pool_address IS NULL
          )
        ORDER BY lower(p.pool_address), po.latest_block DESC NULLS LAST, p.updated_at DESC
        LIMIT $2
        "#,
    )
    .bind(i64::try_from(settings.chain_id)?)
    .bind(args.limit)
    .bind(&pool_filter)
    .bind(args.refresh_existing)
    .fetch_all(&store.pool)
    .await?;

    rows.into_iter()
        .map(|row| {
            let chain_id = row.try_get::<i64, _>("chain_id")?;
            let pool = row.try_get::<String, _>("pool_address")?;
            let latest_block = row
                .try_get::<Option<i64>, _>("latest_block")?
                .map(u64::try_from)
                .transpose()?;
            Ok(BalancerPool {
                chain_id: u64::try_from(chain_id)?,
                pool: Address::from_str(&pool).context("invalid pool address")?,
                latest_block,
            })
        })
        .collect()
}

async fn call_two_uint_arrays(
    provider: &ChainProvider,
    to: Address,
    signature: &str,
    address_arg: Address,
) -> Result<(Vec<U256>, Vec<U256>)> {
    let raw = provider
        .eth_call_from(
            None,
            to,
            &encode_address_call(signature, address_arg),
            signature,
        )
        .await?;
    decode_two_uint_arrays(&raw)
}

async fn call_uint_array(
    provider: &ChainProvider,
    to: Address,
    signature: &str,
    address_arg: Address,
) -> Result<Vec<U256>> {
    let data = if signature.ends_with("(address)") {
        encode_address_call(signature, address_arg)
    } else {
        encode_no_arg_call(signature)
    };
    let raw = provider.eth_call_from(None, to, &data, signature).await?;
    decode_uint_array(&raw)
}

async fn call_address_array(
    provider: &ChainProvider,
    to: Address,
    signature: &str,
    address_arg: Address,
) -> Result<Vec<Address>> {
    let raw = provider
        .eth_call_from(
            None,
            to,
            &encode_address_call(signature, address_arg),
            signature,
        )
        .await?;
    decode_address_array(&raw)
}

async fn call_uint(
    provider: &ChainProvider,
    to: Address,
    signature: &str,
    address_arg: Option<Address>,
) -> Result<U256> {
    let data = match address_arg {
        Some(address) => encode_address_call(signature, address),
        None => encode_no_arg_call(signature),
    };
    let raw = provider.eth_call_from(None, to, &data, signature).await?;
    let words = decode_words(&raw)?;
    words
        .first()
        .copied()
        .context("uint response missing first word")
}

async fn call_amplification_parameter(
    provider: &ChainProvider,
    pool: Address,
) -> Result<(U256, bool, U256)> {
    let raw = provider
        .eth_call_from(
            None,
            pool,
            &encode_no_arg_call("getAmplificationParameter()"),
            "getAmplificationParameter()",
        )
        .await?;
    let words = decode_words(&raw)?;
    if words.len() < 3 {
        bail!("getAmplificationParameter response too short");
    }
    Ok((words[0], !words[1].is_zero(), words[2]))
}

fn encode_no_arg_call(signature: &str) -> String {
    format!("0x{}", hex::encode(&keccak256(signature.as_bytes())[..4]))
}

fn encode_address_call(signature: &str, address: Address) -> String {
    let mut out = keccak256(signature.as_bytes())[..4].to_vec();
    out.extend([0u8; 12]);
    out.extend(address.as_slice());
    format!("0x{}", hex::encode(out))
}

fn decode_uint_array(raw: &str) -> Result<Vec<U256>> {
    let words = decode_words(raw)?;
    let offset = dynamic_offset_words(&words)?;
    let len = words
        .get(offset)
        .copied()
        .context("dynamic array missing length")?;
    let len = usize::try_from(len).context("array length does not fit usize")?;
    let start = offset + 1;
    if words.len() < start + len {
        bail!("dynamic uint array shorter than declared length");
    }
    Ok(words[start..start + len].to_vec())
}

fn decode_address_array(raw: &str) -> Result<Vec<Address>> {
    let words = decode_words(raw)?;
    let offset = dynamic_offset_words(&words)?;
    let len = words
        .get(offset)
        .copied()
        .context("dynamic array missing length")?;
    let len = usize::try_from(len).context("array length does not fit usize")?;
    let start = offset + 1;
    if words.len() < start + len {
        bail!("dynamic address array shorter than declared length");
    }
    Ok(words[start..start + len]
        .iter()
        .map(|word| {
            let bytes = word.to_be_bytes::<32>();
            Address::from_slice(&bytes[12..])
        })
        .collect::<Vec<_>>())
}

fn decode_two_uint_arrays(raw: &str) -> Result<(Vec<U256>, Vec<U256>)> {
    let words = decode_words(raw)?;
    if words.len() < 2 {
        bail!("tuple response missing dynamic array offsets");
    }
    let first = decode_uint_array_at_offset(&words, words[0])?;
    let second = decode_uint_array_at_offset(&words, words[1])?;
    Ok((first, second))
}

fn decode_uint_array_at_offset(words: &[U256], offset_bytes: U256) -> Result<Vec<U256>> {
    if offset_bytes % U256::from(32u64) != U256::ZERO {
        bail!("dynamic offset is not word-aligned");
    }
    let offset =
        usize::try_from(offset_bytes / U256::from(32u64)).context("dynamic offset too large")?;
    let len = words
        .get(offset)
        .copied()
        .context("dynamic array missing length")?;
    let len = usize::try_from(len).context("array length does not fit usize")?;
    let start = offset + 1;
    if words.len() < start + len {
        bail!("dynamic uint array shorter than declared length");
    }
    Ok(words[start..start + len].to_vec())
}

fn dynamic_offset_words(words: &[U256]) -> Result<usize> {
    let offset_bytes = words.first().copied().context("missing dynamic offset")?;
    if offset_bytes % U256::from(32u64) != U256::ZERO {
        bail!("dynamic offset is not word-aligned");
    }
    usize::try_from(offset_bytes / U256::from(32u64)).context("dynamic offset does not fit usize")
}

fn decode_words(raw: &str) -> Result<Vec<U256>> {
    let clean = raw.strip_prefix("0x").unwrap_or(raw);
    if clean.is_empty() {
        bail!("empty eth_call result");
    }
    if clean.len() % 64 != 0 {
        bail!("unexpected eth_call word length");
    }
    clean
        .as_bytes()
        .chunks(64)
        .map(|chunk| {
            let word = std::str::from_utf8(chunk)?;
            U256::from_str_radix(word, 16).context("invalid u256 word")
        })
        .collect()
}

fn address_strings(addresses: &[Address]) -> Vec<String> {
    addresses
        .iter()
        .map(|address| format!("{address:#x}"))
        .collect()
}

fn u256_strings(values: &[U256]) -> Vec<String> {
    values.iter().map(ToString::to_string).collect()
}

fn parse_args() -> Result<Args> {
    let mut args = Args {
        apply: false,
        limit: 100,
        refresh_existing: false,
        pools: HashSet::new(),
    };
    let mut iter = env::args().skip(1);
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--apply" => args.apply = true,
            "--refresh-existing" => args.refresh_existing = true,
            "--limit" => {
                args.limit = iter
                    .next()
                    .ok_or_else(|| anyhow!("--limit requires a value"))?
                    .parse()
                    .context("invalid --limit")?;
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
            _ => bail!("unknown argument: {arg}"),
        }
    }
    if args.limit <= 0 {
        bail!("--limit must be positive");
    }
    Ok(args)
}

fn print_usage() {
    eprintln!(
        "Usage: classify_balancer_v3_models [--apply] [--refresh-existing] [--limit 100] [--pool 0x...]"
    );
}
