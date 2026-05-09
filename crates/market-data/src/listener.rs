use alloy_primitives::U256;
use anyhow::Result;
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::types::{DexKind, PoolState, PoolStateWarning, PoolVariant};
use base_arb_storage::{postgres::PostgresStore, PoolStateStore, RecorderStore};
use std::collections::HashSet;
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use tracing::{info, warn};

const REGISTRY_RELOAD_INTERVAL: Duration = Duration::from_secs(30);
const CALIBRATION_INTERVAL: Duration = Duration::from_secs(30);

pub struct MarketDataService<P> {
    pub settings: Settings,
    pub provider: ChainProvider,
    pub pool_store: P,
    pub recorder: PostgresStore,
}

impl<P> MarketDataService<P>
where
    P: PoolStateStore,
{
    pub async fn run(&self) -> Result<()> {
        info!("event listener started");

        let mut monitored_states = self.load_monitored_states().await?;
        self.publish_monitored_states(&monitored_states).await?;

        let mut last_seen_block = self.provider.get_block_number().await?;
        let mut next_registry_reload = Instant::now() + REGISTRY_RELOAD_INTERVAL;
        let mut next_calibration = Instant::now() + CALIBRATION_INTERVAL;
        info!(last_seen_block, "market-data synchronized at startup");

        let mut ticker = interval(Duration::from_secs(3));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            let latest_block = self.provider.get_block_number().await?;
            if latest_block <= last_seen_block {
                if Instant::now() >= next_registry_reload {
                    monitored_states = self.reload_if_changed(monitored_states).await?;
                    next_registry_reload = Instant::now() + REGISTRY_RELOAD_INTERVAL;
                }
                if Instant::now() >= next_calibration {
                    monitored_states = self
                        .calibrate_states(monitored_states, last_seen_block)
                        .await?;
                    next_calibration = Instant::now() + CALIBRATION_INTERVAL;
                }
                continue;
            }

            if Instant::now() >= next_registry_reload {
                monitored_states = self.reload_if_changed(monitored_states).await?;
                next_registry_reload = Instant::now() + REGISTRY_RELOAD_INTERVAL;
            }

            let events = self
                .provider
                .fetch_relevant_events_for_pools(
                    &monitored_states,
                    last_seen_block + 1,
                    latest_block,
                )
                .await?;

            let mut state_changed = false;
            for event in &events {
                info!(
                    pool = %event.pool_address,
                    block_number = event.block_number,
                    event_type = %event.event_type,
                    "event received"
                );
                self.recorder.record_dex_event(event.clone()).await?;
                for state in &mut monitored_states {
                    if super::state_updater::apply_event_to_pool_state(state, event)? {
                        state_changed = true;
                        info!(
                            pool = %state.pool_id.address,
                            block_number = state.block_number,
                            "pool state locally updated from event"
                        );
                        break;
                    }
                }
            }

            if state_changed {
                self.publish_monitored_states(&monitored_states).await?;
            }

            if Instant::now() >= next_calibration {
                monitored_states = self
                    .calibrate_states(monitored_states, latest_block)
                    .await?;
                next_calibration = Instant::now() + CALIBRATION_INTERVAL;
            }

            last_seen_block = latest_block;
        }
    }

    async fn load_monitored_states(&self) -> Result<Vec<PoolState>> {
        let registry_pools = self.recorder.enabled_registry_pools().await?;
        if registry_pools.is_empty() {
            info!("pool registry is empty; falling back to .env configured pools");
            return self
                .provider
                .bootstrap_configured_pools(&self.settings)
                .await;
        }

        let mut out = Vec::with_capacity(registry_pools.len());
        for entry in &registry_pools {
            out.push(self.provider.fetch_pool_state_from_registry(entry).await?);
        }
        Ok(out)
    }

    async fn reload_if_changed(&self, current: Vec<PoolState>) -> Result<Vec<PoolState>> {
        let next = self.load_monitored_states().await?;
        let current_addresses = address_set(&current);
        let next_addresses = address_set(&next);
        if current_addresses != next_addresses {
            info!(
                previous = current_addresses.len(),
                next = next_addresses.len(),
                "pool registry changed; reloading monitored pools"
            );
            self.publish_monitored_states(&next).await?;
        }
        Ok(next)
    }

    async fn publish_monitored_states(&self, states: &[PoolState]) -> Result<()> {
        for state in states {
            self.pool_store.set_pool_state(state.clone()).await?;
            self.recorder.record_pool_state(state.clone()).await?;
            super::state_updater::log_pool_state_update(state);
            info!(
                pool = %state.pool_id.address,
                dex = ?state.dex,
                variant = ?state.variant,
                "monitoring pool logs"
            );
        }
        Ok(())
    }

    async fn calibrate_states(
        &self,
        mut states: Vec<PoolState>,
        max_processed_block: u64,
    ) -> Result<Vec<PoolState>> {
        let mut corrected = false;

        for state in &mut states {
            if state.dex != DexKind::Aerodrome || state.variant != PoolVariant::AerodromeVolatile {
                continue;
            }

            let onchain = self
                .provider
                .fetch_pool_state_from_registry(&base_arb_common::types::PoolRegistryEntry {
                    pool_address: state.pool_id.address,
                    dex: state.dex,
                    variant: state.variant,
                    token0: state.token0,
                    token1: state.token1,
                    fee_bps: state.fee_bps,
                    tick_spacing: None,
                    stable: None,
                    enabled: true,
                })
                .await?;

            if onchain.block_number > max_processed_block {
                info!(
                    pool = %state.pool_id.address,
                    onchain_block = onchain.block_number,
                    max_processed_block,
                    "skipping calibration for block newer than processed events"
                );
                continue;
            }

            let drift_bps = reserve_drift_bps(state, &onchain);
            if drift_bps > 0 {
                let message = format!(
                    "local Aerodrome volatile reserves drifted from onchain state by {drift_bps} bps"
                );
                warn!(
                    pool = %state.pool_id.address,
                    local_block = state.block_number,
                    onchain_block = onchain.block_number,
                    drift_bps,
                    "pool state calibration mismatch"
                );
                self.recorder
                    .record_pool_state_warning(PoolStateWarning {
                        pool_address: state.pool_id.address,
                        dex: state.dex,
                        variant: state.variant,
                        block_number: onchain.block_number,
                        local_state: state.clone(),
                        onchain_state: onchain.clone(),
                        drift_bps,
                        message,
                        created_at: chrono::Utc::now(),
                    })
                    .await?;
                *state = onchain;
                corrected = true;
            }
        }

        if corrected {
            self.publish_monitored_states(&states).await?;
        }

        Ok(states)
    }
}

pub async fn run<P>(service: &MarketDataService<P>) -> Result<()>
where
    P: PoolStateStore,
{
    service.run().await?;
    Ok(())
}

fn address_set(states: &[PoolState]) -> HashSet<String> {
    states
        .iter()
        .map(|state| format!("{:#x}", state.pool_id.address))
        .collect()
}

fn reserve_drift_bps(local: &PoolState, onchain: &PoolState) -> u64 {
    let reserve0 = value_drift_bps(local.reserve0, onchain.reserve0);
    let reserve1 = value_drift_bps(local.reserve1, onchain.reserve1);
    reserve0.max(reserve1)
}

fn value_drift_bps(local: Option<U256>, onchain: Option<U256>) -> u64 {
    let Some(local) = local else {
        return u64::MAX;
    };
    let Some(onchain) = onchain else {
        return u64::MAX;
    };
    if local == onchain {
        return 0;
    }
    if onchain.is_zero() {
        return u64::MAX;
    }
    let diff = if local > onchain {
        local - onchain
    } else {
        onchain - local
    };
    let bps = diff
        .saturating_mul(U256::from(10_000u64))
        .checked_div(onchain)
        .unwrap_or(U256::MAX);
    u64::try_from(bps).unwrap_or(u64::MAX)
}
