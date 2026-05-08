use anyhow::Result;
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::types::{DiscoveredPool, PoolState};
use base_arb_storage::{postgres::PostgresStore, PoolStateStore, RecorderStore};
use std::collections::HashSet;
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use tracing::info;

const REGISTRY_RELOAD_INTERVAL: Duration = Duration::from_secs(30);

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

        self.sync_env_bootstrap_pools().await?;
        let mut monitored_states = self.load_monitored_states().await?;
        self.publish_monitored_states(&monitored_states).await?;

        let mut last_seen_block = self.provider.get_block_number().await?;
        let mut next_registry_reload = Instant::now() + REGISTRY_RELOAD_INTERVAL;
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

            for event in &events {
                info!(
                    pool = %event.pool_address,
                    block_number = event.block_number,
                    event_type = %event.event_type,
                    "event received"
                );
                self.recorder.record_dex_event(event.clone()).await?;
            }

            if !events.is_empty() {
                monitored_states = self.refresh_current_states(&monitored_states).await?;
                self.publish_monitored_states(&monitored_states).await?;
            }

            last_seen_block = latest_block;
        }
    }

    async fn load_monitored_states(&self) -> Result<Vec<PoolState>> {
        let registry_pools = self.recorder.enabled_registry_pools().await?;
        if registry_pools.is_empty() {
            info!("pool registry is empty; falling back to .env configured pools");
            let states = self
                .provider
                .bootstrap_configured_pools(&self.settings)
                .await?;
            self.seed_registry_from_states(&states).await?;
            return Ok(states);
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

    async fn refresh_current_states(&self, states: &[PoolState]) -> Result<Vec<PoolState>> {
        let registry_entries = states
            .iter()
            .map(|state| base_arb_common::types::PoolRegistryEntry {
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
            .collect::<Vec<_>>();

        let mut out = Vec::with_capacity(registry_entries.len());
        for entry in &registry_entries {
            out.push(self.provider.fetch_pool_state_from_registry(entry).await?);
        }
        Ok(out)
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

    async fn sync_env_bootstrap_pools(&self) -> Result<()> {
        let states = self
            .provider
            .bootstrap_configured_pools(&self.settings)
            .await?;
        self.seed_registry_from_states(&states).await?;
        Ok(())
    }

    async fn seed_registry_from_states(&self, states: &[PoolState]) -> Result<()> {
        for state in states {
            let symbol = short_pair_symbol(state);
            let pair_id = self
                .recorder
                .upsert_token_pair(state.pool_id.chain_id, state.token0, state.token1, &symbol)
                .await?;
            self.recorder
                .upsert_discovered_pool(
                    pair_id,
                    &DiscoveredPool {
                        state: state.clone(),
                        tick_spacing: None,
                        stable: None,
                        source: "env_bootstrap".to_string(),
                    },
                )
                .await?;
            info!(
                pool = %state.pool_id.address,
                symbol,
                "seeded .env bootstrap pool into registry"
            );
        }
        Ok(())
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

fn short_pair_symbol(state: &PoolState) -> String {
    format!(
        "{}/{}",
        short_address(state.token0),
        short_address(state.token1)
    )
}

fn short_address(address: alloy_primitives::Address) -> String {
    let value = format!("{address:#x}");
    value.chars().take(8).collect()
}
