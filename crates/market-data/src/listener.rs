use anyhow::Result;
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_storage::{PoolStateStore, RecorderStore};
use tokio::time::{interval, Duration, MissedTickBehavior};
use tracing::info;

pub struct MarketDataService<P, R> {
    pub settings: Settings,
    pub provider: ChainProvider,
    pub pool_store: P,
    pub recorder: R,
}

impl<P, R> MarketDataService<P, R>
where
    P: PoolStateStore,
    R: RecorderStore,
{
    pub async fn run(&self) -> Result<()> {
        info!("event listener started");

        let initial_states = self
            .provider
            .bootstrap_configured_pools(&self.settings)
            .await?;
        for state in initial_states {
            self.pool_store.set_pool_state(state.clone()).await?;
            self.recorder.record_pool_state(state.clone()).await?;
            super::state_updater::log_pool_state_update(&state);
        }

        let mut last_seen_block = self.provider.get_block_number().await?;
        info!(last_seen_block, "market-data synchronized at startup");

        let mut ticker = interval(Duration::from_secs(3));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            let latest_block = self.provider.get_block_number().await?;
            if latest_block <= last_seen_block {
                continue;
            }

            let events = self
                .provider
                .fetch_relevant_events(&self.settings, last_seen_block + 1, latest_block)
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
                let refreshed_states = self
                    .provider
                    .bootstrap_configured_pools(&self.settings)
                    .await?;
                for state in refreshed_states {
                    self.pool_store.set_pool_state(state.clone()).await?;
                    self.recorder.record_pool_state(state.clone()).await?;
                    super::state_updater::log_pool_state_update(&state);
                }
            }

            last_seen_block = latest_block;
        }
    }
}

pub async fn run<P, R>(service: &MarketDataService<P, R>) -> Result<()>
where
    P: PoolStateStore,
    R: RecorderStore,
{
    service.run().await?;
    Ok(())
}
