use anyhow::Result;
use base_arb_chain::events::DexEvent;
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_storage::{PoolStateStore, RecorderStore};
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

        let initial_states = self.provider.bootstrap_configured_pools(&self.settings).await?;
        for state in initial_states {
            self.pool_store.set_pool_state(state.clone()).await?;
            self.recorder.record_pool_state(state.clone()).await?;
            super::state_updater::log_pool_state_update(&state);
        }

        let event = DexEvent {
            block_number: 1,
            tx_hash: "0xdemo".into(),
            log_index: 0,
            pool_address: self
                .settings
                .aerodrome_usdc_weth_pool
                .unwrap_or(alloy_primitives::address!(
                    "1111111111111111111111111111111111111111"
                )),
            dex: base_arb_common::types::DexKind::Aerodrome,
            event_type: "Sync".into(),
            raw_data_json: serde_json::json!({
                "reserve0": "200000000000",
                "reserve1": "100000000000000000000"
            }),
        };

        info!(
            pool = %event.pool_address,
            event_type = %event.event_type,
            "event received"
        );
        self.recorder.record_dex_event(event).await?;
        Ok(())
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
