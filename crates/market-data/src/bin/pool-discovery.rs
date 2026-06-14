use anyhow::Result;
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_storage::{postgres::PostgresStore, redis::RedisStore};
use market_data::listener;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let settings = Settings::load()?;
    let provider = ChainProvider::from_settings(&settings);
    provider.healthcheck().await?;

    let postgres = PostgresStore::connect(&settings.postgres_url).await?;
    let redis = RedisStore::connect(&settings.redis_url).await?;

    info!("pool-discovery initialized");
    let service = listener::MarketDataService {
        settings: settings.clone(),
        provider,
        pool_store: redis,
        recorder: postgres,
    };
    service.run_pool_discovery().await?;
    Ok(())
}
