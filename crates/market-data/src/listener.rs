use alloy_primitives::{Address, B256, U256};
use anyhow::Result;
use base_arb_chain::events::DexEvent;
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::constants::{
    AERODROME_CLASSIC_FACTORY, AERODROME_SLIPSTREAM_FACTORIES, PANCAKE_V3_FACTORY,
};
use base_arb_common::types::{
    DexKind, PoolId, PoolRegistryEntry, PoolState, PoolStateValidation, PoolStateWarning,
    PoolVariant, TickState,
};
use base_arb_storage::{
    postgres::{FactoryRegistryRecord, PostgresStore},
    CurrentBlockStore, PoolChangeStore, PoolStateStore, RecorderStore, TickChangeStore,
    TickStateStore,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use tokio::time::{interval, sleep, Duration, Instant, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::{debug, info, warn};

const REGISTRY_RELOAD_INTERVAL: Duration = Duration::from_secs(30);
const FLASHBLOCK_RECONNECT_DELAY: Duration = Duration::from_secs(2);
const VALIDATION_DELAY_BLOCKS: u64 = 2;
const VALIDATION_MAX_PER_IDLE_TICK: usize = 8;
const POOL_DISCOVERY_INTERVAL: Duration = Duration::from_millis(500);
const POOL_DISCOVERY_MAX_BLOCK_SPAN: u64 = 50;
const TICK_WARMUP_BATCH_SIZE: usize = 16;
const TICK_WARMUP_BATCH_PAUSE: Duration = Duration::from_millis(50);
const MAX_PENDING_VALIDATIONS: usize = 20_000;
const UNISWAP_V3_FACTORY: &str = "0x33128a8fC17869897dcE68Ed026d694621f6FDfD";
const V3_POOL_CREATED_TOPIC: &str =
    "0x783cca1c0412dd0d695e784568c96da2e9c22ff989357a2e8b1d9b2b4e6b7118";
const CLASSIC_POOL_CREATED_TOPIC: &str =
    "0x2128d88d14c80cb081c1252a5acff7a264671bf199ce226b53788fb26065005e";
const CLASSIC_PAIR_CREATED_TOPIC: &str =
    "0xc4805696c66d7cf352fc1d6bb633ad5ee82f6cb577c453024b6e0eb8306c6fc9";
const SLIPSTREAM_POOL_CREATED_TOPIC: &str =
    "0xab0d57f0df537bb25e80245ef7748fa62353808c54d6e528a9dd20887aed9ac2";
const SLIPSTREAM_POOL_CREATED_WITH_INDEX_TOPIC: &str =
    "0xb4b64a6a7c41cd0232bfad78d5f845870be74762857744ff02be28578c5f4cb9";
const V3_SWAP_TOPIC: &str = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67";
const PANCAKE_V3_SWAP_TOPIC: &str =
    "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83";
const CLASSIC_SWAP_TOPIC: &str =
    "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822";
const ERC20_TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

pub struct MarketDataService<P> {
    pub settings: Settings,
    pub provider: ChainProvider,
    pub pool_store: P,
    pub recorder: PostgresStore,
}

impl<P> MarketDataService<P>
where
    P: PoolStateStore
        + PoolChangeStore
        + CurrentBlockStore
        + TickChangeStore
        + TickStateStore
        + Clone
        + Send
        + Sync
        + 'static,
{
    pub async fn run(&self) -> Result<()> {
        info!("event listener started");
        self.seed_default_factories().await?;

        let mut monitored_states = self.load_monitored_states().await?;
        self.publish_monitored_states(&monitored_states, "onchain_init")
            .await?;
        self.spawn_initialized_tick_warmup(monitored_states.clone(), "onchain_init");
        self.spawn_flashblocks_listener();

        let mut last_seen_block = self.provider.get_block_number().await?;
        self.pool_store.set_current_block(last_seen_block).await?;
        let mut next_registry_reload = Instant::now() + REGISTRY_RELOAD_INTERVAL;
        let mut recent_logs = RecentLogCache::new(20_000);
        let mut pending_validations = VecDeque::new();
        info!(last_seen_block, "market-data synchronized at startup");

        let mut ticker = interval(Duration::from_millis(100));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            let latest_block = self.provider.get_block_number().await?;
            self.pool_store.set_current_block(latest_block).await?;
            if latest_block <= last_seen_block {
                if Instant::now() >= next_registry_reload {
                    monitored_states = self.reload_if_changed(monitored_states).await?;
                    next_registry_reload = Instant::now() + REGISTRY_RELOAD_INTERVAL;
                }
                self.validate_due_snapshots(
                    &mut pending_validations,
                    latest_block,
                    VALIDATION_MAX_PER_IDLE_TICK,
                )
                .await?;
                continue;
            }

            if Instant::now() >= next_registry_reload {
                monitored_states = self.reload_if_changed(monitored_states).await?;
                next_registry_reload = Instant::now() + REGISTRY_RELOAD_INTERVAL;
            }

            let block_started = Instant::now();
            let fetch_started = Instant::now();
            let events = self
                .provider
                .fetch_relevant_events_for_pools(
                    &monitored_states,
                    last_seen_block + 1,
                    latest_block,
                )
                .await?;
            let fetch_ms = fetch_started.elapsed().as_millis() as u64;

            let apply_started = Instant::now();
            let mut changed_pools = HashSet::new();
            let mut validation_snapshots = BTreeMap::new();
            let mut classic_fee_refreshes = HashMap::new();
            let mut slipstream_fee_refreshes = HashMap::new();
            for event in &events {
                let duplicate_log = !recent_logs.insert(event.tx_hash.clone(), event.log_index);
                if duplicate_log {
                    warn!(
                        tx_hash = %event.tx_hash,
                        log_index = event.log_index,
                        pool = %event.pool_address,
                        "duplicate event will skip non-idempotent updates"
                    );
                }
                debug!(
                    pool = %event.pool_address,
                    block_number = event.block_number,
                    event_type = %event.event_type,
                    "event received"
                );
                if !duplicate_log {
                    self.recorder.record_dex_event(event.clone()).await?;
                }
                for state in &mut monitored_states {
                    if state.pool_id.address != event.pool_address {
                        continue;
                    }
                    if !duplicate_log {
                        let tick_deltas =
                            super::state_updater::v3_tick_deltas_from_event(state, event)?;
                        if !tick_deltas.is_empty() {
                            self.apply_tick_deltas(
                                &state.pool_id,
                                &tick_deltas,
                                event.block_number,
                            )
                            .await?;
                        }
                    }
                    if super::state_updater::is_v3_liquidity_event(state, event)? {
                        if duplicate_log {
                            break;
                        }
                        if self
                            .refresh_v3_state_at_block(state, event.block_number)
                            .await?
                        {
                            changed_pools.insert(state.pool_id.address);
                            validation_snapshots
                                .insert((state.block_number, state.pool_id.address), state.clone());
                            debug!(
                                pool = %state.pool_id.address,
                                block_number = state.block_number,
                                "V3 pool state refreshed from block-pinned onchain liquidity event"
                            );
                        }
                    } else if super::state_updater::apply_event_to_pool_state(state, event)? {
                        changed_pools.insert(state.pool_id.address);
                        validation_snapshots
                            .insert((state.block_number, state.pool_id.address), state.clone());
                        if matches!(
                            (state.dex, state.variant),
                            (DexKind::Aerodrome, PoolVariant::AerodromeSlipstream)
                        ) {
                            slipstream_fee_refreshes
                                .insert(state.pool_id.address, event.block_number);
                        } else if matches!(
                            (state.dex, state.variant),
                            (DexKind::Aerodrome, PoolVariant::AerodromeVolatile)
                        ) {
                            classic_fee_refreshes.insert(state.pool_id.address, event.block_number);
                        }
                        debug!(
                            pool = %state.pool_id.address,
                            block_number = state.block_number,
                            "pool state locally updated from event"
                        );
                    }
                    break;
                }
            }
            let apply_ms = apply_started.elapsed().as_millis() as u64;

            let fee_started = Instant::now();
            let mut fee_refreshed_pools = HashSet::new();
            for (pool, block_number) in classic_fee_refreshes {
                let Some(state) = monitored_states
                    .iter_mut()
                    .find(|state| state.pool_id.address == pool)
                else {
                    continue;
                };
                let fee_result = async {
                    let block_hash = self.provider.get_block_hash(block_number).await?;
                    self.provider
                        .fetch_aerodrome_classic_fee_bps_at_block_hash(
                            state.factory_address,
                            pool,
                            state.stable.unwrap_or(false),
                            &block_hash,
                        )
                        .await
                }
                .await;
                match fee_result {
                    Ok(fee_bps) => {
                        state.fee_bps = fee_bps;
                        fee_refreshed_pools.insert(pool);
                        validation_snapshots
                            .insert((state.block_number, state.pool_id.address), state.clone());
                        debug!(
                            pool = %pool,
                            block_number,
                            fee_bps,
                            "Aerodrome Classic factory fee refreshed after reserve event"
                        );
                    }
                    Err(err) => {
                        changed_pools.remove(&pool);
                        validation_snapshots.retain(|(_, address), _| *address != pool);
                        warn!(
                            pool = %pool,
                            block_number,
                            error = %err,
                            "Classic state update withheld because factory fee refresh failed"
                        );
                    }
                }
            }

            for (pool, block_number) in slipstream_fee_refreshes {
                let Some(state) = monitored_states
                    .iter_mut()
                    .find(|state| state.pool_id.address == pool)
                else {
                    continue;
                };
                let fee_result = async {
                    let block_hash = self.provider.get_block_hash(block_number).await?;
                    self.provider
                        .fetch_aerodrome_slipstream_fee_pips_at_block_hash(
                            state.factory_address,
                            pool,
                            &block_hash,
                        )
                        .await
                }
                .await;
                match fee_result {
                    Ok(fee_pips) => {
                        state.fee_pips = Some(fee_pips);
                        state.fee_bps = fee_pips / 100;
                        fee_refreshed_pools.insert(pool);
                        validation_snapshots
                            .insert((state.block_number, state.pool_id.address), state.clone());
                        debug!(
                            pool = %pool,
                            block_number,
                            fee_pips,
                            "Slipstream dynamic fee refreshed after swap event"
                        );
                    }
                    Err(err) => {
                        changed_pools.remove(&pool);
                        validation_snapshots.retain(|(_, address), _| *address != pool);
                        warn!(
                            pool = %pool,
                            block_number,
                            error = %err,
                            "Slipstream state update withheld because dynamic fee refresh failed"
                        );
                    }
                }
            }

            let fee_ms = fee_started.elapsed().as_millis() as u64;

            let publish_started = Instant::now();
            if !changed_pools.is_empty() {
                self.publish_selected_states(&monitored_states, &changed_pools, "local_event")
                    .await?;
            }
            if !fee_refreshed_pools.is_empty() {
                self.publish_selected_states(
                    &monitored_states,
                    &fee_refreshed_pools,
                    "fee_refresh",
                )
                .await?;
            }
            let publish_ms = publish_started.elapsed().as_millis() as u64;

            enqueue_validation_snapshots(&mut pending_validations, validation_snapshots);
            info!(
                from_block = last_seen_block + 1,
                to_block = latest_block,
                block_span = latest_block.saturating_sub(last_seen_block),
                events = events.len(),
                changed_pools = changed_pools.len(),
                fee_refreshed_pools = fee_refreshed_pools.len(),
                watermarked_pools = 0usize,
                fetch_ms,
                apply_ms,
                fee_ms,
                publish_ms,
                total_ms = block_started.elapsed().as_millis() as u64,
                "market-data sealed block summary"
            );

            last_seen_block = latest_block;
        }
    }

    pub async fn run_pool_discovery(&self) -> Result<()> {
        info!("pool discovery started");
        self.seed_default_factories().await?;

        let mut last_scanned_block = self.provider.get_block_number().await?;
        let mut globally_observed_pools = HashSet::new();
        info!(last_scanned_block, "pool discovery synchronized at startup");

        let mut ticker = interval(POOL_DISCOVERY_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            let latest_block = self.provider.get_block_number().await?;
            if latest_block <= last_scanned_block {
                continue;
            }

            let from_block = last_scanned_block + 1;
            let to_block =
                latest_block.min(last_scanned_block.saturating_add(POOL_DISCOVERY_MAX_BLOCK_SPAN));
            let started = Instant::now();
            self.discover_live_pool_creations(from_block, to_block)
                .await?;
            self.discover_global_swap_pools(from_block, to_block, &mut globally_observed_pools)
                .await?;
            info!(
                from_block,
                to_block,
                latest_block,
                lag_blocks = latest_block.saturating_sub(to_block),
                discovery_ms = started.elapsed().as_millis() as u64,
                "pool discovery scan complete"
            );
            last_scanned_block = to_block;
        }
    }

    pub async fn run_competitor_pool_discovery(&self) -> Result<()> {
        if !self.settings.competitor_pool_discovery_enabled {
            info!("competitor pool discovery disabled");
            return Ok(());
        }
        let Some(collector) = self.settings.competitor_collector_address else {
            info!("competitor pool discovery skipped because collector address is not configured");
            return Ok(());
        };

        let latest_block = self.provider.get_block_number().await?;
        let mut last_scanned_block =
            latest_block.saturating_sub(self.settings.competitor_pool_discovery_lookback_blocks);
        let mut recent_txs = RecentTxCache::new(20_000);
        info!(
            collector = %collector,
            last_scanned_block,
            "competitor pool discovery synchronized at startup"
        );

        let mut ticker = interval(Duration::from_millis(
            self.settings.competitor_pool_discovery_interval_ms.max(100),
        ));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            ticker.tick().await;
            let latest_block = self.provider.get_block_number().await?;
            if latest_block <= last_scanned_block {
                continue;
            }
            let from_block = last_scanned_block + 1;
            let to_block = latest_block.min(
                last_scanned_block.saturating_add(
                    self.settings
                        .competitor_pool_discovery_max_block_span
                        .max(1),
                ),
            );
            let started = Instant::now();
            match self
                .discover_competitor_receipt_pools(collector, from_block, to_block, &mut recent_txs)
                .await
            {
                Ok(summary) => {
                    if summary.transfer_txs > 0 || summary.imported > 0 || summary.observed_only > 0
                    {
                        info!(
                            collector = %collector,
                            from_block,
                            to_block,
                            latest_block,
                            transfer_txs = summary.transfer_txs,
                            receipts = summary.receipts,
                            swap_logs = summary.swap_logs,
                            imported = summary.imported,
                            observed_only = summary.observed_only,
                            skipped = summary.skipped,
                            discovery_ms = started.elapsed().as_millis() as u64,
                            "competitor pool discovery scan complete"
                        );
                    }
                }
                Err(err) => {
                    warn!(
                        collector = %collector,
                        from_block,
                        to_block,
                        error = %err,
                        "competitor pool discovery scan failed"
                    );
                }
            }
            last_scanned_block = to_block;
        }
    }

    async fn discover_competitor_receipt_pools(
        &self,
        collector: Address,
        from_block: u64,
        to_block: u64,
        recent_txs: &mut RecentTxCache,
    ) -> Result<CompetitorPoolDiscoverySummary> {
        if from_block > to_block {
            return Ok(CompetitorPoolDiscoverySummary::default());
        }

        let params = json!([{
            "fromBlock": format!("0x{from_block:x}"),
            "toBlock": format!("0x{to_block:x}"),
            "topics": [ERC20_TRANSFER_TOPIC, null, address_topic(collector)]
        }]);
        let logs = match self.provider.get_logs_raw(params).await {
            Ok(logs) => logs,
            Err(err) => {
                warn!(
                    from_block,
                    to_block,
                    error = %err,
                    "competitor transfer getLogs failed"
                );
                return Ok(CompetitorPoolDiscoverySummary::default());
            }
        };
        if logs.is_empty() {
            return Ok(CompetitorPoolDiscoverySummary::default());
        }

        let trusted = self
            .recorder
            .trusted_factory_registry(self.settings.chain_id)
            .await?
            .into_iter()
            .filter_map(|row| {
                let address = row.factory_address.parse::<Address>().ok()?;
                let dex = parse_dex_kind(&row.dex).ok()?;
                let variant = parse_pool_variant(&row.variant).ok()?;
                Some((address, (dex, variant, row)))
            })
            .collect::<HashMap<_, _>>();

        let mut summary = CompetitorPoolDiscoverySummary::default();
        let mut receipt_txs = HashSet::new();
        for raw in logs {
            let Some(tx_hash) = raw_log_tx_hash(&raw) else {
                summary.skipped += 1;
                continue;
            };
            if !recent_txs.insert(tx_hash) {
                continue;
            }
            receipt_txs.insert(tx_hash);
        }
        summary.transfer_txs = receipt_txs.len();

        let mut seen_pools = HashSet::new();
        for tx_hash in receipt_txs {
            let Some(receipt) = self.provider.get_transaction_receipt(tx_hash).await? else {
                summary.skipped += 1;
                continue;
            };
            summary.receipts += 1;
            let Some(logs) = receipt.raw.get("logs").and_then(Value::as_array) else {
                continue;
            };
            for raw in logs {
                let Some(topic0) = raw_log_topic0(raw) else {
                    continue;
                };
                if !is_supported_swap_topic(&topic0) {
                    continue;
                }
                let log = match parse_swap_log(raw.clone()) {
                    Ok(log) => log,
                    Err(err) => {
                        debug!(
                            tx_hash = %tx_hash,
                            error = %err,
                            "competitor receipt swap log skipped"
                        );
                        summary.skipped += 1;
                        continue;
                    }
                };
                summary.swap_logs += 1;
                if !seen_pools.insert(log.pool) {
                    continue;
                }
                match self.process_observed_swap_pool(&log, &trusted).await {
                    Ok(LivePoolDiscoveryOutcome::Imported) => summary.imported += 1,
                    Ok(LivePoolDiscoveryOutcome::ObservedOnly) => summary.observed_only += 1,
                    Ok(LivePoolDiscoveryOutcome::Skipped) => summary.skipped += 1,
                    Err(err) => {
                        debug!(
                            tx_hash = %tx_hash,
                            pool = %log.pool,
                            topic0 = %log.topic0,
                            error = %err,
                            "competitor receipt pool skipped"
                        );
                        summary.skipped += 1;
                    }
                }
            }
        }

        Ok(summary)
    }

    fn spawn_flashblocks_listener(&self) {
        if !self.settings.market_data_flashblocks_enabled {
            info!("Flashblocks pendingLogs listener disabled");
            return;
        }

        let service = MarketDataService {
            settings: self.settings.clone(),
            provider: self.provider.clone(),
            pool_store: self.pool_store.clone(),
            recorder: self.recorder.clone(),
        };
        tokio::spawn(async move {
            service.run_flashblocks_listener().await;
        });
    }

    async fn seed_default_factories(&self) -> Result<()> {
        let chain_id = self.settings.chain_id;
        let aerodrome_classic = self
            .settings
            .aerodrome_pool_factory
            .unwrap_or(AERODROME_CLASSIC_FACTORY.parse()?);
        self.recorder
            .upsert_factory_registry(
                chain_id,
                aerodrome_classic,
                "Aerodrome",
                "AerodromeVolatile",
                true,
                true,
                "default_config",
                Some("official/default Aerodrome Classic factory"),
                None,
                None,
                0,
            )
            .await?;

        let uniswap_v3 = self
            .settings
            .uniswap_v3_factory
            .unwrap_or(UNISWAP_V3_FACTORY.parse()?);
        self.recorder
            .upsert_factory_registry(
                chain_id,
                uniswap_v3,
                "UniswapV3",
                "UniswapV3",
                true,
                true,
                "default_config",
                Some("official/default Uniswap V3 factory"),
                None,
                None,
                0,
            )
            .await?;

        let pancake_v3 = self
            .settings
            .pancake_v3_factory
            .unwrap_or(PANCAKE_V3_FACTORY.parse()?);
        self.recorder
            .upsert_factory_registry(
                chain_id,
                pancake_v3,
                "PancakeSwap",
                "PancakeV3",
                true,
                true,
                "default_config",
                Some("official/default Pancake V3 factory"),
                None,
                None,
                0,
            )
            .await?;

        let mut slipstream_factories = Vec::new();
        if let Some(factory) = self.settings.aerodrome_slipstream_factory {
            slipstream_factories.push(factory);
        }
        for factory in AERODROME_SLIPSTREAM_FACTORIES {
            let factory = factory.parse()?;
            if !slipstream_factories.contains(&factory) {
                slipstream_factories.push(factory);
            }
        }
        for factory in slipstream_factories {
            self.recorder
                .upsert_factory_registry(
                    chain_id,
                    factory,
                    "Aerodrome",
                    "AerodromeSlipstream",
                    true,
                    true,
                    "default_config",
                    Some("official/default Aerodrome Slipstream factory"),
                    None,
                    None,
                    0,
                )
                .await?;
        }
        Ok(())
    }

    async fn discover_live_pool_creations(&self, from_block: u64, to_block: u64) -> Result<()> {
        if from_block > to_block {
            return Ok(());
        }
        let params = json!([{
            "fromBlock": format!("0x{from_block:x}"),
            "toBlock": format!("0x{to_block:x}"),
            "topics": [[
                V3_POOL_CREATED_TOPIC,
                CLASSIC_POOL_CREATED_TOPIC,
                CLASSIC_PAIR_CREATED_TOPIC,
                SLIPSTREAM_POOL_CREATED_TOPIC,
                SLIPSTREAM_POOL_CREATED_WITH_INDEX_TOPIC
            ]]
        }]);
        let logs = match self.provider.get_logs_raw(params).await {
            Ok(logs) => logs,
            Err(err) => {
                warn!(
                    from_block,
                    to_block,
                    error = %err,
                    "live pool discovery getLogs failed"
                );
                return Ok(());
            }
        };
        if logs.is_empty() {
            return Ok(());
        }

        let trusted = self
            .recorder
            .trusted_factory_registry(self.settings.chain_id)
            .await?
            .into_iter()
            .filter_map(|row| {
                let address = row.factory_address.parse::<Address>().ok()?;
                let dex = parse_dex_kind(&row.dex).ok()?;
                let variant = parse_pool_variant(&row.variant).ok()?;
                Some((address, (dex, variant, row)))
            })
            .collect::<HashMap<_, _>>();

        let mut imported = 0usize;
        let mut observed_only = 0usize;
        for raw in logs {
            match self.process_pool_creation_log(raw, &trusted).await {
                Ok(LivePoolDiscoveryOutcome::Imported) => imported += 1,
                Ok(LivePoolDiscoveryOutcome::ObservedOnly) => observed_only += 1,
                Ok(LivePoolDiscoveryOutcome::Skipped) => {}
                Err(err) => {
                    debug!(error = %err, "live pool discovery log skipped");
                }
            }
        }
        if imported > 0 || observed_only > 0 {
            info!(
                imported,
                observed_only, from_block, to_block, "live pool discovery processed creation logs"
            );
        }
        Ok(())
    }

    async fn discover_global_swap_pools(
        &self,
        from_block: u64,
        to_block: u64,
        seen_pools: &mut HashSet<Address>,
    ) -> Result<()> {
        if !self.settings.market_data_global_pool_discovery_enabled || from_block > to_block {
            return Ok(());
        }
        let params = json!([{
            "fromBlock": format!("0x{from_block:x}"),
            "toBlock": format!("0x{to_block:x}"),
            "topics": [[
                V3_SWAP_TOPIC,
                PANCAKE_V3_SWAP_TOPIC,
                CLASSIC_SWAP_TOPIC
            ]]
        }]);
        let logs = match self.provider.get_logs_raw(params).await {
            Ok(logs) => logs,
            Err(err) => {
                warn!(
                    from_block,
                    to_block,
                    error = %err,
                    "global swap pool discovery getLogs failed"
                );
                return Ok(());
            }
        };
        if logs.is_empty() {
            return Ok(());
        }

        let trusted = self
            .recorder
            .trusted_factory_registry(self.settings.chain_id)
            .await?
            .into_iter()
            .filter_map(|row| {
                let address = row.factory_address.parse::<Address>().ok()?;
                let dex = parse_dex_kind(&row.dex).ok()?;
                let variant = parse_pool_variant(&row.variant).ok()?;
                Some((address, (dex, variant, row)))
            })
            .collect::<HashMap<_, _>>();

        let mut imported = 0usize;
        let mut observed_only = 0usize;
        let mut skipped = 0usize;
        for raw in logs {
            let log = match parse_swap_log(raw) {
                Ok(log) => log,
                Err(err) => {
                    debug!(error = %err, "global swap discovery log skipped");
                    skipped += 1;
                    continue;
                }
            };
            if !seen_pools.insert(log.pool) {
                continue;
            }
            match self.process_observed_swap_pool(&log, &trusted).await {
                Ok(LivePoolDiscoveryOutcome::Imported) => imported += 1,
                Ok(LivePoolDiscoveryOutcome::ObservedOnly) => observed_only += 1,
                Ok(LivePoolDiscoveryOutcome::Skipped) => skipped += 1,
                Err(err) => {
                    debug!(
                        pool = %log.pool,
                        topic0 = %log.topic0,
                        error = %err,
                        "global swap discovery pool skipped"
                    );
                    skipped += 1;
                }
            }
        }

        if imported > 0 || observed_only > 0 {
            info!(
                imported,
                observed_only,
                skipped,
                from_block,
                to_block,
                "global swap pool discovery processed swap logs"
            );
        }
        Ok(())
    }

    async fn process_observed_swap_pool(
        &self,
        log: &SwapObservationLog,
        trusted: &HashMap<Address, (DexKind, PoolVariant, FactoryRegistryRecord)>,
    ) -> Result<LivePoolDiscoveryOutcome> {
        let metadata = match self
            .provider
            .resolve_observed_pool_metadata(log.pool, &log.topic0)
            .await
        {
            Ok(metadata) => metadata,
            Err(err) => {
                debug!(
                    pool = %log.pool,
                    topic0 = %log.topic0,
                    error = %err,
                    "global swap discovery metadata resolve failed"
                );
                return Ok(LivePoolDiscoveryOutcome::Skipped);
            }
        };
        let symbol = pair_symbol(&self.provider, metadata.token0, metadata.token1).await;
        let Some(factory) = metadata.factory_address else {
            self.recorder
                .upsert_observed_pool(
                    self.settings.chain_id,
                    log.pool,
                    &log.topic0,
                    swap_family_for_topic(&log.topic0),
                    Some(metadata.token0),
                    Some(metadata.token1),
                    Some(&symbol),
                    None,
                    None,
                    None,
                    metadata.fee_bps,
                    metadata.fee_pips,
                    metadata.tick_spacing,
                    metadata.stable,
                    1,
                    1,
                    Some(i64::try_from(log.block_number)?),
                    Some(i64::try_from(log.block_number)?),
                    "observed_only",
                    Some("pool factory() unavailable; cannot prove executor support"),
                )
                .await?;
            return Ok(LivePoolDiscoveryOutcome::ObservedOnly);
        };

        if let Some((dex, variant, _row)) = trusted.get(&factory) {
            match self
                .provider
                .resolve_pool_for_trusted_factory(log.pool, factory, *dex, *variant)
                .await
            {
                Ok(discovered) => {
                    self.import_discovered_pool(
                        log.pool,
                        &log.topic0,
                        "swap-log",
                        log.block_number,
                        &symbol,
                        *dex,
                        *variant,
                        factory,
                        discovered,
                    )
                    .await?;
                    return Ok(LivePoolDiscoveryOutcome::Imported);
                }
                Err(err) => {
                    self.record_observed_swap_only_pool(
                        log,
                        &metadata,
                        &symbol,
                        Some(factory),
                        "observed_only",
                        Some(&format!("trusted factory pool resolve failed: {err}")),
                    )
                    .await?;
                    return Ok(LivePoolDiscoveryOutcome::ObservedOnly);
                }
            }
        }

        match self
            .provider
            .resolve_observed_pool_for_registry(&self.settings, log.pool, &log.topic0)
            .await
        {
            Ok(discovered) => {
                let dex = discovered.state.dex;
                let variant = discovered.state.variant;
                let factory = discovered.factory_address.unwrap_or(factory);
                self.import_discovered_pool(
                    log.pool,
                    &log.topic0,
                    "swap-log-auto-classified",
                    log.block_number,
                    &symbol,
                    dex,
                    variant,
                    factory,
                    discovered,
                )
                .await?;
                info!(
                    pool = %log.pool,
                    factory = %factory,
                    symbol,
                    dex = ?dex,
                    variant = ?variant,
                    "global swap discovery auto-imported executable observed pool"
                );
                Ok(LivePoolDiscoveryOutcome::Imported)
            }
            Err(err) => {
                self.record_observed_swap_only_pool(
                    log,
                    &metadata,
                    &symbol,
                    Some(factory),
                    "classified_observed_only",
                    Some(&format!(
                        "not executable by configured routers/factories: {err}"
                    )),
                )
                .await?;
                self.recorder
                    .upsert_factory_registry(
                        self.settings.chain_id,
                        factory,
                        inferred_dex_for_swap_topic(&log.topic0),
                        inferred_variant_for_swap_topic(&log.topic0),
                        false,
                        true,
                        "global_swap_discovery",
                        Some("observed from global swap logs; not trusted for execution"),
                        Some(i64::try_from(log.block_number)?),
                        Some(i64::try_from(log.block_number)?),
                        1,
                    )
                    .await?;
                Ok(LivePoolDiscoveryOutcome::ObservedOnly)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn import_discovered_pool(
        &self,
        pool: Address,
        topic0: &str,
        family: &str,
        block_number: u64,
        symbol: &str,
        dex: DexKind,
        variant: PoolVariant,
        factory: Address,
        discovered: base_arb_common::types::DiscoveredPool,
    ) -> Result<()> {
        self.recorder
            .upsert_token_registry(
                self.settings.chain_id,
                discovered.state.token0,
                symbol.split('/').next().unwrap_or_default(),
            )
            .await?;
        self.recorder
            .upsert_token_registry(
                self.settings.chain_id,
                discovered.state.token1,
                symbol.split('/').nth(1).unwrap_or_default(),
            )
            .await?;
        let (token0, token1) = canonical_pair(discovered.state.token0, discovered.state.token1);
        let pair_id = self
            .recorder
            .upsert_token_pair(self.settings.chain_id, token0, token1, symbol)
            .await?;
        self.recorder
            .upsert_discovered_pool(pair_id, &discovered)
            .await?;
        self.recorder
            .upsert_observed_pool(
                self.settings.chain_id,
                pool,
                topic0,
                family,
                Some(discovered.state.token0),
                Some(discovered.state.token1),
                Some(symbol),
                Some(factory),
                Some(dex_to_string(dex)),
                Some(variant_to_string(variant)),
                Some(discovered.state.fee_bps),
                discovered.state.fee_pips,
                discovered.tick_spacing,
                discovered.stable,
                1,
                1,
                Some(i64::try_from(block_number)?),
                Some(i64::try_from(block_number)?),
                "imported",
                None,
            )
            .await?;
        self.recorder
            .upsert_factory_registry(
                self.settings.chain_id,
                factory,
                dex_to_string(dex),
                variant_to_string(variant),
                true,
                true,
                "global_swap_discovery",
                None,
                Some(i64::try_from(block_number)?),
                Some(i64::try_from(block_number)?),
                1,
            )
            .await?;
        info!(
            pool = %pool,
            factory = %factory,
            symbol,
            dex = ?dex,
            variant = ?variant,
            "global swap discovery imported trusted factory pool"
        );
        Ok(())
    }

    async fn process_pool_creation_log(
        &self,
        raw: Value,
        trusted: &HashMap<Address, (DexKind, PoolVariant, FactoryRegistryRecord)>,
    ) -> Result<LivePoolDiscoveryOutcome> {
        let log = parse_creation_log(raw)?;
        let factory = log.factory;
        let pool = match self.find_created_pool_address(&log).await? {
            Some(pool) => pool,
            None => return Ok(LivePoolDiscoveryOutcome::Skipped),
        };
        let metadata = match self
            .provider
            .resolve_observed_pool_metadata(pool, &log.topic0)
            .await
        {
            Ok(metadata) => metadata,
            Err(err) => {
                self.recorder
                    .upsert_factory_registry(
                        self.settings.chain_id,
                        factory,
                        inferred_dex_for_creation_topic(&log.topic0),
                        inferred_variant_for_creation_topic(&log.topic0),
                        false,
                        true,
                        "live_pool_discovery",
                        Some(&format!(
                            "metadata resolve failed for pool {pool:#x}: {err}"
                        )),
                        Some(i64::try_from(log.block_number)?),
                        Some(i64::try_from(log.block_number)?),
                        1,
                    )
                    .await?;
                return Ok(LivePoolDiscoveryOutcome::ObservedOnly);
            }
        };
        let symbol = pair_symbol(&self.provider, metadata.token0, metadata.token1).await;
        let trusted_factory = trusted.get(&factory);
        if let Some((dex, variant, _row)) = trusted_factory {
            match self
                .provider
                .resolve_pool_for_trusted_factory(pool, factory, *dex, *variant)
                .await
            {
                Ok(discovered) => {
                    self.recorder
                        .upsert_token_registry(
                            self.settings.chain_id,
                            discovered.state.token0,
                            symbol.split('/').next().unwrap_or_default(),
                        )
                        .await?;
                    self.recorder
                        .upsert_token_registry(
                            self.settings.chain_id,
                            discovered.state.token1,
                            symbol.split('/').nth(1).unwrap_or_default(),
                        )
                        .await?;
                    let (token0, token1) =
                        canonical_pair(discovered.state.token0, discovered.state.token1);
                    let pair_id = self
                        .recorder
                        .upsert_token_pair(self.settings.chain_id, token0, token1, &symbol)
                        .await?;
                    self.recorder
                        .upsert_discovered_pool(pair_id, &discovered)
                        .await?;
                    self.recorder
                        .upsert_observed_pool(
                            self.settings.chain_id,
                            pool,
                            &log.topic0,
                            "pool-created",
                            Some(discovered.state.token0),
                            Some(discovered.state.token1),
                            Some(&symbol),
                            Some(factory),
                            Some(dex_to_string(*dex)),
                            Some(variant_to_string(*variant)),
                            Some(discovered.state.fee_bps),
                            discovered.state.fee_pips,
                            discovered.tick_spacing,
                            discovered.stable,
                            1,
                            1,
                            Some(i64::try_from(log.block_number)?),
                            Some(i64::try_from(log.block_number)?),
                            "imported",
                            None,
                        )
                        .await?;
                    self.recorder
                        .upsert_factory_registry(
                            self.settings.chain_id,
                            factory,
                            dex_to_string(*dex),
                            variant_to_string(*variant),
                            true,
                            true,
                            "live_pool_discovery",
                            None,
                            Some(i64::try_from(log.block_number)?),
                            Some(i64::try_from(log.block_number)?),
                            1,
                        )
                        .await?;
                    info!(
                        pool = %pool,
                        factory = %factory,
                        symbol,
                        dex = ?dex,
                        variant = ?variant,
                        "live pool discovery imported trusted factory pool"
                    );
                    Ok(LivePoolDiscoveryOutcome::Imported)
                }
                Err(err) => {
                    self.record_observed_only_pool(
                        &log,
                        pool,
                        &metadata,
                        &symbol,
                        "observed_only",
                        Some(&format!("trusted factory pool resolve failed: {err}")),
                    )
                    .await?;
                    Ok(LivePoolDiscoveryOutcome::ObservedOnly)
                }
            }
        } else {
            let compatible_topic = creation_compatible_swap_topic(&log.topic0);
            match self
                .provider
                .resolve_observed_pool_for_registry(&self.settings, pool, compatible_topic)
                .await
            {
                Ok(discovered) => {
                    let dex = discovered.state.dex;
                    let variant = discovered.state.variant;
                    let factory = discovered.factory_address.unwrap_or(factory);
                    self.import_discovered_pool(
                        pool,
                        &log.topic0,
                        "pool-created-auto-classified",
                        log.block_number,
                        &symbol,
                        dex,
                        variant,
                        factory,
                        discovered,
                    )
                    .await?;
                    info!(
                        pool = %pool,
                        factory = %factory,
                        symbol,
                        dex = ?dex,
                        variant = ?variant,
                        "live pool discovery auto-imported executable observed pool"
                    );
                    Ok(LivePoolDiscoveryOutcome::Imported)
                }
                Err(err) => {
                    self.record_observed_only_pool(
                        &log,
                        pool,
                        &metadata,
                        &symbol,
                        "classified_observed_only",
                        Some(&format!(
                            "not executable by configured routers/factories: {err}"
                        )),
                    )
                    .await?;
                    Ok(LivePoolDiscoveryOutcome::ObservedOnly)
                }
            }
        }
    }

    async fn find_created_pool_address(&self, log: &CreationLog) -> Result<Option<Address>> {
        for candidate in creation_log_candidate_addresses(log) {
            if candidate == Address::ZERO {
                continue;
            }
            let Ok(metadata) = self
                .provider
                .resolve_observed_pool_metadata(candidate, &log.topic0)
                .await
            else {
                continue;
            };
            if metadata.factory_address == Some(log.factory) {
                return Ok(Some(candidate));
            }
        }
        Ok(None)
    }

    async fn record_observed_only_pool(
        &self,
        log: &CreationLog,
        pool: Address,
        metadata: &base_arb_chain::provider::ObservedPoolMetadata,
        symbol: &str,
        import_status: &str,
        reason: Option<&str>,
    ) -> Result<()> {
        self.recorder
            .upsert_observed_pool(
                self.settings.chain_id,
                pool,
                &log.topic0,
                "pool-created",
                Some(metadata.token0),
                Some(metadata.token1),
                Some(symbol),
                Some(log.factory),
                None,
                None,
                metadata.fee_bps,
                metadata.fee_pips,
                metadata.tick_spacing,
                metadata.stable,
                1,
                1,
                Some(i64::try_from(log.block_number)?),
                Some(i64::try_from(log.block_number)?),
                import_status,
                reason,
            )
            .await?;
        self.recorder
            .upsert_factory_registry(
                self.settings.chain_id,
                log.factory,
                inferred_dex_for_creation_topic(&log.topic0),
                inferred_variant_for_creation_topic(&log.topic0),
                false,
                true,
                "live_pool_discovery",
                reason,
                Some(i64::try_from(log.block_number)?),
                Some(i64::try_from(log.block_number)?),
                1,
            )
            .await?;
        Ok(())
    }

    async fn record_observed_swap_only_pool(
        &self,
        log: &SwapObservationLog,
        metadata: &base_arb_chain::provider::ObservedPoolMetadata,
        symbol: &str,
        factory: Option<Address>,
        import_status: &str,
        reason: Option<&str>,
    ) -> Result<()> {
        self.recorder
            .upsert_observed_pool(
                self.settings.chain_id,
                log.pool,
                &log.topic0,
                "swap-log",
                Some(metadata.token0),
                Some(metadata.token1),
                Some(symbol),
                factory,
                None,
                None,
                metadata.fee_bps,
                metadata.fee_pips,
                metadata.tick_spacing,
                metadata.stable,
                1,
                1,
                Some(i64::try_from(log.block_number)?),
                Some(i64::try_from(log.block_number)?),
                import_status,
                reason,
            )
            .await?;
        Ok(())
    }

    async fn run_flashblocks_listener(&self) {
        let ws_url = self
            .settings
            .base_rpc_flashblocks_ws
            .as_deref()
            .filter(|url| !url.trim().is_empty())
            .unwrap_or(&self.settings.base_rpc_ws)
            .to_string();
        if ws_url.trim().is_empty() {
            warn!("Flashblocks listener skipped because no websocket URL is configured");
            return;
        }

        loop {
            match self.run_flashblocks_session(&ws_url).await {
                Ok(()) => {
                    sleep(FLASHBLOCK_RECONNECT_DELAY).await;
                }
                Err(err) => {
                    warn!(
                        error = %err,
                        reconnect_secs = FLASHBLOCK_RECONNECT_DELAY.as_secs(),
                        "Flashblocks pendingLogs session stopped; reconnecting"
                    );
                    sleep(FLASHBLOCK_RECONNECT_DELAY).await;
                }
            }
        }
    }

    async fn run_flashblocks_session(&self, ws_url: &str) -> Result<()> {
        let mut monitored_states = self.load_monitored_states().await?;
        let mut next_registry_reload = Instant::now() + REGISTRY_RELOAD_INTERVAL;
        let mut recent_logs = RecentLogCache::new(20_000);
        let mut fallback_log_index = 0u64;

        let mut subscribed_addresses = address_set(&monitored_states);
        let addresses = unique_pool_addresses(&monitored_states);
        if addresses.is_empty() {
            warn!("Flashblocks pendingLogs listener has no monitored pool addresses");
            return Ok(());
        }

        let (mut ws, _) = connect_async(ws_url).await?;
        let subscribe = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "eth_subscribe",
            "params": [
                "pendingLogs",
                {
                    "address": addresses,
                }
            ]
        });
        ws.send(Message::Text(subscribe.to_string())).await?;
        info!(
            pools = monitored_states.len(),
            ws = %ws_url,
            "Flashblocks pendingLogs listener connected"
        );

        while let Some(message) = ws.next().await {
            if Instant::now() >= next_registry_reload {
                monitored_states = self.reload_if_changed(monitored_states).await?;
                let next_addresses = address_set(&monitored_states);
                if next_addresses != subscribed_addresses {
                    info!("Flashblocks monitored pool set changed; reconnecting subscription");
                    return Ok(());
                }
                subscribed_addresses = next_addresses;
                next_registry_reload = Instant::now() + REGISTRY_RELOAD_INTERVAL;
            }

            let message = message?;
            let text = match message {
                Message::Text(text) => text,
                Message::Binary(bytes) => String::from_utf8(bytes)?,
                Message::Ping(payload) => {
                    ws.send(Message::Pong(payload)).await?;
                    continue;
                }
                Message::Pong(_) => continue,
                Message::Close(frame) => {
                    warn!(?frame, "Flashblocks websocket closed");
                    return Ok(());
                }
                Message::Frame(_) => continue,
            };

            let value: Value = match serde_json::from_str(&text) {
                Ok(value) => value,
                Err(err) => {
                    warn!(error = %err, "Flashblocks websocket message was not JSON");
                    continue;
                }
            };
            if value.get("id").and_then(Value::as_u64) == Some(1) {
                if let Some(error) = value.get("error") {
                    anyhow::bail!("Flashblocks pendingLogs subscription failed: {error}");
                }
                let subscription = value
                    .get("result")
                    .map(Value::to_string)
                    .unwrap_or_else(|| "null".to_string());
                info!(
                    subscription = %subscription,
                    "Flashblocks pendingLogs subscription accepted"
                );
                continue;
            }

            let Some(result) = value
                .get("params")
                .and_then(|params| params.get("result"))
                .cloned()
            else {
                debug!(message = %text, "Flashblocks websocket message ignored");
                continue;
            };

            fallback_log_index = fallback_log_index.wrapping_add(1);
            let fallback_block = if result.get("blockNumber").and_then(Value::as_str).is_some() {
                None
            } else {
                Some(self.provider.get_block_number().await?.saturating_add(1))
            };
            let event = match self.provider.decode_relevant_log_for_pools(
                &monitored_states,
                result,
                fallback_block,
                Some(fallback_log_index),
            ) {
                Ok(Some(event)) => event,
                Ok(None) => continue,
                Err(err) => {
                    warn!(error = %err, "Flashblocks pending log decode failed");
                    continue;
                }
            };

            self.process_flashblock_event(&mut monitored_states, event, &mut recent_logs)
                .await?;
        }

        Ok(())
    }

    async fn process_flashblock_event(
        &self,
        monitored_states: &mut [PoolState],
        event: DexEvent,
        recent_logs: &mut RecentLogCache,
    ) -> Result<()> {
        if !recent_logs.insert(event.tx_hash.clone(), event.log_index) {
            return Ok(());
        }

        debug!(
            pool = %event.pool_address,
            block_number = event.block_number,
            event_type = %event.event_type,
            "Flashblocks pending event received"
        );
        self.pool_store
            .set_current_block(event.block_number)
            .await?;
        self.recorder.record_dex_event(event.clone()).await?;

        for state in monitored_states {
            if state.pool_id.address != event.pool_address {
                continue;
            }

            if super::state_updater::is_v3_liquidity_event(state, &event)? {
                debug!(
                    pool = %state.pool_id.address,
                    block_number = event.block_number,
                    "Flashblocks V3 liquidity event waits for sealed block state/tick refresh"
                );
            } else if super::state_updater::apply_event_to_pool_state(state, &event)? {
                state.valid_through_block = state.valid_through_block.max(event.block_number);
                self.pool_store.set_pool_state(state.clone()).await?;
                self.pool_store
                    .mark_changed_pools(vec![state.pool_id.address])
                    .await?;
                self.recorder
                    .record_pool_state_with_source(state.clone(), "flashblock")
                    .await?;
                debug!(
                    pool = %state.pool_id.address,
                    block_number = state.block_number,
                    "pool state locally updated from Flashblocks pending event"
                );
            }
            break;
        }

        Ok(())
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

        let mut seen = HashSet::new();
        let mut out = Vec::with_capacity(registry_pools.len());
        for entry in &registry_pools {
            if !seen.insert(entry.pool_address) {
                debug!(
                    pool = %entry.pool_address,
                    "duplicate registry pool ignored for market-data monitoring"
                );
                continue;
            }
            out.push(self.provider.fetch_pool_state_from_registry(entry).await?);
        }
        Ok(out)
    }

    async fn reload_if_changed(&self, current: Vec<PoolState>) -> Result<Vec<PoolState>> {
        let current_addresses = address_set(&current);
        let registry_pools = self.recorder.enabled_registry_pools().await?;
        if registry_pools.is_empty() {
            return Ok(current);
        }

        let mut seen = HashSet::new();
        let mut registry_entries = Vec::with_capacity(registry_pools.len());
        for entry in registry_pools {
            if seen.insert(entry.pool_address) {
                registry_entries.push(entry);
            }
        }

        let next_addresses = registry_entries
            .iter()
            .map(|entry| format!("{:#x}", entry.pool_address))
            .collect::<HashSet<_>>();

        if current_addresses == next_addresses {
            return Ok(current);
        }

        let removed = current_addresses.difference(&next_addresses).count();
        let mut next = current
            .into_iter()
            .filter(|state| next_addresses.contains(&format!("{:#x}", state.pool_id.address)))
            .collect::<Vec<_>>();

        let added_entries = registry_entries
            .iter()
            .filter(|entry| !current_addresses.contains(&format!("{:#x}", entry.pool_address)))
            .collect::<Vec<_>>();

        let mut added_states = Vec::with_capacity(added_entries.len());
        let mut failed = 0usize;
        for entry in added_entries {
            match self.provider.fetch_pool_state_from_registry(entry).await {
                Ok(state) => {
                    added_states.push(state.clone());
                    next.push(state);
                }
                Err(err) => {
                    failed += 1;
                    warn!(
                        pool = %entry.pool_address,
                        error = %err,
                        "failed to load newly enabled registry pool"
                    );
                }
            }
        }

        if !added_states.is_empty() || removed > 0 || failed > 0 {
            info!(
                previous = current_addresses.len(),
                next = next_addresses.len(),
                added = added_states.len(),
                removed,
                failed,
                "pool registry changed; reloading monitored pools"
            );
        }

        if !added_states.is_empty() {
            let added = added_states
                .iter()
                .map(|state| state.pool_id.address)
                .collect::<HashSet<_>>();
            self.publish_selected_states(&added_states, &added, "registry_reload")
                .await?;
            self.spawn_initialized_tick_warmup(added_states, "registry_reload");
        }

        Ok(next)
    }

    async fn publish_monitored_states(&self, states: &[PoolState], source: &str) -> Result<()> {
        let selected = states
            .iter()
            .map(|state| state.pool_id.address)
            .collect::<HashSet<_>>();
        self.publish_selected_states(states, &selected, source)
            .await
    }

    async fn publish_selected_states(
        &self,
        states: &[PoolState],
        selected: &HashSet<Address>,
        source: &str,
    ) -> Result<()> {
        let mut seen = HashSet::new();
        let mut changed_pools = Vec::new();
        for state in states {
            if !selected.contains(&state.pool_id.address) {
                continue;
            }
            if !seen.insert(state.pool_id.address) {
                warn!(
                    pool = %state.pool_id.address,
                    source,
                    "duplicate monitored pool skipped while publishing state"
                );
                continue;
            }
            self.pool_store
                .set_current_block(state.block_number)
                .await?;
            let previous = self
                .pool_store
                .get_pool_state(state.pool_id.address)
                .await?;
            if previous
                .as_ref()
                .map(|previous| quote_relevant_pool_state_changed(previous, state))
                .unwrap_or(true)
            {
                changed_pools.push(state.pool_id.address);
            }
            self.pool_store.set_pool_state(state.clone()).await?;
            self.recorder
                .record_pool_state_with_source(state.clone(), source)
                .await?;
            super::state_updater::log_pool_state_update(state);
            debug!(
                    pool = %state.pool_id.address,
                    dex = ?state.dex,
                    variant = ?state.variant,
                source,
                "monitoring pool logs"
            );
        }
        self.pool_store.mark_changed_pools(changed_pools).await?;
        Ok(())
    }

    #[allow(dead_code)]
    async fn active_refresh_states(
        &self,
        mut states: Vec<PoolState>,
        cursor: &mut usize,
        block_number: u64,
    ) -> Result<Vec<PoolState>> {
        if states.is_empty() {
            return Ok(states);
        }

        let block_hash = self.provider.get_block_hash(block_number).await?;
        let batch_size = self
            .settings
            .pool_active_refresh_batch_size
            .try_into()
            .unwrap_or(usize::MAX)
            .clamp(1, states.len());
        if *cursor >= states.len() {
            *cursor = 0;
        }
        let indices = (0..batch_size)
            .map(|offset| (*cursor + offset) % states.len())
            .collect::<Vec<_>>();
        *cursor = (*cursor + batch_size) % states.len();
        let mut refreshed_pools = HashSet::new();
        let mut refreshed_v3_states = Vec::new();
        let mut drifted = 0usize;
        let mut failed = 0usize;

        for index in indices {
            let state = &mut states[index];
            let local = state.clone();
            let entry = registry_entry_from_state(&local);
            let onchain = self
                .provider
                .fetch_pool_state_from_registry_at_block_hash(&entry, &block_hash, block_number)
                .await;
            let onchain = match onchain {
                Ok(onchain) => onchain,
                Err(err) => {
                    failed += 1;
                    warn!(
                        pool = %local.pool_id.address,
                        dex = ?local.dex,
                        variant = ?local.variant,
                        block_number,
                        error = %err,
                        "active pool state refresh failed"
                    );
                    continue;
                }
            };

            let drift_bps = state_drift_bps(&local, &onchain);
            let passed = drift_bps == 0;
            let message = if passed {
                "active refresh validation passed".to_string()
            } else {
                format!("active refresh found latest onchain drift by {drift_bps} bps")
            };

            self.recorder
                .record_pool_state_validation(PoolStateValidation {
                    pool_address: local.pool_id.address,
                    dex: local.dex,
                    variant: local.variant,
                    block_number,
                    block_hash: block_hash.clone(),
                    local_state: local.clone(),
                    onchain_state: onchain.clone(),
                    drift_bps,
                    passed,
                    message: message.clone(),
                    created_at: chrono::Utc::now(),
                })
                .await?;

            if !passed {
                drifted += 1;
                warn!(
                    pool = %local.pool_id.address,
                    dex = ?local.dex,
                    variant = ?local.variant,
                    local_block = local.block_number,
                    onchain_block = onchain.block_number,
                    drift_bps,
                    local_fee_bps = local.fee_bps,
                    onchain_fee_bps = onchain.fee_bps,
                    local_fee_pips = ?local.fee_pips,
                    onchain_fee_pips = ?onchain.fee_pips,
                    "active pool state refresh corrected drift"
                );
                self.recorder
                    .record_pool_state_warning(PoolStateWarning {
                        pool_address: local.pool_id.address,
                        dex: local.dex,
                        variant: local.variant,
                        block_number,
                        local_state: local,
                        onchain_state: onchain.clone(),
                        drift_bps,
                        message,
                        created_at: chrono::Utc::now(),
                    })
                    .await?;
            }

            refreshed_pools.insert(onchain.pool_id.address);
            if is_v3_style_state(&onchain) {
                refreshed_v3_states.push(onchain.clone());
            }
            let mut onchain = onchain;
            onchain.valid_through_block = onchain.valid_through_block.max(block_number);
            *state = onchain;
        }

        if !refreshed_pools.is_empty() {
            advance_valid_through_block(&mut states, block_number);
            self.publish_selected_states(&states, &refreshed_pools, "active_refresh")
                .await?;
            self.publish_initialized_ticks(&refreshed_v3_states).await?;
        }

        info!(
            refreshed = refreshed_pools.len(),
            batch_size,
            total_pools = states.len(),
            failed,
            drifted,
            block_number,
            "active pool state refresh complete"
        );

        Ok(states)
    }

    #[allow(dead_code)]
    async fn refresh_aerodrome_fees(&self, mut states: Vec<PoolState>) -> Result<Vec<PoolState>> {
        if states.is_empty() {
            return Ok(states);
        }

        let block_number = self.provider.get_block_number().await?;
        let block_hash = self.provider.get_block_hash(block_number).await?;
        let mut changed_pools = HashSet::new();
        let mut checked = 0usize;
        let mut failed = 0usize;

        for state in &mut states {
            if !is_aerodrome_fee_state(state) {
                continue;
            }
            checked += 1;
            let local = state.clone();
            let fee_result = match state.variant {
                PoolVariant::AerodromeVolatile => self
                    .provider
                    .fetch_aerodrome_classic_fee_bps_at_block_hash(
                        state.factory_address,
                        state.pool_id.address,
                        state.stable.unwrap_or(false),
                        &block_hash,
                    )
                    .await
                    .map(AerodromeFee::Classic),
                PoolVariant::AerodromeSlipstream => self
                    .provider
                    .fetch_aerodrome_slipstream_fee_pips_at_block_hash(
                        state.factory_address,
                        state.pool_id.address,
                        &block_hash,
                    )
                    .await
                    .map(AerodromeFee::Slipstream),
                _ => continue,
            };

            let fee = match fee_result {
                Ok(fee) => fee,
                Err(err) => {
                    failed += 1;
                    warn!(
                        pool = %state.pool_id.address,
                        dex = ?state.dex,
                        variant = ?state.variant,
                        block_number,
                        error = %err,
                        "Aerodrome fee refresh failed"
                    );
                    continue;
                }
            };

            if !apply_aerodrome_fee(state, fee) {
                continue;
            }

            let drift_bps = state_drift_bps(&local, state);
            let message = format!("Aerodrome fee refresh corrected fee drift by {drift_bps} bps");
            self.recorder
                .record_pool_state_warning(PoolStateWarning {
                    pool_address: local.pool_id.address,
                    dex: local.dex,
                    variant: local.variant,
                    block_number,
                    local_state: local,
                    onchain_state: state.clone(),
                    drift_bps,
                    message,
                    created_at: chrono::Utc::now(),
                })
                .await?;
            changed_pools.insert(state.pool_id.address);
            warn!(
                pool = %state.pool_id.address,
                dex = ?state.dex,
                variant = ?state.variant,
                block_number,
                fee_bps = state.fee_bps,
                fee_pips = ?state.fee_pips,
                drift_bps,
                "Aerodrome fee refresh corrected drift"
            );
        }

        if !changed_pools.is_empty() {
            self.publish_selected_states(&states, &changed_pools, "fee_refresh")
                .await?;
        }

        debug!(
            checked,
            changed = changed_pools.len(),
            failed,
            block_number,
            "Aerodrome fee refresh complete"
        );

        Ok(states)
    }

    async fn refresh_v3_state_at_block(
        &self,
        state: &mut PoolState,
        block_number: u64,
    ) -> Result<bool> {
        let block_hash = match self.provider.get_block_hash(block_number).await {
            Ok(block_hash) => block_hash,
            Err(err) => {
                warn!(
                    pool = %state.pool_id.address,
                    block_number,
                    error = %err,
                    "skipping V3 liquidity refresh because block hash lookup failed"
                );
                return Ok(false);
            }
        };
        let refreshed = self
            .provider
            .fetch_pool_state_from_registry_at_block_hash(
                &base_arb_common::types::PoolRegistryEntry {
                    pool_address: state.pool_id.address,
                    dex: state.dex,
                    variant: state.variant,
                    factory_address: state.factory_address,
                    token0: state.token0,
                    token1: state.token1,
                    fee_bps: state.fee_bps,
                    tick_spacing: state.tick_spacing,
                    stable: state.stable,
                    enabled: true,
                },
                &block_hash,
                block_number,
            )
            .await;
        match refreshed {
            Ok(refreshed) => {
                *state = refreshed;
                Ok(true)
            }
            Err(err) => {
                warn!(
                    pool = %state.pool_id.address,
                    block_number,
                    error = %err,
                    "skipping V3 liquidity refresh because block-pinned eth_call failed"
                );
                Ok(false)
            }
        }
    }

    async fn publish_initialized_ticks(&self, states: &[PoolState]) -> Result<()> {
        let word_radius = self.settings.v3_tick_bitmap_word_radius.max(0);
        for state in states {
            if !is_v3_style_state(state) {
                continue;
            }
            let ticks = match self
                .provider
                .fetch_initialized_ticks_around_state(state, word_radius)
                .await
            {
                Ok(ticks) => ticks,
                Err(err) => {
                    warn!(
                        pool = %state.pool_id.address,
                        dex = ?state.dex,
                        variant = ?state.variant,
                        word_radius,
                        error = %err,
                        "initialized tick refresh failed for pool"
                    );
                    continue;
                }
            };
            if ticks.is_empty() {
                debug!(
                    pool = %state.pool_id.address,
                    word_radius,
                    "initialized tick refresh found no ticks"
                );
                continue;
            }
            let count = ticks.len();
            let existing_ticks = self
                .pool_store
                .get_pool_ticks(state.pool_id.address)
                .await?;
            let ticks_changed = tick_states_changed(&existing_ticks, &ticks);
            if ticks_changed {
                self.pool_store
                    .replace_pool_ticks(state.pool_id.address, ticks)
                    .await?;
                self.pool_store
                    .mark_tick_changed_pools(vec![state.pool_id.address])
                    .await?;
            }
            debug!(
                pool = %state.pool_id.address,
                count,
                ticks_changed,
                word_radius,
                "initialized ticks loaded"
            );
        }
        Ok(())
    }

    fn spawn_initialized_tick_warmup(&self, states: Vec<PoolState>, reason: &'static str) {
        let v3_states = states
            .into_iter()
            .filter(is_v3_style_state)
            .collect::<Vec<_>>();
        if v3_states.is_empty() {
            return;
        }

        let service = MarketDataService {
            settings: self.settings.clone(),
            provider: self.provider.clone(),
            pool_store: self.pool_store.clone(),
            recorder: self.recorder.clone(),
        };
        tokio::spawn(async move {
            let started = Instant::now();
            let total = v3_states.len();
            let mut processed = 0usize;
            info!(
                total,
                batch_size = TICK_WARMUP_BATCH_SIZE,
                reason,
                "initialized tick warmup started"
            );
            for chunk in v3_states.chunks(TICK_WARMUP_BATCH_SIZE) {
                if let Err(err) = service.publish_initialized_ticks(chunk).await {
                    warn!(
                        reason,
                        processed,
                        total,
                        error = %err,
                        "initialized tick warmup batch failed"
                    );
                }
                processed += chunk.len();
                sleep(TICK_WARMUP_BATCH_PAUSE).await;
            }
            info!(
                total,
                warmup_ms = started.elapsed().as_millis() as u64,
                reason,
                "initialized tick warmup complete"
            );
        });
    }

    async fn apply_tick_deltas(
        &self,
        pool_id: &PoolId,
        deltas: &[super::state_updater::TickDelta],
        block_number: u64,
    ) -> Result<()> {
        let mut ticks = self.pool_store.get_pool_ticks(pool_id.address).await?;
        let mut updates = Vec::with_capacity(deltas.len());

        for delta in deltas {
            let existing = ticks
                .iter_mut()
                .find(|tick| tick.tick == delta.tick)
                .cloned();
            let mut tick_state = existing.unwrap_or_else(|| TickState {
                pool_id: pool_id.clone(),
                tick: delta.tick,
                liquidity_net: 0,
                liquidity_gross: U256::ZERO,
                block_number,
                updated_at: chrono::Utc::now(),
            });

            tick_state.liquidity_net = tick_state
                .liquidity_net
                .checked_add(delta.liquidity_net_delta)
                .ok_or_else(|| anyhow::anyhow!("liquidity_net overflow"))?;
            tick_state.liquidity_gross =
                apply_signed_u256_delta(tick_state.liquidity_gross, delta.liquidity_gross_delta)?;
            tick_state.block_number = block_number;
            tick_state.updated_at = chrono::Utc::now();
            updates.push(tick_state);
        }

        if !updates.is_empty() {
            self.pool_store.set_tick_states(updates).await?;
            self.pool_store
                .mark_tick_changed_pools(vec![pool_id.address])
                .await?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    async fn calibrate_states(&self, mut states: Vec<PoolState>) -> Result<Vec<PoolState>> {
        let mut corrected_pools = HashSet::new();

        for state in &mut states {
            if !should_calibrate(state) {
                continue;
            }

            let block_hash = match self.provider.get_block_hash(state.block_number).await {
                Ok(block_hash) => block_hash,
                Err(err) => {
                    warn!(
                        pool = %state.pool_id.address,
                        block_number = state.block_number,
                        error = %err,
                        "skipping calibration because block hash lookup failed"
                    );
                    continue;
                }
            };

            let onchain = self
                .provider
                .fetch_pool_state_from_registry_at_block_hash(
                    &base_arb_common::types::PoolRegistryEntry {
                        pool_address: state.pool_id.address,
                        dex: state.dex,
                        variant: state.variant,
                        factory_address: state.factory_address,
                        token0: state.token0,
                        token1: state.token1,
                        fee_bps: state.fee_bps,
                        tick_spacing: state.tick_spacing,
                        stable: state.stable,
                        enabled: true,
                    },
                    &block_hash,
                    state.block_number,
                )
                .await;
            let onchain = match onchain {
                Ok(onchain) => onchain,
                Err(err) => {
                    warn!(
                        pool = %state.pool_id.address,
                        block_number = state.block_number,
                        error = %err,
                        "skipping calibration because block-hash pinned eth_call failed"
                    );
                    continue;
                }
            };

            let drift_bps = state_drift_bps(state, &onchain);
            if drift_bps > 0 {
                let message = calibration_message(state, drift_bps);
                warn!(
                    pool = %state.pool_id.address,
                    dex = ?state.dex,
                    variant = ?state.variant,
                    local_block = state.block_number,
                    onchain_block = onchain.block_number,
                    drift_bps,
                    local_fee_bps = state.fee_bps,
                    onchain_fee_bps = onchain.fee_bps,
                    local_fee_pips = ?state.fee_pips,
                    onchain_fee_pips = ?onchain.fee_pips,
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
                corrected_pools.insert(state.pool_id.address);
            }
        }

        if !corrected_pools.is_empty() {
            self.publish_selected_states(&states, &corrected_pools, "calibration_correction")
                .await?;
        }

        Ok(states)
    }

    async fn validate_due_snapshots(
        &self,
        pending: &mut VecDeque<PendingValidation>,
        latest_block: u64,
        max_items: usize,
    ) -> Result<()> {
        let mut processed = 0usize;
        while matches!(
            pending.front(),
            Some(item) if item.state.block_number + VALIDATION_DELAY_BLOCKS <= latest_block
        ) {
            if processed >= max_items {
                break;
            }
            let Some(item) = pending.pop_front() else {
                break;
            };
            processed += 1;
            let state = item.state;
            let block_hash = match self.provider.get_block_hash(state.block_number).await {
                Ok(block_hash) => block_hash,
                Err(err) => {
                    warn!(
                        pool = %state.pool_id.address,
                        block_number = state.block_number,
                        error = %err,
                        "skipping delayed block validation because block hash lookup failed"
                    );
                    continue;
                }
            };
            let onchain = self
                .provider
                .fetch_pool_state_from_registry_at_block_hash(
                    &base_arb_common::types::PoolRegistryEntry {
                        pool_address: state.pool_id.address,
                        dex: state.dex,
                        variant: state.variant,
                        factory_address: state.factory_address,
                        token0: state.token0,
                        token1: state.token1,
                        fee_bps: state.fee_bps,
                        tick_spacing: state.tick_spacing,
                        stable: state.stable,
                        enabled: true,
                    },
                    &block_hash,
                    state.block_number,
                )
                .await;
            let onchain = match onchain {
                Ok(onchain) => onchain,
                Err(err) => {
                    warn!(
                        pool = %state.pool_id.address,
                        block_number = state.block_number,
                        error = %err,
                        "skipping delayed block validation because block-hash pinned eth_call failed"
                    );
                    continue;
                }
            };

            let drift_bps = state_drift_bps(&state, &onchain);
            let passed = drift_bps == 0;
            let message = if passed {
                "delayed block validation passed".to_string()
            } else {
                format!("delayed block validation drifted by {drift_bps} bps")
            };
            if passed {
                debug!(
                    pool = %state.pool_id.address,
                    block_number = state.block_number,
                    "delayed block validation passed"
                );
            } else if drift_bps >= 50 {
                warn!(
                    pool = %state.pool_id.address,
                    dex = ?state.dex,
                    variant = ?state.variant,
                    block_number = state.block_number,
                    drift_bps,
                    local_fee_bps = state.fee_bps,
                    onchain_fee_bps = onchain.fee_bps,
                    local_fee_pips = ?state.fee_pips,
                    onchain_fee_pips = ?onchain.fee_pips,
                    "delayed block validation mismatch"
                );
            } else {
                debug!(
                    pool = %state.pool_id.address,
                    block_number = state.block_number,
                    drift_bps,
                    "minor delayed block validation mismatch"
                );
            }

            self.recorder
                .record_pool_state_validation(PoolStateValidation {
                    pool_address: state.pool_id.address,
                    dex: state.dex,
                    variant: state.variant,
                    block_number: state.block_number,
                    block_hash,
                    local_state: state,
                    onchain_state: onchain,
                    drift_bps,
                    passed,
                    message,
                    created_at: chrono::Utc::now(),
                })
                .await?;
        }

        Ok(())
    }
}

pub async fn run<P>(service: &MarketDataService<P>) -> Result<()>
where
    P: PoolStateStore
        + PoolChangeStore
        + CurrentBlockStore
        + TickChangeStore
        + TickStateStore
        + Clone
        + Send
        + Sync
        + 'static,
{
    service.run().await?;
    Ok(())
}

fn quote_relevant_pool_state_changed(previous: &PoolState, next: &PoolState) -> bool {
    previous.dex != next.dex
        || previous.variant != next.variant
        || previous.factory_address != next.factory_address
        || previous.token0 != next.token0
        || previous.token1 != next.token1
        || previous.token0_decimals != next.token0_decimals
        || previous.token1_decimals != next.token1_decimals
        || previous.fee_bps != next.fee_bps
        || previous.fee_pips != next.fee_pips
        || previous.stable != next.stable
        || previous.reserve0 != next.reserve0
        || previous.reserve1 != next.reserve1
        || previous.sqrt_price_x96 != next.sqrt_price_x96
        || previous.liquidity != next.liquidity
        || previous.tick != next.tick
        || previous.tick_spacing != next.tick_spacing
}

fn tick_states_changed(previous: &[TickState], next: &[TickState]) -> bool {
    if previous.len() != next.len() {
        return true;
    }
    let mut previous = previous
        .iter()
        .map(tick_state_fingerprint)
        .collect::<Vec<_>>();
    let mut next = next.iter().map(tick_state_fingerprint).collect::<Vec<_>>();
    previous.sort_unstable();
    next.sort_unstable();
    previous != next
}

fn tick_state_fingerprint(tick: &TickState) -> (i32, i128, String) {
    (
        tick.tick,
        tick.liquidity_net,
        tick.liquidity_gross.to_string(),
    )
}

fn is_v3_style_state(state: &PoolState) -> bool {
    matches!(
        (state.dex, state.variant),
        (DexKind::Aerodrome, PoolVariant::AerodromeSlipstream)
            | (DexKind::UniswapV3, PoolVariant::UniswapV3)
            | (DexKind::PancakeSwap, PoolVariant::PancakeV3)
    )
}

#[allow(dead_code)]
fn is_aerodrome_fee_state(state: &PoolState) -> bool {
    if state.dex != DexKind::Aerodrome {
        return false;
    }
    let Some(factory) = state.factory_address else {
        return false;
    };
    match state.variant {
        PoolVariant::AerodromeVolatile => address_eq_str(factory, AERODROME_CLASSIC_FACTORY),
        PoolVariant::AerodromeSlipstream => AERODROME_SLIPSTREAM_FACTORIES
            .iter()
            .any(|factory_address| address_eq_str(factory, factory_address)),
        _ => false,
    }
}

#[allow(dead_code)]
fn address_eq_str(address: Address, expected: &str) -> bool {
    expected
        .parse::<Address>()
        .map(|expected| address == expected)
        .unwrap_or(false)
}

#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
enum AerodromeFee {
    Classic(u32),
    Slipstream(u32),
}

#[allow(dead_code)]
fn apply_aerodrome_fee(state: &mut PoolState, fee: AerodromeFee) -> bool {
    match (state.variant, fee) {
        (PoolVariant::AerodromeVolatile, AerodromeFee::Classic(fee_bps))
            if state.fee_bps != fee_bps =>
        {
            state.fee_bps = fee_bps;
            state.updated_at = chrono::Utc::now();
            true
        }
        (PoolVariant::AerodromeSlipstream, AerodromeFee::Slipstream(fee_pips))
            if state.fee_pips != Some(fee_pips) =>
        {
            state.fee_pips = Some(fee_pips);
            state.fee_bps = fee_pips / 100;
            state.updated_at = chrono::Utc::now();
            true
        }
        _ => false,
    }
}

fn apply_signed_u256_delta(value: U256, delta: i128) -> Result<U256> {
    if delta >= 0 {
        value
            .checked_add(U256::from(delta as u128))
            .ok_or_else(|| anyhow::anyhow!("liquidity_gross overflow"))
    } else {
        let abs = U256::from((-delta) as u128);
        Ok(value.saturating_sub(abs))
    }
}

fn address_set(states: &[PoolState]) -> HashSet<String> {
    states
        .iter()
        .map(|state| format!("{:#x}", state.pool_id.address))
        .collect()
}

fn advance_valid_through_block(states: &mut [PoolState], block_number: u64) -> HashSet<Address> {
    let mut changed = HashSet::new();
    for state in states {
        if state.effective_valid_through_block() < block_number {
            state.valid_through_block = block_number;
            changed.insert(state.pool_id.address);
        }
    }
    changed
}

fn unique_pool_addresses(states: &[PoolState]) -> Vec<String> {
    let mut seen = HashSet::new();
    states
        .iter()
        .filter_map(|state| {
            let address = format!("{:#x}", state.pool_id.address);
            seen.insert(address.clone()).then_some(address)
        })
        .collect()
}

#[allow(dead_code)]
fn registry_entry_from_state(state: &PoolState) -> PoolRegistryEntry {
    PoolRegistryEntry {
        pool_address: state.pool_id.address,
        dex: state.dex,
        variant: state.variant,
        factory_address: state.factory_address,
        token0: state.token0,
        token1: state.token1,
        fee_bps: state.fee_bps,
        tick_spacing: state.tick_spacing,
        stable: state.stable,
        enabled: true,
    }
}

#[allow(dead_code)]
fn should_calibrate(state: &PoolState) -> bool {
    matches!(
        (state.dex, state.variant),
        (DexKind::Aerodrome, PoolVariant::AerodromeVolatile)
    )
}

#[allow(dead_code)]
fn calibration_message(state: &PoolState, drift_bps: u64) -> String {
    match (state.dex, state.variant) {
        (DexKind::Aerodrome, PoolVariant::AerodromeVolatile) => {
            format!(
                "local Aerodrome volatile reserves drifted from onchain state by {drift_bps} bps"
            )
        }
        (DexKind::Aerodrome, PoolVariant::AerodromeSlipstream)
        | (DexKind::UniswapV3, PoolVariant::UniswapV3)
        | (DexKind::PancakeSwap, PoolVariant::PancakeV3) => {
            format!("local V3-style state drifted from onchain slot0/liquidity by {drift_bps} bps")
        }
        _ => format!("local pool state drifted from onchain state by {drift_bps} bps"),
    }
}

fn state_drift_bps(local: &PoolState, onchain: &PoolState) -> u64 {
    match (local.dex, local.variant) {
        (DexKind::Aerodrome, PoolVariant::AerodromeVolatile) => {
            reserve_drift_bps(local, onchain).max(aerodrome_fee_drift_bps(local, onchain))
        }
        (DexKind::Aerodrome, PoolVariant::AerodromeSlipstream) => {
            v3_state_drift_bps(local, onchain).max(aerodrome_fee_drift_bps(local, onchain))
        }
        (DexKind::UniswapV3, PoolVariant::UniswapV3)
        | (DexKind::PancakeSwap, PoolVariant::PancakeV3) => v3_state_drift_bps(local, onchain),
        _ => u64::MAX,
    }
}

fn aerodrome_fee_drift_bps(local: &PoolState, onchain: &PoolState) -> u64 {
    if local.variant == PoolVariant::AerodromeSlipstream {
        return match (local.fee_pips, onchain.fee_pips) {
            (Some(local), Some(onchain)) if local == onchain => 0,
            (Some(local), Some(onchain)) => u64::from(local.abs_diff(onchain).div_ceil(100).max(1)),
            _ => u64::MAX,
        };
    }
    u64::from(local.fee_bps.abs_diff(onchain.fee_bps))
}

fn reserve_drift_bps(local: &PoolState, onchain: &PoolState) -> u64 {
    let reserve0 = value_drift_bps(local.reserve0, onchain.reserve0);
    let reserve1 = value_drift_bps(local.reserve1, onchain.reserve1);
    reserve0.max(reserve1)
}

fn v3_state_drift_bps(local: &PoolState, onchain: &PoolState) -> u64 {
    let sqrt_price = value_drift_bps(local.sqrt_price_x96, onchain.sqrt_price_x96);
    let liquidity = value_drift_bps(local.liquidity, onchain.liquidity);
    let tick = tick_drift(local.tick, onchain.tick);
    sqrt_price.max(liquidity).max(tick)
}

fn tick_drift(local: Option<i32>, onchain: Option<i32>) -> u64 {
    match (local, onchain) {
        (Some(local), Some(onchain)) if local == onchain => 0,
        (Some(local), Some(onchain)) => local.abs_diff(onchain).max(1) as u64,
        _ => u64::MAX,
    }
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

#[derive(Debug, Clone)]
struct CreationLog {
    factory: Address,
    topic0: String,
    topics: Vec<String>,
    data: String,
    block_number: u64,
}

#[derive(Debug, Clone)]
struct SwapObservationLog {
    pool: Address,
    topic0: String,
    block_number: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LivePoolDiscoveryOutcome {
    Imported,
    ObservedOnly,
    Skipped,
}

fn parse_creation_log(raw: Value) -> Result<CreationLog> {
    let factory = raw
        .get("address")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("pool-created log missing address"))?
        .parse::<Address>()?;
    let topic0 = raw
        .get("topics")
        .and_then(Value::as_array)
        .and_then(|topics| topics.first())
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("pool-created log missing topic0"))?
        .to_ascii_lowercase();
    let topics = raw
        .get("topics")
        .and_then(Value::as_array)
        .map(|topics| {
            topics
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_ascii_lowercase)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let data = raw
        .get("data")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("pool-created log missing data"))?
        .to_string();
    let block_number = raw
        .get("blockNumber")
        .and_then(Value::as_str)
        .and_then(parse_hex_u64_local)
        .ok_or_else(|| anyhow::anyhow!("pool-created log missing blockNumber"))?;

    Ok(CreationLog {
        factory,
        topic0,
        topics,
        data,
        block_number,
    })
}

fn creation_log_candidate_addresses(log: &CreationLog) -> Vec<Address> {
    let mut out = topic_word_addresses(&log.topics);
    out.extend(data_word_addresses(&log.data));
    out
}

fn topic_word_addresses(topics: &[String]) -> Vec<Address> {
    topics
        .iter()
        .skip(1)
        .filter_map(|topic| {
            let hex = topic.strip_prefix("0x").unwrap_or(topic);
            if hex.len() != 64 || !hex[..24].eq_ignore_ascii_case("000000000000000000000000") {
                return None;
            }
            format!("0x{}", &hex[24..64]).parse::<Address>().ok()
        })
        .collect()
}

fn parse_swap_log(raw: Value) -> Result<SwapObservationLog> {
    let pool = raw
        .get("address")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("swap log missing address"))?
        .parse::<Address>()?;
    let topic0 = raw
        .get("topics")
        .and_then(Value::as_array)
        .and_then(|topics| topics.first())
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow::anyhow!("swap log missing topic0"))?
        .to_ascii_lowercase();
    let block_number = raw
        .get("blockNumber")
        .and_then(Value::as_str)
        .and_then(parse_hex_u64_local)
        .ok_or_else(|| anyhow::anyhow!("swap log missing blockNumber"))?;

    Ok(SwapObservationLog {
        pool,
        topic0,
        block_number,
    })
}

fn raw_log_topic0(raw: &Value) -> Option<String> {
    raw.get("topics")
        .and_then(Value::as_array)
        .and_then(|topics| topics.first())
        .and_then(Value::as_str)
        .map(str::to_ascii_lowercase)
}

fn raw_log_tx_hash(raw: &Value) -> Option<B256> {
    raw.get("transactionHash")
        .and_then(Value::as_str)
        .and_then(|hash| hash.parse::<B256>().ok())
}

fn address_topic(address: Address) -> String {
    let address = format!("{address:#x}");
    format!("0x{:0>64}", address.trim_start_matches("0x"))
}

fn is_supported_swap_topic(topic0: &str) -> bool {
    let topic0 = topic0.to_ascii_lowercase();
    topic0 == V3_SWAP_TOPIC || topic0 == PANCAKE_V3_SWAP_TOPIC || topic0 == CLASSIC_SWAP_TOPIC
}

fn data_word_addresses(data: &str) -> Vec<Address> {
    let hex = data.strip_prefix("0x").unwrap_or(data);
    if hex.len() < 64 {
        return Vec::new();
    }
    hex.as_bytes()
        .chunks(64)
        .filter_map(|word| std::str::from_utf8(word).ok())
        .filter(|word| word.len() == 64)
        .filter_map(|word| {
            let address_hex = &word[24..64];
            if !word[..24].eq_ignore_ascii_case("000000000000000000000000") {
                return None;
            }
            format!("0x{address_hex}").parse::<Address>().ok()
        })
        .collect()
}

fn parse_hex_u64_local(value: &str) -> Option<u64> {
    u64::from_str_radix(value.strip_prefix("0x").unwrap_or(value), 16).ok()
}

fn canonical_pair(a: Address, b: Address) -> (Address, Address) {
    if a < b {
        (a, b)
    } else {
        (b, a)
    }
}

async fn pair_symbol(provider: &ChainProvider, token0: Address, token1: Address) -> String {
    let token0_symbol = provider
        .fetch_erc20_symbol(token0)
        .await
        .unwrap_or_else(|_| short_address(token0));
    let token1_symbol = provider
        .fetch_erc20_symbol(token1)
        .await
        .unwrap_or_else(|_| short_address(token1));
    format!("{token0_symbol}/{token1_symbol}")
}

fn short_address(address: Address) -> String {
    let value = format!("{address:#x}");
    value.get(0..10).map(ToString::to_string).unwrap_or(value)
}

fn dex_to_string(dex: DexKind) -> &'static str {
    match dex {
        DexKind::Aerodrome => "Aerodrome",
        DexKind::UniswapV3 => "UniswapV3",
        DexKind::PancakeSwap => "PancakeSwap",
    }
}

fn variant_to_string(variant: PoolVariant) -> &'static str {
    match variant {
        PoolVariant::AerodromeVolatile => "AerodromeVolatile",
        PoolVariant::AerodromeSlipstream => "AerodromeSlipstream",
        PoolVariant::UniswapV3 => "UniswapV3",
        PoolVariant::PancakeV3 => "PancakeV3",
    }
}

fn parse_dex_kind(value: &str) -> Result<DexKind> {
    match value {
        "Aerodrome" => Ok(DexKind::Aerodrome),
        "UniswapV3" => Ok(DexKind::UniswapV3),
        "PancakeSwap" => Ok(DexKind::PancakeSwap),
        _ => anyhow::bail!("unknown dex: {value}"),
    }
}

fn parse_pool_variant(value: &str) -> Result<PoolVariant> {
    match value {
        "AerodromeVolatile" => Ok(PoolVariant::AerodromeVolatile),
        "AerodromeSlipstream" => Ok(PoolVariant::AerodromeSlipstream),
        "UniswapV3" => Ok(PoolVariant::UniswapV3),
        "PancakeV3" => Ok(PoolVariant::PancakeV3),
        _ => anyhow::bail!("unknown pool variant: {value}"),
    }
}

fn inferred_dex_for_creation_topic(topic0: &str) -> &'static str {
    let topic0 = topic0.to_ascii_lowercase();
    if topic0 == CLASSIC_POOL_CREATED_TOPIC || topic0 == CLASSIC_PAIR_CREATED_TOPIC {
        "Aerodrome"
    } else {
        "UniswapV3"
    }
}

fn inferred_variant_for_creation_topic(topic0: &str) -> &'static str {
    let topic0 = topic0.to_ascii_lowercase();
    if topic0 == CLASSIC_POOL_CREATED_TOPIC || topic0 == CLASSIC_PAIR_CREATED_TOPIC {
        "AerodromeVolatile"
    } else if topic0 == SLIPSTREAM_POOL_CREATED_TOPIC
        || topic0 == SLIPSTREAM_POOL_CREATED_WITH_INDEX_TOPIC
    {
        "AerodromeSlipstream"
    } else {
        "UniswapV3"
    }
}

fn creation_compatible_swap_topic(topic0: &str) -> &'static str {
    let topic0 = topic0.to_ascii_lowercase();
    if topic0 == CLASSIC_POOL_CREATED_TOPIC || topic0 == CLASSIC_PAIR_CREATED_TOPIC {
        CLASSIC_SWAP_TOPIC
    } else {
        V3_SWAP_TOPIC
    }
}

fn inferred_dex_for_swap_topic(topic0: &str) -> &'static str {
    let topic0 = topic0.to_ascii_lowercase();
    if topic0 == CLASSIC_SWAP_TOPIC {
        "Aerodrome"
    } else if topic0 == PANCAKE_V3_SWAP_TOPIC {
        "PancakeSwap"
    } else {
        "UniswapV3"
    }
}

fn inferred_variant_for_swap_topic(topic0: &str) -> &'static str {
    let topic0 = topic0.to_ascii_lowercase();
    if topic0 == CLASSIC_SWAP_TOPIC {
        "AerodromeVolatile"
    } else if topic0 == PANCAKE_V3_SWAP_TOPIC {
        "PancakeV3"
    } else {
        "UniswapV3"
    }
}

fn swap_family_for_topic(topic0: &str) -> &'static str {
    let topic0 = topic0.to_ascii_lowercase();
    if topic0 == CLASSIC_SWAP_TOPIC {
        "classic-v2"
    } else if topic0 == PANCAKE_V3_SWAP_TOPIC {
        "pancake-v3"
    } else {
        "v3/slipstream"
    }
}

#[derive(Debug, Clone)]
struct PendingValidation {
    state: PoolState,
}

fn enqueue_validation_snapshots(
    pending: &mut VecDeque<PendingValidation>,
    snapshots: BTreeMap<(u64, Address), PoolState>,
) {
    for state in snapshots.into_values() {
        pending.push_back(PendingValidation { state });
    }
    while pending.len() > MAX_PENDING_VALIDATIONS {
        pending.pop_front();
    }
}

struct RecentLogCache {
    limit: usize,
    order: VecDeque<(String, u64)>,
    set: HashSet<(String, u64)>,
}

impl RecentLogCache {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            order: VecDeque::with_capacity(limit),
            set: HashSet::with_capacity(limit),
        }
    }

    fn insert(&mut self, tx_hash: String, log_index: u64) -> bool {
        let key = (tx_hash, log_index);
        if !self.set.insert(key.clone()) {
            return false;
        }
        self.order.push_back(key);
        while self.order.len() > self.limit {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        true
    }
}

#[derive(Debug, Default)]
struct CompetitorPoolDiscoverySummary {
    transfer_txs: usize,
    receipts: usize,
    swap_logs: usize,
    imported: usize,
    observed_only: usize,
    skipped: usize,
}

struct RecentTxCache {
    limit: usize,
    order: VecDeque<B256>,
    set: HashSet<B256>,
}

impl RecentTxCache {
    fn new(limit: usize) -> Self {
        Self {
            limit,
            order: VecDeque::with_capacity(limit),
            set: HashSet::with_capacity(limit),
        }
    }

    fn insert(&mut self, tx_hash: B256) -> bool {
        if !self.set.insert(tx_hash) {
            return false;
        }
        self.order.push_back(tx_hash);
        while self.order.len() > self.limit {
            if let Some(oldest) = self.order.pop_front() {
                self.set.remove(&oldest);
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, U256};
    use chrono::Utc;

    use super::{advance_valid_through_block, apply_aerodrome_fee, state_drift_bps, AerodromeFee};
    use base_arb_common::types::{DexKind, PoolId, PoolState, PoolVariant};

    fn pool_state(variant: PoolVariant) -> PoolState {
        PoolState {
            pool_id: PoolId {
                chain_id: 8453,
                address: Address::ZERO,
            },
            dex: DexKind::Aerodrome,
            variant,
            factory_address: None,
            token0: Address::ZERO,
            token1: Address::ZERO,
            token0_decimals: None,
            token1_decimals: None,
            fee_bps: 30,
            fee_pips: (variant == PoolVariant::AerodromeSlipstream).then_some(3_000),
            stable: Some(false),
            reserve0: Some(U256::from(1_000_000u64)),
            reserve1: Some(U256::from(2_000_000u64)),
            sqrt_price_x96: Some(U256::from(3_000_000u64)),
            liquidity: Some(U256::from(4_000_000u64)),
            tick: Some(1),
            tick_spacing: None,
            block_number: 1,
            valid_through_block: 1,
            updated_at: Utc::now(),
        }
    }

    #[test]
    fn classic_fee_change_is_state_drift_without_reserve_change() {
        let local = pool_state(PoolVariant::AerodromeVolatile);
        let mut onchain = local.clone();
        onchain.fee_bps = 5;

        assert_eq!(state_drift_bps(&local, &onchain), 25);
    }

    #[test]
    fn slipstream_sub_bps_fee_change_is_nonzero_state_drift() {
        let local = pool_state(PoolVariant::AerodromeSlipstream);
        let mut onchain = local.clone();
        onchain.fee_pips = Some(3_001);

        assert_eq!(state_drift_bps(&local, &onchain), 1);
    }

    #[test]
    fn applies_classic_fee_update_only_when_changed() {
        let mut state = pool_state(PoolVariant::AerodromeVolatile);

        assert!(!apply_aerodrome_fee(&mut state, AerodromeFee::Classic(30)));
        assert!(apply_aerodrome_fee(&mut state, AerodromeFee::Classic(5)));
        assert_eq!(state.fee_bps, 5);
        assert_eq!(state.fee_pips, None);
    }

    #[test]
    fn applies_slipstream_fee_update_as_pips() {
        let mut state = pool_state(PoolVariant::AerodromeSlipstream);

        assert!(apply_aerodrome_fee(
            &mut state,
            AerodromeFee::Slipstream(85)
        ));
        assert_eq!(state.fee_pips, Some(85));
        assert_eq!(state.fee_bps, 0);
    }

    #[test]
    fn advances_validity_without_changing_source_block() {
        let mut states = vec![pool_state(PoolVariant::AerodromeVolatile)];

        let changed = advance_valid_through_block(&mut states, 9);

        assert!(changed.contains(&Address::ZERO));
        assert_eq!(states[0].block_number, 1);
        assert_eq!(states[0].valid_through_block, 9);
    }
}
