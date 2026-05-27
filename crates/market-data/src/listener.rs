use alloy_primitives::{Address, U256};
use anyhow::Result;
use base_arb_chain::provider::ChainProvider;
use base_arb_common::config::Settings;
use base_arb_common::types::{
    DexKind, PoolId, PoolRegistryEntry, PoolState, PoolStateValidation, PoolStateWarning,
    PoolVariant, TickState,
};
use base_arb_storage::{postgres::PostgresStore, PoolStateStore, RecorderStore, TickStateStore};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use tokio::time::{interval, Duration, Instant, MissedTickBehavior};
use tracing::{debug, info, warn};

const REGISTRY_RELOAD_INTERVAL: Duration = Duration::from_secs(30);
const CALIBRATION_INTERVAL: Duration = Duration::from_secs(30);
const VALIDATION_DELAY_BLOCKS: u64 = 2;
const MAX_PENDING_VALIDATIONS: usize = 20_000;

pub struct MarketDataService<P> {
    pub settings: Settings,
    pub provider: ChainProvider,
    pub pool_store: P,
    pub recorder: PostgresStore,
}

impl<P> MarketDataService<P>
where
    P: PoolStateStore + TickStateStore,
{
    pub async fn run(&self) -> Result<()> {
        info!("event listener started");

        let mut monitored_states = self.load_monitored_states().await?;
        self.publish_monitored_states(&monitored_states, "onchain_init")
            .await?;
        self.publish_initialized_ticks(&monitored_states).await?;

        let mut last_seen_block = self.provider.get_block_number().await?;
        let mut next_registry_reload = Instant::now() + REGISTRY_RELOAD_INTERVAL;
        let mut next_calibration = Instant::now() + CALIBRATION_INTERVAL;
        let active_refresh_interval =
            Duration::from_secs(self.settings.pool_active_refresh_interval_secs.max(10));
        let mut next_active_refresh = Instant::now() + active_refresh_interval;
        let tick_refresh_interval =
            Duration::from_secs(self.settings.v3_tick_refresh_interval_secs.max(10));
        let mut next_tick_refresh = Instant::now() + tick_refresh_interval;
        let mut recent_logs = RecentLogCache::new(20_000);
        let mut pending_validations = VecDeque::new();
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
                if Instant::now() >= next_active_refresh {
                    monitored_states = self.active_refresh_states(monitored_states).await?;
                    next_active_refresh = Instant::now() + active_refresh_interval;
                    next_calibration = Instant::now() + CALIBRATION_INTERVAL;
                    next_tick_refresh = Instant::now() + tick_refresh_interval;
                } else if Instant::now() >= next_calibration {
                    monitored_states = self.calibrate_states(monitored_states).await?;
                    next_calibration = Instant::now() + CALIBRATION_INTERVAL;
                } else if Instant::now() >= next_tick_refresh {
                    self.publish_initialized_ticks(&monitored_states).await?;
                    next_tick_refresh = Instant::now() + tick_refresh_interval;
                }
                self.validate_due_snapshots(&mut pending_validations, latest_block)
                    .await?;
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

            let mut changed_pools = HashSet::new();
            let mut validation_snapshots = BTreeMap::new();
            let mut classic_fee_refreshes = HashMap::new();
            let mut slipstream_fee_refreshes = HashMap::new();
            for event in &events {
                if !recent_logs.insert(event.tx_hash.clone(), event.log_index) {
                    warn!(
                        tx_hash = %event.tx_hash,
                        log_index = event.log_index,
                        pool = %event.pool_address,
                        "duplicate event skipped before local state update"
                    );
                    continue;
                }
                debug!(
                    pool = %event.pool_address,
                    block_number = event.block_number,
                    event_type = %event.event_type,
                    "event received"
                );
                self.recorder.record_dex_event(event.clone()).await?;
                for state in &mut monitored_states {
                    if state.pool_id.address != event.pool_address {
                        continue;
                    }
                    let tick_deltas =
                        super::state_updater::v3_tick_deltas_from_event(state, event)?;
                    if !tick_deltas.is_empty() {
                        self.apply_tick_deltas(&state.pool_id, &tick_deltas, event.block_number)
                            .await?;
                    }
                    if super::state_updater::is_v3_liquidity_event(state, event)? {
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

            if !changed_pools.is_empty() {
                self.publish_selected_states(&monitored_states, &changed_pools, "local_event")
                    .await?;
            }

            enqueue_validation_snapshots(&mut pending_validations, validation_snapshots);
            self.validate_due_snapshots(&mut pending_validations, latest_block)
                .await?;

            if Instant::now() >= next_active_refresh {
                monitored_states = self.active_refresh_states(monitored_states).await?;
                next_active_refresh = Instant::now() + active_refresh_interval;
                next_calibration = Instant::now() + CALIBRATION_INTERVAL;
                next_tick_refresh = Instant::now() + tick_refresh_interval;
            } else if Instant::now() >= next_calibration {
                monitored_states = self.calibrate_states(monitored_states).await?;
                next_calibration = Instant::now() + CALIBRATION_INTERVAL;
            } else if Instant::now() >= next_tick_refresh {
                self.publish_initialized_ticks(&monitored_states).await?;
                next_tick_refresh = Instant::now() + tick_refresh_interval;
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
        let next = self.load_monitored_states().await?;
        let current_addresses = address_set(&current);
        let next_addresses = address_set(&next);
        if current_addresses != next_addresses {
            info!(
                previous = current_addresses.len(),
                next = next_addresses.len(),
                "pool registry changed; reloading monitored pools"
            );
            self.publish_monitored_states(&next, "registry_reload")
                .await?;
            self.publish_initialized_ticks(&next).await?;
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
        Ok(())
    }

    async fn active_refresh_states(&self, mut states: Vec<PoolState>) -> Result<Vec<PoolState>> {
        if states.is_empty() {
            return Ok(states);
        }

        let block_number = self.provider.get_block_number().await?;
        let block_hash = self.provider.get_block_hash(block_number).await?;
        let mut refreshed_pools = HashSet::new();
        let mut refreshed_v3_states = Vec::new();
        let mut drifted = 0usize;
        let mut failed = 0usize;

        for state in &mut states {
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
            *state = onchain;
        }

        if !refreshed_pools.is_empty() {
            self.publish_selected_states(&states, &refreshed_pools, "active_refresh")
                .await?;
            self.publish_initialized_ticks(&refreshed_v3_states).await?;
        }

        info!(
            refreshed = refreshed_pools.len(),
            failed, drifted, block_number, "active pool state refresh complete"
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
            self.pool_store.set_tick_states(ticks).await?;
            debug!(
                pool = %state.pool_id.address,
                count,
                word_radius,
                "initialized ticks loaded"
            );
        }
        Ok(())
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

        self.pool_store.set_tick_states(updates).await?;
        Ok(())
    }

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
    ) -> Result<()> {
        while matches!(
            pending.front(),
            Some(item) if item.state.block_number + VALIDATION_DELAY_BLOCKS <= latest_block
        ) {
            let Some(item) = pending.pop_front() else {
                break;
            };
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
    P: PoolStateStore + TickStateStore,
{
    service.run().await?;
    Ok(())
}

fn is_v3_style_state(state: &PoolState) -> bool {
    matches!(
        (state.dex, state.variant),
        (DexKind::Aerodrome, PoolVariant::AerodromeSlipstream)
            | (DexKind::UniswapV3, PoolVariant::UniswapV3)
            | (DexKind::PancakeSwap, PoolVariant::PancakeV3)
    )
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

fn should_calibrate(state: &PoolState) -> bool {
    matches!(
        (state.dex, state.variant),
        (DexKind::Aerodrome, PoolVariant::AerodromeVolatile)
    )
}

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

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, U256};
    use chrono::Utc;

    use super::state_drift_bps;
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
}
