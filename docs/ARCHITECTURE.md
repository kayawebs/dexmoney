# Dexmoney Architecture

This document defines the runtime boundaries for the Base arbitrage system. The primary goal is low-latency, data-correct arbitrage. Engineering completeness is secondary to keeping the hot path fast, observable, and easy to debug.

Operational tradeoffs and diagnostic priority order are defined in
[`OPERATING_PRIORITIES.md`](OPERATING_PRIORITIES.md). When implementation
choices conflict, follow that priority order first.

## Supported Protocol Families

### Aerodrome Classic

- Variant: `AerodromeVolatile`.
- State model: V2-style reserves plus factory fee.
- Realtime data: `Sync`, `Swap`, `Fees`, fee refresh.
- Quote model: local reserve math.
- Runtime cache requirement: pool state only. No ticks.

### Aerodrome Slipstream

- Variant: `AerodromeSlipstream`.
- State model: V3-style `sqrt_price_x96`, `liquidity`, current `tick`, initialized ticks.
- Realtime data: `Swap`, `Mint`, `Burn`, fee refresh.
- Quote model: local V3-style cross-tick math.
- Runtime cache requirement: pool state plus initialized ticks for hot pools.

### Uniswap V3 and Pancake V3

- Variants: `UniswapV3`, `PancakeV3`.
- State model: V3-style `slot0`, `liquidity`, current `tick`, initialized ticks.
- Realtime data: `Swap`, `Mint`, `Burn`.
- Quote model: local V3-style cross-tick math.
- Runtime cache requirement: pool state plus initialized ticks for hot pools.

### Uniswap V4

- Variant: `UniswapV4`.
- State model: PoolManager `PoolId`, pool key metadata, current state, initialized ticks.
- Realtime data: PoolManager `Initialize`, `Swap`, `ModifyLiquidity`.
- Quote model: local V3-style cross-tick math for supported static-fee, safe-hook pools.
- Runtime cache requirement: quoteable hot pool state plus initialized ticks.
- Historical requirement: PoolManager `Initialize` metadata and `ModifyLiquidity` tick reconstruction.

### Balancer V3

- Variant: `BalancerV3`.
- Current state: observed, promoted when metadata is complete, quoteable through
  router query, and executable through `BalancerV3Adapter`.
- Durable readiness: `pool_quote_coverage` records offline router-query
  validation status per pool/token direction. This keeps validation RPC out of
  the hot search path.
- Model readiness: `pool_model_coverage` records offline weighted/stable pool
  type probes and the local-model input data needed for future local quoting.
- Runtime bridge: searcher-side Balancer router queries are disabled by default
  and require `SEARCHER_BALANCER_V3_RUNTIME_QUOTE_ENABLED=true`. This is only a
  controlled bridge for validation or narrow dry-runs, not the target production
  hot path.
- Target state: classify pool type and use local math for supported pool families.
- Hot-path rule: searcher should not depend on per-path RPC queries long term.

## Process Responsibilities

### `market-data`

Owns realtime chain state.

- Reads Base RPC/WS/flashblock feeds.
- Applies state-changing events to supported hot pools.
- Writes current pool state to Redis.
- Persists events, pool states, drift validations, and warnings to Postgres.
- Publishes changed pool markers to Redis.
- Must not run historical backfills or heavy discovery loops.

### `pool-discovery`

Owns live pool discovery and promotion.

- Watches global swap logs and protocol manager logs.
- Classifies pools, factories, and protocol observations.
- Promotes only executable and quoteable pools into `pools`.
- Writes canonical discovery data to Postgres.
- Publishes newly quoteable hot pool state to Redis when state is complete.

### Hydrator / Backfill Binaries

Own historical or repair work.

- Backfill factory pools and global swap pools.
- Hydrate Uniswap V4 metadata from PoolManager `Initialize`.
- Hydrate Uniswap V4 initialized ticks from PoolManager `ModifyLiquidity`.
- Repair V3-style initialized ticks for hot pools with missing Redis ticks.
- Keep checkpoints/progress in Postgres when scans are expensive.
- Must not block `market-data` or `searcher`.

### `searcher`

Owns opportunity generation.

- Reads hot pool state and hot initialized ticks from Redis.
- Reads path/search configuration from Postgres.
- Does not call RPC on the hot path.
- Uses changed pools to restrict path exploration.
- Emits only next-block candidates.
- Records opportunities for replay/debug.

### `execution-manager`

Owns simulation and transaction submission.

- Consumes candidates from Redis.
- Drops stale candidates without simulation.
- Simulates fresh candidates.
- Submits transactions only when configured.
- Manages EOA worker lanes and pending nonce state.
- Records simulation and transaction outcomes to Postgres.

### `monitor-web`

Owns human control.

- Displays runtime state, registry, opportunities, simulations, and transactions.
- Provides password-gated registry mutations.
- Should not run heavy scans.

### `health-monitor`

Owns runtime health checks and alerting.

- Reads DB, Redis, and lightweight RPC freshness.
- Reports market-data lag, searcher latency, stale candidates, missing ticks, no-opportunity windows, simulation failures, and executor failures.
- Runs lightweight opportunity-scarcity checks for active market data with low opportunity output, plus funded-pair path readiness; heavy root-cause reports stay in `ops/*_diag.sh`.
- Emits logs first; Telegram alerts are an optional delivery layer.

## Storage Model

### Postgres Is Canonical

Postgres stores durable state and analysis data:

- `tokens`, `token_pairs`, `pools`, `factory_registry`.
- `observed_pools`, `protocol_pool_observations`.
- `pool_states`, `dex_events`.
- `pool_ticks_current` for durable current initialized tick snapshots.
- `pool_tick_coverage` for durable tick readiness status, including `ready`,
  `zero_ticks`, and `refresh_failed`.
- `pool_model_coverage` for durable local quote model readiness, including
  Balancer V3 weighted/stable input probes.
- `opportunities`, `simulations`, `transactions`.
- Hydration run progress tables and drift diagnostics.

Postgres can be large. It is allowed to contain cold pools and unsupported observations.

### Redis Is Hot Runtime Cache

Redis stores only data needed by low-latency runtime:

- `chain:current_block`.
- `pool:<chain_id>:<pool>` current hot pool state.
- `pool_index:<pool>` address lookup.
- `ticks:<chain_id>:<pool>:<tick>` hot initialized ticks.
- `ticks:index:<pool>` hot tick key index.
- `pools:changed`, `ticks:changed`.
- `candidates:priority`.
- `eoa:<address>:state`.

Redis should not hold every historical pool and every historical tick. Hot pool selection should be based on anchor relevance, recent activity, competitor usage, quoteability, and executable support.

## Hot Pool Selection

A pool is hot when all required conditions are true:

- It is tied to a configured anchor token directly or through the active graph.
- It has recent activity or competitor usage.
- It is quoteable by local searcher math.
- It is executable by the deployed contracts/adapters or explicitly kept for dry-run analysis.
- Its required state is complete:
  - V2-style: reserves and fee.
  - V3-style: slot0/liquidity/current tick plus initialized ticks.
  - V4: pool key metadata, state, supported hook/fee mode, initialized ticks.

Cold/unsupported pools remain in Postgres until promoted.

## RPC Ownership Rules

- `market-data`: realtime block/log/state reads only.
- `pool-discovery`: classification probes and live observation.
- Hydrators: historical scans and repair jobs.
- `searcher`: no RPC on the hot path.
- `execution-manager`: simulation/submission RPC only.
- `health-monitor`: lightweight freshness probes only.

If multiple processes need the same expensive data, one process must own the fetch and publish it through Postgres/Redis.

## Tick Coverage Strategy

- V3-style pools (`UniswapV3`, `PancakeV3`, `AerodromeSlipstream`) are repaired by targeted hot-pool backfill, not by chain-wide full tick scans. Candidate pools come from recent activity, opportunities, competitor usage, and coverage gaps.
- `v3-tick-repair` is the low-priority daemon for V3-style tick persistence. It consumes the Redis `ticks:repair` queue produced by the searcher when a V3-style quote exhausts the known tick range, then refreshes that pool with a wider queued radius. When the queue is empty it only checks explicit `pool_tick_coverage` gaps; it must not scan all pools/states on every loop.
- `ops/reconcile_tick_coverage.sh` backfills `pool_tick_coverage` from existing `pool_ticks_current` rows. Run it after historical or repaired tick loads if diagnostics show tick rows present but coverage still missing.
- Uniswap V4 uses PoolManager log hydration into Postgres because PoolId state is manager-scoped; it cannot use the V3 pool `ticks(int24)` repair path. `v4-tick-repair` runs a recent-window PoolManager scan loop with block overlap, writes `pool_ticks_current`/`pool_tick_coverage`, and syncs Redis for promoted hot pools.
- Health and diagnostics must treat `pool_tick_coverage` as the readiness source. A missing `ticks:index:<pool>` means "not in Redis hot cache", not necessarily "tick data missing".
- `zero_ticks` is a valid coverage state for pools whose scan found no initialized ticks in the selected range; it should not be alerted like `refresh_failed` or unscanned hot pools.

## Latency Targets

For next-block execution:

- `market-data` sealed block processing should finish in less than one block under normal load.
- `searcher` changed-pool cycle should finish in less than one block.
- `execution-manager` should receive fresh candidates with lag less than or equal to `execution_max_candidate_lag_blocks`.
- A candidate older than the next block is diagnostic data, not an executable opportunity.

## Current Known Gaps

- V3-style tick persistence: market-data writes refreshed ticks through a
  background Postgres queue so reports use durable coverage while searcher
  keeps using Redis hot ticks.
- Searcher tick loading: per-pool Redis fetches are too expensive at current path scale.
- Price-impact model: V3-style exact quote can succeed while spot-only impact estimation fails; this must not block simulation.
- Balancer V3: execution is available through `BalancerV3Adapter`. Offline quote
  validation writes `pool_quote_coverage`; offline model classification writes
  `pool_model_coverage`. Searcher runtime router quote is explicitly opt-in
  because per-path RPC violates the production hot-path rule. Two-token weighted
  pools are quoted locally from Vault live scaled balances, token rates, decimal
  scaling factors, normalized weights, and swap fee. Stable, boosted, composable,
  and multi-token weighted pools remain unsupported for local production search
  until their exact math and state update rules are implemented.
- V4: metadata/tick hydration is still required for complete coverage.
