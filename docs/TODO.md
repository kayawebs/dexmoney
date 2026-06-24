# TODO

## Current: Aerodrome Quote Correctness

- [x] Decode all known quote-state pool events for the supported pool variants:
  - Aerodrome Classic reserve updates from `Sync(uint256,uint256)`.
  - Aerodrome Slipstream V3-style `Swap`, `Mint`, and `Burn`.
  - Uniswap V3 `Swap`, `Mint`, and `Burn`.
  - Pancake V3 extended `Swap(address,address,int256,int256,uint160,uint128,int24,uint128,uint128)`, plus V3 liquidity events.
- [x] Read Aerodrome Classic and Slipstream fees in the same block-pinned snapshot as reserves/slot0 state.
- [x] Refresh dynamic Slipstream fees when a swap event changes locally published state.
- [x] Refresh Classic factory fees before publishing a locally applied reserve update.
- [x] Refresh Aerodrome fees on a cheap dedicated interval independent of full pool state refresh.
- [x] Include Aerodrome fee mismatches in pool-state validation and drift warnings.
- [ ] Audit the deployed Base Aerodrome Classic factory, Slipstream factories, and configured Slipstream fee modules for every fee-changing code path and emitted event.
- [ ] Subscribe to factory/module fee-change events where the deployed contracts expose them; retain periodic onchain refresh for any fee changes that have no event.
- [ ] If the deployed Slipstream fee module formula is stable and based only on locally available inputs, implement and test local fee evaluation; otherwise document RPC refresh as required input to exact quotes.
- [ ] Add fork/replay tests that compare each supported local path quote with contract output across recorded profitable and `MinProfitNotMet` candidates.

## Current: Pool Discovery / Graph Completeness

- [x] Build a live global swap-log observer:
  - Scan new blocks for supported swap topics without filtering by known pool addresses.
  - Resolve `log.address` as a candidate pool with `token0/token1/factory/fee/tickSpacing/stable` probes.
  - Auto-import pools from trusted executable factories into `tokens`, `token_pairs`, and `pools`.
  - Store unsupported or untrusted pools in `observed_pools` for analysis and later promotion.
- [x] Add historical factory backfill:
  - Scan trusted factory `PoolCreated` / `PairCreated` events over historical block ranges.
  - Resolve and import all old pools that are executable by the current routers/executors.
  - Keep the live observer and historical backfill on the same pool-classification code path.
- [x] Add automatic observed-pool classification:
  - Probe observed pool ABI shape, `factory()`, token metadata, fee/tick/stable fields, and readable pool state.
  - Promote pools into `pools` only when the factory is already known executable by configured routers.
  - Keep unknown V2/V3-compatible factories as classified observed-only until router/executor support is proven.
- [x] Add historical global swap-log backfill:
  - Scan historical supported swap topics without an address filter.
  - Aggregate active pools by frequency and latest activity.
  - Import trusted-factory pools and store unknown-factory pools in `observed_pools`.
- [ ] Add automatic factory execution proof:
  - Probe factory/router ABI shape and bytecode/codehash for known V2/V3-compatible families.
  - Promote factories to trusted only when pool state reads and executor/router dry-run succeed.
  - Keep quote-only factories separate from executable factories.
- [x] Generate paths from the active pool graph:
  - Treat configured funded tokens as anchors.
  - Generate 2/3/4-hop cycles from active pools around anchors.
- [ ] Rank graph edges by recent swap frequency, liquidity/state freshness, and quote reliability before path generation.

## Current: Competitor Protocol Coverage

- [x] Observe Uniswap V4 PoolManager and Balancer V3 Vault logs into `protocol_pool_observations`.
- [x] Hydrate Uniswap V4 observations with matching `Initialize` metadata so PoolId rows can be classified.
- [x] Promote quoteable Uniswap V4 pools into the hot path:
  - Static fee only; dynamic-fee flag pools stay observation-only until dynamic fee reads are modeled.
  - Zero-hook only; nonzero hook pools stay observation-only until hook semantics are proven safe.
  - Require token0/token1/fee/tickSpacing/sqrtPrice/liquidity/tick before publishing state.
- [x] Add Uniswap V4 PoolId-keyed tick liquidity storage and exact cross-tick quote support.
  - Backfill initialized ticks from historical `ModifyLiquidity` before trusting exact V4 quotes.
  - Continue applying live `ModifyLiquidity` deltas from PoolManager logs after backfill.
- [x] Add Balancer V3 router-query quote and adapter execution path:
  - Promote Vault-observed swap edges when token/fee metadata is present.
  - Use router query for runtime quote and adapter execution until local math is added.
- [ ] Add Balancer V3 pool-type classification for local quote:
  - Identify pool type and math family from Vault/pool metadata.
  - Store pool-specific balances, scaling factors, fees, and rate-provider data.
  - Quote only pool types with fully implemented local math.
- [x] Extend competitor reports to separate tick coverage state from missing ticks.
- [x] Audit V3-style tick availability:
  - Explain every persistent `MissingTicks` skip by variant and discovery source.
  - Distinguish async warmup lag, RPC failures, tick bitmap radius misses, and genuinely empty/out-of-range liquidity.
  - Add a repair path that backfills missing initialized ticks without blocking live market-data.

## Immediate

- Replace demo `market-data` bootstrap with real Base RPC/WS initialization.
- Subscribe to real Aerodrome and Uniswap V3 events instead of generating placeholder events.
- Persist real `dex_events` and `pool_states` into Postgres on every update.
- Store real pool state snapshots in Redis keyed by configured pool addresses.

## Search Path

- Replace placeholder Uniswap V3 quote logic with real `eth_call` to `QuoterV2`.
- Make `searcher` read only live Redis pool state, without demo fallback.
- Add explicit candidate priority scoring beyond raw expected profit.
- Record quote inputs and outputs for replay/debug.
- Deep-dive high-profit candidates rejected by price-impact checks:
  - Compare local expected profit, impact model inputs, exact replay output, and competitor execution size.
  - Decide whether the current impact guard is over-conservative or whether the quote is structurally stale/optimistic.

## Execution Path

- Replace synthetic execution flow with real `eth_call` simulation against `EXECUTOR_CONTRACT`.
- Add real calldata encoding for `executeWithOwnFunds`.
- Track actual EOA nonce, ETH balance, pending tx, receipt, and revert reason.
- Add candidate expiry and replay protection tied to block/timestamp.
- [ ] Add EOA address library management:
  - Auto-generate/import execution EOAs into an encrypted/key-managed address pool.
  - Track per-address lane state, nonce, ETH balance, cooldown, and last use.
  - Allocate candidates across lanes without reusing a pending nonce lane.
  - Add operator controls to pause, drain, or retire individual lanes.
- [ ] Add automated gas funding for execution EOAs:
  - Maintain target ETH balances based on measured gas cost percentiles and configured attempts-per-address.
  - Fund only when network gas is below a configurable percentile/threshold.
  - Record all funding transactions and reconcile balances before marking an address ready.
  - Never auto-transfer strategy principal; funding automation is gas-only.
- [ ] Add competitor gas strategy analysis:
  - Cache observed watched-pool tx/receipt data in `observed_transactions`.
  - Compare our submitted txs against same-block watched-pool tx gas ranks.
  - Analyze known competitor addresses/executors over a rolling 30d window.
  - Feed measured p90/p99 priority fee and effective gas into execution gas defaults and address funding targets.
- Add Aerodrome Slipstream executor support without guessing ABI:
  - Confirm the Base Slipstream router and factory addresses from primary sources/onchain config.
  - Confirm the router swap function signature and whether execution requires fee, tick spacing, or encoded path.
  - Extend `Executor` with a dedicated `AerodromeSlipstream` swap branch instead of reusing Classic routing.
  - Extend execution-manager calldata/path encoding with the exact Slipstream parameters.
  - Add fork/eth_call tests against a real Slipstream pool before enabling searcher execution paths.
  - Only enable Slipstream in searcher after executor eth_call succeeds for representative pools.

## Monitoring

- Build a very lightweight web UI that reads directly from Postgres for operational visibility.
- First version should be read-only and minimal:
  - recent `dex_events`
  - latest `pool_states`
  - recent `opportunities`
  - recent `simulations`
  - recent `transactions`
  - per-EOA lane status
- Prefer a small Rust web service or a minimal server-rendered app over a heavy frontend stack.
- Keep it independent from the trading hot path; it should tolerate DB lag and never sit on the realtime critical path.

## Nice To Have

- Add simple SQL views for dashboard queries.
- Add `/healthz` and `/readyz` endpoints for each runtime.
- Add structured metrics export for process-level monitoring.
- Add a single `ops/run-local.sh` or `justfile` to start services consistently.
