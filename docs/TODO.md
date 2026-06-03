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
