# TODO

## Current: Competitor Opportunity Gap

Target competitor: `0x0629da86af5a4ae1ba5e1589b13702558d0fb056`.
Do not treat low local opportunity count as a market-wide dry spell while this
address continues to receive frequent arbitrage profits. Diagnose the local
gap as coverage, quote, path-generation, or execution-model lag until proven
otherwise.

Use [`COMPETITOR_DRIVEN_LOOP.md`](COMPETITOR_DRIVEN_LOOP.md) to convert each
rolling competitor report into this priority list. When a task needs code or
configuration changes, run it through [`DEBUG_WORKFLOW.md`](DEBUG_WORKFLOW.md)
instead of patching from chat memory.

Latest evidence:

- Competitor report: `reports/competitor-gap-20260628T054124Z`.
- Searcher quality report: `reports/searcher-quality-20260628T054645Z`.
- Competitor samples in the latest report: all `3` sampled profit txs are
  `covered_no_opportunity_near_block`.
- Searcher gap split: `recognized_anchor_cycle_but_no_opportunity=1`,
  `recognized_swaps_do_not_form_anchor_cycle=2`.
- Important competitor samples:
  - `0x3b5761584aaaaa15247c9896ca3c85c5f8eef819a96fdc9e103cb02b2f42c2df`:
    two ready V3-style USDC/cbBTC pools, recognized anchor cycle, no local
    opportunity near the block.
  - `0x66687b5961276b98d948ae751e41eb87ffe510a0e90e4cdc1fc1035e89118c4a`:
    Uniswap V3 + Balancer V3 flow; Balancer pool
    `0x6cf6f5adc2a3de26972340a827d2369ff56e82d0` remains
    `balancer_v3_quote_unvalidated`.
  - `0xa800c486733586e35dadf7aef5cded7fd8e6a3a72b191e98cf4bf39832850e5b`:
    Aerodrome + V4 flow; V4 pool
    `0xed93844bad1e39b1d7298a37f636d8169bc6523e` is static/zero-hook but
    `tick_scan_zero`.
- Impact shadow from the searcher-quality report: `price_impact_rejected=1044`,
  shadow pass counts would be `650` at 100 bps, `974` at 150 bps, `978` at
  300 bps, and `1030` at 500 bps; max shadow profit was `21,389` at 100 bps,
  `41,778` at 150 bps, `56,941` at 300 bps, and `848,466` at 500 bps.
- The same searcher-quality report produced only `6` opportunities and `14`
  emitted candidates, so impact50 is a material opportunity-scarcity suspect,
  but it is not yet proven to explain the competitor near-block misses.

Priority order:

- [ ] P0: Explain `covered_no_opportunity_near_block` and
  `recognized_anchor_cycle_but_no_opportunity` for the latest competitor
  samples:
  - Start from `0x3b5761584aaaaa15247c9896ca3c85c5f8eef819a96fdc9e103cb02b2f42c2df`.
  - For each competitor pool/path, prove whether our searcher rejected it due
    to path generation, quote skip, impact guard, min profit, anchor amount, hot
    pool selection, trust/execution filter, or missing Redis state.
  - Produce a durable diagnostic that maps one competitor tx to the exact local
    rejection stage.
- [ ] P1: Validate whether `MAX_PRICE_IMPACT_BPS=50` is blocking real
  opportunities:
  - Replay and/or simulate top `price_impact_rejected` samples from
    `reports/searcher-quality-20260628T054645Z.txt`.
  - Use the shadow data to compare 100/150/300/500 bps, but do not raise the
    live threshold until samples show successful simulation and acceptable
    revert risk.
  - Check whether the competitor P0 sample would have been rejected by impact;
    if yes, impact tuning becomes the first fix. If no, keep it as a separate
    opportunity-volume optimization.
- [ ] P2: Close Balancer V3 readiness gaps:
  - Ensure enabled Balancer V3 pools have `pool_model_coverage` and
    `pool_quote_coverage` rows; current reports still show competitor-used
    Balancer pools as `balancer_v3_quote_unvalidated`.
  - Persist live Balancer state needed by local quote, not just observation rows.
  - Fix missing token decimals/rates/model inputs before promoting pools into
    hot search.
- [ ] P3: Fix V4 readiness gaps that directly appear in competitor samples:
  - Investigate why imported pool
    `0xed93844bad1e39b1d7298a37f636d8169bc6523e` remains `tick_scan_zero`
    despite V4 tick backfill/repair.
  - Decide whether nonzero-hook pool
    `0x3c2828d64180af222763d3e78df47d8cc5454942` is safely supportable; if not,
    classify it explicitly as unsupported so reports do not imply accidental
    coverage debt.
- [ ] P4: Deep-dive Balancer V3 + V3-style flash-cycle routes:
  - Example tx:
    `0x641b0d4f32c1d75ded37045df1fbfcd8f209a2c00456884dd3988d3d24dc8887`.
  - This shape borrows/receives USDC from a V3 pool, swaps most USDC through
    Balancer V3, repays the V3 pool with the output token, and keeps USDC
    residue as profit.
  - Determine whether own-funds forward-cycle search is sufficient to detect
    the same economics, or whether a dedicated flash/exact-output route model
    is required.
- [x] Build a rolling competitor profit/path gap report for the target address:
  - Decode profitable transactions into pool sequence, token flow, protocol
    variants, profit token, and profit amount.
  - Compare each used pool against local states: discovered, imported,
    quoteable, hot Redis, trusted executable, and recently path-generated.
  - Produce top missed pool/protocol/path families over 30m, 2h, and 12h.
- [ ] P5: Make competitor reporting scalable enough for the 30m loop:
  - A 5000-block report with opportunity lookup was still running after 6m.
  - Split fast triage reports from deep reports, or optimize the opportunity
    lookup query path before relying on full-window automation.
- [ ] P6: Reduce V3-style `TickRangeExhausted`:
  - Keep searcher hot path RPC-free; it should only enqueue `ticks:repair`.
  - Watch whether queued repair reduces repeated exhausted pools.
  - If repeated exhaustion persists, add per-pool repeated-failure counters and
    widen queued repair radius adaptively.
- [ ] P7: Re-check MinProfitNotMet after opportunity flow recovers:
  - Use fresh samples only.
  - Split by protocol combo and distinguish state-race failures from local quote
    model optimism.

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
  - Keep searcher runtime router quote behind
    `SEARCHER_BALANCER_V3_RUNTIME_QUOTE_ENABLED=true`; default production search
    must not call RPC per path.
  - Use adapter execution after a path is selected and simulated.
- [x] Add offline Balancer V3 quote validation:
  - `validate_balancer_v3_quotes` checks router-query success by pool/token direction.
  - Results are persisted in `pool_quote_coverage` for reports and readiness checks.
- [x] Add offline Balancer V3 pool-type/input classification:
  - `classify_balancer_v3_models` probes Vault live balances/static fee and
    weighted/stable pool getters.
  - Results are persisted in `pool_model_coverage` for reports and local-math
    readiness.
- [x] Add first Balancer V3 local quote path:
  - Classify 2-token weighted pools with Vault live scaled balances, token rates,
    decimal scaling factors, static fee, and normalized weights.
  - Promote only `weighted_inputs_ready` pools into market-data hot state.
  - Quote 2-token weighted pools locally in searcher; keep runtime router quote
    as explicit opt-in fallback only.
- [ ] Extend Balancer V3 local quote coverage:
  - Implement exact fixed-point weighted math to replace the conservative f64
    first pass.
  - Add stable pool math.
  - Add boosted/rate-provider edge cases and multi-token weighted routing.
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
