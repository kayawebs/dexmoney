# Incidents And Debug Memory

This file is the durable memory for important production issues. Use
`DEBUG_WORKFLOW.md` for the required process before adding new entries.

## Active Incident Queue

Work these in priority order. Do not skip directly to fixes unless the Evidence
section already proves the root cause.

1. `2026-06-28 Competitor Ready Anchor Cycle No Opportunity`
2. `2026-06-24 MinProfitNotMet Root Cause Split`
3. `2026-06-24 Uniswap V4 Adapter NoOutput Verification`
4. `2026-06-24 Submitted Transaction Revert Rate`
5. `2026-06-24 Competitor Pool And Protocol Coverage Gap`
6. `2026-06-24 Balancer V3 Readiness`
7. `2026-06-24 Health Monitor Coverage`

## 2026-06-28 Competitor Ready Anchor Cycle No Opportunity

Status: Fix implemented locally; awaiting deploy/runtime verification
Category: opportunity gap / search pipeline

### Symptom

After `c4ae103` fixed same-block trusted pool state publication, the
post-deploy competitor gap report `reports/competitor-gap-20260628T064024Z`
still showed ready anchor cycles with no local opportunity near the competitor
block. Primary sample:
`0x6a003d20b0657ff6e1bb63a46e008af4f51e3eeb5f05e79605b8962add8e018d`.
It is a ready 4-hop USDC anchor cycle with competitor input `11,496,168` raw
USDC, below the configured USDC max `45,270,872`, and profit `17,803` raw USDC.

### Hypotheses

- Path generation rejects the cycle before quote.
- Quote skips due missing Redis state, missing ticks, or tick range exhaustion.
- Configured amount grid misses the profitable input.
- Min-profit, price-impact, or quote-model edge rejects the quoted route.
- Candidate publish is blocked by trust/execution filters or missing changed-pool
  trigger.

### Evidence

- Added durable diagnostic:
  `ops/competitor_searcher_pipeline_diag.sh --tx-hash <hash>`.
- The diagnostic reconstructs recognized anchor cycles from the competitor tx,
  loads current local Redis pool/tick state, mirrors local searcher graph fanout,
  rough quote, exact quote, min-profit, impact, quote-model edge, and reports the
  first observed stage. It also marks `state_snapshot:
  current_redis_at_run_time`; historical Redis/tick replay is not implemented.
- Primary sample `0x6a003d20...e018d` on the cloud node:
  `recognized_swap_pools=4`, `recognized_anchor_cycles=2`,
  `all_redis_states_present=true`, `changed_pool_trigger_inferred_from_tx=true`,
  and `opportunities_near_block=0` for all four pools.
- Primary sample Cycle 1:
  `path_generated=no`. Reasons:
  `edge excluded by dynamic graph fanout token=USDC pool=0x6b0f53...39a1be`;
  `pool excluded by active-state guard pool=0xfa65a7...727823`;
  `edge excluded by dynamic graph fanout token=VIRTUAL pool=0x7cb770...6f9830`.
  The diagnostic recovered the observed anchor input
  `observed_anchor_input_shadow=11496168`.
- Primary sample Cycle 2:
  `path_generated=no`. Reasons:
  `edge excluded by dynamic graph fanout token=USDC pool=0x7cb770...6f9830`;
  `pool excluded by active-state guard pool=0xfa65a7...727823`.
- Regression sample
  `0x117417c777e2f26d57c1cec10fc8fdd3f331e9a19fe4c3e1e88ad9e7a3eb44b5`
  reconstructed the 2-hop USDC/cbBTC cycles and also stopped at
  `path_generation`: pool `0x9d14ff...6894b0` was excluded by the active-state
  guard in the current Redis snapshot. The diagnostic recovered
  `observed_anchor_input_shadow=477683563` for the matching direction.

### Root Cause / Split

The proven first local pipeline miss for the primary P0b sample is path
generation/hot-pool selection, not missing Redis state, missing ticks,
configured amount cap, changed-pool trigger, min-profit, or impact guard. The
current local graph excludes at least one ready competitor edge via
`MAX_DYNAMIC_EDGE_FANOUT_PER_TOKEN=16`, and one cycle pool is excluded by the
active-state guard, so no production candidate can reach quote or publish.

This does not yet prove the exact historical quote result at tx block
`47919749`, because the diagnostic reads the current Redis snapshot. Forced
quote probes are useful shadow evidence only; they should not be treated as a
historical replay.

### Smallest Next Fix

Implement the narrowest live behavior change that preserves pruning while
preventing the triggering pools from being pruned before path generation:

- changed pools bypass the time-based active guard for the current cycle when
  their state is quote-ready;
- dynamic multihop token adjacency keeps the normal top-16 depth-ranked edges
  and additionally includes current changed-pool edges that fall outside that
  base fanout;
- searcher cycle logs include `dynamic_multihop_priority_edges` so the runtime
  cost of this extra coverage is visible.

This is not a global fanout increase and does not remove later rough quote,
exact quote, min-profit, impact, candidate cap, or execution filters.

### Verification

- Local: `cargo check -p base-arb-recorder --bin competitor_searcher_pipeline_diag`.
- Local: `bash -n ops/competitor_searcher_pipeline_diag.sh`.
- Cloud:
  `ops/competitor_searcher_pipeline_diag.sh --tx-hash 0x6a003d20b0657ff6e1bb63a46e008af4f51e3eeb5f05e79605b8962add8e018d --output /tmp/p0b-6a003d20-diag.txt`.
- Cloud:
  `ops/competitor_searcher_pipeline_diag.sh --tx-hash 0x117417c777e2f26d57c1cec10fc8fdd3f331e9a19fe4c3e1e88ad9e7a3eb44b5 --output /tmp/p0b-117417-diag.txt`.
- After deploy, searcher cycle logs should show
  `dynamic_multihop_priority_edges`; fresh competitor diagnostics should not
  stop at `path_generation` solely because a current changed pool is outside
  the base top-16 fanout or stale by wall-clock age.
- Follow-up on the primary sample after the fanout/active fix showed a second
  P0 blocker: pool `0xfa65a76655f3c0641b79e89de3f51459c3727823` comes from
  factory `0x8909dc15e40173ff4699343b6eb8132c65e18ec6`. The factory returns
  the pool via `getPair(tokenA,tokenB)` and reverts on Aerodrome-style
  `getPool(tokenA,tokenB,stable)`. Code support has been added for this known
  `UNISWAP_V2_FACTORY` as `DirectV2`, including `ExecutorHub` strict
  validation through `getPair`. This part is not live until a new Hub is
  deployed and configured.
- Post-deploy searcher logs showed the first fanout/active fix restored some
  opportunities, but introduced a hot-path performance regression. Recent
  cycles had `changed_pools=1204..1224`, `path_build_ms=66,403..95,581`,
  `dynamic_multihop_invalid_cycle=189,246..227,190`, and only
  `opportunities_created=1` per cycle. `dynamic_multihop_priority_edges`
  exceeded `1,100`, so every token segment expansion received all changed-pool
  priority edges during backlog processing.
- Root cause: the narrow coverage fix made the priority set global to the whole
  cycle. That preserves coverage, but turns `MAX_DYNAMIC_EDGE_FANOUT_PER_TOKEN`
  into `top16 + all changed edges` for hot tokens such as USDC/WETH, causing
  path generation to enumerate huge invalid prefix/suffix combinations before
  rough quote.
- Smallest performance fix: bound the extra priority set per token. The
  triggering changed edge is still inserted explicitly, but only the first
  `32` beyond-top16 changed priority edges per token are injected into segment
  expansion. Searcher logs now include
  `dynamic_multihop_priority_edges_dropped`, and the competitor pipeline
  diagnostic mirrors the same bounded priority rule.
- First runtime verification showed bounded priority alone was insufficient:
  `dynamic_multihop_priority_edges` fell to `196` and
  `dynamic_multihop_priority_edges_dropped=961`, but `path_build_ms` still rose
  to `107,016` with `dynamic_multihop_invalid_cycle=238,505`. The remaining
  hot cost is segment path recursion exploring token branches that cannot
  possibly reach the target token within the remaining edge count.
- Second performance fix: `SegmentPathCache` now includes reachability memoing.
  Before recursively expanding a prefix/suffix segment, searcher checks whether
  `current_token` can reach `end_token` in the remaining number of edges under
  the same bounded candidate-edge graph. This prunes impossible branches without
  dropping any reachable path.

### Regression Guard

Every future `covered_no_opportunity_near_block` or
`recognized_anchor_cycle_but_no_opportunity` report entry should be run through
`ops/competitor_searcher_pipeline_diag.sh`. A usable report must include
`path_generated`, quote skip reason if any, rough quote, exact probe stages,
min-profit result, impact result, quote-model edge result, and whether a
candidate would be publish-eligible if the path had been generated.

## 2026-06-24 MinProfitNotMet Root Cause Split

Status: Open
Category: correctness

### Symptom

`MinProfitNotMet` is currently the largest simulation failure bucket after the
candidate reaches execution-manager. It is not yet known whether the root cause
is local quote optimism, calldata/adapter mismatch, state movement before
simulation/receipt, or too-thin configured margin.

### Impact

Searcher can produce many candidates while execution-manager rejects most of
them before submit, reducing useful attempts and hiding whether route discovery
is actually competitive.

### Hypotheses

- Searcher local quote is optimistic for one or more protocol models.
- Execution calldata or adapter behavior differs from searcher path semantics.
- Candidate is valid at opportunity time but invalid by simulation or receipt
  block due to state movement.
- Expected profit is too close to configured min profit for some paths/amounts.

### Evidence

Collected:

- `minprofit-failure-diag-20260624T063050Z.txt`:
  - 2h window: `opportunities=21592`, `simulations=21219`,
    `simulation_success=6`, `min_profit_not_met=15619`, `submitted_txs=4`.
  - `MinProfitNotMet` candidates are fresh: `p50_sim_age_secs=0.0637`,
    `p90_sim_age_secs=0.1071`, `p50_sim_block_lag=0`, `p90_sim_block_lag=0`.
  - Failure margin is usually not thin:
    `p50_expected_to_min_ratio=55.643`, `p90_expected_to_min_ratio=1369.886`.
  - The dominant protocol combos include `UniswapV4`; top buckets are
    `AerodromeSlipstream -> AerodromeVolatile -> UniswapV4`,
    `PancakeV3 -> UniswapV3 -> AerodromeVolatile -> UniswapV4`, and
    `UniswapV4 -> UniswapV4`.
  - The largest margin bucket is `>=20x` with 11514 failures, so simply raising
    min profit will hide the symptom but not explain the mismatch.
  - Diagnostics show no `tick_range_exhausted` and no `v3_pools_without_ticks`
    for the sampled failing paths.
  - Same-path controls show several high-volume failing paths with zero
    successes, plus a few V4-including families with occasional successes.
- `replay-85959011.txt`:
  - Original failure was `Executor revert: MinProfitNotMet`.
  - Replay failed before meaningful execution with
    `replay_error: dex and pool variant mismatch`.
  - The failing path includes a V4 second step:
    `AerodromeVolatile -> UniswapV4`.
  - Root cause for this replay result is the replay tool lagging current
    `ExecutorHub` adapter semantics: V4/Balancer steps must encode as
    `Adapter=6`.
  - Code fix: `replay_simulations` now encodes V4/Balancer adapter steps and
    uses configured V4 PoolManager/Balancer Vault as factory fallback.
- `replay-85959011-v2.txt`:
  - Replay reached historical `eth_call` against `ExecutorHub`, but the report
    still classified the result as `historical_other` because revert formatting
    truncated the important RPC error details.
  - Structural check still attempted factory-style `getPool` against the V4
    PoolManager, which is not a valid V4 check.
  - Code fix: replay now skips V4/Balancer `getPool` structural checks, parses
    `data=0x...` selectors, trims calldata-heavy eth_call context, and knows
    current Hub custom errors including `BalanceDidNotIncrease`,
    `AdapterNotWhitelisted`, `MissingFactory`, and `InvalidFee`.
- `replay-85959011-v3.txt`:
  - Historical eth_call now decodes cleanly as
    `Executor revert: AdapterNotWhitelisted`.
  - This does not prove the original `MinProfitNotMet` root cause. The replay
    used the current configured V4 adapter against an older opportunity block;
    that adapter was not whitelisted in Hub at that historical block.
  - This sample is invalid for T0 root-cause replay unless the replay uses the
    adapter address that was configured at the original simulation time.
  - Next sample set must be selected after the current adapter was deployed and
    whitelisted, or the replay tool must be extended to reconstruct historical
    runtime config.
- `minprofit-failure-diag-20260624T090600Z.txt`:
  - 30m window has `opportunities=5676` but `simulations=0`.
  - No 30m `MinProfitNotMet` samples exist, so this report cannot advance the
    T0 root-cause split.
  - Before selecting fresh replay targets, verify execution-manager is running,
    consuming Redis candidates, and not dropping candidates as stale/expired.
- `execution-manager` log excerpt at 2026-06-24 09:16-09:18 UTC:
  - Redis candidates were popped as fresh with `stale_by_block=0` and
    `expired=0`; Redis queue ended empty.
  - No matching `execution candidate batch summary` or simulations were
    produced.
  - Code inspection found a loss path: live submit mode popped candidates before
    selecting/refreshing worker EOA and checking circuit breaker. If worker
    readiness or circuit breaker returned early, fresh candidates were already
    removed from Redis and never simulated.
  - Code fix: execution-manager now treats worker readiness as a hard invariant.
    Startup verifies that at least one worker EOA is funded, idle, and already an
    operator for every configured executor. Runtime checks worker readiness
    before popping Redis candidates; if no worker is ready, the cycle fails
    before consuming candidates.
  - No worker funding or `setOperator` maintenance runs in the hot path.
    Funding/operator setup is an explicit deployment/ops step. The only runtime
    reconciliation left is pending-lane receipt/nonce synchronization, which is
    required to know whether a previously submitted worker lane can be reused.
- Batch replay set from the 2026-06-24 part 11 target list:
  - `ops/replay_minprofit_targets.sh` replayed 40 representative opportunities.
  - `summary.tsv` classified all 40 as `historical_min_profit_not_met`.
  - Zero-min-profit replay also failed as `Executor revert: MinProfitNotMet`
    for all 40 samples. This rules out a thin configured min-profit threshold
    for this sample set.
  - Every sample carried old-source-block state evidence:
    `step_source_lag=>30` or equivalent source span greater than 30 blocks.
  - Examples:
    `replay-cd7ffef0.txt` had an AerodromeVolatile -> UniswapV4 path where
    one step used block `47752446` and the V4 step used block `47734900`.
    `replay-c6654a19.txt` and `replay-2ac8198d.txt` showed the same class on
    non-V4 UniswapV3 -> AerodromeSlipstream paths.
  - Correction on 2026-06-24: old `PoolState.block_number` is not by itself a
    correctness bug. It can simply mean the pool had no state-changing events
    since that block. The replay feature is a correlation signal, not a proven
    root cause.
- `summary.tsv` from `/Users/peter/Documents/dexmoney/minprofit`:
  - 20/20 representative failures are `historical_min_profit_not_met`.
  - 20/20 zero-min-profit replay attempts still fail as
    `Executor revert: MinProfitNotMet`, so configured min profit is not the
    immediate cause for this batch.
  - `validate_route` shows `latest_local_profit` and `redis_local_profit`
    remain above the configured `min_profit=5000` for every sample.
  - The samples repeatedly include V4 singleton/vault diagnostic fallback
    steps. Current local/Redis quote says profitable, while replayed executor
    semantics says not profitable.
  - Next proof step is to rerun `ops/minprofit_proof_batch.sh` with
    `EXECUTOR_CALL=1` so the report captures `executor_original` and
    `executor_zero_min` decoded Hub/adapter revert results for the exact
    current calldata.
- 2026-06-27 live proof after Base node restore:
  - `ops/minprofit_recent_focus.sh` over the last 30m showed
    `Executor revert: MinProfitNotMet` as the dominant bucket with 1594 recent
    failures. The failures were concentrated in V4 paths, especially
    `UniswapV4 -> AerodromeSlipstream`, `UniswapV4 -> PancakeV3`,
    `UniswapV4 -> UniswapV4`, and `UniswapV4 -> UniswapV3`.
  - `ops/minprofit_proof_batch.sh` with `EXECUTOR_CALL=1` on six fresh samples
    showed local replay/Redis quote still profitable, but both
    `executor_original` and `executor_zero_min` reverted as
    `ExecutorHub.MinProfitNotMet`. This again rules out configured min profit
    as the direct cause for those samples.
  - A top live sample,
    `24ef0282-69ba-4d18-a775-4b5efb2f5d9c`, used path
    `USDC/cbBTC uni-v4-5686f5 -> aero-slipstream-b3e778`. Local Redis quote
    used V4 state `tick=-63836`, `liquidity=26041638052`,
    `sqrt_price_x96=3256664623231334420946017604` from block `47806424`.
  - Direct V4 `StateView.getSlot0/getLiquidity` for the same PoolId showed
    current onchain state `tick=-64009`, `liquidity=11259056919`,
    `sqrt_price_x96=3228700276848664183336046038`, `lpFee=3000`.
    Therefore the V4 Redis state used by searcher was stale even though the
    route was emitted against a fresh Slipstream state.
  - Code inspection found the architectural cause: ordinary V2/V3/Slipstream
    events are handled in the `market-data` sealed-block path before publishing
    changed pools, but V4/Balancer singleton logs were handled in the separate
    `pool-discovery` loop and then promoted to Redis in capped batches. This
    allowed searcher to see fresh non-V4 state paired with stale V4 state.
  - Code fix: `market-data` now processes protocol singleton logs in the
    sealed-block path and immediately promotes quoteable V4/Balancer states
    before publishing the sealed block summary. The live `pool-discovery`
    protocol-log scan was removed to avoid duplicate observation writes; its
    periodic promotion remains useful for backlog/historical hydration.

Interpretation:

- The 40-sample batch proves the failures are not caused by thin min-profit
  thresholds: zero-min replay still fails.
- Old source blocks are not sufficient evidence of stale state. A local pool
  snapshot can be correct for many blocks if no relevant event occurred.
- A proven 2026-06-27 root cause is V4/Balancer singleton state promotion being
  outside the `market-data` sealed-block path. This can pair fresh non-V4 state
  with stale V4 state and produce deterministic `MinProfitNotMet`, including
  zero-min replay failures.
- Remaining `MinProfitNotMet` after deploying the sealed-block protocol-log fix
  must be split through event coverage, drift, local vs onchain per-step quote
  comparison, tick/metadata coverage, or
  calldata/adapter semantic checks.
- For this class, the issue is not gas policy, candidate queue age, or
  configured min-profit margin.
- The latest 20-sample batch further rules out "old block number" as a
  sufficient explanation: current Redis/latest local quotes still show profit.
  2026-06-27 executor-call proof moved the main branch point to V4/Balancer
  singleton state promotion timing.

Still needed:

- `MinProfitNotMet` counts by 30m, 2h, and 12h windows.
- Failure split by simulation-only vs submitted-onchain revert.
- Top paths, pools, protocols, path length, token, amount, and
  `expected_profit / min_profit` ratio.
- Candidate age from opportunity creation to simulation and submit.
- Replay at opportunity block, simulation block, and receipt/latest block for
  representative samples.

Added local evidence entrypoint:

- `ops/minprofit_failure_diag.sh` writes
  `reports/minprofit-failure-diag-*.txt`.
- The report groups failures by window, protocol combo, path, pool, margin
  bucket, candidate age, block lag, token/amount, path diagnostics, same-path
  success controls, and representative replay targets.
- `ops/replay_minprofit_targets.sh <report>` batch-runs section 11 replay
  targets and writes per-opportunity replay files plus `summary.tsv`.
- `ops/minprofit_proof_batch.sh [report]` batch-runs replay plus
  `validate_route` for representative `MinProfitNotMet` opportunities. Use it
  to split failures into historical executor replay classes and route quote
  buckets such as missing V4/Balancer local quote, onchain state fetch failure,
  factory mismatch, or completed Redis/latest quote.
- `ops/minprofit_proof_batch.sh` now also writes per-target
  `expected_profit`, `min_profit`, `latest_local_profit`, `redis_local_profit`,
  `redis_expected_diff_bps`, first recorded-vs-redis step delta, and a
  `diagnosis_hint`. This is the default batch artifact for deciding whether the
  next fix belongs in market-data state, searcher quote math, adapter calldata,
  or execution semantics.
- `ops/minprofit_proof_batch.sh` can be run with `EXECUTOR_CALL=1` to include
  `executor_original` and `executor_zero_min` decoded eth_call results in
  `summary.tsv`. Use this when replay/local quote evidence disagrees with Hub
  execution.

Replay targets from the report:

- `291e8615-563c-4fa4-a203-48385e700427`: pure V4 path with very large
  expected profit.
- `c71f8660-28a9-4219-ab5c-c33dbde33e04`: high-volume
  AeroSlipstream/AeroVolatile/V4 mixed path.
- `85959011-e3a1-4abe-8d6f-96e19e967415`: Aerodrome classic into V4 path.

### Decision

Focus T0 on local quote vs execution consistency before changing profit
thresholds or gas policy. Fresh same-block failures with large
expected/min-profit ratios are a model/execution consistency problem, not a
profit-threshold tuning problem.

### Fix

No searcher freshness gate is accepted for this class.

- Searcher should consume the latest local pool snapshot and quote it quickly.
- `PoolState.block_number` is metadata for diagnostics, not a reason to reject
  a path.
- If market-data has processed sealed blocks without missing logs, no-event
  pools remain valid even when their `block_number` is old.

Still open:

- Next split is per-step local quote vs onchain execution diff for recent
  failed samples, plus event-coverage/drift checks for the involved pools.
- Run `ops/minprofit_proof_batch.sh` first. If the dominant bucket is
  `local_quote_missing_uniswap_v4` or `local_quote_missing_balancer_v3`, the
  immediate fix is diagnostic/model coverage for that protocol before any
  further production tuning. If `diagnosis_hint` is
  `recorded_vs_redis_quote_diverged`, inspect the first divergent step/pool. If
  it is `executor_adapter_semantics_or_historical_state`, run
  `doctor/arb_doctor.sh --opportunity-id <uuid> --executor-call` on one fresh
  sample from that bucket.

### Verification

This incident is not verified until `MinProfitNotMet` is reduced or split into
a smaller proven cause using per-step replay evidence.

### Regression Guard

Pending. Candidate guards include replay tests for representative routes and a
health-monitor alert for `MinProfitNotMet` spikes by protocol.

## 2026-06-24 Uniswap V4 Adapter NoOutput Verification

Status: Fixed In Code
Category: correctness

### Symptom

V4 simulations showed `UniswapV4Adapter.NoOutput` failures. The adapter delta
handling had been changed in a previous commit in a direction that contradicted
Uniswap V4 exact-input `BalanceDelta` semantics.

### Impact

V4 routes could be discovered and quoted but fail during execution simulation,
blocking V4 participation and distorting route-quality diagnostics.

### Hypotheses

- V4 adapter interpreted exact-input input/output delta signs backwards.
- Some V4 failures may still be caused by unsupported hook/dynamic-fee pools or
  incomplete tick/model state.

### Evidence

Current code fix:

- `07b2fd5 contracts: fix uniswap v4 adapter delta signs`.
- `forge test --match-contract UniswapV4AdapterTest` passed locally.
- `forge build` passed locally.

Needed:

- Deploy a new `UniswapV4Adapter`.
- Set the new adapter in hub whitelist.
- Restart execution-manager with the new `UNISWAP_V4_ADAPTER`.
- Compare `UniswapV4Adapter.NoOutput` before/after deployment.

### Decision

The delta-sign bug is fixed in code. Deployment and live verification are still
pending.

### Fix

Deploy the corrected adapter and update runtime config. Hub redeploy is not
expected to be required for this specific fix.

### Verification

After deploy, `UniswapV4Adapter.NoOutput` should drop sharply for V4 paths. If it
does not, keep this incident open and split remaining failures by hook, dynamic
fee, pool state, and path encoding.

### Regression Guard

Existing unit test covers the exact-input delta sign. Add a live simulation
sample once a stable V4 route succeeds.

## 2026-06-24 Submitted Transaction Revert Rate

Status: Open
Category: correctness

### Symptom

Some candidates pass simulation and are submitted, but recent submitted
transactions have had a high revert rate. The current split between unavoidable
state-race failures and avoidable system failures is not proven.

### Impact

Submitted reverts spend real gas. Avoidable submitted failures are higher
priority than simulation-only failures.

### Hypotheses

- Candidate state changes between simulation and inclusion.
- Duplicate same-block/path/token/amount submits create self-competition.
- Simulation block and receipt block differ enough to invalidate thin-margin
  candidates.
- Contract adapter or route encoding is still wrong for a subset of protocols.

### Evidence

Needed:

- Last 12h submitted tx report for hub address.
- Group by revert reason, path, protocol, token, amount, and block lag.
- Compare opportunity block, simulation block, submit block, and receipt block.
- Compare same-path successful and reverted gas usage.

### Decision

Unknown.

### Fix

Pending evidence. Existing submitted-failure diagnostics should be used before
new code changes.

### Verification

Submitted revert rate should fall, or the remaining revert rate should be
classified as expected state-race failure with bounded gas exposure.

### Regression Guard

Add a health-monitor alert for submitted revert spikes and duplicate submitted
route keys.

## 2026-06-24 Competitor Pool And Protocol Coverage Gap

Status: Open
Category: coverage

### Symptom

Competitor activity still shows profitable or active pools/protocol paths that
may not be fully discovered, quoteable, hot, trusted, or executable by our
system.

### Impact

Even with fast execution, the bot cannot compete on routes it cannot discover,
quote, or execute.

### Hypotheses

- We observe some competitor pools but fail to promote them into hot quoteable
  state.
- Some competitor-used factories remain untrusted because executable proof is
  missing.
- Protocol gaps remain in V4, Balancer V3, or unknown V3-style factories.
- Hot-pool selection may exclude pools that are competitor-relevant.

### Evidence

Needed:

- Competitor pool gap report over recent 30m/2h/12h windows.
- Split competitor-used pools into discovered, imported, quoteable, hot Redis,
  trusted-executable, and unsupported.
- Reuse analysis: repeated competitor pool usage vs one-off new pools.

### Decision

Unknown.

### Fix

Pending evidence. Candidate fixes include automatic factory execution proof,
hot-pool promotion changes, and protocol adapter/model expansion.

### Verification

Coverage improves when competitor-used profitable pools move from
`missing/unquoteable/untrusted` into `quoteable/executable`, and searcher emits
candidate paths involving those pools.

### Regression Guard

Add competitor gap summaries to health-monitor or scheduled diagnostics once the
report is stable.

## 2026-06-24 Balancer V3 Readiness

Status: Open
Category: coverage

### Symptom

Balancer V3 pools are observed, but readiness and actual contribution to
opportunities/simulations are unclear.

### Impact

If competitor uses Balancer V3 routes, missing local quote or adapter support
reduces route coverage.

### Hypotheses

- Current support covers only a subset of weighted 2-token pools.
- Stable, boosted, rate-provider, or multi-token pools are not locally quoteable.
- Metadata/model coverage exists in Postgres but hot Redis/searcher promotion is
  incomplete.
- Adapter execution is present but lacks representative successful simulations.

### Evidence

Needed:

- Balancer pool readiness by metadata, model coverage, quote coverage, hot state,
  and execution simulation.
- Competitor Balancer samples with path and pool type.
- Representative dry-run simulations for supported weighted pools.

### Decision

Unknown.

### Fix

Pending evidence. Do not add runtime RPC quotes to production searcher as the
long-term fix; prefer local model coverage.

### Verification

At least one supported Balancer V3 route should be locally quoted and pass
execution simulation, or the report should prove Balancer is not material in the
current competitor gap.

### Regression Guard

Keep Balancer readiness in protocol coverage diagnostics and health-monitor.

## 2026-06-24 Health Monitor Coverage

Status: Open
Category: observability

### Symptom

Most deep diagnostics still require manual scripts. Health-monitor should catch
the recurring high-level failure classes automatically.

### Impact

Manual-only diagnosis delays detection of lag, opportunity scarcity, simulation
failure spikes, and submitted revert spikes.

### Hypotheses

- The existing health-monitor can be extended with lightweight summary checks
  without burdening the hot path.
- Heavy root-cause reports should stay in `ops/*_diag.sh`, while health-monitor
  reports only trigger conditions and top-line buckets.

### Evidence

Needed:

- Current health-monitor checks and runtime cost.
- Candidate alert thresholds for market-data lag, searcher cycle time,
  opportunity scarcity, simulation failure spike, submitted revert spike, and
  protocol readiness.

### Decision

Unknown.

### Fix

Pending evidence. Add lightweight checks first; Telegram delivery can come after
log-based alerts are stable.

### Verification

Health-monitor should emit clear alerts for known bad states without producing
noise during normal operation.

### Regression Guard

Add alert examples and thresholds to operational docs.

## 2026-06-24 Debug Workflow Adopted

Status: Closed
Category: observability

### Symptom

Repeated fixes were being rediscovered in chat, and some issues were addressed
with narrow patches before the root cause, validation metric, and regression
guard were written down.

### Impact

The project risked repeated fixes for the same class of problem, unclear deploy
requirements, and weak post-fix verification.

### Decision

Complex production issues must now follow the documented workflow:

- record symptom;
- state falsifiable hypotheses;
- collect evidence;
- decide root cause or keep it open;
- make the smallest classified fix;
- verify with a metric;
- add a regression guard;
- leave durable notes in this file and local diagnostic reports.

### Fix

Added `docs/DEBUG_WORKFLOW.md`, this incident log, and local diagnostic report
rules.

### Verification

Future issues should reference a report or incident entry before non-trivial
code changes.

### Regression Guard

`docs/OPERATING_PRIORITIES.md` points to the workflow, and
Local diagnostic scripts and reports provide repeatable evidence for each active
incident.

## Pending Incident Backfill

These recent issues should be backfilled when they are next touched:

- Uniswap V4 adapter delta sign / `NoOutput`.
- Fake pool / trusted factory / adapter safety.
- Old opportunity consumption and candidate freshness.
- V4 tick coverage and hot-pool promotion.
- `MinProfitNotMet` root-cause split.
- Submitted transaction revert rate.

## 2026-06-24 MinProfitNotMet Root-Cause Split

Status: Open
Category: observability

### Symptom

Recent execution simulations are dominated by `Executor revert:
MinProfitNotMet`. A proof batch over representative failures showed every
sample still failed with `MinProfitNotMet` even when replayed with zero
min-profit.

### Impact

Searcher is emitting locally profitable candidates, but execution simulation
often proves no output surplus. Until this is split by protocol/model/callpath,
raising min-profit only hides opportunities and does not explain why local quote
is optimistic.

### Hypotheses

- V4 local quote/model or V4 adapter execution semantics differ.
- Some pool snapshots or tick sets used by the searcher are materially different
  from execution-time state.
- Non-V4 legs are producing optimistic local quotes.

### Evidence

- Proof batch directory: `reports/minprofit-proof-20260624T104122Z` on the
  server; copied samples in `/Users/peter/Documents/dexmoney/minprofit`.
- 12/12 replay samples: `historical_min_profit_not_met`.
- 12/12 replay samples: `historical_zero_min_result: Executor revert:
  MinProfitNotMet`.
- 12/12 sampled paths include `UniswapV4`.
- Reachable non-V4 validation legs were not the first failure signal; for
  example Aerodrome volatile local quote matched pool `getAmountOut` at 0 bps
  in a representative sample.
- `validate_route` previously stopped at V4 with `registry pool state fetch by
  block hash is not implemented for singleton/vault dex UniswapV4`, which is a
  diagnostic-tool limitation, not proof of stale pool state.
- A pool `source_block` older than the current block is not by itself evidence
  of stale state. It means last observed state-changing block for that pool.

### Decision

Root cause is still unknown. Current strongest split is V4-specific, but the
tooling must first distinguish V4 diagnostic coverage gaps from actual model or
adapter failures.

### Fix

Added recorder-side observability fixes:

- `validate_route` now treats Uniswap V4 and Balancer V3 as singleton/vault
  protocols for diagnostics, skips unsupported block-hash state fetch/factory
  checks explicitly, and falls back to Redis current state or recorded quote
  snapshots instead of aborting the route trace.
- `validate_route` now local-quotes Uniswap V4 with the same V3-style tick
  quoter used by searcher diagnostics.
- `ops/minprofit_proof_batch.sh` now sanitizes TSV fields and buckets V4/Balancer
  diagnostic gaps separately from real onchain state failures.
- `doctor/arb_doctor.sh` is the opportunity-level entrypoint for future
  evidence collection. It combines DB context, replay, route validation, and a
  conservative verdict bucket into one artifact.

### Verification

Rerun:

```bash
ops/minprofit_proof_batch.sh /path/to/minprofit-failure-diag.txt
```

Expected next report:

- `summary.tsv` remains nine columns.
- V4 samples no longer collapse into generic `onchain_state_failed`.
- `validate_route` prints per-step Redis/recorded quote traces through V4
  instead of stopping before final profit comparison.

### Regression Guard

Keep `ops/minprofit_proof_batch.sh` as the repeatable split tool for
`MinProfitNotMet`. Use `doctor/arb_doctor.sh --opportunity-id <uuid>` as the
first per-opportunity diagnostic artifact. Do not classify old pool
`source_block` as stale without a drift or event-loss proof.

## 2026-06-27: Market-Data Startup Skipped Sealed Blocks After Node Recovery

### Symptom

Fresh post-restart simulations still failed with `ExecutorHub.MinProfitNotMet`,
including zero-min-profit executor calls. A representative opportunity
`d4759ce7-b1e3-4269-b296-f494996e458c` showed local/Redis quote profit on a
Uniswap V4 -> Uniswap V3 path, but historical `debug_traceCall` showed the V4
leg produced about 16x less output than the local quote expected.

### Root Cause

`market-data` initialized `last_seen_block` from the current RPC head on startup
and immediately advanced `chain:current_block`. If the node or bot was down
while sealed blocks were produced, market-data skipped those blocks instead of
catching up. For the sampled V4 pool, chain logs contained 109 `Swap` and 4
`ModifyLiquidity` events in `47806541..47905337`, while local protocol state
was still at `47806540`.

This is not the normal case where a pool has no events and an old state block is
still accurate. It is an event-loss gap: the pool did have state-changing logs,
but the market-data sealed-block loop never consumed them.

### Fix

`market-data` startup now reads the previous Redis `chain:current_block`
watermark before polling the current RPC head. If the stored watermark is behind
the head, market-data catches up from the stored block instead of jumping to the
head. Sealed catch-up is chunked with `SEALED_EVENT_MAX_BLOCK_SPAN = 50`, and
`chain:current_block` is advanced only after the chunk has been processed.
The initial `onchain_init` state publish uses the stored startup watermark
instead of the highest loaded pool-state block, so loading current DB snapshots
cannot skip sealed replay after downtime.
Flashblocks startup is also delayed until sealed catch-up reaches the chain
head. Otherwise pending-log handling can advance `chain:current_block` before
sealed blocks are replayed, making searcher/executor consume an apparently
fresh watermark backed by stale pool state.

### Verification

After deployment, market-data startup logs should include:

- `startup_head_block`
- `stored_current_block`
- `catchup_blocks`

Sealed summaries should include:

- `chain_head_block`
- `catchup_lag_blocks`

During catch-up, `catchup_lag_blocks` should monotonically decrease. Searcher
should see `chain:current_block` lag until market-data has actually processed
the missing blocks.

## 2026-06-28: Post-Recovery Opportunity Scarcity

Status: Fixed In Code
Category: coverage | observability

### Symptom

After node recovery and bot restart, the last 30m had `0` opportunities, `0`
simulations, and `0` transactions, even though the watched competitor continued
to receive profitable arbitrage transfers. Searcher was not stalled: cycle logs
showed current chain and pool-state blocks, tens of thousands of quote attempts,
and `opportunities_created=0`.

### Impact

The bot could not submit any arbitrage attempts. This is a P0 because it is an
opportunity-production outage, not an execution-manager outage.

### Hypotheses

- Same-block competitor-discovered V3/V4 pools are imported too late or without
  initialized ticks, so searcher cannot quote them before the opportunity is
  gone.
- Ready competitor anchor cycles are being rejected by local amount/min-profit
  settings or impact guard rather than missing pool coverage.
- Some competitor flows are not anchor cycles and need separate classification
  before adding search complexity.

### Evidence

- Competitor report `reports/competitor-gap-20260628T061104Z` sampled three
  competitor profit txs: two `covered_no_opportunity_near_block` and one
  `tick_scan_zero`.
- The report showed `pool_gap_counts`: `covered_no_opportunity_near_block=5`,
  `tick_scan_zero=3`.
- Tx `0x7395b8f98e215feddceeab9ee229a18b4c29a88bb37a7c2e2a49adbc8a478a03`
  included same-block V3/V4 pools classified as `tick_scan_zero`.
- Tx `0x0cfd9a658d8e670194aa8277cb53a406f01b7c7a112a86058ddf13b04655517d`
  used a ready USDC -> cbBTC -> WETH -> USDC cycle, but competitor flow moved
  `457,242,224` raw USDC while local configured max was `45,270,872` raw.
- Searcher logs showed most successful quotes rejected by `min_profit_rejected`;
  some above-min candidates were rejected by `MAX_PRICE_IMPACT_BPS=50`.

### Decision

This is at least two issues. The same-block `tick_scan_zero` path is a real
coverage bug: importing a trusted pool wrote registry rows but did not
immediately publish the discovered state or trigger tick warmup. The ready-cycle
case needs better diagnostics before changing amount/min-profit/impact config.

### Fix

- `market-data` now immediately publishes the `DiscoveredPool.state` after a
  trusted pool import and asynchronously starts initialized tick warmup for that
  pool. This avoids waiting for registry reload or later state refresh before
  searcher can see the pool.
- `competitor_live_compare` now reports anchor-token pool-to-pool flow and its
  ratio to configured max amounts even when the competitor profit token is not
  an anchor token.
- `docs/TODO.md` was split into P0a/P0b/P0c so same-block tick readiness,
  ready-cycle rejection, and non-cycle competitor flows are tracked separately.

### Verification

After deployment, run:

```bash
bash ops/remote/competitor-gap-report.sh --lookback-blocks 100 --limit 3 --top 5
```

Expected:

- Same-block trusted V3/V4 pools should no longer remain `tick_scan_zero`
  unless the pool is explicitly unsupported or RPC tick lookup failed.
- `anchor_input_guess` should include `pool_to_pool_anchor_max_raw` and
  `observed_to_configured_bps` for ready anchor cycles.
- Searcher should create opportunities again when either a newly imported pool
  is quoteable or a ready path passes amount/min-profit/impact filters.

### Regression Guard

Keep the rolling competitor report as the first diagnostic for opportunity
scarcity. A future `covered_no_opportunity_near_block` must be mapped to one of
path generation, quote skip, impact guard, min profit, amount config, or
unsupported flow before changing hot-path search logic.
