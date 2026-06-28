# Incidents And Debug Memory

This file is the durable memory for important production issues. Use
`DEBUG_WORKFLOW.md` for the required process before adding new entries.

## Active Incident Queue

Work these in priority order. Do not skip directly to fixes unless the Evidence
section already proves the root cause.

1. `2026-06-28 DirectV2 Non-Canonical PoolMismatch`
2. `2026-06-28 Submitted Success Simulation Reverted After Same-Block Competition`
3. `2026-06-28 Competitor Ready Anchor Cycle No Opportunity`
4. `2026-06-28 Balancer And Router Vault Coverage`
5. `2026-06-24 MinProfitNotMet Root Cause Split`
6. `2026-06-24 Uniswap V4 Adapter NoOutput Verification`
7. `2026-06-24 Submitted Transaction Revert Rate`
8. `2026-06-24 Competitor Pool And Protocol Coverage Gap`
9. `2026-06-24 Balancer V3 Readiness`
10. `2026-06-24 Health Monitor Coverage`

## 2026-06-28 Balancer And Router Vault Coverage

Status: Diagnostic and repair entry implemented locally; runtime coverage apply pending
Category: competitor coverage / protocol readiness

### Symptom

The lightweight competitor report `competitor-gap-20260628T103106Z` sampled
three competitor profit transactions. None was a directly comparable
`USDC <=45U` anchor-cycle miss, but two involved Balancer/router-vault style
flows that the local reports could not classify precisely:

- `balancer_v3_quote_unvalidated=2`;
- `recognized_swaps_do_not_form_anchor_cycle=2`;
- repeated unknown counterparties included Balancer Vault
  `0xba1333333333a1ba1108e8412f11850a5c319ba9` and Uniswap V4 PoolManager
  `0x498581ff718922c3f8e6a244956af099b2652b2b`.

Representative Balancer pool:
`0x7b4c560f33a71a9f7a500af3c4c65b46fbbafdb7`
(`WETH/cbBTC`, Vault `0xba1333333333a1ba1108e8412f11850a5c319ba9`) was
observed and imported but had no model/quote coverage rows in the report.

### Hypotheses

- The pool is usable, but `pool_model_coverage` and `pool_quote_coverage` were
  never populated.
- The pool is a Balancer type that the local searcher cannot model without a
  new local math implementation or a deliberately enabled bounded router quote.
- The competitor flow is not a standard anchor cycle; the Vault/PoolManager
  singleton must be decoded into underlying pool ids before comparing with
  local searcher output.

### Evidence

`competitor-pool-gap.txt` had a concrete Balancer row:

- pool `0x7b4c560f33a71a9f7a500af3c4c65b46fbbafdb7`;
- topic `balancer_v3`;
- gap `balancer_v3_quote_unvalidated`;
- `quote_ready_count=0`;
- model fields empty;
- factory/Vault `0xba1333333333a1ba1108e8412f11850a5c319ba9`.

The local runtime has `BALANCER_V3_VAULT`, `BALANCER_V3_ROUTER`, and
`BALANCER_V3_ADAPTER` configured. `SEARCHER_BALANCER_V3_RUNTIME_QUOTE_ENABLED`
is not configured and defaults to false, so hot-path RPC quoting should not be
assumed as the fix.

### Root Cause

The current reports mixed two different gaps:

- Balancer readiness gap: competitor-used Balancer pools were present but not
  classified/validated in coverage tables.
- Router-vault flow gap: singleton contracts were bucketed as generic
  `contract_unknown_or_router`, which hid whether they were Balancer Vault,
  Balancer router, Uniswap V4 PoolManager, or some unrelated protocol.

### Fix

- `competitor_flow_probe` now loads configured known singleton contracts from
  settings and classifies Balancer Vault/router, Uniswap V4 PoolManager, local
  adapters, and executor contracts explicitly.
- `competitor_pool_gap` now classifies Balancer gaps by model readiness first:
  weighted two-token pools can proceed to quote coverage, while stable,
  weighted multi-token, unsupported, failed, or unclassified pools are reported
  as explicit model gaps.
- Added `ops/repair_competitor_balancer_v3.sh`, a repeatable repair entry that
  extracts Balancer pools from `competitor-pool-gap.txt` and runs both model
  classification and quote validation. It is dry-run by default and only writes
  coverage with `--apply`.

### Verification

Local static checks:

```bash
cargo fmt --check
cargo check -p base-arb-recorder --bin competitor_flow_probe --bin competitor_pool_gap
bash -n ops/repair_competitor_balancer_v3.sh
```

The report parser extracts the expected pool from
`/private/tmp/competitor-gap-20260628T103106Z/competitor-pool-gap.txt`:
`0x7b4c560f33a71a9f7a500af3c4c65b46fbbafdb7`.

Runtime verification after deploy:

```bash
ops/repair_competitor_balancer_v3.sh \
  --report-dir reports/competitor-gap-20260628T103106Z \
  --apply

ops/competitor_gap_report.sh --lookback-blocks 100 --limit 3 --top 5
```

Expected result: the same pool should move away from
`balancer_v3_quote_unvalidated`. If it becomes a stable/multi-token/model gap,
that is a real protocol support task rather than a missing coverage task.

Runtime result on 2026-06-28 11:00Z:

- Deployed commit `694d6a3` to `/home/ubuntu/dexmoney`.
- Ran `ops/repair_competitor_balancer_v3.sh --report-dir reports/competitor-gap-20260628T103106Z --limit 20 --apply`.
- Pool `0x7b4c560f33a71a9f7a500af3c4c65b46fbbafdb7` classified as
  `family=weighted`, `status=weighted_multi_token_unsupported`.
- Router quote validation wrote query failures for both tested directions:
  `cbBTC -> WETH` and `WETH -> cbBTC`.
- Follow-up report `reports/competitor-gap-20260628T110019Z` no longer sampled
  that Balancer pool in the 100-block window. Its flow probe did verify the new
  singleton classification path: `0x498581...2b2b` is now reported as
  `uniswap_v4_pool_manager` instead of a generic router/unknown contract.

Conclusion: the representative Balancer gap is now classified as a real
multi-token Balancer support gap, not a missing coverage-row issue.

### Regression Guard

Keep competitor reports read-only. Use the repair script only when a report
shows Balancer model/quote gaps. Future reports should never leave configured
Vault/PoolManager singleton addresses under generic `contract_unknown_or_router`.

## 2026-06-28 DirectV2 Non-Canonical PoolMismatch

Status: Fixed In Code; cleanup/deploy pending
Category: safety

### Symptom

After deploying the new `ExecutorHub` with DirectV2 support, execution-manager
started simulating fresh candidates again, but the last 10m failure bucket was
dominated by `Executor revert: PoolMismatch`:

- `opportunities_10m=474`;
- `simulations_10m=139`;
- `transactions_10m=2`;
- `PoolMismatch=111`.

Representative opportunity:
`b58e7fa0-1b3c-4789-bae0-e045def45c08`, block `47924308`,
path
`WETH-000006/USDC-a02913-a02913-000006-aero-classic-44576c-aero-slipstream-59dc59`.

### Impact

The Hub is correctly refusing unsafe execution, so funds are protected. The
runtime impact is severe: searcher and execution-manager spend hot-path work on
candidates that can never pass the contract guard.

### Hypotheses

- The DirectV2 pool is canonical for its configured factory, but the Hub
  validation is wrong.
- The DirectV2 pool is not canonical for its configured factory, and local
  discovery/import accepted a bad pool/factory pair.
- The candidate path factory address is missing or rewritten incorrectly between
  searcher and execution-manager.

### Evidence

The failing first step was:

- pool `0x0a55ebff7663e364101eae168ef471068b44576c`;
- factory `0x8909dc15e40173ff4699343b6eb8132c65e18ec6`;
- token0 WETH `0x4200000000000000000000000000000000000006`;
- token1 USDC `0x833589fcd6edb6e08f4c7c32d4f71b54bda02913`;
- local DB variant `AerodromeVolatile`, `stable=false`.

Onchain factory proof:

```bash
cast call 0x8909dc15e40173ff4699343b6eb8132c65e18ec6 \
  "getPair(address,address)(address)" \
  0x833589fcd6edb6e08f4c7c32d4f71b54bda02913 \
  0x4200000000000000000000000000000000000006
```

returned canonical pair:
`0x88A43bbDF9D098eEC7bCEda4e2494615dfD9bB9C`, not
`0x0a55ebff7663e364101eae168ef471068b44576c`.

The observed pool itself is a V2-style contract with matching tokens/reserves,
but it is not the factory canonical pair for that token pair. Hub
`PoolMismatch` is therefore the expected safety outcome.

### Decision

Root cause: local pool classification/import allowed an enabled DirectV2-style
pool whose configured trusted factory does not return it from
`getPair(token0, token1)`.

This must be rejected at data ingress and cleaned from current runtime state.
Do not relax Hub `PoolMismatch`.

### Fix

- `ChainProvider::resolve_pool_for_trusted_factory` now verifies
  `UNISWAP_V2_FACTORY.getPair(token0, token1) == observed_pool` before treating
  a DirectV2/AerodromeVolatile pool as executable.
- Added `ops/direct_v2_canonical_diag.sh` to scan enabled DirectV2 pools,
  prove canonical status with onchain `getPair`, and optionally disable
  mismatched pools plus remove their Redis pool state.

### Verification

Required before closing:

```bash
cargo check -p base-arb-chain
bash -n ops/direct_v2_canonical_diag.sh
ops/direct_v2_canonical_diag.sh --pool 0x0a55ebff7663e364101eae168ef471068b44576c
ops/direct_v2_canonical_diag.sh --pool 0x0a55ebff7663e364101eae168ef471068b44576c --apply
```

After deploy and cleanup, execution-manager simulation failures should no
longer be dominated by `PoolMismatch` for
`0x0a55ebff7663e364101eae168ef471068b44576c`.

### Regression Guard

Use `ops/direct_v2_canonical_diag.sh` whenever enabling DirectV2 pools or when
`PoolMismatch` clusters by an AerodromeVolatile/DirectV2 path. Future trusted
factory imports must fail closed when canonical factory proof is unavailable or
does not match the observed pool.

### Follow-up

The single `UniswapV2: K` failure observed in the same window is separate and
should be investigated only after the non-canonical pool set is removed from
the hot path.

## 2026-06-28 Submitted Success Simulation Reverted After Same-Block Competition

Status: Root cause proven for sample; fix pending
Category: execution competitiveness

### Symptom

After deploying the canonical DirectV2 guard and cleaning stale Redis pool
state, the fresh simulation bucket no longer showed `PoolMismatch`. In a
3-minute window, two simulations succeeded and one transaction was submitted:

- opportunity `bd3aab0b-12c0-44d0-9b4d-e70148c5510c`;
- tx `0x3a033b28adf3267a667243d9aaa2a85427a4afca80201b52cd55df803a85d1dc`;
- path `cycle3-a02913-aero-slipstream-39a1be-aero-slipstream-0ce10c-aero-slipstream-59dc59`;
- expected profit `26,999` raw USDC, min profit `5,000` raw USDC;
- simulation succeeded at `2026-06-28 09:36:10.047551+00`;
- transaction was included in block `47925012` with status `0`.

### Evidence

Receipt:

- our tx index: `135`;
- `effectiveGasPrice=9,300,000`;
- `gasUsed=544,208`;
- no logs emitted because the transaction reverted.

The three path pools were:

- `0x6b0f53cbd9272d8117e9535fe25371dedf39a1be`;
- `0xf579b16f9b1a4acc872d34a8141fbbd36c0ce10c`;
- `0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59`.

Same block `47925012` contained earlier successful swaps touching the same
pools:

- tx `0xb5c9593cbace54813c52eceac1a39d25a409491154c0fb47c2943826c4c51c95`,
  index `1`, touched all three path pools and paid effective gas price
  `144,796,229`;
- tx `0x799302e90d9286c815940fb824153dd07a8ff86a1fde1bb00c015bad381b4754`,
  index `50`, touched the first path pool.

`cast run` of our transaction on the local archive node succeeds and emits
`Executed(... profit=40684)`, which means the calldata and Hub execution path
are not inherently broken. The failure is explained by block-order state
movement: the opportunity was consumed or changed before our tx at index `135`.

### Root Cause

For this sample, the submitted revert is an execution competitiveness issue:
the bot found a real executable opportunity, but sent or priced it such that it
landed behind earlier transactions touching the same pools in the same block.

Do not classify this sample as a local quote math bug, DirectV2 canonicality
bug, or Hub safety bug.

### Next Fix Candidates

- Measure `simulation_time -> submit_time -> included_block/index` for every
  submitted tx and correlate reverted txs with earlier same-block pool touches.
- Improve transaction inclusion priority for high-confidence opportunities:
  fee policy, private/builder route if available, and worker nonce readiness.
- Consider route-level contention heuristics: if a pool was just touched in the
  included block before us, record the failure as `same_block_competed` for
  analytics instead of lumping it into generic `transaction reverted`.

### Regression Guard

Add a submitted-tx diagnostic that, given a tx hash, prints path pools,
inclusion index, earlier same-block logs for those pools, competing tx gas
price, and whether isolated `cast run` succeeds. This should be a first-line
tool before changing quote math for submitted reverts.

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
- Runtime verification after deploying the reachability memo fix:
  `cycles=128` in one summary window, `changed_pool_scans=23`,
  `changed_pools=2,566`, `path_build_ms=15,453` total, `max_cycle_ms=6,072`,
  `candidates_emitted=45`, and `opportunities_created=19`. This is still an
  optimization target, but it restores multi-cycle throughput compared with the
  previous single-cycle `path_build_ms=107,016`.
- New execution-layer blocker after searcher throughput recovered:
  execution-manager crash-looped while processing a candidate containing
  factory `0x8909dc15e40173ff4699343b6eb8132c65e18ec6`, with fatal error
  `Aerodrome Classic factory ... is not trusted; configured factory is
  0x420dd381b31aef6683db6b902084cb0ffece40da`. This is a candidate-level
  executability issue, not a process-level invariant violation. A single
  untrusted or temporarily unsupported pool must not stop all simulations and
  submissions.
- Smallest execution fix: `live_approval_preflight` now validates candidate
  calldata, approval derivation, and executor selection as preflight checks. If
  any of these fail, execution-manager enqueues a synthetic failed simulation
  with the full reason and skips only that candidate under
  `preflight_unencodable`; it no longer returns the error to the main loop.
- Verification needed after deploying execution-manager: the container should
  stop crash-looping, `execution candidate batch summary` should continue to
  print, and rejected DirectV2/factory candidates should appear as simulation
  failures with `executor preflight rejected: ...` instead of killing the
  process.
- Overnight shadow-capital diagnostics initially showed
  `root_stage=path_generation` for most competitor samples, but the diagnostic
  was under-modeling shadow capital. `--shadow-token-max WETH:<amount>` added
  quote probe amounts but did not add WETH to the effective anchor set used for
  path reconstruction. Representative tx
  `0x1541a8938cf3b97b7555ac52628da7f5a41ba8c40d4a514a8e33705c34a92e54`
  was therefore falsely reported as `recognized_anchor_cycles=0` even though
  the local ready pools form a WETH cycle:
  `WETH -> NFTWIZ -> KOBI -> TOSHI -> WETH`.
- Diagnostic fix: `competitor_searcher_pipeline_diag` now separates
  production anchors from shadow anchors. It prints
  `production_path_generated`, `production_path_generation_reason`, and
  `anchor_config_source`, and treats shadow-only amount grids as
  `production_grid=false`.
- Verification on the representative tx after the diagnostic fix:
  `recognized_anchor_cycles=2`, `anchor_config_source=shadow_only`,
  `production_path_generated=no` with reason
  `missing multihop anchor search config`, and exact probes now fail at
  `MissingTicks` for pool `0xfdc5d1271760af0e6146147850ddfc6844a43a10`.
  This proves at least part of the apparent P0 path-generation gap is actually
  WETH funding/config plus tick coverage, not graph fanout.
- Tick repair root cause: the representative pool was enabled, had current
  UniswapV3 state, and had `pool_tick_coverage.status=zero_ticks`. Runtime
  repair logs repeatedly showed `queued=20..60` but only `pools=4..6` selected
  per pass. Queued pools were drained from Redis before SQL selection, then
  filtered out by `pool_states.updated_at >= now() - max_age_hours`. With the
  daemon default `max_age_hours=2`, a pool with no recent event but accurate
  current state could be dropped from repair without being fixed.
- Tick repair fix: explicit and queued repair pools now load the latest
  available pool state without the age filter; only the background gap scan
  remains hot-age limited. Repair logs include `queued_selected` and
  `queued_unselected` so future queue loss is visible immediately.

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
