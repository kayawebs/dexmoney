# Incidents And Debug Memory

This file is the durable memory for important production issues. Use
`DEBUG_WORKFLOW.md` for the required process before adding new entries.

## Active Incident Queue

Work these in priority order. Do not skip directly to fixes unless the Evidence
section already proves the root cause.

1. `2026-06-24 MinProfitNotMet Root Cause Split`
2. `2026-06-24 Uniswap V4 Adapter NoOutput Verification`
3. `2026-06-24 Submitted Transaction Revert Rate`
4. `2026-06-24 Competitor Pool And Protocol Coverage Gap`
5. `2026-06-24 Balancer V3 Readiness`
6. `2026-06-24 Health Monitor Coverage`

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

Needed:

- `MinProfitNotMet` counts by 30m, 2h, and 12h windows.
- Failure split by simulation-only vs submitted-onchain revert.
- Top paths, pools, protocols, path length, token, amount, and
  `expected_profit / min_profit` ratio.
- Candidate age from opportunity creation to simulation and submit.
- Replay at opportunity block, simulation block, and receipt/latest block for
  representative samples.

### Decision

Unknown.

### Fix

Pending evidence. First action is to add `ops/minprofit_failure_diag.sh` and/or
extend replay tooling to accept explicit `opportunity_id` and `simulation_id`.

### Verification

The fix is not verified until a post-change report shows the dominant
`MinProfitNotMet` bucket reduced or reclassified into a smaller proven cause.

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
- leave durable notes in this file and/or a GitHub issue.

### Fix

Added `docs/DEBUG_WORKFLOW.md`, this incident log, and a GitHub diagnostic issue
template.

### Verification

Future issues should reference a report, GitHub issue, or incident entry before
non-trivial code changes.

### Regression Guard

`docs/OPERATING_PRIORITIES.md` points to the workflow, and
`.github/ISSUE_TEMPLATE/diagnostic.md` provides the same structure for GitHub
tracking.

## Pending Incident Backfill

These recent issues should be backfilled when they are next touched:

- Uniswap V4 adapter delta sign / `NoOutput`.
- Fake pool / trusted factory / adapter safety.
- Old opportunity consumption and candidate freshness.
- V4 tick coverage and hot-pool promotion.
- `MinProfitNotMet` root-cause split.
- Submitted transaction revert rate.
