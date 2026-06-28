# Operating Priorities

This document is the long-term decision memory for Dexmoney optimization and
diagnostics. When priorities conflict, follow the order below.

For non-trivial production issues, follow
[`DEBUG_WORKFLOW.md`](DEBUG_WORKFLOW.md) before changing code. Important issues
must leave durable memory in [`INCIDENTS.md`](INCIDENTS.md) and local diagnostic
reports under `reports/`. The required loop is: symptom, hypotheses, evidence,
decision, fix, verification, regression guard.

For opportunity scarcity and competitor-parity work, use
[`COMPETITOR_DRIVEN_LOOP.md`](COMPETITOR_DRIVEN_LOOP.md) to turn rolling
competitor reports into TODO priority changes and focused debug workflows.

## 0. Performance First

The primary product is fast, accurate arbitrage attempts. Every runtime decision
should protect latency before adding engineering completeness.

Performance means all four hot-path stages:

- Data acquisition: market-data must ingest chain updates faster than block
  production under normal load.
- Data processing: state normalization, tick updates, Redis publish, and
  Postgres persistence must not block realtime state updates.
- Opportunity search: searcher must process fresh changed-pool work quickly
  enough to produce next-block candidates.
- Transaction attempt speed: execution-manager must simulate, approve if needed,
  select an EOA lane, and submit before the opportunity is stale.

Rules:

- Searcher must not call RPC on the production hot path.
- Market-data must not run historical backfills or heavy discovery loops.
- Expensive repair, hydration, validation, and competitor analysis must run in
  background tools or dedicated processes.
- Redis is a hot runtime cache, not a full historical database.
- Postgres is the durable source for analysis, coverage, and cold state.
- A stale candidate is diagnostic data, not an executable opportunity.
- Do not add broad fallback paths that hide data/model problems and slow the hot
  path. Fail loudly, diagnose, and fix the real input or model issue.
- Do not add gas price, token USD pricing, or per-block price conversion into
  simulation. Profit thresholds are configured manually; simulation should stay
  focused on whether the route executes and returns at least configured token
  profit.

Key diagnostics:

- `market-data sealed block summary`: block span, fetch/apply/publish time.
- `searcher cycle summary`: changed pools, path count, quote skips, emit count,
  cycle time.
- `execution candidate batch summary`: candidate lag, simulation throughput,
  failures, submit count.
- Hub submitted-failure diagnostics: separate non-submitted simulation failures
  from actually submitted tx success/revert.

## 1. Improve Submitted Transaction Success Rate

Once a tx is submitted, it spends real gas. Submitted tx success rate is the
highest-value execution metric.

Focus areas:

- Prevent duplicate submissions for the same block/path/token/amount.
- Reject untrusted pools/factories before route construction or calldata
  simulation.
- Ensure contract adapters cannot transfer funds to fake pools or arbitrary
  addresses.
- Keep EOA lanes independent and avoid nonce/pending blockage.
- Track simulation block, submit block, and receipt block so state-race failures
  are visible.
- Compare same-path success/revert gas to detect expensive failure modes.

Accepted reality:

- Some failures are unavoidable because another transaction can change state
  before our tx executes.
- The goal is not zero reverts; the goal is no avoidable reverts from duplicate
  submission, unsafe pool trust, bad calldata, stale candidates, or known model
  errors.

Primary questions when submitted txs revert:

- Was the route simulated successfully before submit?
- Was the candidate still next-block fresh at submit time?
- Did the same path/token/amount get submitted multiple times?
- Was the failing pool/factory trusted and executable?
- Is the failure concentrated in one protocol adapter, pool, path, or token?
- Is reverted gas materially worse than successful gas for similar routes?

## 2. Improve Simulation Pass Rate

If searcher produces many candidates but execution-manager does not submit, the
next bottleneck is simulation pass rate.

Prioritize failure buckets by count and expected profit:

- `MinProfitNotMet`: local quote is optimistic, state changed before simulation,
  or min-profit config is too high for the candidate size.
- `PoolMismatch`: route encoding or pool metadata is structurally wrong.
- `router/no-revert-data`: calldata path is wrong, protocol adapter is missing
  an exact error decode, or the target protocol rejected the call.
- `InsufficientAllowance`: auto-approval or allowance cache is incomplete.
- `InsufficientBalance`: configured search size exceeds the hub balance for that
  token, pair overrides conflict with global defaults, or stale config remains.
- Factory/trust rejections: pool discovery found a pool, but executable trust is
  not proven.

Rules:

- Do not lower the bar by submitting failed simulations.
- Do not hide `MinProfitNotMet` by adding gas math or USD conversion into
  simulation.
- Each high-volume failure bucket needs a concrete path/pool/protocol sample.
- Replay tooling must compare local quote inputs, exact simulation output, and
  route calldata for the same block.
- Pair-specific amount/min-profit overrides must be visible and must not
  silently override global token settings.

## 3. Match Competitor Pool Discovery And Handling

Competitor coverage is the main external benchmark. If the competitor profits
from pools we do not see, cannot classify, cannot quote, or cannot execute, the
system is structurally behind.

Goals:

- Continuously observe competitor transactions and global swap logs.
- Store all discovered pools/protocol observations durably in Postgres.
- Promote only quoteable and executable hot pools into Redis/searcher.
- Use competitor-used pools as a strong signal for hot-pool priority.
- Separate quote-only, observed-only, trusted-executable, and unsupported pools.

Discovery sources should include:

- Trusted factory events.
- Global swap logs for supported event signatures.
- Competitor transaction traces/logs.
- Protocol singleton logs such as Uniswap V4 PoolManager and Balancer Vault.
- Historical backfills for gaps, run outside the hot path.

Factory trust policy:

- A pool is not executable just because it has a familiar ABI.
- Trusted means the factory/protocol is proven compatible with our executor or
  adapter, route calldata, callback assumptions, and token-transfer semantics.
- Automated trust promotion is allowed only after programmatic proof:
  bytecode/codehash family match, pool state probes, dry-run route execution,
  and no unsafe token-transfer behavior.
- Competitor usage can prioritize review; it is not sufficient by itself to
  mark a factory trusted.

Hot pool selection:

- Anchor relevance: tied to funded tokens or active graph paths.
- Recent activity: live swaps, liquidity changes, or competitor use.
- Quoteability: complete state, ticks, metadata, and model coverage.
- Executability: deployed hub/adapters can safely execute the route.
- Expected value: routes produce candidates or explainable near-misses.

## 4. Complete Protocol Adaptation

Protocol support is only complete when all layers work together:

- Discovery: logs/events are observed and classified.
- Metadata: tokens, fees, tick spacing, hooks, pool type, vault data, and manager
  identifiers are stored.
- State: live state updates are applied correctly.
- Durable coverage: Postgres records tick/model/readiness status.
- Hot cache: Redis receives only promoted hot state/ticks needed at runtime.
- Quote model: searcher can quote locally without RPC on the production hot path.
- Execution: hub/adapters encode and execute the path safely.
- Replay: tooling can validate the same route across searcher, simulation, and
  onchain result.

Current protocol priorities:

- V3-style pools: keep the `v3-tick-repair` hot-pool daemon and durable coverage
  reconciliation working. Do not do full chain-wide V3 tick scans unless a
  measured gap justifies it.
- Uniswap V4: maintain full PoolManager metadata/tick coverage in Postgres, then
  use the `v4-tick-repair` incremental PoolManager scan for hot tick freshness
  and promote only supported static-fee/safe-hook quoteable pools to Redis.
- Balancer V3: expand local math coverage beyond the current weighted-pool path
  only when competitor gap reports show material missed profit.
- New protocols: prefer adapter expansion behind the stable Hub entrypoint; avoid
  redeploying strategy principal contracts for each protocol addition.

Validation gates before enabling a protocol in live submit:

- Searcher can produce concrete candidates for that protocol.
- Execution simulation succeeds for representative routes.
- Submitted failure diagnostics do not show concentrated structural reverts.
- Contract safety review confirms no arbitrary transfer/fake-pool route can drain
  hub funds.

## Diagnostic Order

When the bot underperforms, diagnose in this order:

1. Is market-data current and processing blocks within the latency target?
2. Is searcher receiving changed pools and producing candidates fast enough?
3. Are candidates fresh when execution-manager consumes them?
4. Are simulations passing? If not, group by concrete reason/path/protocol.
5. Are submitted txs reverting? If yes, group by path/protocol/block lag.
6. Are competitor profitable pools missing from our discovered/quoteable/hot set?
7. Are unsupported protocol gaps material enough to implement next?

Do not start by tuning min-profit, gas, or balances unless diagnostics show those
are the actual bottleneck.

## What To Optimize Next

Highest priority:

- Keep the hot path fast and measurable.
- Reduce avoidable submitted tx reverts.
- Raise simulation pass rate by fixing concrete route/model/config failures.
- Close competitor pool gaps where pools are quoteable and safely executable.

Medium priority:

- Improve health-monitor alerts for lag, no-candidate windows, stale candidates,
  simulation failure spikes, and submitted revert spikes.
- Improve web registry performance and visibility for effective token/pair
  amount and min-profit settings.
- Add better report joins between opportunities, simulations, transactions, and
  competitor windows.

Lower priority:

- UI polish.
- Broad fallback support for rare protocols.
- Gas profitability automation.
- Heavy dashboard queries that do not help the realtime trading loop.
