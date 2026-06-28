# Searcher Path Generation

This document describes the current production path-generation model. It is
for debugging opportunity gaps against competitor reports.

## Inputs

- Pool state comes from Redis `pool:*` state populated by market-data and
  discovery flows.
- Active pools are selected from the searcher in-memory pool-state cache. Pools
  outside the active guard are excluded from path index and graph snapshots,
  except current changed pools can be forced active when quote-ready.
- Token/amount configuration comes from enabled `token_pairs` plus
  `token_search_defaults`.
- Multihop anchors are only tokens with non-empty multihop search amounts.
  A token appearing in a competitor transaction is not searched unless it is an
  anchor in config.

## Static Two-Pool Paths

Two-pool paths are built into `PathIndex`.

1. Group supported active pools by canonical token pair.
2. For each enabled token-pair config, enumerate ordered pairs of distinct
   pools for the same token pair.
3. Generate both configured directions only when that side has search amounts.
4. During a cycle, select only static paths that include a changed pool.

This covers classic two-pool arbitrage such as `USDC -> WETH -> USDC` across
two pools of the same token pair.

## Dynamic Multihop Paths

Dynamic paths are generated per cycle from current changed pools.

1. Build a directed graph with two edges per supported active pool:
   `token0 -> token1` and `token1 -> token0`.
2. Sort outgoing edges per token by liquidity/depth score.
3. Keep the top `16` edges per token by depth.
4. Add changed-pool priority edges that fell outside the top `16`, capped at
   `32` extra edges per token.
5. For each configured anchor token, each changed-pool edge, and cycle length
   `3` and `4`, build:
   - prefix: `anchor -> changed_edge.token_in`
   - changed edge
   - suffix: `changed_edge.token_out -> anchor`
6. Reject invalid cycles:
   - repeated pool;
   - intermediate token equals anchor;
   - repeated intermediate token.
7. Use reachability memoing before recursive segment expansion to avoid
   branches that cannot reach the target token in the remaining edge count.
8. Rough-quote candidate cycles against configured amount grid.
9. Keep top dynamic paths by rough score/profit, capped at `5,000` paths per
   scan and `20,000` rough candidates before selection.

## Downstream Filters

Path generation only decides which paths reach exact quote. A generated path can
still be rejected by:

- missing Redis pool state;
- missing initialized ticks;
- tick range exhaustion;
- exact quote error;
- min profit;
- price impact;
- quote-model edge guard;
- risk/execution trust checks;
- candidate coalescing or freshness checks.

## Known Blind Spots

- Production only searches configured anchors. If WETH is not configured, WETH
  cycles are not generated even if the competitor executes a profitable WETH
  cycle.
- Current production search only models own-funds forward cycles of length 2,
  3, or 4. It does not model flash/exact-output repayment shapes as a separate
  path family.
- Singleton flows through Balancer Vault or Uniswap V4 PoolManager must be
  decoded into executable pool edges. If transfer/log flow does not form a
  configured anchor cycle, it is out of the current path model.
- The top-16 plus priority-edge fanout is an intentional performance prune.
  It can still miss paths when the useful edge is neither in the depth top-16
  nor in the changed-pool priority set.
- Rough quote can prune paths before exact quote. V3 spot failures and some
  Balancer metadata gaps are allowed through; deterministic missing-state,
  stable-quote, V2-quote, and zero-output failures are dropped.

## Current P0 Evidence

The overnight shadow-capital reports initially over-counted path-generation
misses because shadow WETH amounts did not become shadow WETH anchors in the
diagnostic. After fixing the diagnostic, representative tx
`0x1541a8938cf3b97b7555ac52628da7f5a41ba8c40d4a514a8e33705c34a92e54`
reconstructs WETH cycles under shadow capital, while production correctly shows
`production_path_generated=no` because WETH is not configured as a live anchor.
The next blocker for that sample is missing initialized ticks, not graph fanout.
