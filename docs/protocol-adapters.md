# Protocol Adapter Plan

Goal: close the competitor gap for singleton/router/vault protocols without repeatedly
redeploying the executor entrypoint.

## Scope

Supported direct pools today:

- Aerodrome classic volatile pools
- Aerodrome Slipstream pools
- Uniswap V3-style pools
- Pancake V3-style pools

Protocols to add:

- Uniswap V4 PoolManager-backed pools
- Balancer V3 Vault-backed pools

These are not normal pool contracts. They must be represented as protocol-specific pools
with extra metadata and executor adapter payloads.

## Architecture

### Market Data

Market-data owns correctness. Searcher and executor should not compensate for missing or
stale protocol state.

Required changes:

- Add protocol variants: `UniswapV4`, `BalancerV3`.
- Add singleton-backed pool metadata storage:
  - manager/vault address
  - pool identifier
  - token list
  - fee config
  - protocol-specific JSON payload
- Decode Uniswap V4 `Initialize`/`Swap` events from PoolManager.
- Decode Balancer V3 pool registration and swap events from Vault/router paths.
- Persist normalized pool states into Redis/Postgres with enough fields for quote.
- Record protocol-specific validation metrics in market-data, not searcher.

### Searcher

Searcher consumes normalized pool state and quote models.

Required changes:

- Treat V4/Balancer states as supported only when quote model metadata is complete.
- Add V4 quote module based on PoolKey/PoolId and v3-style concentrated liquidity math.
- Add Balancer quote modules incrementally by pool type:
  - start with weighted pools if competitor coverage confirms they dominate
  - then stable/boosted/gyro only if reports justify it
- Extend rough quote diagnostics until `dynamic_multihop_rough_quote_failed` points to a
  concrete missing field or unsupported pool type.

### Execution Manager

Execution-manager builds calldata. It should not know protocol math.

Required changes:

- Encode protocol-specific `adapterData` into `ExecutorHub.SwapStep.data`.
- Map V4/Balancer steps to `StepKind.Adapter`.
- Route approvals to the adapter contract, not PoolManager/Vault directly, unless the
  adapter design requires a different spender.
- Keep replay/validate tooling in sync with production calldata encoding.

### Contracts

ExecutorHub is the stable entrypoint. Protocol logic lives in adapters.

Current prerequisite completed:

- `ExecutorHub.SwapStep` includes `bytes data`.
- Adapter interface receives `bytes data`.

Next contract changes:

- Add `UniswapV4Adapter` that decodes PoolKey/swap params and interacts with PoolManager.
- Add `BalancerV3Adapter` that decodes vault/pool params and interacts with the Balancer
  router/vault path.
- Keep adapter whitelist on Hub.

## Current Status

Completed:

- Execution plumbing: `ExecutorHub` supports adapter steps and adapter whitelisting.
- Adapter contracts: `UniswapV4Adapter` and `BalancerV3Adapter` compile.
- Deployment scripts: V4 and Balancer adapters can be deployed independently and attached
  to the hub.
- Execution-manager plumbing: V4/Balancer steps map to `StepKind.Adapter` when
  `adapterData` is present.
- Market-data observation: V4 PoolManager and Balancer V3 Vault events are recorded into
  `protocol_pool_observations` for coverage analysis.
- Searcher diagnostics: opaque rough quote failures are sampled by concrete pool/path
  reason.

Not completed:

- V4 pools are not yet promoted into the normal `pools`/`pool_states` quote model. V4 is
  PoolId/singleton-manager based, so treating PoolManager as a pool address would corrupt
  state.
- V4 tick liquidity/net tick state is not yet stored by PoolId, so exact concentrated
  liquidity quote is not available.
- Balancer pools are not yet classified by pool math type. Exact quote requires per-pool
  type metadata or a safe query path.
- Searcher intentionally rejects V4/Balancer as unsupported quote pools until the required
  metadata is complete.

`dynamic_multihop_rough_quote_failed` means dynamic path expansion found a possible graph
edge but could not compute even a conservative rough output for one step. Typical reasons
are unsupported pool type, missing state, missing ticks, exhausted tick range, or invalid
token direction. The cycle summary now includes samples so this bucket is actionable.

## Implementation Order

1. Diagnose current rough quote failures. Done.
2. Add protocol enum/string parsing and metadata columns. Done.
3. Add V4 discovery/state skeleton and report-only quote rejection reasons. Done.
4. Add Balancer discovery/state skeleton and report-only quote rejection reasons. Done.
5. Implement V4 adapter contract and execution-manager adapterData. Done.
6. Implement Balancer adapter contract and execution-manager adapterData. Done.
7. Implement V4 PoolId-keyed state/tick storage and exact quote.
8. Implement Balancer pool type classification and quote for the dominant competitor pool
   type.
9. Promote quoteable V4/Balancer observations into executable search paths.
10. Run competitor live compare and replay against the same block window.

## Validation Gates

Before deploying final contracts:

- Searcher summary exposes no large opaque `rough_quote_failed` bucket.
- V4/Balancer competitor swaps are classified as either:
  - unsupported pool type with exact reason
  - recognized quoteable anchor cycle
  - opportunity existed near block
- Replay validates calldata encoding for at least one V4 candidate and one Balancer
  candidate.
- Executor simulation succeeds for representative adapter paths before live submit.
