# Arb Doctor

`doctor/` is the offline diagnosis entrypoint for production arbitrage failures.
It is intentionally outside the hot path. The bot should stay fast; this tool
can be slower and more complete.

Primary goals:

- turn one failing opportunity or submitted transaction into a repeatable report;
- classify failures into durable verdict buckets instead of chat-only guesses;
- reuse existing recorder diagnostics before adding new one-off SQL;
- leave enough evidence to fix the bot system, not just the single sample.

## Usage

From the project root:

```bash
doctor/arb_doctor.sh --opportunity-id <uuid>
```

Or start from a submitted transaction hash:

```bash
doctor/arb_doctor.sh --tx-hash <0x...>
```

Optional:

```bash
doctor/arb_doctor.sh \
  --opportunity-id <uuid> \
  --out reports/doctor/<case-name> \
  --executor-call
```

By default, executor eth_call is skipped because the first diagnosis pass should
not depend on current hub balances/allowances. Add `--executor-call` when the
executor call itself is the target.

## Output

The output directory contains:

- `doctor-report.txt`: top-level context, evidence, verdict, and next action;
- `db-context.txt`: opportunity, simulation, transaction, path, pool, and protocol rows;
- `replay.txt` / `replay.log`: historical replay result;
- `validate.txt` / `validate.log`: route-level state and quote checks.
- `state-diff.txt` / `state-diff.log`: per-step recorded/local/onchain
  state diff and protocol quote checks. This is the first place to inspect
  quote-model, reserve drift, fee, and tick-readiness problems.

## Verdict Buckets

Initial buckets:

- `v4_model_or_adapter_mismatch_suspected`
- `balancer_model_or_adapter_mismatch_suspected`
- `missing_ticks`
- `factory_or_pool_identity_mismatch`
- `pool_mismatch`
- `approval_config`
- `capital_config`
- `intervening_or_state_change_suspected`
- `classic_state_drift`
- `classic_formula_mismatch`
- `classic_pool_formula_mismatch`
- `classic_k_not_state_or_formula`
- `v3_state_or_tick_needs_review`
- `route_quote_completed_needs_review`
- `unknown_needs_manual_review`

These buckets are deliberately conservative. If the evidence is incomplete, the
verdict should remain open rather than inventing a root cause.
