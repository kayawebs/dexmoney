# Debug Workflow

This workflow is mandatory for non-trivial production issues. The goal is to
avoid patch-by-patch debugging, repeated fixes for the same root cause, and
unclear verification.

Use this process for issues that affect:

- submitted transaction success rate;
- simulation pass rate;
- opportunity scarcity;
- market-data/searcher/execution latency;
- pool/protocol coverage;
- contract safety or fund movement;
- recurring failures that have already appeared before.

Small mechanical changes can skip the full template, but they must still include
a clear verification step.

## Required Flow

### 1. Record The Symptom

Write the observable failure before changing code.

Include:

- UTC time window;
- affected process or contract;
- SQL/log/report command;
- count and magnitude;
- representative path, pool, transaction, opportunity, or simulation id.

Bad:

- "V4 is broken."

Good:

- "In the last 30m, simulations had 814 `UniswapV4Adapter.NoOutput` failures,
  all on V4 paths, while searcher produced 5,373 opportunities."

### 2. State Hypotheses

List at most three hypotheses. Each hypothesis must be falsifiable.

Examples:

- The local quote model is using the wrong token delta sign.
- The candidate is fresh when emitted but stale by simulation time.
- A protocol adapter calldata field differs from the searcher path model.

Do not change code before there is evidence that distinguishes the hypotheses.

### 3. Collect Evidence

Prefer durable scripts over one-off SQL. Temporary SQL is allowed for
exploration, but useful checks should be promoted into `ops/*_diag.sh` or a
recorder binary.

Evidence should answer:

- Is the failure concentrated by protocol, pool, path, token, or block lag?
- Does replay fail at the opportunity block, simulation block, or receipt block?
- Is the error deterministic for the same input?
- Does the failure happen before submit, after submit, or only onchain?
- Is this a model/data problem, a speed/staleness problem, or a config problem?

### 4. Decide Root Cause

Record the root cause explicitly. If the root cause is not proven, mark it as
`Unknown` and keep the issue open.

Do not close an incident only because a plausible patch was merged.

### 5. Make The Smallest Fix

Classify the fix:

- `safety`: prevents fund loss or unsafe execution;
- `correctness`: fixes math, state, calldata, or protocol behavior;
- `performance`: reduces hot-path latency or work;
- `coverage`: discovers, classifies, quotes, or executes more pools/protocols;
- `observability`: improves diagnosis, health checks, reports, or logs;
- `config`: changes deployed parameters or runtime settings.

Keep hot-path performance rules from `OPERATING_PRIORITIES.md`.

### 6. Verify With A Metric

Every fix needs a command and an expected metric.

Examples:

- `UniswapV4Adapter.NoOutput` should drop sharply after deploying the new
  adapter.
- Submitted duplicate tx count should be zero for the same
  `(block,path,token,amount)`.
- `quote_skipped_missing_ticks` should remain near zero after tick repair.
- `MinProfitNotMet` should move from "all protocols" to a smaller identified
  bucket, or reduce by a measurable amount.

If deployment is required, the verification is not complete until the deployed
service or contract is checked.

### 7. Add A Regression Guard

Choose at least one:

- unit/fork test;
- replay test;
- diagnostic script;
- health-monitor alert;
- structured log field;
- contract guard;
- documented incident entry.

## Incident Record Template

Copy this into `docs/INCIDENTS.md` for material issues.

```md
## YYYY-MM-DD Short Title

Status: Open | Fixed In Code | Deployed | Verified | Closed
Category: safety | correctness | performance | coverage | observability | config

### Symptom

### Impact

### Hypotheses

### Evidence

### Decision

### Fix

### Verification

### Regression Guard

### Follow-up
```

## GitHub Issue Policy

Use a GitHub issue when:

- the problem spans more than one turn or one process;
- a fix requires deployment plus later verification;
- multiple hypotheses remain open;
- it affects funds, submitted txs, or protocol coverage;
- it is likely to recur.

Use `docs/INCIDENTS.md` for long-term project memory even when a GitHub issue is
also opened.

## Do Not Do

- Do not tune balances, gas, min-profit, or search amount before diagnostics
  show those are the bottleneck.
- Do not add fallback logic that hides a bad model or stale data.
- Do not use competitor usage alone as executable trust.
- Do not call an issue fixed until there is post-change verification.
- Do not rely only on chat history for project memory.
