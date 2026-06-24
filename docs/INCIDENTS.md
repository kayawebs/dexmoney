# Incidents And Debug Memory

This file is the durable memory for important production issues. Use
`DEBUG_WORKFLOW.md` for the required process before adding new entries.

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
