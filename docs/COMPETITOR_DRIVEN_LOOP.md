# Competitor Driven Work Loop

This document connects three local project mechanisms:

- rolling competitor gap reports;
- the project TODO priority list;
- the mandatory debug workflow for non-trivial fixes.

The goal is to avoid ad-hoc patching. A competitor profit that we did not
detect, simulate, or execute is treated as evidence. The main agent ranks the
evidence, then a focused fix agent works one problem through the debug workflow.

## Recurring Report

Run the lightweight competitor gap report every 30 minutes from the local
machine:

```bash
bash ops/remote/competitor-gap-report.sh --lookback-blocks 100 --limit 3 --top 5
```

The wrapper runs the heavy analysis on `ssh base` and copies the report archive
into:

```text
~/Documents/dexmoney/
```

Default target competitor:

```text
0x0629da86af5a4ae1ba5e1589b13702558d0fb056
```

The recurring report is read-only diagnostics. It must not restart services,
change runtime config, deploy contracts, or mutate chain state.

Use wider reports only as manual deep dives after a specific hypothesis is
chosen:

```bash
bash ops/remote/competitor-gap-report.sh --lookback-blocks 5000 --limit 200 --top 50
```

If the wide report takes longer than a few minutes, treat that as a reporting
scalability issue rather than blocking the 30m triage loop.

## Main-Agent Triage

After each report, the main agent should inspect the newest report directory and
update the TODO priority order. Rank issues by production impact:

1. Competitor profit paths we cannot classify, quote, or execute.
2. Competitor paths covered by local pools but with no local opportunity near
   the same block.
3. Local search finds a related opportunity but simulation rejects it.
4. Local simulation passes but submitted transactions revert.
5. Internal cleanup that does not change opportunity coverage or real tx
   success.

Do not treat low local opportunity count as a market-wide dry spell while the
target competitor is still receiving frequent arbitrage profits.

## Fix-Agent Contract

When the user chooses the highest-priority item, the fix agent must follow
[`DEBUG_WORKFLOW.md`](DEBUG_WORKFLOW.md):

- record the concrete symptom and time window;
- state at most three falsifiable hypotheses;
- collect durable evidence with a reusable script or recorder binary where
  practical;
- decide root cause or explicitly mark it unknown;
- make the smallest fix;
- provide verification commands and expected metrics;
- add a regression guard.

The fix agent should write evidence to `reports/`, update `docs/INCIDENTS.md`
for material issues, and update `docs/TODO.md` when the priority changes.

## Large-Issue Split Rule

If the selected problem is too large for one working session, the fix agent
should stop after evidence collection and write a split proposal into the
current incident or TODO section. The proposal must include:

- why the issue is too large;
- which hypothesis is already proven or disproven;
- the smallest independent subproblems;
- the recommended first subproblem;
- verification for each proposed subproblem.

That ends the current fix workflow as `Pending Review`. The main agent then
reviews the split proposal and turns it into smaller TODO entries.

## Current Gap Buckets

Use these labels when mapping competitor reports to TODO items:

- `balancer_v3_quote_unvalidated`: Balancer V3 pool appears in competitor
  profit path but local model or quote coverage is missing.
- `covered_no_opportunity_near_block`: local pool coverage exists, but searcher
  did not emit a nearby candidate.
- `recognized_anchor_cycle_but_no_opportunity`: token/pool sequence is
  recognizable, but local path generation or quote rejected it.
- `recognized_swaps_do_not_form_anchor_cycle`: competitor flow is not modeled
  as our current anchor cycle shape; investigate flash/exact-output or
  non-cycle execution shape.
- `tick_scan_zero`: V3-style pool is discovered, but required initialized ticks
  are missing or not hot-loaded.

## Closure Criteria

A competitor-driven item is not closed by code merge alone. It is closed only
when post-change reports show one of:

- the competitor path family is now classified and quoteable;
- local opportunities appear near the same competitor path/block family;
- simulation failure moved to a narrower downstream bucket;
- submitted tx success/revert metrics show the intended improvement;
- the path is explicitly out of scope with a documented reason.
