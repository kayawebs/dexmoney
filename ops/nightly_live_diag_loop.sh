#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

usage() {
  cat <<'EOF'
Usage:
  ops/nightly_live_diag_loop.sh [options]

Read-only overnight live bot diagnostics. It writes periodic snapshots and does
not restart services, publish candidates, submit txs, or mutate live state.

Options:
  --interval-secs N     Sleep between snapshots. Default: 1800.
  --hours N             Stop after roughly N hours. Default: 10.
  --iterations N        Exact snapshot count. Overrides --hours.
  --out-dir DIR         Report directory. Default: reports/nightly-live-<stamp>.
  --once               Run one snapshot and exit.
  -h, --help           Show help.
EOF
}

INTERVAL_SECS="${INTERVAL_SECS:-1800}"
HOURS="${HOURS:-10}"
ITERATIONS="${ITERATIONS:-}"
OUT_DIR="${OUT_DIR:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --interval-secs)
      INTERVAL_SECS="$2"; shift 2 ;;
    --hours)
      HOURS="$2"; shift 2 ;;
    --iterations)
      ITERATIONS="$2"; shift 2 ;;
    --out-dir)
      OUT_DIR="$2"; shift 2 ;;
    --once)
      ITERATIONS="1"; shift ;;
    -h|--help)
      usage; exit 0 ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2 ;;
  esac
done

if [[ -z "${ITERATIONS}" ]]; then
  ITERATIONS=$(( (HOURS * 3600 + INTERVAL_SECS - 1) / INTERVAL_SECS ))
fi

if [[ -z "${OUT_DIR}" ]]; then
  OUT_DIR="reports/nightly-live-$(date -u +%Y%m%dT%H%M%SZ)"
fi

mkdir -p "${OUT_DIR}"

echo "nightly live diagnostics starting out_dir=${OUT_DIR} iterations=${ITERATIONS} interval_secs=${INTERVAL_SECS}"
for ((i = 1; i <= ITERATIONS; i++)); do
  echo "loop=${i}/${ITERATIONS} start=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  INTERVAL="30 minutes" LOG_SINCE=30m ./ops/searcher_quality_diag.sh "${OUT_DIR}" || true
  INTERVAL="30 minutes" LOG_SINCE=30m ./ops/opportunity_scarcity_diag.sh "${OUT_DIR}" || true
  INTERVAL="30 minutes" OUT_DIR="${OUT_DIR}" ./ops/minprofit_recent_focus.sh || true
  INTERVAL="30 minutes" OUT_DIR="${OUT_DIR}" ./ops/hub_submitted_failure_diag.sh || true
  echo "loop=${i}/${ITERATIONS} done=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  if ((i < ITERATIONS)); then
    sleep "${INTERVAL_SECS}"
  fi
done
