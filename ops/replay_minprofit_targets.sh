#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env"
  set +a
fi

if [[ -f ".env.docker" ]]; then
  set -a
  # shellcheck disable=SC1091
  source ".env.docker"
  set +a
fi

REPORT_FILE="${1:-}"
LIMIT="${LIMIT:-12}"
DRY_RUN="${DRY_RUN:-0}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${OUT_DIR:-reports/replay-minprofit-$STAMP}"

if [[ -z "$REPORT_FILE" ]]; then
  REPORT_FILE="$(ls -t reports/minprofit-failure-diag-*.txt 2>/dev/null | head -1 || true)"
fi

if [[ -z "$REPORT_FILE" || ! -f "$REPORT_FILE" ]]; then
  cat >&2 <<EOF
usage:
  $0 <minprofit-failure-diag-report>

examples:
  LIMIT=12 $0 reports/minprofit-failure-diag-20260624T094446Z.txt
  LIMIT=0  $0 /Users/peter/Documents/dexmoney/minprofit-failure-diag-20260624T094446Z.txt

No report file found.
EOF
  exit 1
fi

mkdir -p "$OUT_DIR"

extract_ids_from_commands() {
  sed -n '/11\. representative replay targets/,/12\. /p' "$REPORT_FILE" \
    | grep -oE -- '--opportunity-id [0-9a-fA-F-]{36}' \
    | awk '{print tolower($2)}'
}

extract_ids_from_table() {
  awk -F'|' '
    /11\. representative replay targets/ { in_section = 1; next }
    /12\. / { in_section = 0 }
    in_section && NF >= 7 {
      id = $7
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", id)
      if (id ~ /^[0-9a-fA-F-]{36}$/) print tolower(id)
    }
  ' "$REPORT_FILE"
}

IDS=()
while IFS= read -r opportunity_id; do
  IDS+=("$opportunity_id")
done < <(
  {
    extract_ids_from_commands
    extract_ids_from_table
  } | awk 'NF && !seen[$0]++'
)

if (( ${#IDS[@]} == 0 )); then
  echo "no replay target opportunity ids found in section 11: $REPORT_FILE" >&2
  exit 1
fi

if [[ "$LIMIT" != "0" ]]; then
  LIMITED_IDS=()
  while IFS= read -r opportunity_id; do
    LIMITED_IDS+=("$opportunity_id")
  done < <(printf '%s\n' "${IDS[@]}" | head -n "$LIMIT")
  IDS=("${LIMITED_IDS[@]}")
fi

SUMMARY="$OUT_DIR/summary.tsv"
{
  echo -e "idx\topportunity_id\tstatus\tclassification\tout_file"
} >"$SUMMARY"

echo "report: $REPORT_FILE"
echo "out_dir: $OUT_DIR"
echo "targets: ${#IDS[@]}"
echo "limit: $LIMIT"
echo "dry_run: $DRY_RUN"
echo

idx=0
for opportunity_id in "${IDS[@]}"; do
  idx=$((idx + 1))
  short="${opportunity_id:0:8}"
  out_file="$OUT_DIR/replay-$short.txt"
  echo "[$idx/${#IDS[@]}] replay $opportunity_id -> $out_file"

  if [[ "$DRY_RUN" == "1" ]]; then
    echo -e "$idx\t$opportunity_id\tdry_run\t-\t$out_file" >>"$SUMMARY"
    continue
  fi

  status="ok"
  if ! cargo run -p base-arb-recorder --bin replay_simulations -- \
    --opportunity-id "$opportunity_id" \
    --out "$out_file"; then
    status="failed"
  fi

  classification="$(
    awk '
      /^== Summary ==/ { in_summary = 1; next }
      in_summary && NF { print; exit }
    ' "$out_file" 2>/dev/null || true
  )"
  if [[ -z "$classification" ]]; then
    classification="no-summary"
  fi

  echo -e "$idx\t$opportunity_id\t$status\t$classification\t$out_file" >>"$SUMMARY"
done

echo
echo "summary: $SUMMARY"
column -t -s $'\t' "$SUMMARY" || cat "$SUMMARY"
