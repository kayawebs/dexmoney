#!/usr/bin/env bash
set -euo pipefail

REMOTE_SSH_TARGET="${REMOTE_SSH_TARGET:-base}"
REMOTE_DEXMONEY_DIR="${REMOTE_DEXMONEY_DIR:-/home/ubuntu/dexmoney}"

die() {
  echo "error: $*" >&2
  exit 1
}

usage() {
  cat <<'EOF'
Usage:
  ops/remote/competitor-gap-report.sh [report options]

Runs ops/competitor_gap_report.sh on the remote dexmoney host through ssh,
creates a tarball, and copies it to the local report directory.

Common options passed through:
  --address ADDR
  --lookback-blocks N
  --limit N
  --top N
  --hydrate

Environment:
  LOCAL_REPORT_DIR   Local destination. Default: ~/Documents/dexmoney
EOF
}

case "${1:-}" in
  -h|--help)
    usage
    exit 0
    ;;
esac

LOCAL_REPORT_DIR="${LOCAL_REPORT_DIR:-${HOME}/Documents/dexmoney}"
mkdir -p "${LOCAL_REPORT_DIR}"

quoted_args=""
for arg in "$@"; do
  quoted_args+=" $(printf '%q' "${arg}")"
done
remote_dir_quoted="$(printf '%q' "${REMOTE_DEXMONEY_DIR}")"

tmp_log="$(mktemp)"
trap 'rm -f "${tmp_log}"' EXIT

ssh "${REMOTE_SSH_TARGET}" "cd ${remote_dir_quoted} && ./ops/competitor_gap_report.sh --tar${quoted_args}" | tee "${tmp_log}"

remote_tgz="$(awk -F= '/^REPORT_TGZ=/ {print $2}' "${tmp_log}" | tail -n 1)"
if [[ -z "${remote_tgz}" ]]; then
  die "remote report did not print REPORT_TGZ"
fi
if [[ "${remote_tgz}" != /* ]]; then
  remote_tgz="${REMOTE_DEXMONEY_DIR%/}/${remote_tgz}"
fi

scp "${REMOTE_SSH_TARGET}:${remote_tgz}" "${LOCAL_REPORT_DIR}/" >/dev/null
local_tgz="${LOCAL_REPORT_DIR}/$(basename "${remote_tgz}")"

echo "LOCAL_REPORT_TGZ=${local_tgz}"
echo "To inspect:"
echo "  tar -tzf ${local_tgz} | head"
