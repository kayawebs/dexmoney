#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.apps.yml"
ENV_FILE="${ROOT_DIR}/.env.docker"

usage() {
  cat <<'EOF'
Usage: ./restart.sh <all|market-data|searcher|execution-manager>

Restarts dexmoney app containers with docker compose rebuilds.

Targets:
  all                 Restart market-data, searcher, and execution-manager
  market-data         Restart market-data only
  searcher            Restart searcher only
  execution-manager   Restart execution-manager only
EOF
}

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 2
fi

target="$1"
profile_args=()
services=()

case "${target}" in
  all)
    profile_args=(--profile executor)
    services=(market-data searcher execution-manager)
    ;;
  market-data)
    services=(market-data)
    ;;
  searcher)
    services=(searcher)
    ;;
  execution-manager)
    profile_args=(--profile executor)
    services=(execution-manager)
    ;;
  -h|--help|help)
    usage
    exit 0
    ;;
  *)
    echo "unknown target: ${target}" >&2
    usage >&2
    exit 2
    ;;
esac

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "missing env file: ${ENV_FILE}" >&2
  exit 1
fi

cd "${ROOT_DIR}"
echo "restarting: ${services[*]}"
sudo docker compose \
  --env-file "${ENV_FILE}" \
  -f "${COMPOSE_FILE}" \
  "${profile_args[@]}" \
  up -d --build "${services[@]}"
