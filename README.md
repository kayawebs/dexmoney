# Base Arb Bot MVP

Rust workspace for a Base-chain DEX arbitrage MVP focused on pipeline correctness:

- real-time path: Base node -> Redis / memory -> quoter -> candidate -> execution-manager -> Executor contract
- record path: events / quotes / simulations / tx results -> Postgres

## Scope

- own funds only, no flashloan
- `USDC -> WETH -> USDC` two-hop arbitrage
- DEXes: Aerodrome + Uniswap V3
- quoting with local math plus `eth_call` validation
- initial capital target: `100~500 USDC`

## Workspace

```text
base-arb-bot/
  Cargo.toml
  .env.example
  README.md
  migrations/
  docs/
  crates/
    common/
    chain/
    storage/
    dex/
    recorder/
    market-data/
    searcher/
    execution-manager/
  contracts/
```

## Crates

- `common`: config, shared types, constants, errors
- `chain`: provider and contract-facing chain utilities skeleton
- `storage`: Postgres / Redis access skeleton and schema notes
- `dex`: DEX adapters, pool state models, quoters
- `recorder`: replay/debug record models
- `market-data`: RPC log poller and pool state updater
- `searcher`: opportunity generation and risk gate skeleton
- `execution-manager`: candidate execution, simulation, lane state skeleton
- `monitor-web`: lightweight Postgres dashboard and pool registry admin

## Quick Start

1. Copy `.env.example` to `.env`.
2. Start local Postgres and Redis:
   - Base node with HTTP + WS
   - Postgres on `localhost:5632`
   - Redis on `localhost:6779`

```bash
cp .env.example .env
docker compose up -d postgres redis
```

3. Build:

```bash
cargo build
```

4. Run each process in its own terminal:

```bash
cargo run -p market-data
cargo run -p searcher
cargo run -p execution-manager
cargo run -p monitor-web
```

5. Optional health checks:

```bash
docker compose ps
cargo test --workspace
curl -sS http://127.0.0.1:8085/healthz
```

6. Deploy and initialize the Executor contract when you are ready to run `eth_call` simulation:

```bash
set -a
source .env
set +a

forge script contracts/script/DeployExecutor.s.sol:DeployExecutor \
  --root contracts \
  --rpc-url "$BASE_RPC_HTTP" \
  --broadcast
```

After deployment, set `EXECUTOR_CONTRACT` in `.env` to the deployed address. The
deployment script configures `EXECUTOR_OPERATOR`, USDC/WETH, configured routers,
factories, and pool whitelist slots. It also grants max token approvals from the
Executor to the configured Aerodrome and Uniswap V3 routers.

7. Open the monitor:

```bash
open http://127.0.0.1:8085
```

The monitor includes a protected form for adding token pairs. Set `MONITOR_WEB_PASSWORD`
in `.env`, then submit two token addresses. The app discovers Aerodrome Classic,
Aerodrome Slipstream, and Uniswap V3 pools, writes them into `pools`, and
`market-data` reloads enabled pools from Postgres every 30 seconds.

## Docker

Build a specific process image with `APP_BIN`:

```bash
docker build --build-arg APP_BIN=market-data -t base-arb-market-data .
docker build --build-arg APP_BIN=searcher -t base-arb-searcher .
docker build --build-arg APP_BIN=execution-manager -t base-arb-execution-manager .
```

The compose file in this repo currently provisions only Postgres and Redis. The Base node is expected to run separately.

## Notes

- The current `.env.example` is prefilled so the current scaffold can boot without manual editing.
- `AERODROME_USDC_WETH_POOL` is still supported as a fallback bootstrap address when the `pools` registry is empty.
- `monitor-web` runs on `http://127.0.0.1:8085`.

## Current Status

This initialization provides:

- workspace and crate structure
- base shared types
- Aerodrome volatile pool quote math
- searcher / execution skeletons
- Postgres migration and Redis key conventions
- token pair and pool registry with web-driven discovery
- Executor contract with own-funds two-hop simulation support for Aerodrome Classic and Uniswap V3
- execution-manager `eth_call` calldata for the Executor

It does not yet submit real transactions or execute Aerodrome Slipstream swaps in the Executor.
