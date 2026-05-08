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
- `market-data`: WS listener and state updater skeleton
- `searcher`: opportunity generation and risk gate skeleton
- `execution-manager`: candidate execution, simulation, lane state skeleton

## Quick Start

1. Copy `.env.example` to `.env` and fill Base / DB / Redis / contract addresses.
2. Create the Postgres database and run `migrations/0001_init.sql`.
3. Start local services:
   - Base node with HTTP + WS
   - Postgres
   - Redis
4. Build:

```bash
cargo build
```

5. Run processes in separate terminals:

```bash
cargo run -p market-data
cargo run -p searcher
cargo run -p execution-manager
```

## Current Status

This initialization provides:

- workspace and crate structure
- base shared types
- Aerodrome volatile pool quote math
- searcher / execution skeletons
- Postgres migration and Redis key conventions
- Foundry contract skeleton and baseline tests

It does not yet provide production-ready chain integration, router calldata encoding, or full Uniswap V3 local tick simulation.

