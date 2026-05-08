# Redis Key Design

## Realtime Pool State

- `pool:{chain_id}:{pool_address}`
  - value: serialized `PoolState`
  - writer: `market-data`
  - reader: `searcher`, `execution-manager`

## Candidate Priority Queue

- `candidates:priority`
  - type: sorted set or stream-backed queue, implementation choice left to runtime layer
  - score: priority derived from expected profit and freshness
  - member: serialized `Candidate`

## EOA Lane State

- `eoa:{address}:state`
  - value: serialized `EoaLaneState`
  - writer: `execution-manager`
  - reader: observability / failover tooling

## Failure Tracking

- `failures:{path_hash}`
  - value: recent failure counters or timestamps
  - writer: `execution-manager`
  - reader: `searcher` risk gate, operator tooling

## Design Notes

- Postgres is not on the decision critical path.
- Redis stores the latest state and queue primitives only.
- Replay/debug durability belongs to Postgres.

