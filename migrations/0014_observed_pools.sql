CREATE TABLE IF NOT EXISTS observed_pools (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    chain_id BIGINT NOT NULL,
    pool_address TEXT NOT NULL,
    topic0 TEXT NOT NULL,
    family TEXT NOT NULL,
    token0 TEXT,
    token1 TEXT,
    symbol TEXT,
    factory_address TEXT,
    dex TEXT,
    variant TEXT,
    fee_bps BIGINT,
    fee_pips BIGINT,
    tick_spacing BIGINT,
    stable BOOLEAN,
    txs_30d BIGINT NOT NULL DEFAULT 0,
    logs_30d BIGINT NOT NULL DEFAULT 0,
    first_block BIGINT,
    latest_block BIGINT,
    import_status TEXT NOT NULL,
    import_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, pool_address)
);

CREATE INDEX IF NOT EXISTS observed_pools_status_idx
    ON observed_pools (chain_id, import_status, txs_30d DESC);

CREATE INDEX IF NOT EXISTS observed_pools_symbol_idx
    ON observed_pools (chain_id, symbol, txs_30d DESC);
