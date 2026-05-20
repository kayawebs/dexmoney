CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS token_pairs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    chain_id BIGINT NOT NULL,
    token0 TEXT NOT NULL,
    token1 TEXT NOT NULL,
    symbol TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, token0, token1)
);

CREATE INDEX IF NOT EXISTS token_pairs_enabled_idx
    ON token_pairs (enabled, updated_at DESC);

CREATE TABLE IF NOT EXISTS pools (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    token_pair_id UUID REFERENCES token_pairs(id),
    chain_id BIGINT NOT NULL,
    pool_address TEXT NOT NULL,
    dex TEXT NOT NULL,
    variant TEXT NOT NULL,
    token0 TEXT NOT NULL,
    token1 TEXT NOT NULL,
    fee_bps BIGINT,
    tick_spacing BIGINT,
    stable BOOLEAN,
    factory_address TEXT,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    source TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, pool_address)
);

CREATE INDEX IF NOT EXISTS pools_enabled_idx
    ON pools (enabled, updated_at DESC);
CREATE INDEX IF NOT EXISTS pools_pair_idx
    ON pools (token_pair_id, enabled);
