CREATE TABLE IF NOT EXISTS tokens (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    chain_id BIGINT NOT NULL,
    token_address TEXT NOT NULL,
    symbol TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, token_address)
);

CREATE INDEX IF NOT EXISTS tokens_enabled_idx
    ON tokens (chain_id, enabled, symbol);
