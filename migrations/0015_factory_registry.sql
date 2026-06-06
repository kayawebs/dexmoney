CREATE TABLE IF NOT EXISTS factory_registry (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    chain_id BIGINT NOT NULL,
    factory_address TEXT NOT NULL,
    dex TEXT NOT NULL,
    variant TEXT NOT NULL,
    trusted BOOLEAN NOT NULL DEFAULT FALSE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    source TEXT NOT NULL,
    notes TEXT,
    observed_pools BIGINT NOT NULL DEFAULT 0,
    first_seen_block BIGINT,
    latest_seen_block BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, factory_address)
);

CREATE INDEX IF NOT EXISTS factory_registry_trusted_idx
    ON factory_registry (chain_id, trusted, enabled, updated_at DESC);

CREATE INDEX IF NOT EXISTS factory_registry_observed_idx
    ON factory_registry (chain_id, observed_pools DESC, updated_at DESC);
