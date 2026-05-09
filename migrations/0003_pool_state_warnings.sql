CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS pool_state_warnings (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pool_address TEXT NOT NULL,
    dex TEXT NOT NULL,
    variant TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    local_state_json JSONB NOT NULL,
    onchain_state_json JSONB NOT NULL,
    drift_bps BIGINT NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS pool_state_warnings_created_idx
    ON pool_state_warnings (created_at DESC);
CREATE INDEX IF NOT EXISTS pool_state_warnings_pool_created_idx
    ON pool_state_warnings (pool_address, created_at DESC);
