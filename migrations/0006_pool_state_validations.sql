CREATE TABLE IF NOT EXISTS pool_state_validations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pool_address TEXT NOT NULL,
    dex TEXT NOT NULL,
    variant TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash TEXT NOT NULL,
    local_state_json JSONB NOT NULL,
    onchain_state_json JSONB NOT NULL,
    drift_bps BIGINT NOT NULL,
    passed BOOLEAN NOT NULL,
    message TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (pool_address, block_number)
);

CREATE INDEX IF NOT EXISTS pool_state_validations_created_idx
    ON pool_state_validations (created_at DESC);

CREATE INDEX IF NOT EXISTS pool_state_validations_pool_block_idx
    ON pool_state_validations (pool_address, block_number DESC);
