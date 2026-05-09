CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS dex_events (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    chain_id BIGINT NOT NULL,
    block_number BIGINT NOT NULL,
    tx_hash TEXT NOT NULL,
    log_index BIGINT NOT NULL,
    pool_address TEXT NOT NULL,
    dex TEXT NOT NULL,
    event_type TEXT NOT NULL,
    token0 TEXT,
    token1 TEXT,
    raw_data_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS dex_events_chain_tx_log_idx
    ON dex_events (chain_id, tx_hash, log_index);
CREATE INDEX IF NOT EXISTS dex_events_pool_block_idx
    ON dex_events (pool_address, block_number DESC);

CREATE TABLE IF NOT EXISTS pool_states (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pool_address TEXT NOT NULL,
    dex TEXT NOT NULL,
    token0 TEXT NOT NULL,
    token1 TEXT NOT NULL,
    fee BIGINT,
    reserve0 TEXT,
    reserve1 TEXT,
    sqrt_price_x96 TEXT,
    liquidity TEXT,
    tick BIGINT,
    block_number BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS pool_states_pool_updated_idx
    ON pool_states (pool_address, updated_at DESC);

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

CREATE TABLE IF NOT EXISTS opportunities (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    block_number BIGINT NOT NULL,
    strategy TEXT NOT NULL,
    token_in TEXT NOT NULL,
    amount_in TEXT NOT NULL,
    expected_amount_out TEXT NOT NULL,
    expected_profit TEXT NOT NULL,
    min_profit TEXT NOT NULL,
    path_json JSONB NOT NULL,
    status TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS opportunities_created_idx
    ON opportunities (created_at DESC);

CREATE TABLE IF NOT EXISTS simulations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    opportunity_id UUID NOT NULL REFERENCES opportunities(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    success BOOLEAN NOT NULL,
    simulated_profit TEXT,
    gas_estimate TEXT,
    revert_reason TEXT,
    calldata TEXT,
    raw_result JSONB
);

CREATE INDEX IF NOT EXISTS simulations_opportunity_idx
    ON simulations (opportunity_id, created_at DESC);

CREATE TABLE IF NOT EXISTS transactions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    opportunity_id UUID REFERENCES opportunities(id),
    simulation_id UUID REFERENCES simulations(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    eoa TEXT NOT NULL,
    tx_hash TEXT,
    nonce BIGINT NOT NULL,
    status TEXT NOT NULL,
    gas_used TEXT,
    effective_gas_price TEXT,
    realized_profit TEXT,
    revert_reason TEXT,
    receipt_json JSONB
);

CREATE INDEX IF NOT EXISTS transactions_eoa_created_idx
    ON transactions (eoa, created_at DESC);
