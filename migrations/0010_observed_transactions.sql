CREATE TABLE IF NOT EXISTS observed_transactions (
    tx_hash TEXT PRIMARY KEY,
    block_number BIGINT NOT NULL,
    transaction_index BIGINT,
    from_address TEXT,
    to_address TEXT,
    nonce BIGINT,
    status BOOLEAN,
    gas_limit TEXT,
    gas_used TEXT,
    effective_gas_price TEXT,
    max_fee_per_gas TEXT,
    max_priority_fee_per_gas TEXT,
    tx_json JSONB NOT NULL,
    receipt_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS observed_transactions_block_idx
    ON observed_transactions (block_number, transaction_index);

CREATE INDEX IF NOT EXISTS observed_transactions_from_idx
    ON observed_transactions (lower(from_address));

CREATE INDEX IF NOT EXISTS observed_transactions_to_idx
    ON observed_transactions (lower(to_address));

CREATE TABLE IF NOT EXISTS observed_address_transfers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    seed_address TEXT NOT NULL,
    direction TEXT NOT NULL,
    tx_hash TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    log_index BIGINT NOT NULL,
    token_address TEXT NOT NULL,
    from_address TEXT NOT NULL,
    to_address TEXT NOT NULL,
    counterparty TEXT NOT NULL,
    amount TEXT NOT NULL,
    raw_log_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (seed_address, direction, tx_hash, log_index)
);

CREATE INDEX IF NOT EXISTS observed_address_transfers_seed_block_idx
    ON observed_address_transfers (lower(seed_address), block_number DESC);

CREATE INDEX IF NOT EXISTS observed_address_transfers_counterparty_idx
    ON observed_address_transfers (lower(counterparty));

CREATE INDEX IF NOT EXISTS observed_address_transfers_tx_idx
    ON observed_address_transfers (lower(tx_hash));
