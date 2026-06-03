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
