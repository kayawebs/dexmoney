CREATE TABLE IF NOT EXISTS token_search_defaults (
    chain_id BIGINT NOT NULL,
    token_address TEXT NOT NULL,
    search_amounts TEXT NOT NULL,
    min_profit TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (chain_id, token_address)
);
