ALTER TABLE simulations
    ADD COLUMN IF NOT EXISTS block_number BIGINT,
    ADD COLUMN IF NOT EXISTS token_in TEXT,
    ADD COLUMN IF NOT EXISTS amount_in TEXT,
    ADD COLUMN IF NOT EXISTS expected_profit TEXT,
    ADD COLUMN IF NOT EXISTS min_profit TEXT,
    ADD COLUMN IF NOT EXISTS path_name TEXT,
    ADD COLUMN IF NOT EXISTS base_fee_per_gas TEXT,
    ADD COLUMN IF NOT EXISTS max_fee_per_gas TEXT,
    ADD COLUMN IF NOT EXISTS max_priority_fee_per_gas TEXT,
    ADD COLUMN IF NOT EXISTS gas_cost_cap TEXT,
    ADD COLUMN IF NOT EXISTS net_simulated_profit TEXT;

CREATE INDEX IF NOT EXISTS simulations_block_idx
    ON simulations (block_number DESC);

CREATE INDEX IF NOT EXISTS simulations_path_created_idx
    ON simulations (path_name, created_at DESC);
