ALTER TABLE token_pairs
    ADD COLUMN IF NOT EXISTS token0_search_amounts TEXT,
    ADD COLUMN IF NOT EXISTS token1_search_amounts TEXT,
    ADD COLUMN IF NOT EXISTS token0_min_profit TEXT,
    ADD COLUMN IF NOT EXISTS token1_min_profit TEXT;

