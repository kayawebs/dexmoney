CREATE INDEX IF NOT EXISTS dex_events_pool_block_type_idx
    ON dex_events (pool_address, block_number DESC, event_type);
