DELETE FROM transactions old
USING transactions newer
WHERE old.tx_hash IS NOT NULL
  AND old.tx_hash = newer.tx_hash
  AND (
    old.created_at < newer.created_at
    OR (old.created_at = newer.created_at AND old.id::text < newer.id::text)
  );

CREATE UNIQUE INDEX IF NOT EXISTS transactions_tx_hash_unique_idx
    ON transactions (tx_hash)
    WHERE tx_hash IS NOT NULL;
