-- Add chain_id to contracts
ALTER TABLE contracts ADD COLUMN IF NOT EXISTS chain_id INTEGER DEFAULT 1 NOT NULL;

-- Add last_finalized to checkpoints
ALTER TABLE checkpoints ADD COLUMN IF NOT EXISTS last_finalized BIGINT;

-- Create canonical_blocks table with composite PK
CREATE TABLE IF NOT EXISTS canonical_blocks (
    chain_id INTEGER NOT NULL DEFAULT 1,
    number BIGINT NOT NULL,
    hash VARCHAR(66) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chain_id, number)
);

-- Add chain_id to events table
ALTER TABLE events ADD COLUMN IF NOT EXISTS chain_id INTEGER DEFAULT 1 NOT NULL;

-- Update EventRecord unique constraint to include log_index and chain_id
DROP INDEX IF EXISTS ix_event_contract_block_tx;
CREATE UNIQUE INDEX ix_event_identity
    ON events (chain_id, contract_id, block_number, transaction_hash, log_index);

-- Make checkpoint.contract_id unique (one checkpoint per contract)
DROP INDEX IF EXISTS ix_checkpoint_contract_block;
CREATE UNIQUE INDEX ix_checkpoint_contract
    ON checkpoints (contract_id);
CREATE INDEX ix_checkpoint_last_block
    ON checkpoints (contract_id, last_block);
