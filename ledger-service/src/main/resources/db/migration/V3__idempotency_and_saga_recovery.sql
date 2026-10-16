-- A command's idempotency key can produce at most one event per aggregate.
-- This is the backstop if Redis loses the idempotency marker (eviction,
-- restart): the database itself refuses a second debit for the same key.
CREATE UNIQUE INDEX IF NOT EXISTS uq_events_idempotency_key
    ON events (aggregate_id, (payload->>'idempotencyKey'))
    WHERE payload ? 'idempotencyKey';

-- The recovery worker scans for sagas stuck in a non-terminal state.
CREATE INDEX IF NOT EXISTS idx_saga_state_updated ON saga_state (state, updated_at);

-- Outbox rows the relay has not published yet, oldest first.
CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox_events (id) WHERE status = 'PENDING';
