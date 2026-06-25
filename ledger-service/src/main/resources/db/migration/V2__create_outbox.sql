-- Transactional outbox for reliable event publishing
CREATE TABLE IF NOT EXISTS outbox_events (
    id              BIGSERIAL   PRIMARY KEY,
    aggregate_id    UUID        NOT NULL,
    event_type      VARCHAR(100) NOT NULL,
    topic           VARCHAR(100) NOT NULL,
    payload         JSONB       NOT NULL,
    status          VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ
);

CREATE INDEX idx_outbox_status     ON outbox_events(status, created_at);
CREATE INDEX idx_outbox_aggregate  ON outbox_events(aggregate_id);
