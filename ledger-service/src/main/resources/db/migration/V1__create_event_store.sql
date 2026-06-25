-- Event Store: immutable append-only log of all domain events
CREATE TABLE IF NOT EXISTS events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    UUID        NOT NULL,
    aggregate_type  VARCHAR(50) NOT NULL,
    event_type      VARCHAR(100) NOT NULL,
    payload         JSONB       NOT NULL,
    version         BIGINT      NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id  UUID,
    causation_id    UUID,

    CONSTRAINT uq_aggregate_version UNIQUE (aggregate_id, version)
);

CREATE INDEX idx_events_aggregate_id ON events(aggregate_id);
CREATE INDEX idx_events_event_type    ON events(event_type);
CREATE INDEX idx_events_created_at    ON events(created_at DESC);
CREATE INDEX idx_events_payload_gin   ON events USING GIN (payload);

-- Read model: account balances (projection)
CREATE TABLE IF NOT EXISTS account_projections (
    account_id      UUID        PRIMARY KEY,
    owner_id        VARCHAR(100) NOT NULL,
    currency        VARCHAR(3)  NOT NULL DEFAULT 'USD',
    balance         NUMERIC(19,4) NOT NULL DEFAULT 0,
    status          VARCHAR(20) NOT NULL DEFAULT 'ACTIVE',
    version         BIGINT      NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_account_owner ON account_projections(owner_id);

-- Read model: transaction history (projection)
CREATE TABLE IF NOT EXISTS transaction_projections (
    transaction_id  UUID        PRIMARY KEY,
    account_id      UUID        NOT NULL,
    type            VARCHAR(20) NOT NULL,
    amount          NUMERIC(19,4) NOT NULL,
    currency        VARCHAR(3)  NOT NULL DEFAULT 'USD',
    status          VARCHAR(20) NOT NULL,
    reference       VARCHAR(255),
    idempotency_key VARCHAR(255),
    fraud_score     NUMERIC(5,4),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (account_id) REFERENCES account_projections(account_id)
);

CREATE INDEX idx_tx_account_id   ON transaction_projections(account_id);
CREATE INDEX idx_tx_created_at   ON transaction_projections(created_at DESC);
CREATE INDEX idx_tx_idempotency  ON transaction_projections(idempotency_key);

-- Saga state tracker for compensating transactions
CREATE TABLE IF NOT EXISTS saga_state (
    saga_id         UUID        PRIMARY KEY,
    saga_type       VARCHAR(50) NOT NULL,
    state           VARCHAR(30) NOT NULL,
    payload         JSONB       NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_saga_state ON saga_state(state);
