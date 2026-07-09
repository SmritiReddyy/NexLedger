# NexLedger — Architecture

## Overview

NexLedger is a high-throughput distributed payment ledger built on **CQRS + Event Sourcing**. Every state change is an immutable event appended to a PostgreSQL event store. The write side handles commands; the read side projects events into optimized read models via Kafka.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         REST Clients / Gatling                       │
└──────────────────────────┬──────────────────────────────────────────┘
                           │ HTTP/1.1
           ┌───────────────▼────────────────┐
           │         ledger-service          │
           │       Spring WebFlux            │
           │                                 │
           │  ┌─────────────┐  ┌──────────┐ │
           │  │ Write Side  │  │Read Side │ │
           │  │             │  │          │ │
           │  │  Commands   │  │ Queries  │ │
           │  │  Handlers   │  │ Handlers │ │
           │  └──────┬──────┘  └────┬─────┘ │
           │         │              │        │
           │  ┌──────▼──────┐  ┌───▼──────┐ │
           │  │ Event Store │  │ Redis    │ │
           │  │ (PostgreSQL)│  │  Cache   │ │
           │  └──────┬──────┘  └──────────┘ │
           │         │                       │
           │  ┌──────▼──────┐               │
           │  │   Kafka     │               │
           │  │  Publisher  │               │
           │  └──────┬──────┘               │
           └─────────│───────────────────────┘
                     │
           ┌─────────▼──────────┐
           │      Kafka          │
           │  account-events     │
           │  transaction-events │
           │  saga-events        │
           └─────────┬──────────┘
                     │
           ┌─────────▼──────────┐
           │   Event Consumers   │
           │ (Read-side projector│
           │  in ledger-service) │
           └─────────┬──────────┘
                     │
           ┌─────────▼──────────┐
           │  Read Model (PG)   │
           │  account_projections│
           │  transaction_proj   │
           └────────────────────┘

           ┌────────────────────┐
           │  fraud-service      │
           │  FastAPI + XGBoost  │
           │  Rule engine        │
           │  Velocity (Redis)   │
           └────────────────────┘
```

## CQRS Pattern

### Write Side
- **Commands** are validated and routed to **Command Handlers**
- Each handler appends an **immutable event** to the PostgreSQL `events` table
- **Optimistic concurrency** via `version` column — concurrent writes to the same aggregate fail fast
- **Idempotency** — Redis `SETNX` ensures duplicate HTTP retries with the same `Idempotency-Key` header are rejected before any processing

### Read Side
- A Kafka consumer (`EventProjectionConsumer`) listens to all event topics
- Events are projected into denormalized `account_projections` and `transaction_projections` tables
- `AccountCacheService` maintains a 5-minute Redis cache of account state

## Event Store Schema

```sql
CREATE TABLE events (
    id              BIGSERIAL PRIMARY KEY,
    aggregate_id    UUID NOT NULL,
    aggregate_type  VARCHAR(50) NOT NULL,
    event_type      VARCHAR(100) NOT NULL,
    payload         JSONB NOT NULL,
    version         BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    correlation_id  UUID,
    causation_id    UUID,
    CONSTRAINT uq_aggregate_version UNIQUE (aggregate_id, version)
);
```

The `UNIQUE(aggregate_id, version)` constraint enforces optimistic locking at the DB level.

## Saga Pattern — Transfer Funds

```
TransferFunds Command
        │
        ▼
  1. Debit source account → AccountDebited event
        │  success
        ▼
  2. Credit target account → AccountCredited event
        │  success
        ▼
  3. Saga COMPLETED
        │
        ─── on credit failure ─────────────────────────►
                                            Compensate: Credit source back
                                            → SagaCompensated event
```

## Fraud Service

Each debit/transfer is synchronously scored before the event is written:

1. **Rule Engine** — deterministic checks (amount threshold, velocity, geo)
2. **XGBoost model** — trained on 50K synthetic transactions, AUC ~0.97
3. **Final score** = max(rule_score, ml_score); blocked if ≥ 0.75

The fraud service is fail-open: if unavailable, the transaction proceeds with score 0.0.

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Core API | Spring Boot 3.2 + WebFlux (reactive, non-blocking) |
| Event Store | PostgreSQL 16 (JSONB events table) |
| Read Model | PostgreSQL 16 (denormalized projections) |
| Caching + Locking | Redis 7 (read cache + SETNX idempotency) |
| Messaging | Kafka 3.6 (event streaming) |
| Schema Migrations | Flyway |
| Fraud Scoring | FastAPI (Python) + XGBoost |
| Load Testing | Gatling 3.10 |
| Containerization | Docker Compose |
