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

## Write path

1. Take the per-account Redis lock (`SET lock:account:{id} <token> NX PX 5000`). It is released by a Lua compare-and-delete, so an expired holder can never release someone else's lock.
2. If this idempotency key already produced an event for the account, return that transaction. This makes retries after a crash safe.
3. Fold the account's events into its current state: balance, currency and version.
4. Validate the command (funds, fraud score) against that state.
5. In one Postgres transaction:
   - insert the event with `version = folded + 1`
   - insert an outbox row with the Kafka envelope

   A writer that lost the race fails `UNIQUE(aggregate_id, version)` and retries from step 3.
6. `OutboxRelay` claims pending outbox rows (`FOR UPDATE SKIP LOCKED`), publishes them keyed by account id, and marks them published. Delivery is at-least-once.
7. `EventProjectionConsumer` applies an event only if its version is newer than the projection's (`WHERE version < :version`). It commits the Kafka offset only after the projection is saved.

## Saga Pattern — Transfer Funds

```
POST /transfers (Idempotency-Key K)   saga id = UUIDv3("transfer:" + K)

STARTED ──debit (key saga:debit)──► DEBITED ──credit (key saga:credit)──► COMPLETED
   │                                    │
   │ insufficient funds / fraud /       │ target missing / closed
   │ source missing                     ▼
   ▼                         refund source (key saga:compensate) ──► COMPENSATED + SagaCompensated event
 FAILED

Any other error (crash, timeout, broker down): state is left as is.
SagaRecovery resumes STARTED/DEBITED sagas idle longer than nexledger.saga.stuck-after-ms.
```

- **Transitions** are compare-and-set (`UPDATE ... WHERE state = :from`), so a live run and the recovery worker can never both advance the same saga.
- **Retries** re-run steps with the same derived keys, so a resumed step returns the earlier result instead of moving money twice.
- **Refunds** only start from DEBITED.

## Fraud Service

Each debit/transfer is synchronously scored before the event is written:

1. **Rule Engine** — deterministic checks (amount threshold, velocity, geo)
2. **XGBoost model** — trained at image build time on 50K synthetic transactions (the synthetic classes are trivially separable, so test AUC is 1.00 and says nothing about real fraud)
3. **Final score** = max(rule_score, ml_score); blocked if ≥ 0.75

The fraud service is fail-open: if unavailable, the transaction proceeds with score 0.0.

## Technology Stack

| Layer | Technology |
|-------|-----------|
| Core API | Spring Boot 3.2 + WebFlux (reactive, non-blocking) |
| Event Store | PostgreSQL 16 (JSONB events table) |
| Read Model | PostgreSQL 16 (denormalized projections) |
| Caching + Locking | Redis 7 (read cache, per-account locks, idempotency results) |
| Messaging | Kafka 3.6 (event streaming) |
| Schema Migrations | Flyway |
| Fraud Scoring | FastAPI (Python) + XGBoost |
| Load Testing | Gatling 3.10 |
| Containerization | Docker Compose |
