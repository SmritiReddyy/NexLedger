# NexLedger

**High-throughput distributed payment ledger with ACID guarantees, CQRS + Event Sourcing, and real-time fraud detection.**

## Architecture

```
REST API ──► Spring WebFlux ──► Command Handlers ──► PostgreSQL Event Store
                                                  └──► Kafka ──► Read-side Projector
                                                                └──► account_projections (PG + Redis cache)

Fraud Service (FastAPI + XGBoost) ── scores every debit/transfer in < 50ms
```

See [docs/architecture.md](docs/architecture.md) for the full diagram and design explanation.

## Features

- **Event Sourcing** — full immutable audit trail; any account state can be reconstructed by replaying events
- **CQRS** — write side (commands) and read side (queries) are independently scalable
- **Optimistic Concurrency** — `version` column enforces consistency without distributed locks on the write path
- **Idempotency** — Redis SETNX prevents duplicate charges under retries; `Idempotency-Key` header required for all mutating endpoints
- **Saga / Compensating Transactions** — multi-step `TransferFunds` uses the saga pattern; failed credit automatically debits back the source account
- **Fraud Scoring Sidecar** — rule engine (velocity, amount threshold, geo anomaly) + XGBoost model; blocks score ≥ 0.75
- **Reactive I/O** — Spring WebFlux + R2DBC + reactor-kafka; no blocking threads under load

## Project Structure

```
NexLedger/
├── ledger-service/          # Spring Boot + WebFlux core service
│   ├── src/main/java/com/nexledger/ledger/
│   │   ├── command/         # Command models + handlers (write side)
│   │   ├── event/           # Event store + Kafka publisher
│   │   ├── query/           # Kafka consumer projections + Redis cache (read side)
│   │   ├── saga/            # Transfer saga + compensating transactions
│   │   ├── fraud/           # Fraud service HTTP client
│   │   ├── idempotency/     # Redis SETNX idempotency service
│   │   ├── api/             # REST controllers + DTOs + exception handler
│   │   └── config/          # Kafka, Redis, Jackson config
│   └── src/main/resources/
│       └── db/migration/    # Flyway SQL migrations
├── fraud-service/           # FastAPI Python fraud scoring sidecar
│   ├── app/
│   │   ├── main.py          # FastAPI app + /score endpoint
│   │   ├── rule_engine.py   # Deterministic fraud rules
│   │   ├── ml_scorer.py     # XGBoost model + heuristic fallback
│   │   └── velocity_tracker.py  # Redis-backed velocity tracking
│   └── data/
│       └── generate_training_data.py  # Synthetic data + model training
├── infra/                   # Infrastructure config
│   └── postgres/init.sql
├── load-testing/            # Gatling simulation
│   └── src/test/scala/com/nexledger/LedgerSimulation.scala
├── docs/
│   └── architecture.md
└── docker-compose.yml
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Java 21 (to build the ledger service JAR)
- Maven 3.9+

### 1. Build the ledger service JAR

```bash
cd ledger-service
mvn clean package -DskipTests
cd ..
```

### 2. Start all services

```bash
docker compose up --build
```

This starts:
- PostgreSQL 16 (port 5432)
- Redis 7 (port 6379)
- Kafka + Zookeeper (port 9092)
- Fraud Service — FastAPI (port 8090) — trains XGBoost model on first start
- Ledger Service — Spring WebFlux (port 8080) — runs Flyway migrations

Wait ~60s for all health checks to pass.

### 3. Verify

```bash
# Health check
curl http://localhost:8080/actuator/health

# Fraud service health
curl http://localhost:8090/health
```

## API Reference

### Create Account
```bash
curl -X POST http://localhost:8080/api/v1/accounts \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"ownerId":"alice","currency":"USD","initialBalance":10000.00}'
```

### Debit Account
```bash
curl -X POST http://localhost:8080/api/v1/accounts/{accountId}/debit \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"amount":250.00,"currency":"USD","reference":"PAYMENT-001"}'
```

### Credit Account
```bash
curl -X POST http://localhost:8080/api/v1/accounts/{accountId}/credit \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"amount":500.00,"currency":"USD","reference":"REFUND-001"}'
```

### Transfer Funds (Saga)
```bash
curl -X POST http://localhost:8080/api/v1/transfers \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"sourceAccountId":"...","targetAccountId":"...","amount":1000.00,"currency":"USD"}'
```

### Audit Log (event replay)
```bash
curl http://localhost:8080/api/v1/audit-log/{accountId}
```

### Transaction History
```bash
curl http://localhost:8080/api/v1/transactions/{accountId}?limit=50&offset=0
```

## Load Testing (Gatling)

```bash
# Pre-seed account IDs into load-testing/src/test/scala/account_ids.csv
# Then run:
cd load-testing
mvn gatling:test
```

The simulation ramps to ~1,800 RPS across all endpoints. HTML report generated at `target/gatling/*/index.html`.

**Target assertions:**
- p95 latency < 200ms
- Success rate > 95%
- Throughput > 1,000 req/sec

## Fraud Service

Score a transaction directly:
```bash
curl -X POST http://localhost:8090/score \
  -H "Content-Type: application/json" \
  -d '{
    "account_id": "test-123",
    "amount": 9999.99,
    "currency": "USD",
    "transaction_type": "DEBIT",
    "idempotency_key": "test-idem-1"
  }'
```

Response:
```json
{
  "score": 0.87,
  "decision": "BLOCK",
  "reason": "ML model threshold exceeded",
  "rule_triggered": "LARGE_AMOUNT"
}
```

## Event Sourcing — Key Concepts

**Every state change is an event:**
```json
{
  "aggregateId": "uuid",
  "aggregateType": "Account",
  "eventType": "AccountDebited",
  "payload": {
    "transactionId": "uuid",
    "accountId": "uuid",
    "amount": "250.0000",
    "currency": "USD",
    "fraudScore": 0.12
  },
  "version": 3,
  "createdAt": "2024-04-15T14:23:01Z"
}
```

Account balance is never stored on the write side — it's derived from projecting events on the read side.

## Idempotency

Every mutating request must include an `Idempotency-Key` header. The flow:

1. `SETNX idempotency:<key>` in Redis with TTL 24h
2. If key already exists → `409 Conflict` returned immediately
3. If acquired → process command, append event
4. On success → store result in Redis under the key

This guarantees exactly-once processing even under aggressive HTTP retries.
