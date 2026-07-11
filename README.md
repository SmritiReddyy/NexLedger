# NexLedger

A production-grade payment ledger built to handle high-throughput transactions without breaking a sweat. Every dollar movement is an immutable event. Nothing is ever updated in place. The full history of every account is always one replay away.

```
Command → Event Store (Postgres) → Kafka → Read Projections → Redis Cache
                                         ↘ Fraud Sidecar (FastAPI + XGBoost)
```

---

## What makes it interesting

**No mutable state on the write path.** Debits, credits, and transfers all produce immutable events stored in Postgres. The account balance you query is a projection, rebuilt from those events by a Kafka consumer.

**Duplicate charges are impossible.** Every request carries an `Idempotency-Key`. Redis SETNX locks it before a single line of business logic runs. Retry as aggressively as you want — the ledger won't double-charge.

**Transfers are crash-safe.** A fund transfer is a saga: debit source, credit target. If the credit fails after the debit succeeds, a compensating event automatically reverses it. No half-transferred money, ever.

**Fraud scores every transaction in < 50ms.** A Python sidecar runs a rule engine (velocity checks, geo anomalies, amount thresholds) followed by an XGBoost model trained on 50K synthetic transactions. Score ≥ 0.75 blocks the transaction before the event is written.

---

## Stack

| | |
|---|---|
| Core service | Spring Boot 3.2 + WebFlux (reactive, non-blocking) |
| Event store | PostgreSQL 16 — JSONB events table, optimistic concurrency via version |
| Read models | PostgreSQL projections + Redis cache |
| Messaging | Kafka — decouples write and read sides |
| Fraud sidecar | FastAPI + XGBoost |
| Load testing | Gatling — ramps to ~1,800 RPS |
| Local infra | Docker Compose (one command) |

---

## Running it locally

```bash
# 1. Build the JAR
cd ledger-service && mvn clean package -DskipTests && cd ..

# 2. Start everything
docker compose up --build
```

Postgres, Redis, Kafka, the fraud service, and the ledger service all come up together. Flyway runs migrations automatically. The fraud service trains the XGBoost model on first boot (~30s).

Health checks:
```bash
curl http://localhost:8080/actuator/health
curl http://localhost:8090/health
```

---

## Docs

- [API Reference](docs/api.md) — every endpoint with example curl commands
- [Architecture](docs/architecture.md) — CQRS, event sourcing, saga pattern, fraud scoring deep dive
- [Load Testing](docs/load-testing.md) — how to run Gatling and interpret the results
