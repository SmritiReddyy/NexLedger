# NexLedger

An event-sourced payment ledger. Every money movement is an immutable event, nothing is updated in place, and any account's balance can be replayed to any past moment.

```
Command ──► per-account Redis lock ──► fold account from its events ──► validate ──► append event + outbox row (one Postgres tx)
                                                                                         │
                         Fraud sidecar (FastAPI + XGBoost) ◄── debits only               ▼
                                                                         Outbox relay ──► Kafka ──► idempotent projections ──► Redis cache
```

---

## How it works

**Writes validate against the event stream, not the read model.**
- Each command rebuilds the account by folding its own events.
- It checks the balance and appends with the folded version as the expected version.
- A concurrent writer that got in first makes the append fail the version check, and the step retries against fresh state.
- A balance check can never pass on stale data, which is why 50 parallel debits against one account produce exactly the right number of successes.

**Point-in-time replay.** `GET /accounts/{id}/balance?asOf=2026-10-15T18:00:00Z` (or `?version=N`) folds the stream up to that point. It reads the source of truth, not the cached projection, so it is the figure to use for audit.

**Concurrency and idempotency.**
- **Per-account lock:** a Redis lock (`SET NX PX` with a random token, released by a Lua compare-and-delete) serialises writes to an account across instances. The version check stays as the safety net if a lock lease ever expires.
- **Idempotent retries:** every request carries an `Idempotency-Key`. A retry returns the original transaction id. In-flight duplicates are rejected.
- **Database backstop:** a unique index on `(aggregate_id, payload->>'idempotencyKey')` means even losing Redis cannot produce a second debit.

**Transactional outbox.**
- An event and its Kafka message are written in the same Postgres transaction.
- A relay publishes outbox rows using `FOR UPDATE SKIP LOCKED`. Delivery is at-least-once.
- Only one relay publishes at a time across replicas (a Postgres advisory lock), so each account's events reach Kafka in version order.
- Projections apply each event version once, so redelivery and replays are harmless.

**Crash-safe transfers.**
- A transfer is a persisted saga: STARTED → DEBITED → COMPLETED, or FAILED, or COMPENSATED.
- Each state is written with a compare-and-set before the next step runs, and each step's idempotency key is derived from the saga id.
- An infrastructure failure (crash, timeout, broker down) leaves the saga where it was, and a recovery worker resumes it.
- A refund only ever runs from DEBITED, so money that was never debited is never refunded.

**Fraud scoring on the debit path.**
- A FastAPI sidecar runs velocity, geo and amount rules, then an XGBoost model.
- A debit waits for the score, with a 500 ms timeout that fails open, so a blocked transaction never writes an event.
- Measured scoring latency is p50 3.5 ms and p95 6.2 ms.

## Measured

Full method and conditions: [docs/results.md](docs/results.md). Single M3 laptop, entire stack in Docker.

| | |
|---|---|
| Integration tests (real Postgres, Redis, Kafka via Testcontainers) | **14/14 passing**: replay at 6 checkpoints, 50-way concurrent debits (exactly 33 succeed, balance 10.00), duplicate requests, outbox delivery, two racing outbox relays keep events in order, crash between debit and credit recovered |
| Sustained mixed load, strict checks | **537 req/s, 0 failures over 81,090 requests**, p95 32 ms. At 300 req/s: p95 11 ms. |
| Consistency after all load runs | 4,521 accounts, 39,061 events: **0** projection/replay mismatches, **0** negative balances, **0** duplicate idempotency keys, outbox fully drained |
| Fraud `/score` latency | **p50 3.5 ms, p95 6.2 ms** (1 client); p50 4.8 ms at 1,166 req/s |
| Breaking point | ~900 req/s peak: lock polling saturates the shared Redis connection (see results) |
| **Kubernetes**, 2 ledger replicas ([deploy/k8s](deploy/k8s/README.md)) | Rolling restarts under load: **0 failed requests** in 3 runs, after adding preStop and graceful shutdown (3–9 per restart before). JVM `SIGKILL` mid-load: the 15–20 requests in flight resolve exactly once on retry, and the event store matches every balance. |

---

## Stack

| | |
|---|---|
| Core service | Java 21, Spring Boot 3.2 + WebFlux, R2DBC |
| Event store | PostgreSQL 16: JSONB events, optimistic concurrency on version, transactional outbox, Flyway |
| Concurrency | Redis per-account locks (Lua release), Redis idempotency results |
| Read models | PostgreSQL projections + Redis cache |
| Messaging | Kafka, keyed by account for per-account ordering |
| Fraud sidecar | FastAPI + XGBoost |
| Tests | JUnit 5, Testcontainers |
| Load testing | Gatling |
| Local infra | Docker Compose (one command), or Kubernetes with Kustomize ([deploy/k8s](deploy/k8s/README.md)) |

---

## Running it locally

```bash
# 1. Build the JAR (JDK 21 or 22)
cd ledger-service && mvn clean package -DskipTests && cd ..

# 2. Start everything
docker compose up --build

# 3. Tests (needs Docker running)
cd ledger-service && mvn test
```

Postgres, Redis, Kafka, the fraud service and the ledger all come up together. Flyway runs the migrations, and the fraud model is trained when the image is built.

```bash
curl http://localhost:8080/actuator/health
curl http://localhost:8090/health
```

---

## Docs

- [API Reference](docs/api.md): every endpoint with example curl commands
- [Architecture](docs/architecture.md): CQRS, event sourcing, saga pattern, fraud scoring
- [Load Testing](docs/load-testing.md): how to run Gatling
- [Results](docs/results.md): what was measured and how
- [Kubernetes](deploy/k8s/README.md): manifests, rollout and crash tests, and two bugs that only two replicas showed
