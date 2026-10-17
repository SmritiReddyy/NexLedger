# Measured results

Everything here was measured on one machine: an Apple M3 laptop with 8 cores and 8 GB RAM, running Docker Desktop with the whole stack from `docker-compose.yml`:
- Postgres 16
- Redis 7
- Kafka 7.6 with ZooKeeper
- the fraud service (4 uvicorn workers)
- the ledger (JVM, `-Xmx1g`)

The load generator ran on the same laptop, so treat these as single-box numbers, not cluster capacity.

## Correctness

### Integration tests

13 tests, run with `mvn test` against real Postgres, Redis and Kafka via Testcontainers:

| Property | Test | Result |
|---|---|---|
| Replaying events to any past instant or version gives the balance at that moment | `pointInTimeReplayMatchesTheBalanceAtEveryCheckpoint` (60 random operations, 6 checkpoints) | exact at every checkpoint |
| Concurrent debits never overdraw | `concurrentDebitsOnOneAccountNeverOverdraw` (50 parallel debits of 30.00 against 1,000.00) | exactly 33 succeed, 17 rejected, final balance 10.00 |
| A retried request returns the original transaction | `retriedRequestReturnsTheOriginalTransaction` | same transaction id, one debit |
| Concurrent duplicates write once | `concurrentDuplicatesWriteExactlyOneEvent` (10 parallel copies) | exactly 1 event |
| Every event reaches the read model | `everyEventReachesKafkaThroughTheOutbox` | outbox drains, projection matches |
| A crash between debit and credit is finished, not lost or doubled | `crashAfterTheDebitIsFinishedByRecovery` | saga pauses at DEBITED, recovery completes it, total money conserved |
| A credit to a missing account is refunded | `creditToMissingAccountIsCompensated` | COMPENSATED, source restored |
| Insufficient funds never reaches compensation | `insufficientFundsFailsWithoutTouchingEitherAccount` | FAILED, neither account touched |

### After load

These checks ran directly against Postgres after all three load runs, including the overloaded one:

| Check | Result |
|---|---|
| Accounts / events | 4,521 / 39,061 |
| Accounts whose projection balance or version differs from a full replay | **0** |
| Accounts with a negative balance | **0** |
| Event streams with a version gap | **0** |
| Idempotency keys that produced more than one event | **0** |
| Outbox rows left unpublished | **0** |

## Throughput and latency (Gatling, `load-testing/`)

The traffic mix is:
- 120/s account reads
- 60/s debits
- 60/s credits
- 30/s replayed balances
- 25/s audit logs
- 5/s account creations

All of these are multiplied by `RATE_SCALE`. Each run has a 30 s ramp and then 120 s steady, against 500 seeded accounts.

Checks are strict: a debit passes only on 200, or 403 when the fraud check blocks it, and everything else must be 200.

| Run | Requests | Failed | Mean req/s (incl. ramp) | p50 | p95 | p99 |
|---|---|---|---|---|---|---|
| `RATE_SCALE=1` (peak ~300/s) | 40,590 | **0** | 269 | 2 ms | 11 ms | 67 ms |
| `RATE_SCALE=2` (peak ~600/s) | 81,090 | **0** | 537 | 2 ms | 32 ms | 295 ms |
| `RATE_SCALE=3` (peak ~900/s) | 121,590 | 57% | — | — | — | — |

**Where it breaks.** At scale 3 the ledger's Redis client timed out (about 15,000 commands over 2 s). Under contention, every queued writer polls `SET NX` for the per-account lock, and that polling saturates the single shared Redis connection. Most of the failed requests in that run were the load generator itself running out of sockets. Even so, the post-load checks above show nothing was lost, duplicated or overdrawn.

The next steps would be:
- wake waiting writers with Redis pub/sub instead of polling
- add a Lettuce connection pool
- add account snapshots, because each write folds the account's full event history, which gets more expensive the longer the history grows

## Fraud scoring latency (`fraud-service/bench/latency.py`)

`POST /score` on the Docker image (4 workers, Redis velocity lookups and writes), 5,000 requests per run, measured from the client:

| Concurrency | p50 | p95 | p99 | Throughput |
|---|---|---|---|---|
| 1 | **3.5 ms** | **6.2 ms** | 10.7 ms | 270 req/s |
| 8 | 4.8 ms | 16.5 ms | 27.8 ms | 1,166 req/s |

The fraud check sits on the debit path by design: a debit waits for a score, so a fraudulent one is blocked before its event is written. The ledger gives it 500 ms and fails open if the service is down.

The XGBoost model trains at image build time on 50,000 synthetic transactions. Its 1.00 test AUC only shows the synthetic classes are easy to separate. It is not a claim about real-world fraud detection.
