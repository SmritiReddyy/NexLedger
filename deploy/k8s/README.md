# NexLedger on Kubernetes

Plain manifests with Kustomize: Postgres and Kafka (KRaft, no ZooKeeper) as
StatefulSets, Redis, the fraud service, and **two replicas of the ledger
service** behind one Service. Tested on a single-node [kind](https://kind.sigs.k8s.io)
cluster on an M3 laptop.

```bash
kind create cluster --name nexledger
(cd ledger-service && mvn -q -DskipTests package)
docker build -t nexledger/ledger-service:local ledger-service
docker build -t nexledger/fraud-service:local fraud-service
kind load docker-image nexledger/ledger-service:local nexledger/fraud-service:local busybox:1.36 --name nexledger
kubectl apply -k deploy/k8s
kubectl -n nexledger wait --for=condition=available deploy --all --timeout=300s
```

From an empty namespace, every pod is ready in about 20 s with **0 restarts**.
The ledger's init container waits for Postgres, Redis and Kafka, so the app
starts once, not crash-looping until its dependencies are up.

## What was tested

`test/rollout_test.sh MODE` runs `test/rolling_load.py` as a Job inside the
cluster. Eight workers send credits and debits through the Service, spread over
both replicas, across 200 accounts for 90 s. 25 s in, the script disrupts the
ledger:

| Mode | Disruption |
|---|---|
| `baseline` | none |
| `rollout` | `kubectl rollout restart`: a normal deploy |
| `crash` | `SIGKILL` of one replica's JVM from the node: it dies mid-request and the container restarts |

A request **fails** if its first attempt gets a connection error, a timeout or
a 5xx. Every failed request is then retried with the same `Idempotency-Key`
until its real outcome is known. At the end, each account's expected balance
is compared with the event-store replay (`/balance`) and with the read
projection, which is polled until it converges.

## Results (`results/`)

| | Requests | Failed on first attempt | Unresolved | Replay mismatches | Projection mismatches |
|---|---|---|---|---|---|
| Baseline | 14,958 | 0 | 0 | 0 | 0 |
| Rolling restart, before the fix (3 runs) | 89,199 | **4, 3, 9** | 0 | 0 | 0 |
| Rolling restart, after the fix (3 runs) | 69,642 | **0, 0, 0** | 0 | 0 | 0 |
| JVM `SIGKILL` (2 runs, ~650 req/s) | 116,601 | 15, 20 | 0 | 0 | 0 |

- **Rolling deploys drop nothing.** Before the fix, each restart failed 3–9
  requests with connection errors in the second the old pod stopped. Endpoint
  removal is asynchronous, so new connections still reached a pod that was
  closing its sockets. The fix:
  - a 10 s `preStop` sleep, so the pod keeps serving while it is taken out of
    the Service;
  - Spring's graceful shutdown, so in-flight requests finish;
  - `maxUnavailable: 0`, so capacity never drops during the rollout.
- **A crash loses no money.** Killing a replica mid-request fails the requests
  it was holding (15–20 of about 58,000). Every one resolved exactly once on
  retry, and the event store matched the expected balance of every account to
  the cent.
- **The read model catches up.** It is fed through the outbox and Kafka, so it
  lags the write side. After a crash it matched all 200 accounts within 3 s of
  the load ending.

## Two bugs that only two replicas showed

On Docker Compose there is one ledger instance. Running two on Kubernetes
turned up two problems.

**The projection silently dropped events.** In the first baseline run, all 20
accounts disagreed with the event store at the same version: for example
100,711.00 against 100,720.00, at version 740.
- Each replica runs an outbox relay. `SELECT ... FOR UPDATE SKIP LOCKED` stops
  two relays publishing the same row, but not two relays publishing an
  account's consecutive events at the same time.
- So version N+1 could reach Kafka before N. The projection skips versions
  older than the one it holds, so N was lost for good.
- `twoRelayInstancesKeepEachAccountsEventsInOrder` reproduces it: 300 credits
  with two relays draining a backlog left the projection at **232.00** instead
  of 300.00.
- **Fix:** each relay run takes a transaction-scoped Postgres advisory lock, so
  one replica publishes at a time, in order. If that replica dies, the lock goes
  with its transaction and the other replica takes over on its next poll.

**The read model stalled after a crash.** With Kafka's default 45 s consumer
session timeout, a dead replica's partitions sat unconsumed until the
coordinator gave up on it. Lag climbed to about 10,000 events and only drained
after about 90 s (`results/crash_consumer_lag_session_timeout_45s.txt`). With a
10 s session timeout and 3 s heartbeats, the logs show the surviving replica
taking all partitions 10 s after the crash.

> `LAG=1` samples Kafka consumer lag during a test, but each sample starts a
> JVM inside the Kafka pod. On a laptop-sized cluster that halves throughput and
> stretches recovery, so the headline numbers above come from runs without it.
