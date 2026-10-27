"""In-cluster load for measuring what a rollout or a pod failure costs clients.

Runs inside the cluster (see load-job.yaml), so requests go through the
ledger-service Service and are spread over the replicas the way real traffic is.

Each worker sends credits and debits of 1.00 against random accounts, one at a
time. A request "fails" if its first attempt gets a connection error, a timeout
or a 5xx. Every failed request is then retried with the same Idempotency-Key
until it resolves, so its real outcome is known: either it was applied once or
not at all, never twice. At the end the expected balance of every account is
compared with the event-store replay (/balance) and the read projection.

Standard library only. Prints one line: RESULT {json}.
"""
import json
import os
import random
import threading
import time
import urllib.error
import urllib.request
import uuid
from decimal import Decimal

TARGET = os.environ.get("TARGET", "http://ledger-service:8080/api/v1")
WORKERS = int(os.environ.get("WORKERS", "8"))
SECONDS = int(os.environ.get("SECONDS", "90"))
ACCOUNTS = int(os.environ.get("ACCOUNTS", "20"))
INITIAL = Decimal("100000.00")


def call(method, path, body=None, key=None, timeout=5):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(TARGET + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if key:
        req.add_header("Idempotency-Key", key)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception as e:  # connection refused/reset, timeout
        return type(e).__name__, None


def wait_ready():
    for _ in range(120):
        s, _ = call("GET", "/accounts/00000000-0000-0000-0000-000000000000", timeout=2)
        if s == 404:
            return
        time.sleep(1)
    raise SystemExit("ledger-service not reachable")


wait_ready()
accounts = []
for i in range(ACCOUNTS):
    key = str(uuid.uuid4())
    while True:
        s, b = call("POST", "/accounts", {"ownerId": f"load-{i}", "currency": "USD",
                                          "initialBalance": str(INITIAL), "idempotencyKey": key})
        if s == 201:
            accounts.append(b["accountId"])
            break
        time.sleep(0.5)

lock = threading.Lock()
applied = {a: Decimal(0) for a in accounts}  # net effect of every request that took effect
stats = {"requests": 0, "ok": 0, "blocked_by_fraud": 0, "insufficient": 0, "other_4xx": 0,
         "first_attempt_failures": 0, "failures_by_kind": {}, "unresolved": 0, "retries": 0}
per_second_failures = {}
latencies = []
start = time.time()
stop_at = start + SECONDS


def resolve(path, body, key):
    """Retry with the same key until the outcome is known; returns the final status."""
    deadline = time.time() + 60
    while time.time() < deadline:
        time.sleep(0.2)
        with lock:
            stats["retries"] += 1
        s, _ = call("POST", path, body, key)
        if s in (200, 403, 422):
            return s
    return None


def worker(seed):
    rng = random.Random(seed)
    while time.time() < stop_at:
        acct = rng.choice(accounts)
        credit = rng.random() < 0.5
        path = f"/accounts/{acct}/{'credit' if credit else 'debit'}"
        body = {"amount": "1.00", "currency": "USD", "reference": "load"}
        key = str(uuid.uuid4())
        t0 = time.time()
        s, _ = call("POST", path, body, key)
        dt = time.time() - t0
        failed = not (s in (200, 403, 422) or (isinstance(s, int) and 400 <= s < 500))
        if failed:
            with lock:
                stats["first_attempt_failures"] += 1
                kind = str(s)
                stats["failures_by_kind"][kind] = stats["failures_by_kind"].get(kind, 0) + 1
                sec = int(t0 - start)
                per_second_failures[sec] = per_second_failures.get(sec, 0) + 1
            s = resolve(path, body, key)
        with lock:
            stats["requests"] += 1
            if not failed:
                latencies.append(dt * 1000)
            if s == 200:
                stats["ok"] += 1
                applied[acct] += Decimal("1.00") if credit else Decimal("-1.00")
            elif s == 403:
                stats["blocked_by_fraud"] += 1
            elif s == 422:
                stats["insufficient"] += 1
            elif s is None:
                stats["unresolved"] += 1
            else:
                stats["other_4xx"] += 1


threads = [threading.Thread(target=worker, args=(i,)) for i in range(WORKERS)]
for t in threads:
    t.start()
for t in threads:
    t.join()
elapsed = time.time() - start

# Consistency. The event-store replay is the source of truth and must match
# exactly, immediately. The read projection is eventually consistent: poll it
# until it matches too, and report how long that took.
replay_mismatch = 0
for a in accounts:
    _, b = call("GET", f"/accounts/{a}/balance")
    if b is None or Decimal(str(b["balance"])) != INITIAL + applied[a]:
        replay_mismatch += 1


def projection_mismatches():
    n = 0
    for a in accounts:
        _, p = call("GET", f"/accounts/{a}")
        if p is None or Decimal(str(p["balance"])) != INITIAL + applied[a]:
            n += 1
    return n


load_end = time.time()
projection_mismatch = projection_mismatches()
mismatched_at_end = projection_mismatch
while projection_mismatch and time.time() - load_end < 180:
    time.sleep(1)
    projection_mismatch = projection_mismatches()
projection_converged_s = round(time.time() - load_end, 1) if mismatched_at_end else 0.0

latencies.sort()
pct = lambda q: round(latencies[min(len(latencies) - 1, int(q * len(latencies)))], 1) if latencies else None
stats.update({
    "seconds": round(elapsed, 1),
    "workers": WORKERS,
    "accounts": ACCOUNTS,
    "requests_per_sec": round(stats["requests"] / elapsed, 1),
    "latency_ms": {"p50": pct(0.5), "p99": pct(0.99)},
    "failures_per_second": dict(sorted(per_second_failures.items())),
    "replay_mismatches": replay_mismatch,
    "projection_mismatched_at_end": mismatched_at_end,
    "projection_converged_after_s": projection_converged_s,
    "projection_mismatches": projection_mismatch,
})
print("RESULT " + json.dumps(stats), flush=True)
