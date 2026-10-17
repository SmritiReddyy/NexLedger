"""Latency of POST /score, measured from a client.

Sends realistic scoring requests (random accounts and amounts, so the Redis
velocity lookups and writes are real) at a fixed concurrency and reports
p50/p95/p99 and throughput. Run against the service as deployed (Docker image,
4 uvicorn workers, Redis alongside), not a dev server.

usage: python bench/latency.py [base_url] [requests] [concurrency]
"""
import asyncio
import json
import os
import random
import statistics
import sys
import time
import uuid

import httpx

BASE = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:18090"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 5000
CONCURRENCY = int(sys.argv[3]) if len(sys.argv) > 3 else 8
ACCOUNTS = [str(uuid.uuid4()) for _ in range(500)]


def body():
    return {
        "account_id": random.choice(ACCOUNTS),
        "amount": round(random.lognormvariate(5.5, 1.2), 2),
        "currency": "USD",
        "transaction_type": "DEBIT",
        "idempotency_key": str(uuid.uuid4()),
    }


async def worker(client, n, latencies, decisions):
    for _ in range(n):
        t0 = time.perf_counter()
        r = await client.post("/score", json=body())
        latencies.append((time.perf_counter() - t0) * 1000)
        r.raise_for_status()
        decisions[r.json()["decision"]] = decisions.get(r.json()["decision"], 0) + 1


async def run(n, concurrency):
    latencies, decisions = [], {}
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(base_url=BASE, limits=limits, timeout=5) as client:
        await asyncio.gather(*(worker(client, 50, [], {}) for _ in range(concurrency)))   # warm-up
        t0 = time.perf_counter()
        await asyncio.gather(*(worker(client, n // concurrency, latencies, decisions) for _ in range(concurrency)))
        elapsed = time.perf_counter() - t0
    q = statistics.quantiles(latencies, n=100)
    return {
        "requests": len(latencies),
        "concurrency": concurrency,
        "p50_ms": round(q[49], 2),
        "p95_ms": round(q[94], 2),
        "p99_ms": round(q[98], 2),
        "max_ms": round(max(latencies), 2),
        "throughput_rps": round(len(latencies) / elapsed),
        "decisions": decisions,
    }


if __name__ == "__main__":
    results = [asyncio.run(run(N, c)) for c in (1, CONCURRENCY)]
    for r in results:
        print(f"concurrency {r['concurrency']:>2}: p50 {r['p50_ms']} ms  p95 {r['p95_ms']} ms  "
              f"p99 {r['p99_ms']} ms  max {r['max_ms']} ms  {r['throughput_rps']} req/s  {r['decisions']}")
    os.makedirs("bench/results", exist_ok=True)
    with open("bench/results/latency.json", "w") as f:
        json.dump({"service": BASE, "results": results}, f, indent=2)
