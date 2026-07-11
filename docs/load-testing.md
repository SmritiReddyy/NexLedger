# Load Testing

Gatling simulations are in `load-testing/`. There are two:

- **`WarmupSimulation`** — creates 500 seed accounts and writes their IDs to `account_ids.csv`
- **`LedgerSimulation`** — the main load test, reads from that CSV to drive realistic traffic

---

## Running

```bash
# Step 1: seed accounts
cd load-testing
mvn gatling:test -Dgatling.simulationClass=com.nexledger.WarmupSimulation

# Step 2: run the load test
mvn gatling:test -Dgatling.simulationClass=com.nexledger.LedgerSimulation
```

The HTML report is written to `target/gatling/*/index.html`.

---

## What the simulation does

Ramps five concurrent user populations:

| Scenario | Peak RPS |
|----------|----------|
| Create Account | 50 |
| Debit | 500 |
| Credit | 400 |
| Get Account | 800 |
| Audit Log | 100 |

Total sustained: ~1,850 RPS.

---

## Assertions

The test fails if:

- p95 latency exceeds **200ms**
- Success rate drops below **95%**
- Throughput falls below **1,000 req/sec**
