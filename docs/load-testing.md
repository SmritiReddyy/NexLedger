# Load Testing

Gatling simulations are in `load-testing/`. There are two:

- **`WarmupSimulation`** creates 500 seed accounts and writes their IDs to `src/test/resources/account_ids.csv`.
- **`LedgerSimulation`** is the main load test. It reads that CSV to drive mixed traffic.

---

## Running

```bash
# with the stack up (docker compose up --build)
cd load-testing

# Step 1: seed accounts (re-run whenever the database is reset)
mvn gatling:test -Dgatling.simulationClass=com.nexledger.WarmupSimulation

# Step 2: run the load test; RATE_SCALE multiplies every arrival rate
RATE_SCALE=1.0 mvn gatling:test
RATE_SCALE=2.0 mvn gatling:test
```

The HTML report is written to `target/gatling/*/index.html`. `STEADY_SECONDS` (default 120) sets how long the peak rate is held after the 30 s ramp.

---

## What the simulation does

| Scenario | Rate at `RATE_SCALE=1` | Passes only on |
|----------|----------|----------|
| Get account (projection) | 120/s | 200 |
| Debit | 60/s | 200, or 403 (fraud block) |
| Credit | 60/s | 200 |
| Replayed balance | 30/s | 200 |
| Audit log | 25/s | 200 |
| Create account | 5/s | 201 |

That's about 300 req/s in total at scale 1.

Earlier versions of this test counted 409 Conflict as a successful debit. That hid a version bug in which most writes were being rejected. Conflicts, lock timeouts and 5xx responses now all count as failures. The run asserts a success rate above 99%.

Results are in [results.md](results.md).
