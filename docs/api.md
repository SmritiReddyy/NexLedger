# API Reference

All mutating endpoints require an `Idempotency-Key` header. Use a UUID per request.

Base URL: `http://localhost:8080/api/v1`

---

### Accounts

**Create account**
```bash
curl -X POST /accounts \
  -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d '{"ownerId":"alice","currency":"USD","initialBalance":10000.00}'
```

**Get account** (read projection, eventually consistent)
```bash
curl /accounts/{accountId}
```

**Balance, rebuilt from the event store** (now, at an instant, or at a version)
```bash
curl /accounts/{accountId}/balance
curl "/accounts/{accountId}/balance?asOf=2026-10-15T18:00:00Z"
curl "/accounts/{accountId}/balance?version=12"
# {"accountId":"...","balance":742.50,"currency":"USD","version":12,"eventsApplied":13,"lastEventAt":"...","asOf":null}
```

Retrying a debit or credit with the same `Idempotency-Key` returns the original `transactionId`. Nothing is applied twice.

**Debit**
```bash
curl -X POST /accounts/{accountId}/debit \
  -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d '{"amount":250.00,"currency":"USD","reference":"PAYMENT-001"}'
```

**Credit**
```bash
curl -X POST /accounts/{accountId}/credit \
  -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d '{"amount":500.00,"currency":"USD","reference":"REFUND-001"}'
```

---

### Transfers

Transfers run as a persisted saga: debit, then credit. If the credit cannot succeed, the source is refunded. If the process dies mid-way, the recovery worker finishes the transfer. The response carries the saga state (`COMPLETED`, or `DEBITED` / `STARTED` if it paused on an infrastructure error and will be resumed). Reusing the `Idempotency-Key` returns the same saga.

```bash
curl -X POST /transfers \
  -H "Idempotency-Key: $(uuidgen)" \
  -H "Content-Type: application/json" \
  -d '{
    "sourceAccountId": "...",
    "targetAccountId": "...",
    "amount": 1000.00,
    "currency": "USD"
  }'
```

**Transfer status**
```bash
curl /transfers/{sagaId}
```

---

### Audit & History

**Full event log for an account** (replays the raw event stream)
```bash
curl /audit-log/{accountId}
```

**Transaction history** (from the read projection)
```bash
curl /transactions/{accountId}?limit=50&offset=0
```

---

### Error responses

All errors follow [RFC 7807](https://datatracker.ietf.org/doc/html/rfc7807):

| Status | Condition |
|--------|-----------|
| `409 Conflict` | Same idempotency key while the first request is still in flight |
| `503 Service Unavailable` | Account lock not acquired within 3 s (heavy contention); safe to retry |
| `403 Forbidden` | Transaction blocked by fraud detection |
| `422 Unprocessable Entity` | Insufficient funds |
| `404 Not Found` | Account does not exist |

---

### Fraud service

Score a transaction directly against the sidecar:

```bash
curl -X POST http://localhost:8090/score \
  -H "Content-Type: application/json" \
  -d '{
    "account_id": "test-123",
    "amount": 9999.99,
    "currency": "USD",
    "transaction_type": "DEBIT",
    "idempotency_key": "test-1"
  }'
```

```json
{
  "score": 0.87,
  "decision": "BLOCK",
  "reason": "ML model threshold exceeded",
  "rule_triggered": "LARGE_AMOUNT"
}
```
