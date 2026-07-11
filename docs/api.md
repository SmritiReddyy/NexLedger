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

**Get account**
```bash
curl /accounts/{accountId}
```

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

Transfers are executed as a saga — debit + credit in sequence, with automatic rollback if either step fails.

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
| `409 Conflict` | Duplicate idempotency key |
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
