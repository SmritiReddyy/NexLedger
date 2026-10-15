package com.nexledger.ledger.query.projection;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Table("transaction_projections")
public class TransactionProjection implements Persistable<UUID> {
    @Id
    private UUID transactionId;
    @Transient
    private boolean isNew = false;
    private UUID accountId;
    private String type;
    private BigDecimal amount;
    private String currency;
    private String status;
    private String reference;
    private String idempotencyKey;
    private BigDecimal fraudScore;
    private Instant createdAt;

    public TransactionProjection() {}

    public TransactionProjection(UUID transactionId, UUID accountId, String type,
                                  BigDecimal amount, String currency, String status,
                                  String reference, String idempotencyKey, BigDecimal fraudScore) {
        this.transactionId = transactionId;
        this.accountId = accountId;
        this.type = type;
        this.amount = amount;
        this.currency = currency;
        this.status = status;
        this.reference = reference;
        this.idempotencyKey = idempotencyKey;
        this.fraudScore = fraudScore;
        this.createdAt = Instant.now();
        this.isNew = true;
    }

    @Override public UUID getId() { return transactionId; }
    @Override public boolean isNew() { return isNew; }

    public UUID getTransactionId() { return transactionId; }
    public UUID getAccountId() { return accountId; }
    public String getType() { return type; }
    public BigDecimal getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public String getStatus() { return status; }
    public String getReference() { return reference; }
    public String getIdempotencyKey() { return idempotencyKey; }
    public BigDecimal getFraudScore() { return fraudScore; }
    public Instant getCreatedAt() { return createdAt; }
}
