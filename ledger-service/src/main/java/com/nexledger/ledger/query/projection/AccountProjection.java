package com.nexledger.ledger.query.projection;

import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Table("account_projections")
public class AccountProjection implements Persistable<UUID> {
    @Id
    private UUID accountId;
    @Transient
    private boolean isNew = false;
    private String ownerId;
    private String currency;
    private BigDecimal balance;
    private String status;
    private long version;
    private Instant createdAt;
    private Instant updatedAt;

    public AccountProjection() {}

    public AccountProjection(UUID accountId, String ownerId, String currency,
                              BigDecimal balance, String status, long version) {
        this.accountId = accountId;
        this.ownerId = ownerId;
        this.currency = currency;
        this.balance = balance;
        this.status = status;
        this.version = version;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
        this.isNew = true;
    }

    @Override public UUID getId() { return accountId; }
    @Override public boolean isNew() { return isNew; }

    public UUID getAccountId() { return accountId; }
    public String getOwnerId() { return ownerId; }
    public String getCurrency() { return currency; }
    public BigDecimal getBalance() { return balance; }
    public String getStatus() { return status; }
    public long getVersion() { return version; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }

    public void setBalance(BigDecimal balance) { this.balance = balance; }
    public void setVersion(long version) { this.version = version; }
    public void setUpdatedAt(Instant updatedAt) { this.updatedAt = updatedAt; }
    public void setStatus(String status) { this.status = status; }
}
