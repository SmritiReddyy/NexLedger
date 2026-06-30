package com.nexledger.ledger.saga;

import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

@Table("saga_state")
public class SagaState {
    @Id
    private UUID sagaId;
    private String sagaType;
    private String state;
    private String payload;
    private Instant createdAt;
    private Instant updatedAt;

    public SagaState(UUID sagaId, String sagaType, String state, String payload) {
        this.sagaId = sagaId;
        this.sagaType = sagaType;
        this.state = state;
        this.payload = payload;
        this.createdAt = Instant.now();
        this.updatedAt = Instant.now();
    }

    public UUID getSagaId() { return sagaId; }
    public String getSagaType() { return sagaType; }
    public String getState() { return state; }
    public String getPayload() { return payload; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
