package com.nexledger.ledger.saga;

import io.r2dbc.postgresql.codec.Json;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Transient;
import org.springframework.data.domain.Persistable;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

@Table("saga_state")
public class SagaState implements Persistable<UUID> {
    @Id
    private UUID sagaId;
    @Transient
    private boolean isNew = false;
    private String sagaType;
    private String state;
    private Json payload;
    private Instant createdAt;
    private Instant updatedAt;

    public SagaState() {}

    public static SagaState started(UUID sagaId, String sagaType, String payloadJson) {
        SagaState s = new SagaState();
        s.sagaId = sagaId;
        s.sagaType = sagaType;
        s.state = TransferSaga.STARTED;
        s.payload = Json.of(payloadJson);
        s.createdAt = Instant.now();
        s.updatedAt = s.createdAt;
        s.isNew = true;
        return s;
    }

    @Override public UUID getId() { return sagaId; }
    @Override public boolean isNew() { return isNew; }

    public UUID getSagaId() { return sagaId; }
    public String getSagaType() { return sagaType; }
    public String getState() { return state; }
    public Json getPayload() { return payload; }
    public Instant getCreatedAt() { return createdAt; }
    public Instant getUpdatedAt() { return updatedAt; }
}
