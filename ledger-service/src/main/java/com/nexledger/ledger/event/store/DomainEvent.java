package com.nexledger.ledger.event.store;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

@Table("events")
public record DomainEvent(
    @Id Long id,
    UUID aggregateId,
    String aggregateType,
    String eventType,
    JsonNode payload,
    long version,
    Instant createdAt,
    UUID correlationId,
    UUID causationId
) {
    public static DomainEvent of(UUID aggregateId, String aggregateType,
                                  String eventType, JsonNode payload, long version) {
        return new DomainEvent(null, aggregateId, aggregateType, eventType,
            payload, version, Instant.now(), UUID.randomUUID(), null);
    }
}
