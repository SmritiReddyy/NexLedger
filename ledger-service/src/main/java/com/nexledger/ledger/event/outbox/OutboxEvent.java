package com.nexledger.ledger.event.outbox;

import io.r2dbc.postgresql.codec.Json;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.Instant;
import java.util.UUID;

/** A Kafka message written in the same transaction as the event it announces. */
@Table("outbox_events")
public record OutboxEvent(
    @Id Long id,
    UUID aggregateId,
    String eventType,
    String topic,
    Json payload,
    String status,
    Instant createdAt,
    Instant publishedAt
) {
    public static OutboxEvent pending(UUID aggregateId, String eventType, String topic, String envelopeJson) {
        return new OutboxEvent(null, aggregateId, eventType, topic, Json.of(envelopeJson), "PENDING", Instant.now(), null);
    }
}
