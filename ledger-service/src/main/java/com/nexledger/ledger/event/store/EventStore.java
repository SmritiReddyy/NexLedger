package com.nexledger.ledger.event.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexledger.ledger.event.outbox.OutboxEvent;
import com.nexledger.ledger.event.outbox.OutboxRepository;
import com.nexledger.ledger.event.publisher.EventPublisher;
import io.r2dbc.postgresql.codec.Json;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Append-only event store with a transactional outbox.
 *
 * The event row and the outbox row announcing it commit in one Postgres
 * transaction, so an event is never stored without eventually reaching Kafka
 * (and never reaches Kafka without being stored). OutboxRelay publishes the
 * outbox rows; that is at-least-once, and the projections are idempotent on
 * event version.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class EventStore {

    private final EventRepository eventRepository;
    private final OutboxRepository outboxRepository;
    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper;
    private final TransactionalOperator transactionalOperator;

    public Mono<DomainEvent> appendEvent(UUID aggregateId, String aggregateType,
                                          String eventType, JsonNode payload,
                                          long expectedVersion) {
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            return Mono.error(e);
        }
        Mono<DomainEvent> append = eventRepository.findMaxVersionByAggregateId(aggregateId)
            .defaultIfEmpty(-1L)
            .flatMap(currentVersion -> {
                if (currentVersion != expectedVersion) {
                    return Mono.error(new OptimisticConcurrencyException(
                        "Concurrency conflict for aggregate " + aggregateId +
                        ": expected version " + expectedVersion + " but found " + currentVersion));
                }
                DomainEvent event = DomainEvent.of(aggregateId, aggregateType,
                    eventType, Json.of(payloadJson), expectedVersion + 1);
                return eventRepository.save(event);
            })
            .flatMap(saved -> outboxRepository.save(OutboxEvent.pending(
                    saved.aggregateId(), saved.eventType(),
                    eventPublisher.resolveTopic(saved.eventType()), eventPublisher.envelope(saved)))
                .thenReturn(saved));

        return transactionalOperator.transactional(append)
            // Two writers that both read version N race on (aggregate_id, version);
            // the loser's insert fails the unique constraint. Same meaning as a stale read.
            .onErrorMap(DataIntegrityViolationException.class, e -> isIdempotencyViolation(e)
                ? new DuplicateEventException(aggregateId, e)
                : new OptimisticConcurrencyException("Concurrency conflict for aggregate " + aggregateId))
            .doOnError(e -> log.debug("Append rejected for aggregate {}: {}", aggregateId, e.getMessage()));
    }

    private static boolean isIdempotencyViolation(DataIntegrityViolationException e) {
        return e.getMessage() != null && e.getMessage().contains("uq_events_idempotency_key");
    }

    public Flux<DomainEvent> loadEvents(UUID aggregateId) {
        return eventRepository.findByAggregateId(aggregateId);
    }

    public Flux<DomainEvent> loadEventsAfterVersion(UUID aggregateId, long version) {
        return eventRepository.findByAggregateIdAfterVersion(aggregateId, version);
    }
}
