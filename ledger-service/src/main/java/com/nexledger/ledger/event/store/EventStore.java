package com.nexledger.ledger.event.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.nexledger.ledger.event.publisher.EventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventStore {

    private final EventRepository eventRepository;
    private final EventPublisher eventPublisher;

    @Transactional
    public Mono<DomainEvent> appendEvent(UUID aggregateId, String aggregateType,
                                          String eventType, JsonNode payload,
                                          long expectedVersion) {
        return eventRepository.findMaxVersionByAggregateId(aggregateId)
            .defaultIfEmpty(-1L)
            .flatMap(currentVersion -> {
                if (currentVersion != expectedVersion) {
                    return Mono.error(new OptimisticConcurrencyException(
                        "Concurrency conflict for aggregate " + aggregateId +
                        ": expected version " + expectedVersion + " but found " + currentVersion));
                }
                DomainEvent event = DomainEvent.of(aggregateId, aggregateType,
                    eventType, payload, expectedVersion + 1);
                return eventRepository.save(event);
            })
            .doOnSuccess(event -> eventPublisher.publish(event).subscribe())
            .doOnError(e -> log.error("Failed to append event for aggregate {}: {}", aggregateId, e.getMessage()));
    }

    public Flux<DomainEvent> loadEvents(UUID aggregateId) {
        return eventRepository.findByAggregateId(aggregateId);
    }

    public Flux<DomainEvent> loadEventsAfterVersion(UUID aggregateId, long version) {
        return eventRepository.findByAggregateIdAfterVersion(aggregateId, version);
    }
}
