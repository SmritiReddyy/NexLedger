package com.nexledger.ledger.event.store;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

public interface EventRepository extends ReactiveCrudRepository<DomainEvent, Long> {

    @Query("SELECT * FROM events WHERE aggregate_id = :aggregateId ORDER BY version ASC")
    Flux<DomainEvent> findByAggregateId(UUID aggregateId);

    @Query("SELECT MAX(version) FROM events WHERE aggregate_id = :aggregateId")
    Mono<Long> findMaxVersionByAggregateId(UUID aggregateId);

    @Query("SELECT * FROM events WHERE aggregate_id = :aggregateId AND version > :afterVersion ORDER BY version ASC")
    Flux<DomainEvent> findByAggregateIdAfterVersion(UUID aggregateId, long afterVersion);

    /** The stream as it stood at a point in time, for point-in-time replay. */
    @Query("SELECT * FROM events WHERE aggregate_id = :aggregateId AND created_at <= :asOf ORDER BY version ASC")
    Flux<DomainEvent> findByAggregateIdUpTo(UUID aggregateId, Instant asOf);

    /** The stream up to and including a version. */
    @Query("SELECT * FROM events WHERE aggregate_id = :aggregateId AND version <= :version ORDER BY version ASC")
    Flux<DomainEvent> findByAggregateIdUpToVersion(UUID aggregateId, long version);

    /** The event a command with this idempotency key produced, if it already ran. */
    @Query("SELECT * FROM events WHERE aggregate_id = :aggregateId AND payload->>'idempotencyKey' = :idempotencyKey LIMIT 1")
    Mono<DomainEvent> findByIdempotencyKey(UUID aggregateId, String idempotencyKey);

    @Query("SELECT * FROM events WHERE event_type = :eventType ORDER BY created_at DESC LIMIT :limit")
    Flux<DomainEvent> findByEventType(String eventType, int limit);
}
