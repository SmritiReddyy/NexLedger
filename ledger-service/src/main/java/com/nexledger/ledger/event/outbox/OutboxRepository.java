package com.nexledger.ledger.event.outbox;

import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;

public interface OutboxRepository extends ReactiveCrudRepository<OutboxEvent, Long> {

    /**
     * Claim the oldest pending rows. SKIP LOCKED lets several relay instances
     * run without publishing the same row twice; must be called inside a transaction.
     */
    @Query("SELECT * FROM outbox_events WHERE status = 'PENDING' ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED")
    Flux<OutboxEvent> claimPending(int limit);

    /**
     * Transaction-scoped advisory lock that makes one relay, across all replicas,
     * the publisher at a time. Released when the transaction ends, including when
     * the replica holding it dies.
     */
    @Query("SELECT pg_try_advisory_xact_lock(:key)")
    Mono<Boolean> tryRelayLock(long key);

    @Modifying
    @Query("UPDATE outbox_events SET status = 'PUBLISHED', published_at = NOW() WHERE id IN (:ids)")
    Mono<Integer> markPublished(Collection<Long> ids);

    @Query("SELECT COUNT(*) FROM outbox_events WHERE status = 'PENDING'")
    Mono<Long> countPending();
}
