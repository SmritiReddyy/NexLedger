package com.nexledger.ledger.saga;

import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

public interface SagaStateRepository extends ReactiveCrudRepository<SagaState, UUID> {

    /**
     * Compare-and-set transition. Returns 1 only for the caller that moved the
     * saga out of `from`, so a live run and the recovery worker can never both
     * advance the same saga.
     */
    @Modifying
    @Query("UPDATE saga_state SET state = :to, updated_at = NOW() WHERE saga_id = :sagaId AND state = :from")
    Mono<Integer> transition(UUID sagaId, String from, String to);

    /** Refresh updated_at so a saga a worker is actively driving is not picked up as stuck. */
    @Modifying
    @Query("UPDATE saga_state SET updated_at = NOW() WHERE saga_id = :sagaId AND state = :state")
    Mono<Integer> touch(UUID sagaId, String state);

    @Query("SELECT * FROM saga_state WHERE state IN ('STARTED', 'DEBITED') AND updated_at < :olderThan ORDER BY updated_at LIMIT :limit")
    Flux<SagaState> findStuck(Instant olderThan, int limit);
}
