package com.nexledger.ledger.saga;

import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Mono;

import java.util.UUID;

public interface SagaStateRepository extends ReactiveCrudRepository<SagaState, UUID> {

    @Modifying
    @Query("UPDATE saga_state SET state = :state, updated_at = NOW() WHERE saga_id = :sagaId")
    Mono<Integer> updateState(UUID sagaId, String state);
}
