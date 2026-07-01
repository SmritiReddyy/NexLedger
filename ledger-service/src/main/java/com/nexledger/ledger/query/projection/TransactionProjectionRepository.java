package com.nexledger.ledger.query.projection;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

import java.util.UUID;

public interface TransactionProjectionRepository extends ReactiveCrudRepository<TransactionProjection, UUID> {

    @Query("SELECT * FROM transaction_projections WHERE account_id = :accountId ORDER BY created_at DESC LIMIT :limit OFFSET :offset")
    Flux<TransactionProjection> findByAccountId(UUID accountId, int limit, int offset);

    @Query("SELECT * FROM transaction_projections WHERE idempotency_key = :key LIMIT 1")
    Flux<TransactionProjection> findByIdempotencyKey(String key);
}
