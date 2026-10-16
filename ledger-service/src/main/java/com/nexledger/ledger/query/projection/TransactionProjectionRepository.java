package com.nexledger.ledger.query.projection;

import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.UUID;

public interface TransactionProjectionRepository extends ReactiveCrudRepository<TransactionProjection, UUID> {

    @Query("SELECT * FROM transaction_projections WHERE account_id = :accountId ORDER BY created_at DESC LIMIT :limit OFFSET :offset")
    Flux<TransactionProjection> findByAccountId(UUID accountId, int limit, int offset);

    @Query("SELECT * FROM transaction_projections WHERE idempotency_key = :key LIMIT 1")
    Flux<TransactionProjection> findByIdempotencyKey(String key);

    /** Idempotent insert: a redelivered event does not duplicate the row. */
    @Modifying
    @Query("INSERT INTO transaction_projections (transaction_id, account_id, type, amount, currency, status, " +
           "reference, idempotency_key, fraud_score) VALUES (:transactionId, :accountId, :type, :amount, :currency, " +
           "'COMPLETED', :reference, :idempotencyKey, :fraudScore) ON CONFLICT (transaction_id) DO NOTHING")
    Mono<Integer> insertIfAbsent(UUID transactionId, UUID accountId, String type, BigDecimal amount, String currency,
                                 String reference, String idempotencyKey, BigDecimal fraudScore);
}
