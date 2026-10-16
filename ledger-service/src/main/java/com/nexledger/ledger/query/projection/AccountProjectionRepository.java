package com.nexledger.ledger.query.projection;

import org.springframework.data.r2dbc.repository.Modifying;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.UUID;

public interface AccountProjectionRepository extends ReactiveCrudRepository<AccountProjection, UUID> {

    Flux<AccountProjection> findByOwnerId(String ownerId);

    /** Idempotent create: a redelivered AccountCreated is a no-op. */
    @Modifying
    @Query("INSERT INTO account_projections (account_id, owner_id, currency, balance, status, version) " +
           "VALUES (:accountId, :ownerId, :currency, :balance, 'ACTIVE', :version) ON CONFLICT (account_id) DO NOTHING")
    Mono<Integer> insertIfAbsent(UUID accountId, String ownerId, String currency, BigDecimal balance, long version);

    /**
     * Apply one balance change, only if this event is newer than what the
     * projection has seen. Redelivered or replayed events update 0 rows.
     */
    @Modifying
    @Query("UPDATE account_projections SET balance = balance + :delta, version = :version, updated_at = NOW() " +
           "WHERE account_id = :accountId AND version < :version")
    Mono<Integer> applyDelta(UUID accountId, BigDecimal delta, long version);

    @Modifying
    @Query("UPDATE account_projections SET status = :status, updated_at = NOW() WHERE account_id = :accountId")
    Mono<Integer> updateStatus(UUID accountId, String status);
}
