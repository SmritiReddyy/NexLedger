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

    @Modifying
    @Query("UPDATE account_projections SET balance = balance + :delta, version = :version, updated_at = NOW() WHERE account_id = :accountId")
    Mono<Integer> updateBalance(UUID accountId, BigDecimal delta, long version);

    @Modifying
    @Query("UPDATE account_projections SET status = :status, updated_at = NOW() WHERE account_id = :accountId")
    Mono<Integer> updateStatus(UUID accountId, String status);
}
