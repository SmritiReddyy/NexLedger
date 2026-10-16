package com.nexledger.ledger.saga;

import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Extension point between saga steps. Production uses the no-op default; tests
 * replace it to fail the process at an exact point (e.g. after the debit
 * committed, before the credit) and prove recovery finishes the transfer.
 */
public interface SagaHooks {
    default Mono<Void> afterDebit(UUID sagaId) {
        return Mono.empty();
    }
}
