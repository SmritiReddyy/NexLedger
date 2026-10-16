package com.nexledger.ledger.event.store;

import java.util.UUID;

/** The database refused a second event for an idempotency key that already produced one. */
public class DuplicateEventException extends RuntimeException {
    public DuplicateEventException(UUID aggregateId, Throwable cause) {
        super("An event with this idempotency key already exists for aggregate " + aggregateId, cause);
    }
}
