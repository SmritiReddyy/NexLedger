package com.nexledger.ledger.concurrency;

public class LockTimeoutException extends RuntimeException {
    public LockTimeoutException(String message) {
        super(message);
    }
}
