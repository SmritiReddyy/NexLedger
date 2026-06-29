package com.nexledger.ledger.command.handler;

import java.math.BigDecimal;
import java.util.UUID;

public class InsufficientFundsException extends RuntimeException {
    public InsufficientFundsException(UUID accountId, BigDecimal balance, BigDecimal requested) {
        super("Insufficient funds in account " + accountId +
              ": balance=" + balance + ", requested=" + requested);
    }
}
