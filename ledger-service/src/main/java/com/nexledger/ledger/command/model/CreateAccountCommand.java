package com.nexledger.ledger.command.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public record CreateAccountCommand(
    @NotBlank String ownerId,
    @NotBlank String currency,
    @NotNull BigDecimal initialBalance,
    String idempotencyKey
) {}
