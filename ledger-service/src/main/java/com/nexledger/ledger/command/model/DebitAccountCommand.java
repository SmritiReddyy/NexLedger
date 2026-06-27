package com.nexledger.ledger.command.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.UUID;

public record DebitAccountCommand(
    @NotNull UUID accountId,
    @NotNull @Positive BigDecimal amount,
    @NotBlank String currency,
    @NotBlank String idempotencyKey,
    String reference
) {}
