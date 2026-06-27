package com.nexledger.ledger.command.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.UUID;

public record TransferFundsCommand(
    @NotNull UUID sourceAccountId,
    @NotNull UUID targetAccountId,
    @NotNull @Positive BigDecimal amount,
    @NotBlank String currency,
    @NotBlank String idempotencyKey,
    String reference
) {}
