package com.nexledger.ledger.api.controller;

import com.nexledger.ledger.api.dto.CreateAccountRequest;
import com.nexledger.ledger.api.dto.DebitRequest;
import com.nexledger.ledger.api.dto.CreditRequest;
import com.nexledger.ledger.command.handler.AccountCommandHandler;
import com.nexledger.ledger.command.model.CreateAccountCommand;
import com.nexledger.ledger.command.model.CreditAccountCommand;
import com.nexledger.ledger.command.model.DebitAccountCommand;
import com.nexledger.ledger.query.cache.AccountCacheService;
import com.nexledger.ledger.query.projection.AccountProjection;
import com.nexledger.ledger.query.projection.AccountProjectionRepository;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/accounts")
@RequiredArgsConstructor
public class AccountController {

    private final AccountCommandHandler commandHandler;
    private final AccountCacheService cacheService;
    private final AccountProjectionRepository accountRepo;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<Map<String, String>> createAccount(@Valid @RequestBody CreateAccountRequest req) {
        CreateAccountCommand cmd = new CreateAccountCommand(
            req.ownerId(), req.currency(), req.initialBalance(), req.idempotencyKey());
        return commandHandler.handle(cmd)
            .map(id -> Map.of("accountId", id.toString(), "status", "CREATED"));
    }

    @GetMapping("/{accountId}")
    public Mono<ResponseEntity<AccountProjection>> getAccount(@PathVariable UUID accountId) {
        return cacheService.get(accountId)
            .map(ResponseEntity::ok)
            .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("/owner/{ownerId}")
    public Flux<AccountProjection> getAccountsByOwner(@PathVariable String ownerId) {
        return accountRepo.findByOwnerId(ownerId);
    }

    @PostMapping("/{accountId}/debit")
    public Mono<Map<String, String>> debit(
            @PathVariable UUID accountId,
            @Valid @RequestBody DebitRequest req,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {
        DebitAccountCommand cmd = new DebitAccountCommand(
            accountId, req.amount(), req.currency(), idempotencyKey, req.reference());
        return commandHandler.handle(cmd)
            .map(txId -> Map.of("transactionId", txId.toString(), "status", "DEBITED"));
    }

    @PostMapping("/{accountId}/credit")
    public Mono<Map<String, String>> credit(
            @PathVariable UUID accountId,
            @Valid @RequestBody CreditRequest req,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {
        CreditAccountCommand cmd = new CreditAccountCommand(
            accountId, req.amount(), req.currency(), idempotencyKey, req.reference());
        return commandHandler.handle(cmd)
            .map(txId -> Map.of("transactionId", txId.toString(), "status", "CREDITED"));
    }
}
