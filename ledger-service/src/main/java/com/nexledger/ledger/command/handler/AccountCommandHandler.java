package com.nexledger.ledger.command.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexledger.ledger.command.model.CreateAccountCommand;
import com.nexledger.ledger.command.model.CreditAccountCommand;
import com.nexledger.ledger.command.model.DebitAccountCommand;
import com.nexledger.ledger.event.store.EventStore;
import com.nexledger.ledger.fraud.FraudClient;
import com.nexledger.ledger.idempotency.IdempotencyService;
import com.nexledger.ledger.query.projection.AccountProjection;
import com.nexledger.ledger.query.projection.AccountProjectionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountCommandHandler {

    private final EventStore eventStore;
    private final IdempotencyService idempotencyService;
    private final FraudClient fraudClient;
    private final AccountProjectionRepository accountProjectionRepository;
    private final ObjectMapper objectMapper;

    public Mono<UUID> handle(CreateAccountCommand cmd) {
        UUID accountId = UUID.randomUUID();
        ObjectNode payload = objectMapper.createObjectNode()
            .put("accountId", accountId.toString())
            .put("ownerId", cmd.ownerId())
            .put("currency", cmd.currency())
            .put("initialBalance", cmd.initialBalance().toPlainString());

        return eventStore.appendEvent(accountId, "Account", "AccountCreated", payload, -1)
            .map(event -> accountId)
            .doOnSuccess(id -> log.info("Account created: {}", id));
    }

    public Mono<UUID> handle(DebitAccountCommand cmd) {
        return idempotencyService.acquireLock(cmd.idempotencyKey())
            .flatMap(acquired -> {
                if (!acquired) {
                    return Mono.error(new DuplicateRequestException(
                        "Duplicate debit request: " + cmd.idempotencyKey()));
                }
                return accountProjectionRepository.findById(cmd.accountId())
                    .switchIfEmpty(Mono.error(new AccountNotFoundException(cmd.accountId())))
                    .flatMap(account -> {
                        BigDecimal balance = account.getBalance();
                        if (balance.compareTo(cmd.amount()) < 0) {
                            return Mono.error(new InsufficientFundsException(cmd.accountId(), balance, cmd.amount()));
                        }
                        return fraudClient.assess(cmd.accountId(), cmd.amount(), cmd.currency(), "DEBIT", cmd.idempotencyKey())
                            .flatMap(assessment -> {
                                if (assessment.isBlocked()) {
                                    return Mono.error(new FraudBlockedException(assessment.reason(), assessment.score()));
                                }
                                UUID transactionId = UUID.randomUUID();
                                ObjectNode payload = objectMapper.createObjectNode()
                                    .put("transactionId", transactionId.toString())
                                    .put("accountId", cmd.accountId().toString())
                                    .put("amount", cmd.amount().toPlainString())
                                    .put("currency", cmd.currency())
                                    .put("idempotencyKey", cmd.idempotencyKey())
                                    .put("reference", cmd.reference())
                                    .put("fraudScore", assessment.score());
                                return eventStore.appendEvent(cmd.accountId(), "Account",
                                    "AccountDebited", payload, account.getVersion())
                                    .map(e -> transactionId);
                            });
                    })
                    .doOnError(e -> idempotencyService.releaseLock(cmd.idempotencyKey()).subscribe());
            });
    }

    public Mono<UUID> handle(CreditAccountCommand cmd) {
        return idempotencyService.acquireLock(cmd.idempotencyKey())
            .flatMap(acquired -> {
                if (!acquired) {
                    return Mono.error(new DuplicateRequestException(
                        "Duplicate credit request: " + cmd.idempotencyKey()));
                }
                return accountProjectionRepository.findById(cmd.accountId())
                    .switchIfEmpty(Mono.error(new AccountNotFoundException(cmd.accountId())))
                    .flatMap(account -> {
                        UUID transactionId = UUID.randomUUID();
                        ObjectNode payload = objectMapper.createObjectNode()
                            .put("transactionId", transactionId.toString())
                            .put("accountId", cmd.accountId().toString())
                            .put("amount", cmd.amount().toPlainString())
                            .put("currency", cmd.currency())
                            .put("idempotencyKey", cmd.idempotencyKey())
                            .put("reference", cmd.reference());
                        return eventStore.appendEvent(cmd.accountId(), "Account",
                            "AccountCredited", payload, account.getVersion())
                            .map(e -> transactionId);
                    })
                    .doOnError(e -> idempotencyService.releaseLock(cmd.idempotencyKey()).subscribe());
            });
    }
}
