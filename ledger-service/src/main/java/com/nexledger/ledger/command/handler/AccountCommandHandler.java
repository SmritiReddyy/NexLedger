package com.nexledger.ledger.command.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexledger.ledger.command.model.CreateAccountCommand;
import com.nexledger.ledger.command.model.CreditAccountCommand;
import com.nexledger.ledger.command.model.DebitAccountCommand;
import com.nexledger.ledger.concurrency.AccountLock;
import com.nexledger.ledger.domain.AccountState;
import com.nexledger.ledger.event.store.DomainEvent;
import com.nexledger.ledger.event.store.DuplicateEventException;
import com.nexledger.ledger.event.store.EventRepository;
import com.nexledger.ledger.event.store.EventStore;
import com.nexledger.ledger.event.store.OptimisticConcurrencyException;
import com.nexledger.ledger.fraud.FraudClient;
import com.nexledger.ledger.idempotency.IdempotencyService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Function;

/**
 * Command side of the ledger.
 *
 * Every write validates against the account as rebuilt from its own events
 * (AccountStateLoader), inside a per-account Redis lock, and appends with the
 * folded version as the expected version. If another writer got in first the
 * append fails the version check and the whole read-validate-append step is
 * retried against fresh state, so a balance check can never pass on stale data.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class AccountCommandHandler {

    private static final Retry ON_CONFLICT = Retry.backoff(8, Duration.ofMillis(5))
        .maxBackoff(Duration.ofMillis(100))
        .jitter(0.5)
        .filter(OptimisticConcurrencyException.class::isInstance)
        .onRetryExhaustedThrow((spec, signal) -> signal.failure());

    private final EventStore eventStore;
    private final EventRepository eventRepository;
    private final AccountStateLoader stateLoader;
    private final AccountLock accountLock;
    private final IdempotencyService idempotencyService;
    private final FraudClient fraudClient;
    private final ObjectMapper objectMapper;

    public Mono<UUID> handle(CreateAccountCommand cmd) {
        UUID accountId = UUID.randomUUID();
        ObjectNode payload = objectMapper.createObjectNode()
            .put("accountId", accountId.toString())
            .put("ownerId", cmd.ownerId())
            .put("currency", cmd.currency())
            .put("initialBalance", cmd.initialBalance().toPlainString());

        Mono<UUID> create = eventStore.appendEvent(accountId, "Account", "AccountCreated", payload, -1)
            .map(event -> accountId)
            .doOnSuccess(id -> log.info("Account created: {}", id));
        return cmd.idempotencyKey() == null ? create : idempotencyService.execute(cmd.idempotencyKey(), () -> create);
    }

    public Mono<UUID> handle(DebitAccountCommand cmd) {
        return idempotencyService.execute(cmd.idempotencyKey(), () ->
            write(cmd.accountId(), cmd.idempotencyKey(), account -> {
                if (account.balance().compareTo(cmd.amount()) < 0) {
                    return Mono.error(new InsufficientFundsException(cmd.accountId(), account.balance(), cmd.amount()));
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
                        return eventStore.appendEvent(cmd.accountId(), "Account", "AccountDebited",
                                payload, account.version())
                            .thenReturn(transactionId);
                    });
            }));
    }

    public Mono<UUID> handle(CreditAccountCommand cmd) {
        return idempotencyService.execute(cmd.idempotencyKey(), () ->
            write(cmd.accountId(), cmd.idempotencyKey(), account -> {
                UUID transactionId = UUID.randomUUID();
                ObjectNode payload = objectMapper.createObjectNode()
                    .put("transactionId", transactionId.toString())
                    .put("accountId", cmd.accountId().toString())
                    .put("amount", cmd.amount().toPlainString())
                    .put("currency", cmd.currency())
                    .put("idempotencyKey", cmd.idempotencyKey())
                    .put("reference", cmd.reference());
                return eventStore.appendEvent(cmd.accountId(), "Account", "AccountCredited",
                        payload, account.version())
                    .thenReturn(transactionId);
            }));
    }

    /**
     * Lock the account, return the earlier result if this key already wrote an
     * event, otherwise fold current state and run the step; retry on version conflict.
     */
    private Mono<UUID> write(UUID accountId, String idempotencyKey, Function<AccountState, Mono<UUID>> step) {
        return accountLock.withLock(accountId, () ->
                existingTransaction(accountId, idempotencyKey)
                    .switchIfEmpty(Mono.defer(() -> stateLoader.current(accountId).flatMap(account -> {
                        if (!account.exists()) {
                            return Mono.error(new AccountNotFoundException(accountId));
                        }
                        return step.apply(account);
                    }))))
            .retryWhen(ON_CONFLICT)
            // Lost a race on the unique idempotency index: the other writer's event is the answer.
            .onErrorResume(DuplicateEventException.class, e -> existingTransaction(accountId, idempotencyKey));
    }

    private Mono<UUID> existingTransaction(UUID accountId, String idempotencyKey) {
        return eventRepository.findByIdempotencyKey(accountId, idempotencyKey)
            .map(this::transactionId);
    }

    private UUID transactionId(DomainEvent e) {
        try {
            return UUID.fromString(objectMapper.readTree(e.payload().asString()).get("transactionId").asText());
        } catch (Exception ex) {
            throw new IllegalStateException("Event " + e.id() + " has no transactionId", ex);
        }
    }
}
