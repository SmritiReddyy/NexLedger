package com.nexledger.ledger.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexledger.ledger.command.handler.AccountCommandHandler;
import com.nexledger.ledger.command.handler.AccountNotFoundException;
import com.nexledger.ledger.command.handler.FraudBlockedException;
import com.nexledger.ledger.command.handler.InsufficientFundsException;
import com.nexledger.ledger.command.model.CreditAccountCommand;
import com.nexledger.ledger.command.model.DebitAccountCommand;
import com.nexledger.ledger.command.model.TransferFundsCommand;
import com.nexledger.ledger.event.store.EventStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Transfer = debit source, then credit target, as a persisted state machine.
 *
 *   STARTED --debit ok--> DEBITED --credit ok--> COMPLETED
 *      |                     |
 *      | business failure    | business failure (e.g. target closed)
 *      v                     v
 *    FAILED            compensating credit --> COMPENSATED
 *
 * Every transition is written before the next step starts, and every step uses
 * an idempotency key derived from the saga id, so re-running a step is a no-op.
 * An infrastructure failure (crash, timeout, broker down) leaves the saga in its
 * current state; SagaRecovery picks it up and resumes from there. Compensation
 * only ever runs from DEBITED, so money that was never debited is never refunded.
 */
@Slf4j
@Service
public class TransferSaga {

    public static final String STARTED = "STARTED";
    public static final String DEBITED = "DEBITED";
    public static final String COMPLETED = "COMPLETED";
    public static final String FAILED = "FAILED";
    public static final String COMPENSATED = "COMPENSATED";

    public record Result(UUID sagaId, String state) {}

    private final AccountCommandHandler accounts;
    private final EventStore eventStore;
    private final SagaStateRepository sagas;
    private final ObjectMapper objectMapper;
    private final SagaHooks hooks;

    public TransferSaga(AccountCommandHandler accounts, EventStore eventStore, SagaStateRepository sagas,
                        ObjectMapper objectMapper, ObjectProvider<SagaHooks> hooks) {
        this.accounts = accounts;
        this.eventStore = eventStore;
        this.sagas = sagas;
        this.objectMapper = objectMapper;
        this.hooks = hooks.getIfAvailable(() -> new SagaHooks() {});
    }

    /** The saga id is derived from the client's idempotency key, so a retried request finds the same saga. */
    public static UUID sagaIdFor(String idempotencyKey) {
        return UUID.nameUUIDFromBytes(("transfer:" + idempotencyKey).getBytes(StandardCharsets.UTF_8));
    }

    public Mono<Result> execute(TransferFundsCommand cmd) {
        UUID sagaId = sagaIdFor(cmd.idempotencyKey());
        String payload;
        try {
            payload = objectMapper.writeValueAsString(cmd);
        } catch (Exception e) {
            return Mono.error(e);
        }
        return sagas.save(SagaState.started(sagaId, "TRANSFER", payload))
            .then(Mono.defer(() -> drive(sagaId, cmd, STARTED)))
            // Same idempotency key again: report the existing saga instead of starting a second transfer.
            .onErrorResume(DuplicateKeyException.class, e -> sagas.findById(sagaId)
                .map(s -> new Result(sagaId, s.getState())));
    }

    /** Resume a saga from the state it was persisted in. Used by execute and by SagaRecovery. */
    public Mono<Result> drive(UUID sagaId, TransferFundsCommand cmd, String from) {
        Mono<Result> run = switch (from) {
            case STARTED -> debit(sagaId, cmd).then(Mono.defer(() -> credit(sagaId, cmd)));
            case DEBITED -> credit(sagaId, cmd);
            default -> Mono.just(new Result(sagaId, from));
        };
        return run.onErrorResume(e -> !isBusinessFailure(e), e -> {
            // Unknown outcome: keep the persisted state and let recovery finish the job.
            log.warn("Saga {} paused in {} after infrastructure error: {}", sagaId, from, e.toString());
            return sagas.findById(sagaId).map(s -> new Result(sagaId, s.getState()));
        });
    }

    private Mono<Void> debit(UUID sagaId, TransferFundsCommand cmd) {
        DebitAccountCommand debit = new DebitAccountCommand(cmd.sourceAccountId(), cmd.amount(), cmd.currency(),
            sagaId + ":debit", cmd.reference());
        return accounts.handle(debit)
            .onErrorResume(InsufficientFundsException.class, e -> fail(sagaId, e))
            .onErrorResume(AccountNotFoundException.class, e -> fail(sagaId, e))
            .onErrorResume(FraudBlockedException.class, e -> fail(sagaId, e))
            .then(sagas.transition(sagaId, STARTED, DEBITED))
            .doOnNext(n -> log.info("Saga {}: source debited", sagaId))
            .then(hooks.afterDebit(sagaId));
    }

    private Mono<Result> credit(UUID sagaId, TransferFundsCommand cmd) {
        CreditAccountCommand credit = new CreditAccountCommand(cmd.targetAccountId(), cmd.amount(), cmd.currency(),
            sagaId + ":credit", cmd.reference());
        return accounts.handle(credit)
            .then(sagas.transition(sagaId, DEBITED, COMPLETED))
            .map(n -> new Result(sagaId, COMPLETED))
            .doOnNext(r -> log.info("Saga {} completed", sagaId))
            .onErrorResume(TransferSaga::isBusinessFailure, e -> compensate(sagaId, cmd, e));
    }

    private <T> Mono<T> fail(UUID sagaId, Throwable cause) {
        return sagas.transition(sagaId, STARTED, FAILED).then(Mono.error(cause));
    }

    private Mono<Result> compensate(UUID sagaId, TransferFundsCommand cmd, Throwable cause) {
        log.warn("Saga {}: credit failed ({}), refunding source", sagaId, cause.getMessage());
        CreditAccountCommand refund = new CreditAccountCommand(cmd.sourceAccountId(), cmd.amount(), cmd.currency(),
            sagaId + ":compensate", "COMPENSATING: " + cmd.reference());
        ObjectNode event = objectMapper.createObjectNode()
            .put("sagaId", sagaId.toString())
            .put("sourceAccountId", cmd.sourceAccountId().toString())
            .put("targetAccountId", cmd.targetAccountId().toString())
            .put("amount", cmd.amount().toPlainString())
            .put("currency", cmd.currency())
            .put("reason", cause.getMessage());
        return accounts.handle(refund)
            .then(sagas.transition(sagaId, DEBITED, COMPENSATED))
            .flatMap(moved -> moved == 1
                ? eventStore.appendEvent(sagaId, "Saga", "SagaCompensated", event, -1).then()
                : Mono.empty())
            .then(Mono.error(cause));
    }

    static boolean isBusinessFailure(Throwable e) {
        return e instanceof InsufficientFundsException
            || e instanceof AccountNotFoundException
            || e instanceof FraudBlockedException;
    }
}
