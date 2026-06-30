package com.nexledger.ledger.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nexledger.ledger.command.handler.AccountCommandHandler;
import com.nexledger.ledger.command.model.CreditAccountCommand;
import com.nexledger.ledger.command.model.DebitAccountCommand;
import com.nexledger.ledger.command.model.TransferFundsCommand;
import com.nexledger.ledger.event.store.DomainEvent;
import com.nexledger.ledger.event.store.EventStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class TransferSaga {

    private final AccountCommandHandler accountCommandHandler;
    private final EventStore eventStore;
    private final SagaStateRepository sagaStateRepository;
    private final ObjectMapper objectMapper;

    public Mono<UUID> execute(TransferFundsCommand cmd) {
        UUID sagaId = UUID.randomUUID();
        log.info("Starting TransferSaga {} for transfer {} -> {}", sagaId, cmd.sourceAccountId(), cmd.targetAccountId());

        return saveSagaState(sagaId, "STARTED", cmd)
            .then(executeDebit(sagaId, cmd))
            .flatMap(debitTxId -> executeCredit(sagaId, cmd, debitTxId))
            .flatMap(creditTxId -> completeSaga(sagaId, creditTxId))
            .onErrorResume(e -> compensate(sagaId, cmd, e));
    }

    private Mono<UUID> executeDebit(UUID sagaId, TransferFundsCommand cmd) {
        DebitAccountCommand debit = new DebitAccountCommand(
            cmd.sourceAccountId(), cmd.amount(), cmd.currency(),
            cmd.idempotencyKey() + ":debit", cmd.reference());
        return accountCommandHandler.handle(debit)
            .doOnSuccess(id -> log.info("Saga {}: debit succeeded, txId={}", sagaId, id))
            .doOnSuccess(id -> saveSagaState(sagaId, "DEBITED", cmd).subscribe());
    }

    private Mono<UUID> executeCredit(UUID sagaId, TransferFundsCommand cmd, UUID debitTxId) {
        CreditAccountCommand credit = new CreditAccountCommand(
            cmd.targetAccountId(), cmd.amount(), cmd.currency(),
            cmd.idempotencyKey() + ":credit", cmd.reference());
        return accountCommandHandler.handle(credit)
            .doOnSuccess(id -> log.info("Saga {}: credit succeeded, txId={}", sagaId, id));
    }

    private Mono<UUID> completeSaga(UUID sagaId, UUID creditTxId) {
        return sagaStateRepository.updateState(sagaId, "COMPLETED")
            .thenReturn(sagaId)
            .doOnSuccess(id -> log.info("Saga {} completed successfully", sagaId));
    }

    private Mono<UUID> compensate(UUID sagaId, TransferFundsCommand cmd, Throwable error) {
        log.warn("Saga {} failed, initiating compensation: {}", sagaId, error.getMessage());
        CreditAccountCommand compensatingCredit = new CreditAccountCommand(
            cmd.sourceAccountId(), cmd.amount(), cmd.currency(),
            cmd.idempotencyKey() + ":compensate", "COMPENSATING: " + cmd.reference());
        return accountCommandHandler.handle(compensatingCredit)
            .then(sagaStateRepository.updateState(sagaId, "COMPENSATED"))
            .then(publishCompensationEvent(sagaId, cmd))
            .then(Mono.error(error));
    }

    private Mono<DomainEvent> publishCompensationEvent(UUID sagaId, TransferFundsCommand cmd) {
        ObjectNode payload = objectMapper.createObjectNode()
            .put("sagaId", sagaId.toString())
            .put("sourceAccountId", cmd.sourceAccountId().toString())
            .put("targetAccountId", cmd.targetAccountId().toString())
            .put("amount", cmd.amount().toPlainString())
            .put("currency", cmd.currency());
        return eventStore.appendEvent(sagaId, "Saga", "SagaCompensated", payload, -1);
    }

    private Mono<Void> saveSagaState(UUID sagaId, String state, TransferFundsCommand cmd) {
        try {
            String payloadJson = objectMapper.writeValueAsString(cmd);
            SagaState sagaState = new SagaState(sagaId, "TRANSFER", state, payloadJson);
            return sagaStateRepository.save(sagaState).then();
        } catch (Exception e) {
            return Mono.error(e);
        }
    }
}
