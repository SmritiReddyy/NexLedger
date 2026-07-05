package com.nexledger.ledger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexledger.ledger.command.handler.AccountCommandHandler;
import com.nexledger.ledger.command.handler.InsufficientFundsException;
import com.nexledger.ledger.command.model.CreateAccountCommand;
import com.nexledger.ledger.command.model.DebitAccountCommand;
import com.nexledger.ledger.event.publisher.EventPublisher;
import com.nexledger.ledger.event.store.DomainEvent;
import com.nexledger.ledger.event.store.EventRepository;
import com.nexledger.ledger.event.store.EventStore;
import com.nexledger.ledger.fraud.FraudAssessment;
import com.nexledger.ledger.fraud.FraudClient;
import com.nexledger.ledger.idempotency.IdempotencyService;
import com.nexledger.ledger.query.projection.AccountProjection;
import com.nexledger.ledger.query.projection.AccountProjectionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.math.BigDecimal;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AccountCommandHandlerTest {

    @Mock EventRepository eventRepository;
    @Mock EventPublisher eventPublisher;
    @Mock IdempotencyService idempotencyService;
    @Mock FraudClient fraudClient;
    @Mock AccountProjectionRepository accountProjectionRepository;

    AccountCommandHandler handler;
    ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        EventStore eventStore = new EventStore(eventRepository, eventPublisher);
        handler = new AccountCommandHandler(eventStore, idempotencyService, fraudClient,
            accountProjectionRepository, objectMapper);
    }

    @Test
    void createAccount_shouldEmitAccountId() {
        when(eventRepository.findMaxVersionByAggregateId(any())).thenReturn(Mono.just(-1L));
        when(eventRepository.save(any(DomainEvent.class))).thenAnswer(inv -> Mono.just(inv.getArgument(0)));
        when(eventPublisher.publish(any())).thenReturn(Mono.empty());

        CreateAccountCommand cmd = new CreateAccountCommand("owner-1", "USD", BigDecimal.valueOf(1000), "idem-1");

        StepVerifier.create(handler.handle(cmd))
            .expectNextMatches(id -> id != null)
            .verifyComplete();
    }

    @Test
    void debit_shouldFailWithInsufficientFunds() {
        UUID accountId = UUID.randomUUID();
        AccountProjection account = new AccountProjection(accountId, "owner-1", "USD",
            BigDecimal.valueOf(50), "ACTIVE", 1L);

        when(idempotencyService.acquireLock(anyString())).thenReturn(Mono.just(true));
        when(accountProjectionRepository.findById(accountId)).thenReturn(Mono.just(account));
        when(fraudClient.assess(any(), any(), any(), any(), any()))
            .thenReturn(Mono.just(FraudAssessment.allowed(0.1)));
        when(idempotencyService.releaseLock(anyString())).thenReturn(Mono.just(true));

        DebitAccountCommand cmd = new DebitAccountCommand(
            accountId, BigDecimal.valueOf(200), "USD", "idem-debit-1", "test");

        StepVerifier.create(handler.handle(cmd))
            .expectError(InsufficientFundsException.class)
            .verify();
    }
}
