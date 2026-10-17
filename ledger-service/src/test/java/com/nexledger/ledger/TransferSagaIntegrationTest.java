package com.nexledger.ledger;

import com.nexledger.ledger.saga.SagaRecovery;
import com.nexledger.ledger.saga.SagaStateRepository;
import com.nexledger.ledger.saga.TransferSaga;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Transfers as a persisted saga: success, business failures, and a crash between steps. */
class TransferSagaIntegrationTest extends IntegrationTestBase {

    @Autowired SagaStateRepository sagas;
    @Autowired SagaRecovery recovery;

    private WebTestClient.ResponseSpec transfer(UUID from, UUID to, String amount, String key) {
        return web.post().uri("/api/v1/transfers").header("Idempotency-Key", key)
            .bodyValue(Map.of("sourceAccountId", from, "targetAccountId", to, "amount", amount,
                "currency", "USD", "reference", "test"))
            .exchange();
    }

    private String state(String key) {
        return sagas.findById(TransferSaga.sagaIdFor(key)).block().getState();
    }

    @Test
    void transferMovesMoney() {
        UUID a = createAccount("500.00"), b = createAccount("0.00");
        transfer(a, b, "200.00", "t-ok").expectStatus().isAccepted()
            .expectBody(Map.class).value(body -> assertThat(body.get("status")).isEqualTo("COMPLETED"));
        assertThat(replayedBalance(a, "")).isEqualByComparingTo("300.00");
        assertThat(replayedBalance(b, "")).isEqualByComparingTo("200.00");
    }

    @Test
    void insufficientFundsFailsWithoutTouchingEitherAccount() {
        UUID a = createAccount("50.00"), b = createAccount("0.00");
        transfer(a, b, "80.00", "t-poor").expectStatus().isEqualTo(422);
        assertThat(state("t-poor")).isEqualTo(TransferSaga.FAILED);
        assertThat(replayedBalance(a, "")).isEqualByComparingTo("50.00");
        assertThat(replayedBalance(b, "")).isEqualByComparingTo("0.00");
    }

    @Test
    void creditToMissingAccountIsCompensated() {
        UUID a = createAccount("500.00");
        transfer(a, UUID.randomUUID(), "120.00", "t-missing").expectStatus().isNotFound();
        assertThat(state("t-missing")).isEqualTo(TransferSaga.COMPENSATED);
        assertThat(replayedBalance(a, "")).isEqualByComparingTo("500.00");   // debited, then refunded
        assertThat(replayedBalance(a, "?version=1")).isEqualByComparingTo("380.00");
    }

    @Test
    void crashAfterTheDebitIsFinishedByRecovery() {
        UUID a = createAccount("500.00"), b = createAccount("100.00");
        AFTER_DEBIT.set(id -> Mono.error(new IllegalStateException("simulated crash after debit")));

        transfer(a, b, "75.00", "t-crash").expectStatus().isAccepted()
            .expectBody(Map.class).value(body -> assertThat(body.get("status")).isEqualTo("DEBITED"));
        assertThat(replayedBalance(a, "")).isEqualByComparingTo("425.00");
        assertThat(replayedBalance(b, "")).isEqualByComparingTo("100.00");   // money in flight, saga paused

        AFTER_DEBIT.set(id -> Mono.empty());
        recovery.recoverStuck().block();

        assertThat(state("t-crash")).isEqualTo(TransferSaga.COMPLETED);
        assertThat(replayedBalance(a, "")).isEqualByComparingTo("425.00");   // not debited twice
        assertThat(replayedBalance(b, "")).isEqualByComparingTo("175.00");
        assertThat(replayedBalance(a, "").add(replayedBalance(b, ""))).isEqualByComparingTo("600.00");
    }

    @Test
    void retriedTransferRunsOnce() {
        UUID a = createAccount("300.00"), b = createAccount("0.00");
        transfer(a, b, "100.00", "t-twice").expectStatus().isAccepted();
        transfer(a, b, "100.00", "t-twice").expectStatus().isAccepted()
            .expectBody(Map.class).value(body -> assertThat(body.get("status")).isEqualTo("COMPLETED"));
        assertThat(replayedBalance(a, "")).isEqualByComparingTo(new BigDecimal("200.00"));
        assertThat(replayedBalance(b, "")).isEqualByComparingTo(new BigDecimal("100.00"));
    }
}
