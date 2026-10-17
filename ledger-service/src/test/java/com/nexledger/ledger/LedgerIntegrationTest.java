package com.nexledger.ledger;

import com.nexledger.ledger.event.outbox.OutboxRepository;
import com.nexledger.ledger.event.store.EventRepository;
import com.nexledger.ledger.query.projection.AccountProjectionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Event sourcing, replay, concurrency and idempotency on the real stack. */
class LedgerIntegrationTest extends IntegrationTestBase {

    @Autowired EventRepository events;
    @Autowired OutboxRepository outbox;
    @Autowired AccountProjectionRepository projections;

    @Test
    void pointInTimeReplayMatchesTheBalanceAtEveryCheckpoint() throws Exception {
        UUID account = createAccount("1000.00");
        Random rnd = new Random(42);
        BigDecimal expected = new BigDecimal("1000.00");
        record Checkpoint(Instant at, long version, BigDecimal balance) {}
        List<Checkpoint> checkpoints = new ArrayList<>();

        for (int i = 0; i < 60; i++) {
            BigDecimal amount = BigDecimal.valueOf(1 + rnd.nextInt(90)).setScale(2);
            boolean doDebit = rnd.nextBoolean() && expected.compareTo(amount) >= 0;
            if (doDebit) {
                debit(account, amount.toPlainString(), "replay-" + i).expectStatus().isOk();
                expected = expected.subtract(amount);
            } else {
                credit(account, amount.toPlainString(), "replay-" + i).expectStatus().isOk();
                expected = expected.add(amount);
            }
            if (i % 10 == 9) {
                Thread.sleep(15);   // keep checkpoint timestamps strictly between events
                checkpoints.add(new Checkpoint(Instant.now(), i + 1, expected));
                Thread.sleep(15);
            }
        }

        for (Checkpoint c : checkpoints) {
            assertThat(replayedBalance(account, "?asOf=" + c.at())).isEqualByComparingTo(c.balance());
            assertThat(replayedBalance(account, "?version=" + c.version())).isEqualByComparingTo(c.balance());
        }
        assertThat(replayedBalance(account, "")).isEqualByComparingTo(expected);

        // The read model, fed through outbox -> Kafka -> projection, converges on the same figure.
        BigDecimal finalBalance = expected;
        await().atMost(SETTLE).untilAsserted(() ->
            assertThat(projections.findById(account).block().getBalance()).isEqualByComparingTo(finalBalance));
        assertThat(projections.findById(account).block().getVersion()).isEqualTo(60);
    }

    @Test
    void concurrentDebitsOnOneAccountNeverOverdraw() {
        UUID account = createAccount("1000.00");
        int requests = 50;

        List<HttpStatus> statuses = Flux.range(0, requests)
            .parallel(requests).runOn(Schedulers.boundedElastic())
            .map(i -> (HttpStatus) debit(account, "30.00", "race-" + account + "-" + i)
                .returnResult(String.class).getStatus())
            .sequential().collectList().block(Duration.ofMinutes(2));

        long ok = statuses.stream().filter(HttpStatus.OK::equals).count();
        long insufficient = statuses.stream().filter(HttpStatus.UNPROCESSABLE_ENTITY::equals).count();
        assertThat(ok).isEqualTo(33);                       // floor(1000 / 30)
        assertThat(insufficient).isEqualTo(requests - 33);  // every other request saw the real balance
        assertThat(replayedBalance(account, "")).isEqualByComparingTo("10.00");
        assertThat(events.findByAggregateId(account).count().block()).isEqualTo(1 + 33);
    }

    @Test
    void retriedRequestReturnsTheOriginalTransaction() {
        UUID account = createAccount("100.00");
        String first = debit(account, "40.00", "retry-1").expectStatus().isOk()
            .expectBody(Map.class).returnResult().getResponseBody().get("transactionId").toString();
        String second = debit(account, "40.00", "retry-1").expectStatus().isOk()
            .expectBody(Map.class).returnResult().getResponseBody().get("transactionId").toString();

        assertThat(second).isEqualTo(first);
        assertThat(replayedBalance(account, "")).isEqualByComparingTo("60.00");
    }

    @Test
    void concurrentDuplicatesWriteExactlyOneEvent() {
        UUID account = createAccount("100.00");
        String key = "dup-" + account;
        List<Integer> codes = Flux.range(0, 10).parallel(10).runOn(Schedulers.boundedElastic())
            .map(i -> debit(account, "10.00", key).returnResult(String.class).getStatus().value())
            .sequential().collectList().block(Duration.ofMinutes(1));

        assertThat(codes).allMatch(c -> c == 200 || c == 409);
        assertThat(events.findByAggregateId(account).count().block()).isEqualTo(2);   // created + one debit
        assertThat(replayedBalance(account, "")).isEqualByComparingTo("90.00");
    }

    @Test
    void everyEventReachesKafkaThroughTheOutbox() {
        UUID account = createAccount("50.00");
        credit(account, "5.00", "outbox-1").expectStatus().isOk();
        await().atMost(SETTLE).pollInterval(50, TimeUnit.MILLISECONDS)
            .untilAsserted(() -> assertThat(outbox.countPending().block()).isZero());
        await().atMost(SETTLE).untilAsserted(() ->
            assertThat(projections.findById(account).block().getBalance()).isEqualByComparingTo("55.00"));
    }
}
