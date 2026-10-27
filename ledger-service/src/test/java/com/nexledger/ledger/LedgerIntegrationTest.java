package com.nexledger.ledger;

import com.nexledger.ledger.event.outbox.OutboxRelay;
import com.nexledger.ledger.event.outbox.OutboxRepository;
import com.nexledger.ledger.event.publisher.EventPublisher;
import com.nexledger.ledger.event.store.EventRepository;
import com.nexledger.ledger.query.projection.AccountProjectionRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.reactive.TransactionalOperator;
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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Event sourcing, replay, concurrency and idempotency on the real stack. */
class LedgerIntegrationTest extends IntegrationTestBase {

    @Autowired EventRepository events;
    @Autowired OutboxRepository outbox;
    @Autowired AccountProjectionRepository projections;
    @Autowired OutboxRelay relay;
    @Autowired EventPublisher publisher;
    @Autowired TransactionalOperator transactionalOperator;

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

    /**
     * Two service replicas each run an outbox relay. If both publish at once,
     * version N+1 of an account can reach Kafka before version N, and the
     * projection (which ignores versions older than the one it holds) silently
     * drops N. The relays must hand off, not race.
     */
    @Test
    void twoRelayInstancesKeepEachAccountsEventsInOrder() throws Exception {
        OutboxRelay second = new OutboxRelay(outbox, publisher, transactionalOperator);   // the other replica's
        ReflectionTestUtils.setField(second, "batchSize", 3);
        Object ownBatch = ReflectionTestUtils.getField(relay, "batchSize");
        ReflectionTestUtils.setField(relay, "batchSize", 3);   // small batches make the two interleave
        // Hold the scheduled relay so a backlog builds, as it does under real load.
        AtomicBoolean scheduledBusy = (AtomicBoolean) ReflectionTestUtils.getField(relay, "running");
        await().atMost(SETTLE).until(() -> scheduledBusy.compareAndSet(false, true));
        UUID account = createAccount("0.00");
        int applied = 0;
        try {
            for (int i = 0; i < 300; i++) {
                if (credit(account, "1.00", "order-" + account + "-" + i).returnResult(Void.class).getStatus().value() == 200) {
                    applied++;
                }
            }
            // Release the backlog to both relays at once.
            AtomicBoolean stop = new AtomicBoolean();
            List<Thread> relays = new ArrayList<>();
            for (OutboxRelay r : List.of(relay, second)) {
                Thread t = new Thread(() -> {
                    while (!stop.get()) r.relayBatch().block(Duration.ofSeconds(10));
                });
                t.start();
                relays.add(t);
            }
            await().atMost(SETTLE).untilAsserted(() -> assertThat(outbox.countPending().block()).isZero());
            stop.set(true);
            for (Thread t : relays) t.join();
        } finally {
            ReflectionTestUtils.setField(relay, "batchSize", ownBatch);
            scheduledBusy.set(false);
        }
        BigDecimal expected = new BigDecimal(applied);
        assertThat(applied).isEqualTo(300);
        assertThat(replayedBalance(account, "")).isEqualByComparingTo(expected);
        await().atMost(SETTLE).untilAsserted(() ->
            assertThat(projections.findById(account).block().getBalance()).isEqualByComparingTo(expected));
    }
}
