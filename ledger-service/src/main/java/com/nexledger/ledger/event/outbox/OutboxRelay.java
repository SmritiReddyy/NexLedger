package com.nexledger.ledger.event.outbox;

import com.nexledger.ledger.event.publisher.EventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Publishes committed outbox rows to Kafka, oldest first.
 *
 * Rows are claimed with SELECT ... FOR UPDATE SKIP LOCKED inside a transaction
 * and marked PUBLISHED only after Kafka acknowledges them. A crash between the
 * send and the mark republishes the batch on the next run: delivery is
 * at-least-once, and the projections ignore an event version they already applied.
 *
 * Only one relay publishes at a time, across all replicas: each run first takes
 * a transaction-scoped advisory lock and does nothing if another replica holds
 * it. SKIP LOCKED alone stops two relays publishing the same row, but not two
 * relays publishing an account's consecutive events concurrently. Then version
 * N+1 can reach Kafka before N, and the projection, which skips versions older
 * than the one it holds, drops N for good.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxRepository outboxRepository;
    private final EventPublisher eventPublisher;
    private final TransactionalOperator transactionalOperator;
    private final AtomicBoolean running = new AtomicBoolean(false);

    @Value("${nexledger.outbox.batch-size:200}")
    private int batchSize;

    @Scheduled(fixedDelayString = "${nexledger.outbox.poll-ms:50}")
    public void poll() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        try {
            // keep draining while full batches come back, so a backlog clears without waiting for ticks
            int published;
            do {
                published = relayBatch().block(Duration.ofSeconds(30));
            } while (published == batchSize);
        } catch (Exception e) {
            log.warn("Outbox relay run failed, will retry: {}", e.getMessage());
        } finally {
            running.set(false);
        }
    }

    public Mono<Integer> relayBatch() {
        Mono<Integer> batch = outboxRepository.tryRelayLock(RELAY_LOCK_KEY)
            .flatMap(acquired -> !acquired ? Mono.just(0) : outboxRepository.claimPending(batchSize)
                .concatMap(row -> eventPublisher.send(row.topic(), row.aggregateId(), row.payload().asString())
                    .thenReturn(row.id()))
                .collectList()
                .flatMap((List<Long> ids) -> ids.isEmpty()
                    ? Mono.just(0)
                    : outboxRepository.markPublished(ids).thenReturn(ids.size())));
        return transactionalOperator.transactional(batch);
    }

    /** Arbitrary, but fixed: every replica must use the same key. */
    static final long RELAY_LOCK_KEY = 0x4E58_4C45_4447_4552L;   // "NXLEDGER"
}
