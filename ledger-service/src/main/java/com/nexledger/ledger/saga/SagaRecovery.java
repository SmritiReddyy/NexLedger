package com.nexledger.ledger.saga;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexledger.ledger.command.model.TransferFundsCommand;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

/**
 * Finishes transfers that stopped mid-way: the process died, a dependency
 * timed out, or the broker was down. Any saga sitting in STARTED or DEBITED for
 * longer than `stuck-after` is driven forward from its persisted state.
 * Because every step is idempotent, resuming a saga that was in fact still
 * running only re-reads results that already exist.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SagaRecovery {

    private final SagaStateRepository sagas;
    private final TransferSaga transferSaga;
    private final ObjectMapper objectMapper;

    @Value("${nexledger.saga.stuck-after-ms:10000}")
    private long stuckAfterMs;

    @Scheduled(fixedDelayString = "${nexledger.saga.recovery-interval-ms:5000}", initialDelay = 5000)
    public void run() {
        try {
            Long n = recoverStuck().block(Duration.ofMinutes(1));
            if (n != null && n > 0) {
                log.info("Saga recovery resumed {} saga(s)", n);
            }
        } catch (Exception e) {
            log.warn("Saga recovery run failed: {}", e.getMessage());
        }
    }

    public Mono<Long> recoverStuck() {
        Instant cutoff = Instant.now().minusMillis(stuckAfterMs);
        return sagas.findStuck(cutoff, 100)
            .concatMap(this::resume)
            .count();
    }

    private Flux<TransferSaga.Result> resume(SagaState saga) {
        TransferFundsCommand cmd;
        try {
            cmd = objectMapper.readValue(saga.getPayload().asString(), TransferFundsCommand.class);
        } catch (Exception e) {
            log.error("Saga {} has an unreadable payload; leaving it for manual review", saga.getSagaId());
            return Flux.empty();
        }
        log.info("Resuming saga {} from {}", saga.getSagaId(), saga.getState());
        return sagas.touch(saga.getSagaId(), saga.getState())
            .then(transferSaga.drive(saga.getSagaId(), cmd, saga.getState()))
            .onErrorResume(e -> {
                log.info("Saga {} ended with {}", saga.getSagaId(), e.getMessage());
                return sagas.findById(saga.getSagaId()).map(s -> new TransferSaga.Result(s.getSagaId(), s.getState()));
            })
            .flux();
    }
}
