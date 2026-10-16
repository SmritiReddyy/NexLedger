package com.nexledger.ledger.idempotency;

import com.nexledger.ledger.command.handler.DuplicateRequestException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Exactly-once command handling keyed by the client's Idempotency-Key.
 *
 * A completed command's result is cached, so a retry returns the original
 * transaction id instead of an error or a second debit. While a command is in
 * flight, a short-lived PROCESSING marker rejects concurrent duplicates. The
 * marker's short TTL means a crash mid-command does not block the key for a
 * day; the event store's unique idempotency index stops the retry from
 * writing twice even if the marker or the cached result is lost.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private static final String RESULT_PREFIX = "idempotency:";
    private static final String PROCESSING_PREFIX = "lock:idempotency:";
    private static final String PROCESSING = "PROCESSING";

    private final ReactiveRedisTemplate<String, String> redisTemplate;

    @Value("${nexledger.idempotency.ttl-seconds:86400}")
    private long resultTtlSeconds;

    @Value("${nexledger.idempotency.processing-ttl-seconds:30}")
    private long processingTtlSeconds;

    public Mono<UUID> execute(String idempotencyKey, Supplier<Mono<UUID>> command) {
        return getResult(idempotencyKey)
            .map(UUID::fromString)
            .doOnNext(id -> log.debug("Replaying stored result for idempotency key {}", idempotencyKey))
            .switchIfEmpty(Mono.defer(() -> markProcessing(idempotencyKey).flatMap(acquired -> {
                if (!acquired) {
                    return Mono.error(new DuplicateRequestException(
                        "A request with idempotency key " + idempotencyKey + " is already in progress"));
                }
                return command.get()
                    .flatMap(result -> storeResult(idempotencyKey, result.toString()).thenReturn(result))
                    .doFinally(signal -> clearProcessing(idempotencyKey).subscribe());
            })));
    }

    Mono<Boolean> markProcessing(String idempotencyKey) {
        return redisTemplate.opsForValue()
            .setIfAbsent(PROCESSING_PREFIX + idempotencyKey, PROCESSING, Duration.ofSeconds(processingTtlSeconds));
    }

    public Mono<Boolean> storeResult(String idempotencyKey, String result) {
        return redisTemplate.opsForValue()
            .set(RESULT_PREFIX + idempotencyKey, result, Duration.ofSeconds(resultTtlSeconds));
    }

    public Mono<String> getResult(String idempotencyKey) {
        return redisTemplate.opsForValue().get(RESULT_PREFIX + idempotencyKey);
    }

    Mono<Boolean> clearProcessing(String idempotencyKey) {
        return redisTemplate.delete(PROCESSING_PREFIX + idempotencyKey).map(count -> count > 0);
    }
}
