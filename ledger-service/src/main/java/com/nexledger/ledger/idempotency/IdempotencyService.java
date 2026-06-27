package com.nexledger.ledger.idempotency;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private static final String KEY_PREFIX = "idempotency:";
    private static final String LOCK_PREFIX = "lock:";
    private static final String PROCESSING = "PROCESSING";

    private final ReactiveRedisTemplate<String, String> redisTemplate;

    @Value("${nexledger.idempotency.ttl-seconds:86400}")
    private long ttlSeconds;

    /**
     * Attempts to acquire an idempotency lock via Redis SETNX.
     * Returns true if the lock was acquired (first request), false if duplicate.
     */
    public Mono<Boolean> acquireLock(String idempotencyKey) {
        String redisKey = LOCK_PREFIX + KEY_PREFIX + idempotencyKey;
        return redisTemplate.opsForValue()
            .setIfAbsent(redisKey, PROCESSING, Duration.ofSeconds(ttlSeconds))
            .doOnNext(acquired -> {
                if (acquired) {
                    log.debug("Idempotency lock acquired for key: {}", idempotencyKey);
                } else {
                    log.warn("Duplicate request detected for idempotency key: {}", idempotencyKey);
                }
            });
    }

    /**
     * Stores the result of a successful operation so subsequent duplicate
     * requests can return the same response without reprocessing.
     */
    public Mono<Boolean> storeResult(String idempotencyKey, String result) {
        String redisKey = KEY_PREFIX + idempotencyKey;
        return redisTemplate.opsForValue()
            .set(redisKey, result, Duration.ofSeconds(ttlSeconds));
    }

    public Mono<String> getResult(String idempotencyKey) {
        String redisKey = KEY_PREFIX + idempotencyKey;
        return redisTemplate.opsForValue().get(redisKey);
    }

    public Mono<Boolean> releaseLock(String idempotencyKey) {
        String lockKey = LOCK_PREFIX + KEY_PREFIX + idempotencyKey;
        return redisTemplate.delete(lockKey).map(count -> count > 0);
    }
}
