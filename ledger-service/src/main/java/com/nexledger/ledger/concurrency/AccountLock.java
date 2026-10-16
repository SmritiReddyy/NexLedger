package com.nexledger.ledger.concurrency;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Per-account distributed lock in Redis (SET key token NX PX lease).
 *
 * It serialises writes to one account across every ledger instance, so
 * concurrent debits queue instead of all reading the same version and all but
 * one failing the optimistic check. Release is a compare-and-delete in Lua, so
 * an instance whose lease expired can never delete a lock someone else now
 * holds. The lock is an efficiency measure, not the safety guarantee: if a
 * lease expires mid-write, the event store's version check still rejects the
 * stale writer.
 */
@Slf4j
@Component
public class AccountLock {

    private static final RedisScript<Long> RELEASE = RedisScript.of(
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
        Long.class);

    private final ReactiveRedisTemplate<String, String> redis;
    private final Duration lease;
    private final Duration maxWait;

    public AccountLock(ReactiveRedisTemplate<String, String> redis,
                       @Value("${nexledger.lock.lease-ms:5000}") long leaseMs,
                       @Value("${nexledger.lock.max-wait-ms:3000}") long maxWaitMs) {
        this.redis = redis;
        this.lease = Duration.ofMillis(leaseMs);
        this.maxWait = Duration.ofMillis(maxWaitMs);
    }

    public <T> Mono<T> withLock(UUID accountId, Supplier<Mono<T>> body) {
        String key = "lock:account:" + accountId;
        return Mono.usingWhen(
            acquire(key),
            token -> body.get(),
            token -> release(key, token),
            (token, err) -> release(key, token),
            token -> release(key, token));
    }

    private Mono<String> acquire(String key) {
        String token = UUID.randomUUID().toString();
        return redis.opsForValue().setIfAbsent(key, token, lease)
            .flatMap(ok -> ok ? Mono.just(token) : Mono.error(new LockBusy()))
            .retryWhen(Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(2))
                .maxBackoff(Duration.ofMillis(25))
                .jitter(0.5)
                .filter(LockBusy.class::isInstance))
            .timeout(maxWait)
            .onErrorMap(java.util.concurrent.TimeoutException.class,
                e -> new LockTimeoutException("Account is busy, retry shortly: " + key));
    }

    private Mono<Void> release(String key, String token) {
        return redis.execute(RELEASE, List.of(key), List.of(token)).then();
    }

    private static final class LockBusy extends RuntimeException {
        LockBusy() { super(null, null, false, false); }
    }
}
