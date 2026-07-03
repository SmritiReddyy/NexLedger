package com.nexledger.ledger.query.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexledger.ledger.query.projection.AccountProjection;
import com.nexledger.ledger.query.projection.AccountProjectionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AccountCacheService {

    private static final String CACHE_PREFIX = "account:";
    private static final Duration CACHE_TTL = Duration.ofMinutes(5);

    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final AccountProjectionRepository accountRepo;
    private final ObjectMapper objectMapper;

    public Mono<AccountProjection> get(UUID accountId) {
        String key = CACHE_PREFIX + accountId;
        return redisTemplate.opsForValue().get(key)
            .flatMap(json -> {
                try {
                    return Mono.just(objectMapper.readValue(json, AccountProjection.class));
                } catch (Exception e) {
                    return Mono.empty();
                }
            })
            .switchIfEmpty(accountRepo.findById(accountId)
                .flatMap(account -> cache(account).thenReturn(account)));
    }

    public Mono<Void> cache(AccountProjection account) {
        String key = CACHE_PREFIX + account.getAccountId();
        try {
            String json = objectMapper.writeValueAsString(account);
            return redisTemplate.opsForValue().set(key, json, CACHE_TTL).then();
        } catch (Exception e) {
            log.warn("Failed to cache account {}: {}", account.getAccountId(), e.getMessage());
            return Mono.empty();
        }
    }

    public Mono<Void> evict(UUID accountId) {
        String key = CACHE_PREFIX + accountId;
        return redisTemplate.delete(key).then()
            .doOnSuccess(v -> log.debug("Cache evicted for account {}", accountId));
    }
}
