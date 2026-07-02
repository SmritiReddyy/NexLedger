package com.nexledger.ledger.query.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexledger.ledger.query.cache.AccountCacheService;
import com.nexledger.ledger.query.projection.AccountProjection;
import com.nexledger.ledger.query.projection.AccountProjectionRepository;
import com.nexledger.ledger.query.projection.TransactionProjection;
import com.nexledger.ledger.query.projection.TransactionProjectionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventProjectionConsumer {

    private final AccountProjectionRepository accountRepo;
    private final TransactionProjectionRepository transactionRepo;
    private final AccountCacheService cacheService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${nexledger.kafka.topics.account-events}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeAccountEvent(ConsumerRecord<String, String> record) {
        try {
            JsonNode envelope = objectMapper.readTree(record.value());
            String eventType = envelope.get("eventType").asText();
            JsonNode payload = envelope.get("payload");
            log.debug("Projecting event: {}", eventType);
            projectEvent(eventType, payload).subscribe(
                v -> {},
                e -> log.error("Projection failed for event {}: {}", eventType, e.getMessage())
            );
        } catch (Exception e) {
            log.error("Failed to deserialize event: {}", e.getMessage());
        }
    }

    @KafkaListener(topics = "${nexledger.kafka.topics.transaction-events}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeTransactionEvent(ConsumerRecord<String, String> record) {
        consumeAccountEvent(record);
    }

    private Mono<Void> projectEvent(String eventType, JsonNode payload) {
        return switch (eventType) {
            case "AccountCreated" -> projectAccountCreated(payload);
            case "AccountDebited" -> projectAccountDebited(payload);
            case "AccountCredited" -> projectAccountCredited(payload);
            default -> Mono.empty();
        };
    }

    private Mono<Void> projectAccountCreated(JsonNode payload) {
        UUID accountId = UUID.fromString(payload.get("accountId").asText());
        AccountProjection projection = new AccountProjection(
            accountId,
            payload.get("ownerId").asText(),
            payload.get("currency").asText(),
            new BigDecimal(payload.get("initialBalance").asText()),
            "ACTIVE",
            1L
        );
        return accountRepo.save(projection)
            .flatMap(saved -> cacheService.evict(accountId))
            .then();
    }

    private Mono<Void> projectAccountDebited(JsonNode payload) {
        UUID accountId = UUID.fromString(payload.get("accountId").asText());
        UUID transactionId = UUID.fromString(payload.get("transactionId").asText());
        BigDecimal amount = new BigDecimal(payload.get("amount").asText());
        String currency = payload.get("currency").asText();
        long version = payload.has("version") ? payload.get("version").asLong() : 0L;

        TransactionProjection tx = new TransactionProjection(
            transactionId, accountId, "DEBIT", amount, currency, "COMPLETED",
            payload.has("reference") ? payload.get("reference").asText() : null,
            payload.has("idempotencyKey") ? payload.get("idempotencyKey").asText() : null,
            payload.has("fraudScore") ? payload.get("fraudScore").decimalValue() : null
        );

        return transactionRepo.save(tx)
            .then(accountRepo.updateBalance(accountId, amount.negate(), version))
            .then(cacheService.evict(accountId))
            .then();
    }

    private Mono<Void> projectAccountCredited(JsonNode payload) {
        UUID accountId = UUID.fromString(payload.get("accountId").asText());
        UUID transactionId = UUID.fromString(payload.get("transactionId").asText());
        BigDecimal amount = new BigDecimal(payload.get("amount").asText());
        String currency = payload.get("currency").asText();
        long version = payload.has("version") ? payload.get("version").asLong() : 0L;

        TransactionProjection tx = new TransactionProjection(
            transactionId, accountId, "CREDIT", amount, currency, "COMPLETED",
            payload.has("reference") ? payload.get("reference").asText() : null,
            payload.has("idempotencyKey") ? payload.get("idempotencyKey").asText() : null,
            null
        );

        return transactionRepo.save(tx)
            .then(accountRepo.updateBalance(accountId, amount, version))
            .then(cacheService.evict(accountId))
            .then();
    }
}
