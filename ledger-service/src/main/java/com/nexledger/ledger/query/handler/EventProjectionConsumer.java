package com.nexledger.ledger.query.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexledger.ledger.query.cache.AccountCacheService;
import com.nexledger.ledger.query.projection.AccountProjectionRepository;
import com.nexledger.ledger.query.projection.TransactionProjectionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.UUID;

/**
 * Builds the read models from the event stream.
 *
 * Each record is projected to completion before the listener returns, so the
 * container only commits the offset of an event whose projection is saved. A
 * failure is rethrown and the record is redelivered. Every write is idempotent
 * on event version, so redelivery and full replays leave the read model unchanged.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class EventProjectionConsumer {

    private static final Duration PROJECTION_TIMEOUT = Duration.ofSeconds(10);

    private final AccountProjectionRepository accountRepo;
    private final TransactionProjectionRepository transactionRepo;
    private final AccountCacheService cacheService;
    private final ObjectMapper objectMapper;

    @KafkaListener(topics = "${nexledger.kafka.topics.account-events}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeAccountEvent(ConsumerRecord<String, String> record) throws IOException {
        JsonNode envelope = objectMapper.readTree(record.value());
        String eventType = envelope.get("eventType").asText();
        long version = envelope.get("version").asLong();
        log.debug("Projecting {} v{}", eventType, version);
        project(eventType, envelope.get("payload"), version).block(PROJECTION_TIMEOUT);
    }

    @KafkaListener(topics = "${nexledger.kafka.topics.transaction-events}", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeTransactionEvent(ConsumerRecord<String, String> record) throws IOException {
        consumeAccountEvent(record);
    }

    Mono<Void> project(String eventType, JsonNode payload, long version) {
        return switch (eventType) {
            case "AccountCreated" -> projectAccountCreated(payload, version);
            case "AccountDebited" -> projectMovement(payload, version, "DEBIT");
            case "AccountCredited" -> projectMovement(payload, version, "CREDIT");
            default -> Mono.empty();
        };
    }

    private Mono<Void> projectAccountCreated(JsonNode payload, long version) {
        UUID accountId = UUID.fromString(payload.get("accountId").asText());
        return accountRepo.insertIfAbsent(accountId, payload.get("ownerId").asText(), payload.get("currency").asText(),
                new BigDecimal(payload.get("initialBalance").asText()), version)
            .then(cacheService.evict(accountId));
    }

    private Mono<Void> projectMovement(JsonNode payload, long version, String type) {
        UUID accountId = UUID.fromString(payload.get("accountId").asText());
        BigDecimal amount = new BigDecimal(payload.get("amount").asText());
        BigDecimal delta = "DEBIT".equals(type) ? amount.negate() : amount;
        return transactionRepo.insertIfAbsent(
                UUID.fromString(payload.get("transactionId").asText()), accountId, type, amount,
                payload.get("currency").asText(), text(payload, "reference"), text(payload, "idempotencyKey"),
                payload.has("fraudScore") ? payload.get("fraudScore").decimalValue() : null)
            .then(accountRepo.applyDelta(accountId, delta, version))
            .then(cacheService.evict(accountId));
    }

    private static String text(JsonNode node, String field) {
        return node.hasNonNull(field) ? node.get(field).asText() : null;
    }
}
