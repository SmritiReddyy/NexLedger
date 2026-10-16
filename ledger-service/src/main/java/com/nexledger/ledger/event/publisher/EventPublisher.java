package com.nexledger.ledger.event.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexledger.ledger.event.store.DomainEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventPublisher {

    private final KafkaSender<String, String> kafkaSender;
    private final ObjectMapper objectMapper;

    @Value("${nexledger.kafka.topics.account-events}")
    private String accountEventsTopic;

    @Value("${nexledger.kafka.topics.transaction-events}")
    private String transactionEventsTopic;

    @Value("${nexledger.kafka.topics.saga-events}")
    private String sagaEventsTopic;

    /** The JSON envelope consumers receive: event metadata plus its payload. */
    public String envelope(DomainEvent event) {
        try {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("eventId", event.id() != null ? event.id() : 0);
            m.put("aggregateId", event.aggregateId().toString());
            m.put("aggregateType", event.aggregateType());
            m.put("eventType", event.eventType());
            m.put("payload", objectMapper.readTree(event.payload().asString()));
            m.put("version", event.version());
            m.put("createdAt", event.createdAt().toString());
            m.put("correlationId", event.correlationId() != null ? event.correlationId().toString() : "");
            return objectMapper.writeValueAsString(m);
        } catch (Exception e) {
            throw new IllegalStateException("Could not serialise event " + event.id(), e);
        }
    }

    /** Publish an already-built envelope; keyed by aggregate so one account's events stay in order. */
    public Mono<Void> send(String topic, UUID aggregateId, String envelopeJson) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, aggregateId.toString(), envelopeJson);
        return kafkaSender.send(Mono.just(SenderRecord.create(record, aggregateId.toString())))
            .next()
            .flatMap(result -> result.exception() == null ? Mono.empty() : Mono.<Void>error(result.exception()))
            .doOnError(e -> log.error("Failed to publish to {}: {}", topic, e.getMessage()))
            .then();
    }

    public String resolveTopic(String eventType) {
        if (eventType.startsWith("Account")) return accountEventsTopic;
        if (eventType.startsWith("Transfer") || eventType.startsWith("Saga")) return sagaEventsTopic;
        return transactionEventsTopic;
    }
}
