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

import java.util.Map;

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

    public Mono<Void> publish(DomainEvent event) {
        String topic = resolveTopic(event.eventType());
        try {
            String payload = objectMapper.writeValueAsString(Map.of(
                "eventId", event.id() != null ? event.id() : 0,
                "aggregateId", event.aggregateId().toString(),
                "aggregateType", event.aggregateType(),
                "eventType", event.eventType(),
                "payload", event.payload(),
                "version", event.version(),
                "createdAt", event.createdAt().toString(),
                "correlationId", event.correlationId() != null ? event.correlationId().toString() : ""
            ));
            ProducerRecord<String, String> record =
                new ProducerRecord<>(topic, event.aggregateId().toString(), payload);
            SenderRecord<String, String, String> senderRecord =
                SenderRecord.create(record, event.aggregateId().toString());
            return kafkaSender.send(Mono.just(senderRecord))
                .doOnNext(r -> log.debug("Published event {} to topic {}", event.eventType(), topic))
                .doOnError(e -> log.error("Failed to publish event {}: {}", event.eventType(), e.getMessage()))
                .then();
        } catch (Exception e) {
            return Mono.error(e);
        }
    }

    private String resolveTopic(String eventType) {
        if (eventType.startsWith("Account")) return accountEventsTopic;
        if (eventType.startsWith("Transfer") || eventType.startsWith("Saga")) return sagaEventsTopic;
        return transactionEventsTopic;
    }
}
