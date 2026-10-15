package com.nexledger.ledger.api.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexledger.ledger.event.store.EventRepository;
import com.nexledger.ledger.query.projection.TransactionProjection;
import com.nexledger.ledger.query.projection.TransactionProjectionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.time.Instant;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class AuditController {

    private final EventRepository eventRepository;
    private final TransactionProjectionRepository transactionRepo;
    private final ObjectMapper objectMapper;

    record AuditEntry(Long id, UUID aggregateId, String aggregateType, String eventType,
                      JsonNode payload, long version, Instant createdAt) {}

    @GetMapping("/audit-log/{aggregateId}")
    public Flux<AuditEntry> getAuditLog(@PathVariable UUID aggregateId) {
        return eventRepository.findByAggregateId(aggregateId)
            .map(e -> {
                try {
                    JsonNode payload = objectMapper.readTree(e.payload().asString());
                    return new AuditEntry(e.id(), e.aggregateId(), e.aggregateType(),
                        e.eventType(), payload, e.version(), e.createdAt());
                } catch (Exception ex) {
                    return new AuditEntry(e.id(), e.aggregateId(), e.aggregateType(),
                        e.eventType(), objectMapper.createObjectNode(), e.version(), e.createdAt());
                }
            });
    }

    @GetMapping("/transactions/{accountId}")
    public Flux<TransactionProjection> getTransactions(
            @PathVariable UUID accountId,
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(defaultValue = "0") int offset) {
        return transactionRepo.findByAccountId(accountId, limit, offset);
    }
}
