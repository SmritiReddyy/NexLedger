package com.nexledger.ledger.api.controller;

import com.nexledger.ledger.event.store.DomainEvent;
import com.nexledger.ledger.event.store.EventRepository;
import com.nexledger.ledger.query.projection.TransactionProjection;
import com.nexledger.ledger.query.projection.TransactionProjectionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;

import java.util.UUID;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
public class AuditController {

    private final EventRepository eventRepository;
    private final TransactionProjectionRepository transactionRepo;

    @GetMapping("/audit-log/{aggregateId}")
    public Flux<DomainEvent> getAuditLog(@PathVariable UUID aggregateId) {
        return eventRepository.findByAggregateId(aggregateId);
    }

    @GetMapping("/transactions/{accountId}")
    public Flux<TransactionProjection> getTransactions(
            @PathVariable UUID accountId,
            @RequestParam(defaultValue = "50") int limit,
            @RequestParam(defaultValue = "0") int offset) {
        return transactionRepo.findByAccountId(accountId, limit, offset);
    }
}
