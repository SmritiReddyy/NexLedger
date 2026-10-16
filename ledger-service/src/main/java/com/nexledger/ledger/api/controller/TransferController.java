package com.nexledger.ledger.api.controller;

import com.nexledger.ledger.api.dto.TransferRequest;
import com.nexledger.ledger.command.model.TransferFundsCommand;
import com.nexledger.ledger.saga.SagaStateRepository;
import com.nexledger.ledger.saga.TransferSaga;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import org.springframework.http.ResponseEntity;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/transfers")
@RequiredArgsConstructor
public class TransferController {

    private final TransferSaga transferSaga;
    private final SagaStateRepository sagas;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<Map<String, String>> transfer(
            @Valid @RequestBody TransferRequest req,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {
        TransferFundsCommand cmd = new TransferFundsCommand(
            req.sourceAccountId(), req.targetAccountId(),
            req.amount(), req.currency(), idempotencyKey, req.reference());
        return transferSaga.execute(cmd)
            .map(r -> Map.of("sagaId", r.sagaId().toString(), "status", r.state()));
    }

    @GetMapping("/{sagaId}")
    public Mono<ResponseEntity<Map<String, String>>> status(@PathVariable UUID sagaId) {
        return sagas.findById(sagaId)
            .map(s -> ResponseEntity.ok(Map.of("sagaId", sagaId.toString(), "status", s.getState(),
                "updatedAt", s.getUpdatedAt().toString())))
            .defaultIfEmpty(ResponseEntity.notFound().build());
    }
}
