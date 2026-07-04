package com.nexledger.ledger.api.controller;

import com.nexledger.ledger.api.dto.TransferRequest;
import com.nexledger.ledger.command.model.TransferFundsCommand;
import com.nexledger.ledger.saga.TransferSaga;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/transfers")
@RequiredArgsConstructor
public class TransferController {

    private final TransferSaga transferSaga;

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public Mono<Map<String, String>> transfer(
            @Valid @RequestBody TransferRequest req,
            @RequestHeader("Idempotency-Key") String idempotencyKey) {
        TransferFundsCommand cmd = new TransferFundsCommand(
            req.sourceAccountId(), req.targetAccountId(),
            req.amount(), req.currency(), idempotencyKey, req.reference());
        return transferSaga.execute(cmd)
            .map(sagaId -> Map.of("sagaId", sagaId.toString(), "status", "ACCEPTED"));
    }
}
