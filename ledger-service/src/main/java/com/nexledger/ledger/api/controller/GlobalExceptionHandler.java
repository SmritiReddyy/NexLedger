package com.nexledger.ledger.api.controller;

import com.nexledger.ledger.command.handler.*;
import com.nexledger.ledger.concurrency.LockTimeoutException;
import com.nexledger.ledger.event.store.DuplicateEventException;
import com.nexledger.ledger.event.store.OptimisticConcurrencyException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import reactor.core.publisher.Mono;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(AccountNotFoundException.class)
    public Mono<ProblemDetail> handleNotFound(AccountNotFoundException e) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, e.getMessage());
        pd.setTitle("Account Not Found");
        return Mono.just(pd);
    }

    @ExceptionHandler(InsufficientFundsException.class)
    public Mono<ProblemDetail> handleInsufficientFunds(InsufficientFundsException e) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, e.getMessage());
        pd.setTitle("Insufficient Funds");
        return Mono.just(pd);
    }

    @ExceptionHandler(DuplicateRequestException.class)
    public Mono<ProblemDetail> handleDuplicate(DuplicateRequestException e) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
        pd.setTitle("Duplicate Request");
        return Mono.just(pd);
    }

    @ExceptionHandler(FraudBlockedException.class)
    public Mono<ProblemDetail> handleFraudBlocked(FraudBlockedException e) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.FORBIDDEN, e.getMessage());
        pd.setTitle("Transaction Blocked");
        pd.setProperty("fraudScore", e.getScore());
        return Mono.just(pd);
    }

    @ExceptionHandler(OptimisticConcurrencyException.class)
    public Mono<ProblemDetail> handleConcurrency(OptimisticConcurrencyException e) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
        pd.setTitle("Concurrency Conflict");
        return Mono.just(pd);
    }

    @ExceptionHandler(DuplicateEventException.class)
    public Mono<ProblemDetail> handleDuplicateEvent(DuplicateEventException e) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, e.getMessage());
        pd.setTitle("Duplicate Request");
        return Mono.just(pd);
    }

    @ExceptionHandler(LockTimeoutException.class)
    public Mono<ProblemDetail> handleLockTimeout(LockTimeoutException e) {
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
        pd.setTitle("Account Busy");
        return Mono.just(pd);
    }

    @ExceptionHandler(Exception.class)
    public Mono<ProblemDetail> handleGeneric(Exception e) {
        log.error("Unhandled exception", e);
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred");
        pd.setTitle("Internal Server Error");
        return Mono.just(pd);
    }
}
