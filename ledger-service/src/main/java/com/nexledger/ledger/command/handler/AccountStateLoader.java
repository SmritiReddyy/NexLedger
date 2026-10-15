package com.nexledger.ledger.command.handler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexledger.ledger.domain.AccountState;
import com.nexledger.ledger.event.store.EventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

/** Rebuilds an account from the event store, now or as of an earlier point. */
@Service
@RequiredArgsConstructor
public class AccountStateLoader {

    private final EventRepository eventRepository;
    private final ObjectMapper objectMapper;

    public Mono<AccountState> current(UUID accountId) {
        return eventRepository.findByAggregateId(accountId).collectList()
            .map(events -> AccountState.fold(accountId, events, objectMapper));
    }

    public Mono<AccountState> asOf(UUID accountId, Instant asOf) {
        return eventRepository.findByAggregateIdUpTo(accountId, asOf).collectList()
            .map(events -> AccountState.fold(accountId, events, objectMapper));
    }

    public Mono<AccountState> atVersion(UUID accountId, long version) {
        return eventRepository.findByAggregateIdUpToVersion(accountId, version).collectList()
            .map(events -> AccountState.fold(accountId, events, objectMapper));
    }
}
