package com.nexledger.ledger.domain;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexledger.ledger.event.store.DomainEvent;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * An account rebuilt by folding its events in version order.
 *
 * This is the write side's source of truth: commands validate against the
 * folded state, never the read-side projection (which lags behind Kafka). The
 * same fold over a prefix of the stream answers "what was the balance at time
 * T", which is what point-in-time replay is.
 *
 * version is the version of the last applied event, -1 if there are none.
 */
public record AccountState(
    UUID accountId,
    boolean exists,
    String ownerId,
    String currency,
    BigDecimal balance,
    long version,
    int eventsApplied,
    Instant lastEventAt
) {
    public static AccountState empty(UUID accountId) {
        return new AccountState(accountId, false, null, null, BigDecimal.ZERO, -1L, 0, null);
    }

    public static AccountState fold(UUID accountId, List<DomainEvent> events, ObjectMapper mapper) {
        AccountState s = empty(accountId);
        for (DomainEvent e : events) {
            s = s.apply(e, mapper);
        }
        return s;
    }

    public AccountState apply(DomainEvent e, ObjectMapper mapper) {
        if (e.version() != version + 1) {
            throw new IllegalStateException("Event stream for " + accountId + " has a gap: expected version "
                + (version + 1) + " but got " + e.version());
        }
        JsonNode p = payload(e, mapper);
        return switch (e.eventType()) {
            case "AccountCreated" -> new AccountState(accountId, true, p.get("ownerId").asText(),
                p.get("currency").asText(), new BigDecimal(p.get("initialBalance").asText()),
                e.version(), eventsApplied + 1, e.createdAt());
            case "AccountDebited" -> withBalance(balance.subtract(new BigDecimal(p.get("amount").asText())), e);
            case "AccountCredited" -> withBalance(balance.add(new BigDecimal(p.get("amount").asText())), e);
            default -> new AccountState(accountId, exists, ownerId, currency, balance,
                e.version(), eventsApplied + 1, e.createdAt());
        };
    }

    private AccountState withBalance(BigDecimal newBalance, DomainEvent e) {
        return new AccountState(accountId, exists, ownerId, currency, newBalance,
            e.version(), eventsApplied + 1, e.createdAt());
    }

    private static JsonNode payload(DomainEvent e, ObjectMapper mapper) {
        try {
            return mapper.readTree(e.payload().asString());
        } catch (Exception ex) {
            throw new IllegalStateException("Unreadable payload on event " + e.id(), ex);
        }
    }
}
