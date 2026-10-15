package com.nexledger.ledger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexledger.ledger.domain.AccountState;
import com.nexledger.ledger.event.store.DomainEvent;
import io.r2dbc.postgresql.codec.Json;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The fold the write side and point-in-time replay are built on. */
class AccountCommandHandlerTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final UUID id = UUID.randomUUID();

    private DomainEvent event(String type, long version, String json) {
        return new DomainEvent((long) version, id, "Account", type, Json.of(json), version,
            Instant.parse("2026-01-01T00:00:00Z").plusSeconds(version), null, null);
    }

    @Test
    void foldsCreateDebitAndCreditIntoTheBalance() {
        AccountState s = AccountState.fold(id, List.of(
            event("AccountCreated", 0, "{\"ownerId\":\"o\",\"currency\":\"USD\",\"initialBalance\":\"100.00\"}"),
            event("AccountDebited", 1, "{\"amount\":\"30.50\"}"),
            event("AccountCredited", 2, "{\"amount\":\"10.25\"}")), mapper);

        assertThat(s.exists()).isTrue();
        assertThat(s.balance()).isEqualByComparingTo(new BigDecimal("79.75"));
        assertThat(s.version()).isEqualTo(2);
        assertThat(s.eventsApplied()).isEqualTo(3);
    }

    @Test
    void anEmptyStreamIsAnAccountThatDoesNotExist() {
        AccountState s = AccountState.fold(id, List.of(), mapper);
        assertThat(s.exists()).isFalse();
        assertThat(s.version()).isEqualTo(-1);
    }

    @Test
    void aGapInTheStreamIsRejectedRatherThanSilentlyFolded() {
        assertThatThrownBy(() -> AccountState.fold(id, List.of(
            event("AccountCreated", 0, "{\"ownerId\":\"o\",\"currency\":\"USD\",\"initialBalance\":\"1\"}"),
            event("AccountDebited", 2, "{\"amount\":\"1\"}")), mapper))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("gap");
    }
}
