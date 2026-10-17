package com.nexledger.ledger;

import com.nexledger.ledger.fraud.FraudAssessment;
import com.nexledger.ledger.fraud.FraudClient;
import com.nexledger.ledger.saga.SagaHooks;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

/**
 * Boots the whole service against real Postgres, Redis and Kafka containers,
 * started once for the test run. The fraud service is the only stub.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
    "nexledger.outbox.poll-ms=20",
    "nexledger.saga.recovery-interval-ms=3600000",   // tests trigger recovery explicitly
    "nexledger.saga.stuck-after-ms=0",
})
@AutoConfigureWebTestClient(timeout = "30s")
@Import(IntegrationTestBase.Hooks.class)   // nested @TestConfiguration is only auto-detected on the test class itself
public abstract class IntegrationTestBase {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
        .withDatabaseName("nexledger").withUsername("nexledger").withPassword("nexledger");
    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);
    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    static {
        POSTGRES.start();
        REDIS.start();
        KAFKA.start();
    }

    @DynamicPropertySource
    static void containers(DynamicPropertyRegistry r) {
        String hostPort = POSTGRES.getHost() + ":" + POSTGRES.getMappedPort(5432) + "/nexledger";
        r.add("spring.r2dbc.url", () -> "r2dbc:postgresql://" + hostPort);
        r.add("spring.flyway.url", () -> "jdbc:postgresql://" + hostPort);
        r.add("spring.data.redis.host", REDIS::getHost);
        r.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
        r.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    /** Lets a test inject a fault between saga steps. */
    static final AtomicReference<Function<UUID, Mono<Void>>> AFTER_DEBIT = new AtomicReference<>(id -> Mono.empty());

    @TestConfiguration
    static class Hooks {
        @Bean
        SagaHooks sagaHooks() {
            return new SagaHooks() {
                @Override
                public Mono<Void> afterDebit(UUID sagaId) {
                    return AFTER_DEBIT.get().apply(sagaId);
                }
            };
        }
    }

    @MockBean
    FraudClient fraudClient;

    @Autowired
    WebTestClient web;

    @BeforeEach
    void allowAllTransactions() {
        when(fraudClient.assess(any(), any(), any(), any(), any())).thenReturn(Mono.just(FraudAssessment.allowed(0.05)));
        AFTER_DEBIT.set(id -> Mono.empty());
    }

    UUID createAccount(String balance) {
        return UUID.fromString(web.post().uri("/api/v1/accounts")
            .bodyValue(java.util.Map.of("ownerId", "owner-" + UUID.randomUUID(), "currency", "USD",
                "initialBalance", balance))
            .exchange().expectStatus().isCreated()
            .expectBody(java.util.Map.class).returnResult().getResponseBody().get("accountId").toString());
    }

    WebTestClient.ResponseSpec debit(UUID account, String amount, String key) {
        return web.post().uri("/api/v1/accounts/{id}/debit", account).header("Idempotency-Key", key)
            .bodyValue(java.util.Map.of("amount", amount, "currency", "USD", "reference", "test")).exchange();
    }

    WebTestClient.ResponseSpec credit(UUID account, String amount, String key) {
        return web.post().uri("/api/v1/accounts/{id}/credit", account).header("Idempotency-Key", key)
            .bodyValue(java.util.Map.of("amount", amount, "currency", "USD", "reference", "test")).exchange();
    }

    java.math.BigDecimal replayedBalance(UUID account, String query) {
        Object b = web.get().uri("/api/v1/accounts/{id}/balance" + query, account).exchange()
            .expectStatus().isOk().expectBody(java.util.Map.class).returnResult().getResponseBody().get("balance");
        return new java.math.BigDecimal(b.toString());
    }

    static final Duration SETTLE = Duration.ofSeconds(15);
}
