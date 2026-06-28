package com.nexledger.ledger.fraud;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Component
public class FraudClient {

    private final WebClient webClient;
    private final double scoreThreshold;
    private final long timeoutMs;

    public FraudClient(WebClient.Builder webClientBuilder,
                       @Value("${nexledger.fraud.service-url}") String fraudServiceUrl,
                       @Value("${nexledger.fraud.score-threshold:0.75}") double scoreThreshold,
                       @Value("${nexledger.fraud.timeout-ms:500}") long timeoutMs) {
        this.webClient = webClientBuilder.baseUrl(fraudServiceUrl).build();
        this.scoreThreshold = scoreThreshold;
        this.timeoutMs = timeoutMs;
    }

    public Mono<FraudAssessment> assess(UUID accountId, BigDecimal amount, String currency,
                                         String transactionType, String idempotencyKey) {
        Map<String, Object> request = Map.of(
            "account_id", accountId.toString(),
            "amount", amount,
            "currency", currency,
            "transaction_type", transactionType,
            "idempotency_key", idempotencyKey
        );
        return webClient.post()
            .uri("/score")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .retrieve()
            .bodyToMono(FraudAssessment.class)
            .timeout(Duration.ofMillis(timeoutMs))
            .onErrorResume(e -> {
                log.warn("Fraud service unavailable, allowing transaction: {}", e.getMessage());
                return Mono.just(FraudAssessment.allowed(0.0));
            });
    }

    public boolean isBlocked(FraudAssessment assessment) {
        return assessment.score() >= scoreThreshold;
    }
}
