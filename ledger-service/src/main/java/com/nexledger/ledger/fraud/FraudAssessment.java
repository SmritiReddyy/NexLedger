package com.nexledger.ledger.fraud;

import com.fasterxml.jackson.annotation.JsonProperty;

public record FraudAssessment(
    double score,
    String decision,
    String reason,
    @JsonProperty("rule_triggered") String ruleTriggered
) {
    public static FraudAssessment allowed(double score) {
        return new FraudAssessment(score, "ALLOW", "Passed all checks", null);
    }

    public boolean isBlocked() {
        return "BLOCK".equals(decision);
    }
}
