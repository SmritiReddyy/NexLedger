package com.nexledger.ledger.command.handler;

public class FraudBlockedException extends RuntimeException {
    private final double score;

    public FraudBlockedException(String reason, double score) {
        super("Transaction blocked by fraud detection: " + reason + " (score=" + score + ")");
        this.score = score;
    }

    public double getScore() {
        return score;
    }
}
