from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from app.models import ScoreRequest


@dataclass
class RuleResult:
    block: bool
    rule: Optional[str]
    reason: str
    risk_score: float


LARGE_AMOUNT_THRESHOLD = Decimal("10000")
VELOCITY_LIMIT_COUNT = 10
VELOCITY_LIMIT_AMOUNT = Decimal("50000")
HIGH_RISK_COUNTRIES = {"NG", "VN", "PK", "MM"}


class RuleEngine:
    def evaluate(self, req: ScoreRequest, velocity: dict) -> RuleResult:
        # Rule 1: Large single transaction
        if req.amount > LARGE_AMOUNT_THRESHOLD:
            return RuleResult(
                block=True,
                rule="LARGE_AMOUNT",
                reason=f"Transaction amount {req.amount} exceeds threshold {LARGE_AMOUNT_THRESHOLD}",
                risk_score=0.9,
            )

        # Rule 2: Velocity — too many transactions in window
        tx_count = velocity.get("count_1h", 0)
        if tx_count >= VELOCITY_LIMIT_COUNT:
            return RuleResult(
                block=True,
                rule="VELOCITY_COUNT",
                reason=f"Account has {tx_count} transactions in last hour (limit={VELOCITY_LIMIT_COUNT})",
                risk_score=0.95,
            )

        # Rule 3: Velocity — total amount in window
        total_amount = Decimal(str(velocity.get("amount_1h", 0)))
        if total_amount + req.amount > VELOCITY_LIMIT_AMOUNT:
            return RuleResult(
                block=True,
                rule="VELOCITY_AMOUNT",
                reason=f"Cumulative amount {total_amount + req.amount} exceeds {VELOCITY_LIMIT_AMOUNT} per hour",
                risk_score=0.9,
            )

        # Rule 4: High-risk merchant country
        if req.merchant_country and req.merchant_country in HIGH_RISK_COUNTRIES:
            return RuleResult(
                block=False,
                rule="HIGH_RISK_COUNTRY",
                reason=f"Merchant country {req.merchant_country} is elevated risk",
                risk_score=0.5,
            )

        # Rule 5: Suspicious amount pattern (round large amounts)
        if req.amount >= Decimal("1000") and req.amount % Decimal("100") == 0:
            risk_boost = 0.2
        else:
            risk_boost = 0.0

        return RuleResult(block=False, rule=None, reason="Passed all rules", risk_score=risk_boost)
