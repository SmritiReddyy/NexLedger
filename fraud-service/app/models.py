from pydantic import BaseModel, Field
from decimal import Decimal
from typing import Optional


class ScoreRequest(BaseModel):
    account_id: str
    amount: Decimal
    currency: str = "USD"
    transaction_type: str  # DEBIT, CREDIT
    idempotency_key: str
    merchant_country: Optional[str] = None
    ip_address: Optional[str] = None


class ScoreResponse(BaseModel):
    score: float = Field(ge=0.0, le=1.0)
    decision: str  # ALLOW or BLOCK
    reason: str
    rule_triggered: Optional[str] = None
