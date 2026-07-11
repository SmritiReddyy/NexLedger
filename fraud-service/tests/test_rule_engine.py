import pytest
from decimal import Decimal
from app.models import ScoreRequest
from app.rule_engine import RuleEngine


@pytest.fixture
def engine():
    return RuleEngine()


def make_request(**kwargs):
    defaults = {
        "account_id": "test-account",
        "amount": Decimal("100"),
        "currency": "USD",
        "transaction_type": "DEBIT",
        "idempotency_key": "test-key",
    }
    defaults.update(kwargs)
    return ScoreRequest(**defaults)


def test_large_amount_blocked(engine):
    req = make_request(amount=Decimal("15000"))
    result = engine.evaluate(req, {})
    assert result.block is True
    assert result.rule == "LARGE_AMOUNT"


def test_velocity_count_blocked(engine):
    req = make_request(amount=Decimal("100"))
    velocity = {"count_1h": 10, "amount_1h": 1000.0}
    result = engine.evaluate(req, velocity)
    assert result.block is True
    assert result.rule == "VELOCITY_COUNT"


def test_normal_transaction_allowed(engine):
    req = make_request(amount=Decimal("50"))
    result = engine.evaluate(req, {})
    assert result.block is False


def test_high_risk_country_elevated_but_not_blocked(engine):
    req = make_request(amount=Decimal("200"), merchant_country="NG")
    result = engine.evaluate(req, {})
    assert result.block is False
    assert result.risk_score >= 0.5


def test_velocity_amount_blocked(engine):
    req = make_request(amount=Decimal("5000"))
    velocity = {"count_1h": 2, "amount_1h": 46000.0}
    result = engine.evaluate(req, velocity)
    assert result.block is True
    assert result.rule == "VELOCITY_AMOUNT"
