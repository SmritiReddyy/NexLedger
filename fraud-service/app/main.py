from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
import logging

from app.models import ScoreRequest, ScoreResponse
from app.rule_engine import RuleEngine
from app.ml_scorer import MLScorer
from app.velocity_tracker import VelocityTracker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="NexLedger Fraud Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

Instrumentator().instrument(app).expose(app)

rule_engine = RuleEngine()
ml_scorer = MLScorer()
velocity_tracker = VelocityTracker()


@app.get("/health")
async def health():
    return {"status": "UP", "service": "fraud-service"}


@app.post("/score", response_model=ScoreResponse)
async def score_transaction(req: ScoreRequest) -> ScoreResponse:
    logger.info(f"Scoring transaction for account {req.account_id}, amount={req.amount}")

    # Velocity check (Redis-backed)
    velocity = await velocity_tracker.get_velocity(req.account_id)

    # Rule engine — fast deterministic checks
    rule_result = rule_engine.evaluate(req, velocity)
    if rule_result.block:
        logger.warning(f"Rule block: {rule_result.rule} for account {req.account_id}")
        return ScoreResponse(
            score=1.0,
            decision="BLOCK",
            reason=rule_result.reason,
            rule_triggered=rule_result.rule,
        )

    # ML model scoring
    ml_score = ml_scorer.predict(req, velocity)

    # Combine: rule engine provides floor, ML model provides nuance
    final_score = max(rule_result.risk_score, ml_score)

    decision = "BLOCK" if final_score >= 0.75 else "ALLOW"
    reason = "ML model threshold exceeded" if decision == "BLOCK" else "Passed all checks"

    await velocity_tracker.record(req.account_id, float(req.amount))

    return ScoreResponse(
        score=round(final_score, 4),
        decision=decision,
        reason=reason,
        rule_triggered=rule_result.rule if rule_result.rule else None,
    )
