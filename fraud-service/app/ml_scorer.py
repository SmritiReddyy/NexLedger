import numpy as np
import xgboost as xgb
import logging
import os
from decimal import Decimal
from typing import Optional

from app.models import ScoreRequest

logger = logging.getLogger(__name__)

MODEL_PATH = os.getenv("MODEL_PATH", "models/fraud_model.json")


class MLScorer:
    def __init__(self):
        self.model: Optional[xgb.Booster] = None
        self._load_model()

    def _load_model(self):
        if os.path.exists(MODEL_PATH):
            try:
                self.model = xgb.Booster()
                self.model.load_model(MODEL_PATH)
                logger.info("XGBoost fraud model loaded from %s", MODEL_PATH)
            except Exception as e:
                logger.warning("Failed to load model: %s — using heuristic fallback", e)
                self.model = None
        else:
            logger.info("No model file found at %s — using heuristic scoring", MODEL_PATH)

    def predict(self, req: ScoreRequest, velocity: dict) -> float:
        features = self._extract_features(req, velocity)
        if self.model is not None:
            try:
                dmatrix = xgb.DMatrix(np.array([features]))
                score = float(self.model.predict(dmatrix)[0])
                return max(0.0, min(1.0, score))
            except Exception as e:
                logger.warning("Model prediction failed: %s — using heuristic", e)
        return self._heuristic_score(features)

    def _extract_features(self, req: ScoreRequest, velocity: dict) -> list:
        amount = float(req.amount)
        return [
            amount,
            amount / 10000.0,                          # normalized amount
            1.0 if req.transaction_type == "DEBIT" else 0.0,
            float(velocity.get("count_1h", 0)),
            float(velocity.get("amount_1h", 0)) / 50000.0,
            float(velocity.get("count_24h", 0)),
            float(velocity.get("amount_24h", 0)) / 100000.0,
            1.0 if req.merchant_country in {"NG", "VN", "PK", "MM"} else 0.0,
            1.0 if amount >= 1000 and amount % 100 == 0 else 0.0,
        ]

    def _heuristic_score(self, features: list) -> float:
        amount_norm = features[1]
        velocity_count_norm = features[3] / 10.0
        velocity_amount_norm = features[4]
        high_risk_country = features[7]
        round_amount = features[8]

        score = (
            0.3 * min(amount_norm, 1.0) +
            0.25 * min(velocity_count_norm, 1.0) +
            0.2 * min(velocity_amount_norm, 1.0) +
            0.15 * high_risk_country +
            0.1 * round_amount
        )
        return max(0.0, min(1.0, score))
