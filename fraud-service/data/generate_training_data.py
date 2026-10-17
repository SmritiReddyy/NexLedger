"""
Generates synthetic fraud training data and trains the XGBoost model.
Run once: python data/generate_training_data.py
"""
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
import os

np.random.seed(42)
N = 50_000
FRAUD_RATE = 0.05

n_fraud = int(N * FRAUD_RATE)
n_legit = N - n_fraud


def derive_amount_features(df):
    """Compute amount-derived features exactly as app/ml_scorer.py does at serving time."""
    df["amount_norm"] = df["amount"] / 10000.0
    df["round_amount"] = ((df["amount"] >= 1000) & (df["amount"] % 100 == 0)).astype(float)
    return df


def generate_legit(n):
    df = pd.DataFrame({
        "amount": np.random.lognormal(mean=5.5, sigma=1.2, size=n),
        "amount_norm": np.zeros(n),
        "is_debit": np.random.binomial(1, 0.7, n).astype(float),
        "velocity_count_1h": np.random.poisson(0.5, n).astype(float),
        "velocity_amount_1h_norm": np.random.exponential(0.05, n),
        "velocity_count_24h": np.random.poisson(3, n).astype(float),
        "velocity_amount_24h_norm": np.random.exponential(0.2, n),
        "high_risk_country": np.random.binomial(1, 0.02, n).astype(float),
        "round_amount": np.zeros(n),
        "label": np.zeros(n, dtype=int),
    })
    return derive_amount_features(df)


def generate_fraud(n):
    df = pd.DataFrame({
        # half just under/over a reporting threshold, half suspiciously round amounts
        "amount": np.random.permutation(np.concatenate([
            np.random.uniform(9500, 12000, n // 2),
            np.random.choice([500.0, 1000.0, 2000.0, 5000.0, 10000.0], n - n // 2),
        ])),
        "amount_norm": np.ones(n),
        "is_debit": np.ones(n),
        "velocity_count_1h": np.random.poisson(8, n).astype(float),
        "velocity_amount_1h_norm": np.random.exponential(0.6, n),
        "velocity_count_24h": np.random.poisson(20, n).astype(float),
        "velocity_amount_24h_norm": np.random.exponential(0.8, n),
        "high_risk_country": np.random.binomial(1, 0.35, n).astype(float),
        "round_amount": np.ones(n),
        "label": np.ones(n, dtype=int),
    })
    return derive_amount_features(df)


df = pd.concat([generate_legit(n_legit), generate_fraud(n_fraud)], ignore_index=True)
df = df.sample(frac=1, random_state=42).reset_index(drop=True)

feature_cols = [c for c in df.columns if c != "label"]
X = df[feature_cols].values
y = df["label"].values

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

dtrain = xgb.DMatrix(X_train, label=y_train)
dtest = xgb.DMatrix(X_test, label=y_test)

params = {
    "objective": "binary:logistic",
    "eval_metric": "auc",
    "max_depth": 6,
    "eta": 0.1,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "scale_pos_weight": n_legit / n_fraud,
    "seed": 42,
}

model = xgb.train(
    params, dtrain,
    num_boost_round=150,
    evals=[(dtest, "test")],
    early_stopping_rounds=15,
    verbose_eval=10,
)

preds = model.predict(dtest)
auc = roc_auc_score(y_test, preds)
print(f"\nTest AUC: {auc:.4f}")
print(classification_report(y_test, (preds >= 0.5).astype(int)))

os.makedirs("models", exist_ok=True)
model.save_model("models/fraud_model.json")
print("Model saved to models/fraud_model.json")
