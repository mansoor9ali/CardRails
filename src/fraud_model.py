"""LightGBM-backed risk scorer with a deterministic stub fallback.

`score(txn)` returns `(probability, signals, using_ml)`:
  - `probability` ∈ [0, 1]
  - `signals` is a human-readable list of risk flags (e.g. `HIGH_AMOUNT`)
  - `using_ml` is `True` when the trained booster produced the probability,
    `False` when the stub did

The stub activates when (a) `lightgbm`/`numpy` are not installed or (b) no
model file exists at `model_training/models/fraud_model.txt`. This lets
the pipeline run end-to-end before the model has been trained — train it via
`model_training/train.py` to switch to real ML scoring.
"""
from __future__ import annotations

import os

from fraud_features import CARD_NOT_PRESENT_CHANNELS, extract

_MODEL_PATH = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "model_training", "models", "fraud_model.txt")
)

try:
    import lightgbm as lgb
    import numpy as np
    _HAS_ML = True
except ImportError:
    _HAS_ML = False

_booster = None
_load_attempted = False


def _try_load_booster():
    global _booster, _load_attempted
    if _load_attempted:
        return _booster
    _load_attempted = True
    if not _HAS_ML or not os.path.exists(_MODEL_PATH):
        return None
    try:
        _booster = lgb.Booster(model_file=_MODEL_PATH)
    except Exception as exc:
        print(f"⚠️  fraud_model: failed to load {_MODEL_PATH}: {exc}; using stub")
        _booster = None
    return _booster


def _rule_signals(txn: dict) -> list[str]:
    """Same rule signals the original fraud_detection.py emitted — kept for
    operator visibility in both ML and stub modes."""
    signals: list[str] = []
    if txn.get("amount", 0) > 1000:
        signals.append("HIGH_AMOUNT")
    card = txn.get("card") or {}
    merchant = txn.get("merchant") or {}
    if card.get("issuer_country") != merchant.get("country"):
        signals.append("CROSS_BORDER")
    if txn.get("channel") in CARD_NOT_PRESENT_CHANNELS:
        signals.append("CARD_NOT_PRESENT")
    if merchant.get("mcc_category") == "AIRLINE" and txn.get("amount", 0) > 500:
        signals.append("AIRLINE_HIGH_TICKET")
    return signals


def _stub_probability(signals: list[str]) -> float:
    # Deterministic mapping from rule signals to a probability ∈ [0, 1].
    # Weights mirror the original integer-score weights (25/20/10/15) so the
    # stub's decisions stay in the same ballpark as the rule-based SD it
    # replaces, until a real model is trained.
    weights = {
        "HIGH_AMOUNT": 0.30,
        "CROSS_BORDER": 0.25,
        "CARD_NOT_PRESENT": 0.15,
        "AIRLINE_HIGH_TICKET": 0.20,
    }
    return min(sum(weights[s] for s in signals), 1.0)


def score(txn: dict) -> tuple[float, list[str], bool]:
    signals = _rule_signals(txn)
    booster = _try_load_booster()
    if booster is None:
        return _stub_probability(signals), signals, False
    features = np.array([extract(txn)], dtype=float)
    prob = float(booster.predict(features)[0])
    return prob, signals, True
