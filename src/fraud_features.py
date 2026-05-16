"""Feature engineering for the ML fraud detector.

A CardRails transaction is a nested JSON envelope produced upstream by the
Card Terminal and Card Authorization stages. This module flattens it into a
fixed-order numeric feature vector that both the LightGBM trainer
(`model_training/train.py`) and the runtime scorer (`src/fraud_model.py`)
consume. Train and predict MUST agree on column count and order; the order is
defined by `feature_names()`.

Pure stdlib + math — no numpy/pandas — so this module can be imported even
when ML libraries are not installed (the scorer falls back to a stub in that
case; see `fraud_model.py`).
"""
from __future__ import annotations

import math
from datetime import datetime

from config import CARD_NETWORKS, CARD_TIERS, COUNTRIES, CURRENCIES

CHANNELS = ("POS", "ECOMMERCE", "MOTO", "ATM")
MCC_CATEGORIES = ("RETAIL", "TRANSPORT", "GROCERY", "AIRLINE", "RESTAURANT")

CARD_NOT_PRESENT_CHANNELS = {"ECOMMERCE", "MOTO"}


def _one_hot(value: str, vocab: tuple[str, ...], prefix: str) -> list[tuple[str, float]]:
    return [(f"{prefix}_{v}", 1.0 if value == v else 0.0) for v in vocab]


def _parse_hour(timestamp: str) -> int:
    # Tolerate the `Z` suffix some producers emit instead of `+00:00`.
    try:
        return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).hour
    except (ValueError, AttributeError):
        return 12


def _feature_pairs(txn: dict) -> list[tuple[str, float]]:
    amount = float(txn.get("amount", 0.0))
    card = txn.get("card") or {}
    merchant = txn.get("merchant") or {}
    issuer_country = card.get("issuer_country", "")
    merchant_country = merchant.get("country", "")
    channel = txn.get("channel", "")

    pairs: list[tuple[str, float]] = [
        ("amount", amount),
        ("log_amount", math.log1p(max(amount, 0.0))),
        ("hour_of_day", float(_parse_hour(txn.get("timestamp", "")))),
        ("is_cross_border", 1.0 if issuer_country != merchant_country else 0.0),
        ("is_card_not_present", 1.0 if channel in CARD_NOT_PRESENT_CHANNELS else 0.0),
        ("is_high_amount", 1.0 if amount > 1000.0 else 0.0),
    ]
    pairs += _one_hot(card.get("network", ""), CARD_NETWORKS, "network")
    pairs += _one_hot(card.get("tier", ""), CARD_TIERS, "tier")
    pairs += _one_hot(txn.get("currency", ""), CURRENCIES, "currency")
    pairs += _one_hot(issuer_country, COUNTRIES, "issuer")
    pairs += _one_hot(merchant_country, COUNTRIES, "merchant_country")
    pairs += _one_hot(merchant.get("mcc_category", ""), MCC_CATEGORIES, "mcc")
    pairs += _one_hot(channel, CHANNELS, "channel")
    return pairs


def feature_names() -> list[str]:
    """Stable column order. Used by the trainer to label CSV columns."""
    sample = {
        "amount": 0.0,
        "timestamp": "",
        "card": {},
        "merchant": {},
        "currency": "",
        "channel": "",
    }
    return [name for name, _ in _feature_pairs(sample)]


def extract(txn: dict) -> list[float]:
    """Return the feature row for a single transaction in `feature_names()` order."""
    return [value for _, value in _feature_pairs(txn)]
