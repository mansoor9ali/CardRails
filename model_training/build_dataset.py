"""Generate a synthetic, CardRails-shaped training set for the fraud model.

Each row is a labeled feature vector matching `src/fraud_features.feature_names()`,
written to `data/training.csv`. Labels are sampled from a logistic function of
the same rule signals the live SD emits, plus Gaussian noise — so the rules
are predictive but not deterministic, and LightGBM has something non-trivial
to learn.

Usage:
    uv run --group train python model_training/build_dataset.py --n 50000
"""
from __future__ import annotations

import argparse
import csv
import math
import os
import random
import sys

# Reuse the live feature extractor + the Card Terminal's random_transaction()
# so training and inference cannot drift apart.
HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.abspath(os.path.join(HERE, "..", "src"))
sys.path.insert(0, SRC)

from fraud_features import extract, feature_names  # noqa: E402
from main import random_transaction  # noqa: E402


def _fraud_probability(txn: dict, rng: random.Random) -> float:
    """Latent risk model the LightGBM classifier is asked to recover.

    Mixes the rule-based signals with smooth amount/hour effects and Gaussian
    noise, then squashes through a logistic. The intercept is tuned to yield
    ~2-5% positives, which is the regime LightGBM's `is_unbalance=True` is
    designed for.
    """
    card = txn.get("card") or {}
    merchant = txn.get("merchant") or {}
    amount = float(txn.get("amount", 0.0))

    z = -6.5
    if amount > 1500:
        z += 1.5
    if card.get("issuer_country") != merchant.get("country"):
        z += 1.0
    if txn.get("channel") in ("ECOMMERCE", "MOTO"):
        z += 0.7
    if merchant.get("mcc_category") == "AIRLINE" and amount > 500:
        z += 1.2
    z += 0.0003 * amount
    z += rng.gauss(0, 0.5)
    return 1.0 / (1.0 + math.exp(-z))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=50000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--out",
        default=os.path.join(HERE, "data", "training.csv"),
    )
    args = parser.parse_args()

    rng = random.Random(args.seed)
    random.seed(args.seed)  # random_transaction() reads from the module-level RNG

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    columns = feature_names() + ["label"]
    positives = 0

    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        for _ in range(args.n):
            txn = random_transaction()
            row = extract(txn)
            prob = _fraud_probability(txn, rng)
            label = 1 if rng.random() < prob else 0
            positives += label
            writer.writerow(row + [label])

    rate = positives / args.n if args.n else 0.0
    print(f"✅ wrote {args.n} rows to {args.out} (positive rate = {rate:.3%})")


if __name__ == "__main__":
    main()
