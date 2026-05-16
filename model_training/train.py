"""Train the LightGBM fraud classifier on the synthetic CardRails dataset.

Reads `data/training.csv` (produced by `build_dataset.py`) and writes
`models/fraud_model.txt`. At runtime, `src/fraud_model.py` picks up that file
automatically; if it is absent, the SD falls back to the rule stub.

Usage:
    uv run --group train python model_training/build_dataset.py --n 50000
    uv run --group train python model_training/train.py
"""
from __future__ import annotations

import argparse
import os

import lightgbm as lgb
import pandas as pd
from lightgbm import early_stopping
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA = os.path.join(HERE, "data", "training.csv")
DEFAULT_OUT = os.path.join(HERE, "models", "fraud_model.txt")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", default=DEFAULT_DATA)
    parser.add_argument("--out", default=DEFAULT_OUT)
    parser.add_argument("--rounds", type=int, default=300)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    df = pd.read_csv(args.data)
    y = df.pop("label")
    X = df

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=args.seed, stratify=y
    )

    params = {
        "objective": "binary",
        "metric": "auc",
        "boosting_type": "gbdt",
        "is_unbalance": True,
        "learning_rate": 0.05,
        "num_leaves": 31,
        "verbose": -1,
        "seed": args.seed,
    }
    train_set = lgb.Dataset(X_train, label=y_train)
    valid_set = lgb.Dataset(X_test, label=y_test, reference=train_set)
    model = lgb.train(
        params,
        train_set,
        num_boost_round=args.rounds,
        valid_sets=[valid_set],
        callbacks=[early_stopping(stopping_rounds=20)],
    )

    auc = roc_auc_score(y_test, model.predict(X_test))
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    model.save_model(args.out)
    print(f"✅ test AUC = {auc:.4f} — saved to {args.out}")


if __name__ == "__main__":
    main()
