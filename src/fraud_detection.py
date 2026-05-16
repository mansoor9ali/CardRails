"""BIAN Service Domain: Fraud Detection (Fraud / AML).

Scores authorized transactions for fraud risk using a LightGBM model. The
heavy lifting lives in `fraud_model.py` (which falls back to a deterministic
stub when the model has not been trained yet) and `fraud_features.py`
(feature engineering shared with the trainer).

Operates independently of authorization: an approved transaction can still be
BLOCKED or sent to REVIEW based on the model's risk probability.

Consumes: card.authorization.decided
Produces: card.fraud.scored
"""
import json

from confluent_kafka import Consumer, Producer

from config import (
    BOOTSTRAP,
    TOPIC_AUTHORIZATION_DECIDED,
    TOPIC_FRAUD_SCORED,
)
from fraud_model import score as ml_score

GROUP_ID = "sd-fraud-detection"
REVIEW_THRESHOLD = 70
BLOCK_THRESHOLD = 75


def score_transaction(txn: dict) -> dict:
    probability, signals, using_ml = ml_score(txn)
    score = min(int(round(probability * 100)), 100)
    if score >= BLOCK_THRESHOLD:
        decision = "BLOCKED"
    elif score >= REVIEW_THRESHOLD:
        decision = "REVIEW"
    else:
        decision = "CLEARED"
    return {
        "score": score,
        "decision": decision,
        "signals": signals,
        "model": "lightgbm" if using_ml else "stub",
    }


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([TOPIC_AUTHORIZATION_DECIDED])
    print(f"🟢 [Fraud Detection SD] subscribed to {TOPIC_AUTHORIZATION_DECIDED}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("❌ Error:", msg.error())
                continue

            txn = json.loads(msg.value().decode("utf-8"))
            txn["fraud"] = score_transaction(txn)

            icon = {"CLEARED": "🟢", "REVIEW": "🟡", "BLOCKED": "🔴"}[txn["fraud"]["decision"]]
            print(f"{icon} FRAUD {txn['transaction_id'][:8]} "
                  f"score={txn['fraud']['score']:3d} {txn['fraud']['decision']:7s} "
                  f"model={txn['fraud']['model']:8s} "
                  f"signals={txn['fraud']['signals']}")

            producer.produce(
                topic=TOPIC_FRAUD_SCORED,
                key=txn["transaction_id"].encode("utf-8"),
                value=json.dumps(txn).encode("utf-8"),
            )
            producer.poll(0)
    except KeyboardInterrupt:
        print("\n🔴 Stopping Fraud Detection SD")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
