"""BIAN Service Domain: Fraud Detection (Fraud / AML).

Scores authorized transactions for fraud risk. Operates independently of
authorization: an approved transaction can still be BLOCKED or sent to
REVIEW based on velocity, geography, channel and amount signals.

Consumes: card.authorization.decided
Produces: card.fraud.scored
"""
import json
import random

from confluent_kafka import Consumer, Producer

from config import (
    BOOTSTRAP,
    TOPIC_AUTHORIZATION_DECIDED,
    TOPIC_FRAUD_SCORED,
)

GROUP_ID = "sd-fraud-detection"
REVIEW_THRESHOLD = 70
BLOCK_THRESHOLD = 90


def score_transaction(txn: dict) -> dict:
    signals = []
    score = random.randint(0, 25)  # baseline noise

    if txn["amount"] > 1000:
        score += 25
        signals.append("HIGH_AMOUNT")
    if txn["card"]["issuer_country"] != txn["merchant"]["country"]:
        score += 20
        signals.append("CROSS_BORDER")
    if txn["channel"] in ("ECOMMERCE", "MOTO"):
        score += 10
        signals.append("CARD_NOT_PRESENT")
    if txn["merchant"]["mcc_category"] == "AIRLINE" and txn["amount"] > 500:
        score += 15
        signals.append("AIRLINE_HIGH_TICKET")

    score = min(score, 100)
    if score >= BLOCK_THRESHOLD:
        decision = "BLOCKED"
    elif score >= REVIEW_THRESHOLD:
        decision = "REVIEW"
    else:
        decision = "CLEARED"

    return {"score": score, "decision": decision, "signals": signals}


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
