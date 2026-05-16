"""BIAN Service Domain: Card Authorization.

Decides APPROVED / DECLINED on incoming transaction requests based on
basic policy checks (amount limits, supported networks). Issues an
authorization code on approval.

Consumes: card.transaction.requested
Produces: card.authorization.decided
"""
import json
import random
import uuid

from confluent_kafka import Consumer, Producer

from config import (
    BOOTSTRAP,
    CARD_NETWORKS,
    TOPIC_AUTHORIZATION_DECIDED,
    TOPIC_TRANSACTION_REQUESTED,
)

GROUP_ID = "sd-card-authorization"
HIGH_VALUE_THRESHOLD = 2000.00


def authorize(txn: dict) -> dict:
    amount = txn["amount"]
    network = txn["card"]["network"]

    if network not in CARD_NETWORKS:
        return {"decision": "DECLINED", "reason": "UNSUPPORTED_NETWORK", "auth_code": None}

    # Simulate stand-in random declines on high-value to mimic issuer limits.
    if amount > HIGH_VALUE_THRESHOLD and random.random() < 0.25:
        return {"decision": "DECLINED", "reason": "INSUFFICIENT_FUNDS", "auth_code": None}

    if random.random() < 0.03:
        return {"decision": "DECLINED", "reason": "DO_NOT_HONOR", "auth_code": None}

    return {
        "decision": "APPROVED",
        "reason": "OK",
        "auth_code": uuid.uuid4().hex[:6].upper(),
    }


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([TOPIC_TRANSACTION_REQUESTED])
    print(f"🟢 [Card Authorization SD] subscribed to {TOPIC_TRANSACTION_REQUESTED}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("❌ Error:", msg.error())
                continue

            txn = json.loads(msg.value().decode("utf-8"))
            txn["authorization"] = authorize(txn)

            icon = "✅" if txn["authorization"]["decision"] == "APPROVED" else "🚫"
            print(f"{icon} AUTH {txn['transaction_id'][:8]} "
                  f"{txn['authorization']['decision']:8s} "
                  f"({txn['authorization']['reason']}) "
                  f"{txn['amount']} {txn['currency']}")

            producer.produce(
                topic=TOPIC_AUTHORIZATION_DECIDED,
                key=txn["transaction_id"].encode("utf-8"),
                value=json.dumps(txn).encode("utf-8"),
            )
            producer.poll(0)
    except KeyboardInterrupt:
        print("\n🔴 Stopping Card Authorization SD")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
