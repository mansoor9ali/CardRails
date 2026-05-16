"""BIAN Service Domain: Card Clearing.

Clears a priced transaction: assigns a clearing reference, computes the
net amount due to the merchant (gross amount minus total fees) and emits
the cleared event for capture/settlement.

Consumes: card.fee.calculated
Produces: card.transaction.cleared
"""
import json
import uuid
from datetime import datetime, timezone

from confluent_kafka import Consumer, Producer

from config import (
    BOOTSTRAP,
    TOPIC_FEE_CALCULATED,
    TOPIC_TRANSACTION_CLEARED,
)

GROUP_ID = "sd-card-clearing"


def clear(txn: dict) -> dict:
    if txn["fees"] is None:
        return {
            "status": "NOT_CLEARED",
            "clearing_id": None,
            "net_amount": 0.0,
            "settlement_currency": None,
            "cleared_at": None,
        }

    gross = txn["amount"]
    fees_total = txn["fees"]["total"]
    return {
        "status": "CLEARED",
        "clearing_id": f"CLR-{uuid.uuid4().hex[:10].upper()}",
        "net_amount": round(gross - fees_total, 2),
        "settlement_currency": txn["fees"]["settlement_currency"],
        "cleared_at": datetime.now(timezone.utc).isoformat(),
    }


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([TOPIC_FEE_CALCULATED])
    print(f"🟢 [Card Clearing SD] subscribed to {TOPIC_FEE_CALCULATED}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("❌ Error:", msg.error())
                continue

            txn = json.loads(msg.value().decode("utf-8"))
            txn["clearing"] = clear(txn)

            c = txn["clearing"]
            if c["status"] == "CLEARED":
                print(f"🏦 CLR   {txn['transaction_id'][:8]} {c['clearing_id']} "
                      f"net={c['net_amount']} {c['settlement_currency']}")
            else:
                print(f"⛔ CLR   {txn['transaction_id'][:8]} NOT_CLEARED")

            producer.produce(
                topic=TOPIC_TRANSACTION_CLEARED,
                key=txn["transaction_id"].encode("utf-8"),
                value=json.dumps(txn).encode("utf-8"),
            )
            producer.poll(0)
    except KeyboardInterrupt:
        print("\n🔴 Stopping Card Clearing SD")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
