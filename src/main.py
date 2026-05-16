"""Card Terminal simulator — origin of the BIAN card-transaction pipeline.

Represents the point-of-sale / e-commerce gateway producing raw authorization
requests onto `card.transaction.requested`. In BIAN terms this is upstream of
the Card Authorization Service Domain (it is the channel/terminal, not an SD).
"""
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

from config import (
    BOOTSTRAP,
    CARD_NETWORKS,
    CARD_TIERS,
    COUNTRIES,
    CURRENCIES,
    TOPIC_TRANSACTION_REQUESTED,
)

MERCHANTS = [
    ("MCH-001", "Amazon",          "RETAIL",      "US"),
    ("MCH-002", "Uber",            "TRANSPORT",   "US"),
    ("MCH-003", "Tesco",           "GROCERY",     "GB"),
    ("MCH-004", "Daraz",           "RETAIL",      "PK"),
    ("MCH-005", "Lufthansa",       "AIRLINE",     "DE"),
    ("MCH-006", "Starbucks Tokyo", "RESTAURANT",  "JP"),
]


def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        key = msg.key().decode() if msg.key() else "-"
        print(f"✅ Produced txn {key} → {msg.topic()} [p{msg.partition()} @ {msg.offset()}]")


def _masked_pan() -> str:
    # Realistic-looking masked PAN: BIN + last4. Never generate real card numbers.
    bin6 = random.choice(["411111", "550000", "340000", "601100"])
    last4 = f"{random.randint(0, 9999):04d}"
    return f"{bin6}******{last4}"


def random_transaction() -> dict:
    merchant_id, merchant_name, mcc_category, merchant_country = random.choice(MERCHANTS)
    network = random.choice(CARD_NETWORKS)
    return {
        "transaction_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "card": {
            "pan_masked": _masked_pan(),
            "network": network,
            "tier": random.choice(CARD_TIERS),
            "issuer_country": random.choice(COUNTRIES),
        },
        "merchant": {
            "id": merchant_id,
            "name": merchant_name,
            "mcc_category": mcc_category,
            "country": merchant_country,
        },
        "amount": round(random.uniform(1.00, 2500.00), 2),
        "currency": random.choice(CURRENCIES),
        "channel": random.choice(["POS", "ECOMMERCE", "MOTO", "ATM"]),
    }


def main(n: int = 20, delay: float = 0.3):
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    print(f"🟢 Card Terminal producing {n} transactions → {TOPIC_TRANSACTION_REQUESTED}")

    for _ in range(n):
        txn = random_transaction()
        producer.produce(
            topic=TOPIC_TRANSACTION_REQUESTED,
            key=txn["transaction_id"].encode("utf-8"),
            value=json.dumps(txn).encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(delay)

    producer.flush()
    print("🔵 Card Terminal finished.")


if __name__ == "__main__":
    main()
