"""BIAN Service Domain: Card Fee Pricing.

Calculates the itemised fee stack applied to a card transaction. Real-world
card fee structure follows four-party-model economics:

  • Interchange  — paid by acquirer → issuer (largest component, varies by card tier)
  • Assessment  — paid by acquirer → card network (Visa/MC/Amex/Discover)
  • Processor   — acquirer / payment processor markup
  • Cross-border — applied when issuer country ≠ merchant country
  • FX          — applied when transaction currency ≠ merchant settlement currency

Consumes: card.fraud.scored
Produces: card.fee.calculated
"""
import json

from confluent_kafka import Consumer, Producer

from config import (
    BOOTSTRAP,
    TOPIC_FEE_CALCULATED,
    TOPIC_FRAUD_SCORED,
)

GROUP_ID = "sd-card-fee-pricing"

# (percentage_rate, flat_fee_in_settlement_ccy)
INTERCHANGE_RATES = {
    "DEBIT":           (0.0050, 0.10),
    "CREDIT_STANDARD": (0.0180, 0.10),
    "CREDIT_REWARDS": (0.0210, 0.10),
    "CREDIT_PREMIUM": (0.0240, 0.10),
}

ASSESSMENT_RATES = {
    "VISA":       0.0013,
    "MASTERCARD": 0.0013,
    "DISCOVER":   0.0013,
    "AMEX":       0.0015,
}

PROCESSOR_RATE = 0.0015
PROCESSOR_FLAT = 0.05
CROSS_BORDER_RATE = 0.0100
FX_RATE = 0.0100

MERCHANT_SETTLEMENT_CCY = {
    "US": "USD", "GB": "GBP", "DE": "EUR",
    "FR": "EUR", "PK": "PKR", "JP": "JPY",
}


def _round(x: float) -> float:
    return round(x, 2)


def calculate_fees(txn: dict) -> dict:
    amount = txn["amount"]
    tier = txn["card"]["tier"]
    network = txn["card"]["network"]
    issuer_country = txn["card"]["issuer_country"]
    merchant_country = txn["merchant"]["country"]
    txn_ccy = txn["currency"]
    settle_ccy = MERCHANT_SETTLEMENT_CCY.get(merchant_country, "USD")

    ic_rate, ic_flat = INTERCHANGE_RATES[tier]
    interchange = amount * ic_rate + ic_flat
    assessment = amount * ASSESSMENT_RATES[network]
    processor = amount * PROCESSOR_RATE + PROCESSOR_FLAT
    cross_border = amount * CROSS_BORDER_RATE if issuer_country != merchant_country else 0.0
    fx = amount * FX_RATE if txn_ccy != settle_ccy else 0.0
    total = interchange + assessment + processor + cross_border + fx

    return {
        "settlement_currency": settle_ccy,
        "interchange":  _round(interchange),
        "assessment":   _round(assessment),
        "processor":    _round(processor),
        "cross_border": _round(cross_border),
        "fx":           _round(fx),
        "total":        _round(total),
    }


def _should_price(txn: dict) -> bool:
    return (
        txn["authorization"]["decision"] == "APPROVED"
        and txn["fraud"]["decision"] != "BLOCKED"
    )


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([TOPIC_FRAUD_SCORED])
    print(f"🟢 [Card Fee Pricing SD] subscribed to {TOPIC_FRAUD_SCORED}")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("❌ Error:", msg.error())
                continue

            txn = json.loads(msg.value().decode("utf-8"))
            if _should_price(txn):
                txn["fees"] = calculate_fees(txn)
                f = txn["fees"]
                print(f"💰 FEES  {txn['transaction_id'][:8]} "
                      f"ic={f['interchange']} as={f['assessment']} "
                      f"pr={f['processor']} xb={f['cross_border']} fx={f['fx']} "
                      f"→ total={f['total']} {f['settlement_currency']}")
            else:
                txn["fees"] = None
                print(f"⚪ FEES  {txn['transaction_id'][:8]} skipped "
                      f"(auth={txn['authorization']['decision']}, "
                      f"fraud={txn['fraud']['decision']})")

            producer.produce(
                topic=TOPIC_FEE_CALCULATED,
                key=txn["transaction_id"].encode("utf-8"),
                value=json.dumps(txn).encode("utf-8"),
            )
            producer.poll(0)
    except KeyboardInterrupt:
        print("\n🔴 Stopping Card Fee Pricing SD")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
