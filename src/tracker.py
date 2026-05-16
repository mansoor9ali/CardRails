"""BIAN Service Domains: Card Transaction (Capture) + Merchant Settlement.

Terminal consumer of the pipeline. Persists the captured transaction record
(in-memory ledger here; in production this would be the system-of-record)
and emits the merchant settlement posting.

Consumes: card.transaction.cleared
Produces: merchant.settlement.posted
"""
import json
from collections import defaultdict

from confluent_kafka import Consumer, Producer

from config import (
    BOOTSTRAP,
    TOPIC_MERCHANT_SETTLED,
    TOPIC_TRANSACTION_CLEARED,
)

GROUP_ID = "sd-card-capture"


def _print_ledger_entry(txn: dict):
    tid = txn["transaction_id"][:8]
    auth = txn["authorization"]
    fraud = txn["fraud"]
    fees = txn["fees"]
    clr = txn["clearing"]
    merchant = txn["merchant"]["name"]

    print("─" * 78)
    print(f"📒 CAPTURED txn={tid}  merchant={merchant} "
          f"channel={txn['channel']}  amount={txn['amount']} {txn['currency']}")
    print(f"   auth={auth['decision']} ({auth['reason']}) "
          f"code={auth['auth_code']}")
    print(f"   fraud={fraud['decision']} score={fraud['score']} "
          f"signals={fraud['signals']}")
    if fees:
        print(f"   fees: interchange={fees['interchange']} "
              f"assessment={fees['assessment']} processor={fees['processor']} "
              f"cross_border={fees['cross_border']} fx={fees['fx']} "
              f"→ total={fees['total']} {fees['settlement_currency']}")
        print(f"   clearing: {clr['clearing_id']} "
              f"net_to_merchant={clr['net_amount']} {clr['settlement_currency']}")
    else:
        print("   fees: skipped (txn not eligible)")


def main():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })
    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    consumer.subscribe([TOPIC_TRANSACTION_CLEARED])
    print(f"🟢 [Card Capture SD] subscribed to {TOPIC_TRANSACTION_CLEARED}")

    merchant_totals = defaultdict(float)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("❌ Error:", msg.error())
                continue

            txn = json.loads(msg.value().decode("utf-8"))
            _print_ledger_entry(txn)

            if txn["clearing"]["status"] == "CLEARED":
                key = (txn["merchant"]["id"], txn["clearing"]["settlement_currency"])
                merchant_totals[key] += txn["clearing"]["net_amount"]
                running = round(merchant_totals[key], 2)
                print(f"   💵 merchant running settlement: "
                      f"{txn['merchant']['name']}={running} {key[1]}")

                settlement = {
                    "transaction_id": txn["transaction_id"],
                    "merchant_id": txn["merchant"]["id"],
                    "merchant_name": txn["merchant"]["name"],
                    "clearing_id": txn["clearing"]["clearing_id"],
                    "net_amount": txn["clearing"]["net_amount"],
                    "currency": txn["clearing"]["settlement_currency"],
                    "running_total": running,
                }
                producer.produce(
                    topic=TOPIC_MERCHANT_SETTLED,
                    key=txn["merchant"]["id"].encode("utf-8"),
                    value=json.dumps(settlement).encode("utf-8"),
                )
                producer.poll(0)
    except KeyboardInterrupt:
        print("\n🔴 Stopping Card Capture SD")
    finally:
        producer.flush()
        consumer.close()


if __name__ == "__main__":
    main()
