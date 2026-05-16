# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

CardRails is a learning/demo project that simulates a BIAN-aligned credit-card
acquiring pipeline on Apache Kafka. Each BIAN Service Domain (SD) is a separate
Python consumer/producer process; there are no direct service-to-service calls —
everything flows through Kafka topics.

## Common commands

Bring up the broker stack and create topics:
```bash
docker compose up -d              # kafka-cluster + kafka-ui on the external `kafka-net` bridge
./scripts/create-topics.sh        # creates the 6 BIAN topics (PARTITIONS=, REPLICATION= overridable)
```

> The `kafka-net` network is declared `external: true` in `docker-compose.yaml`.
> If `docker compose up` complains it doesn't exist, create it first with
> `docker network create kafka-net`.

Run the pipeline (6 terminals from `src/`, since `src/*.py` use sibling
imports like `from config import ...` — they must be run with `src/` as CWD):
```bash
cd src
python authorization.py
python fraud_detection.py
python fee_pricing.py
python clearing.py
python tracker.py
python main.py                    # producer; run last to push N transactions
```

Inspect / operate:
```bash
./scripts/describe-groups.sh                       # all 5 pipeline groups
./scripts/describe-groups.sh --list                # every group in the cluster
./scripts/describe-groups.sh sd-fraud-detection    # one group

docker exec kafka-cluster kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic card.fee.calculated --from-beginning
```

Reset a consumer group to replay from earliest:
```bash
docker exec kafka-cluster kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group sd-card-fee-pricing \
  --reset-offsets --to-earliest --all-topics --execute
```

Tear down (use `-v` to also drop the `kafka_data` volume and wipe topics):
```bash
docker compose down
docker compose down -v
```

Web UIs: kafka-ui at http://localhost:7000, Lenses fast-data-dev at http://localhost:3030.

## Python environment

- Python 3.14+, single runtime dependency: `confluent-kafka`.
- Dependencies are managed with `uv` (`uv.lock` is committed). `uv sync`
  installs into `.venv/`.
- There is no test suite, linter, or formatter configured.

## Architecture

The pipeline is a strict linear chain of 6 stages — Card Terminal → Authorization →
Fraud → Fee Pricing → Clearing → Capture/Settlement — with one topic and one
consumer group per stage. The full topic / group / file mapping is in
`README.md`; do not duplicate it here.

Two architectural invariants matter for any change:

**1. Key = `transaction_id` everywhere (except the final settlement event).**
   Every `producer.produce(...)` in `src/*.py` keys on `transaction_id` so that
   all events for a single transaction hash to the same partition across every
   topic, preserving per-transaction order. The one exception is `tracker.py`,
   which emits `merchant.settlement.posted` keyed by `merchant_id` — that topic
   is consumed/aggregated per merchant, not per transaction. Preserve this
   distinction when adding new produce sites.

**2. Each SD enriches in place, never rewrites.** A stage parses the incoming
   JSON, attaches its own block (`txn["authorization"] = ...`,
   `txn["fraud"] = ...`, etc.), and re-publishes the whole envelope to the next
   topic. The payload grows monotonically. Don't drop upstream fields, and don't
   move a field from one block to another — downstream stages and `tracker.py`'s
   ledger print depend on the exact nested shape (`txn["authorization"]["decision"]`,
   `txn["fraud"]["decision"]`, `txn["fees"]["total"]`, `txn["clearing"]["status"]`,
   `txn["clearing"]["net_amount"]`, etc.).

**Short-circuit behavior.** A `DECLINED` authorization or `BLOCKED` fraud verdict
collapses the rest of the pipeline: `fee_pricing.py` sets `txn["fees"] = None`,
`clearing.py` returns `status=NOT_CLEARED`, and `tracker.py` writes a ledger
entry but emits no settlement event. When touching any of these stages, make
sure both the happy path and the short-circuit path still produce a
well-formed event — `tracker.py` branches on `txn["fees"]` being truthy and
on `txn["clearing"]["status"] == "CLEARED"`.

## Configuration

`src/config.py` is the single source of truth for the bootstrap server, the 6
topic names, and the card-domain enums (`CARD_NETWORKS`, `CARD_TIERS`,
`CURRENCIES`, `COUNTRIES`). Other modules import from it — never inline a topic
name or hard-code `localhost:9092` in a stage file.

Consumer group IDs live as `GROUP_ID = "sd-..."` constants at the top of each
stage file and are mirrored in `scripts/describe-groups.sh`. If you rename a
group, update both places.

## Conventions worth preserving

- Every stage follows the same skeleton: `Consumer({bootstrap, group.id,
  auto.offset.reset=earliest})` + `Producer({bootstrap})`, a `while True:
  consumer.poll(1.0)` loop, `producer.poll(0)` after each produce, and
  `producer.flush()` + `consumer.close()` in the `finally` block on
  `KeyboardInterrupt`. New stages should match this shape.
- All events are UTF-8 JSON; keys are UTF-8 strings. No Schema Registry / Avro
  wiring is in use despite Schema Registry being exposed by the broker container.
- No real PANs are ever generated — `main.py::_masked_pan` produces masked
  test-BIN strings. Keep it that way.
