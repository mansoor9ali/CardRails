# CardRails

**Real-time, BIAN-aligned credit-card transaction processing on Apache Kafka.**

CardRails simulates a production-style card-acquiring pipeline. A raw
authorization request enters from a POS / e-commerce channel and flows through
five independent **BIAN Service Domains** — Authorization, Fraud Detection,
Fee Pricing, Clearing, and Capture & Merchant Settlement — each implemented as
a separate Kafka consumer/producer process. The transaction is progressively
enriched at every hop until it lands as a fully-priced, fully-settled record.

---

## Why this design

| Concern               | Choice                                                    |
|-----------------------|-----------------------------------------------------------|
| Domain language       | **BIAN Service Domains** — vendor-neutral, bank-standard  |
| Coupling              | **Event-driven via Kafka** — no service calls another     |
| Ordering              | **Key = transaction_id** — co-locates a txn's events      |
| Enrichment            | **Each SD appends its block** to the event payload        |
| Horizontal scale      | **Each SD has its own consumer group** — add instances    |
| Replay & audit        | **Kafka retains topics** — backed by `kafka_data` volume  |
| Local ops UI          | **kafka-ui** on :7000, **Kafdrop** on :9000, **Lenses fast-data-dev** on :3030 |

---

## Architecture

### High-level data flow

```
                       ┌──────────────────────────────┐
                       │  Card Terminal (src/main.py) │
                       │  POS · ECOMMERCE · MOTO · ATM│
                       └──────────────┬───────────────┘
                                      │ produces
                                      ▼
                       ┌──────────────────────────────┐
              topic →  │ card.transaction.requested   │
                       └──────────────┬───────────────┘
                                      │
   group: sd-card-authorization       ▼
                       ┌──────────────────────────────┐
   BIAN SD ··········· │ Card Authorization           │  src/authorization.py
                       │  approve / decline · auth_code│
                       └──────────────┬───────────────┘
                                      │ produces
                                      ▼
                       ┌──────────────────────────────┐
              topic →  │ card.authorization.decided   │
                       └──────────────┬───────────────┘
                                      │
   group: sd-fraud-detection          ▼
                       ┌──────────────────────────────┐
   BIAN SD ··········· │ Fraud Detection (LightGBM)   │  src/fraud_detection.py
                       │  risk score 0-100 · signals  │
                       └──────────────┬───────────────┘
                                      │ produces
                                      ▼
                       ┌──────────────────────────────┐
              topic →  │ card.fraud.scored            │
                       └──────────────┬───────────────┘
                                      │
   group: sd-card-fee-pricing         ▼
                       ┌──────────────────────────────┐
   BIAN SD ··········· │ Card Fee Pricing             │  src/fee_pricing.py
                       │  interchange · assessment    │
                       │  processor · cross-border · fx│
                       └──────────────┬───────────────┘
                                      │ produces
                                      ▼
                       ┌──────────────────────────────┐
              topic →  │ card.fee.calculated          │
                       └──────────────┬───────────────┘
                                      │
   group: sd-card-clearing            ▼
                       ┌──────────────────────────────┐
   BIAN SD ··········· │ Card Clearing                │  src/clearing.py
                       │  clearing_id · net_amount    │
                       └──────────────┬───────────────┘
                                      │ produces
                                      ▼
                       ┌──────────────────────────────┐
              topic →  │ card.transaction.cleared     │
                       └──────────────┬───────────────┘
                                      │
   group: sd-card-capture             ▼
                       ┌──────────────────────────────┐
   BIAN SD ··········· │ Card Capture +               │  src/tracker.py
                       │ Merchant Settlement          │
                       │  ledger entry · running totals│
                       └──────────────┬───────────────┘
                                      │ produces
                                      ▼
                       ┌──────────────────────────────┐
              topic →  │ merchant.settlement.posted   │
                       └──────────────────────────────┘
```

### BIAN Service Domain mapping

| Service                  | BIAN Service Domain        | Consumes                       | Produces                       | Consumer group         |
|--------------------------|----------------------------|--------------------------------|--------------------------------|------------------------|
| `main.py`                | _(Channel: Card Terminal)_ | _(generates events)_           | `card.transaction.requested`   | _(producer-only)_      |
| `authorization.py`       | **Card Authorization**     | `card.transaction.requested`   | `card.authorization.decided`   | `sd-card-authorization`|
| `fraud_detection.py`     | **Fraud Detection**        | `card.authorization.decided`   | `card.fraud.scored`            | `sd-fraud-detection`   |
| `fee_pricing.py`         | **Card Fee Pricing**       | `card.fraud.scored`            | `card.fee.calculated`          | `sd-card-fee-pricing`  |
| `clearing.py`            | **Card Clearing**          | `card.fee.calculated`          | `card.transaction.cleared`     | `sd-card-clearing`     |
| `tracker.py`             | **Card Transaction Capture** + **Merchant Settlement** | `card.transaction.cleared` | `merchant.settlement.posted` | `sd-card-capture`     |

### Event enrichment pattern

Each Service Domain appends its own block onto the event before re-publishing.
A transaction's payload **grows** as it walks the pipeline:

```text
card.transaction.requested
└─ { transaction_id, timestamp, card, merchant, amount, currency, channel }

card.authorization.decided                       (+authorization block)
└─ { ... , authorization: { decision, reason, auth_code } }

card.fraud.scored                                (+fraud block)
└─ { ... , fraud: { score, decision, signals, model } }

card.fee.calculated                              (+fees block)
└─ { ... , fees: { interchange, assessment, processor,
                   cross_border, fx, total, settlement_currency } }

card.transaction.cleared                         (+clearing block)
└─ { ... , clearing: { status, clearing_id, net_amount,
                       settlement_currency, cleared_at } }

merchant.settlement.posted                       (settlement summary)
└─ { transaction_id, merchant_id, merchant_name, clearing_id,
     net_amount, currency, running_total }
```

### Partitioning & ordering

- Every produce call uses `key = transaction_id`.
- Kafka hashes the key, so **all 6 events for a single transaction land on the
  same partition across every topic**, preserving per-transaction order.
- Topics default to **3 partitions, replication-factor 1** — tune via
  `PARTITIONS=` / `REPLICATION=` env vars on `scripts/create-topics.sh`.

### Decision interaction

Authorization, Fraud, and Fee Pricing each make independent decisions, and
downstream stages compose them:

```
Authorization.decision == APPROVED   AND   Fraud.decision != BLOCKED
                            │
                            ▼
                  Fee Pricing computes fees
                            │
                            ▼
                  Clearing.status = CLEARED
                            │
                            ▼
                  Capture emits settlement
```

A `DECLINED` auth or a `BLOCKED` fraud verdict short-circuits the pipeline:
fees are skipped, clearing returns `NOT_CLEARED`, and Capture still writes a
ledger entry (for audit) but emits no settlement event.

---

## Fee model (`src/fee_pricing.py`)

CardRails models the four-party acquiring economic structure: amounts flow
from cardholder → issuer → network → acquirer → merchant, with each party
taking a slice.

| Fee component | Driver                                          | Rate                  | Goes to       |
|---------------|-------------------------------------------------|-----------------------|---------------|
| Interchange   | Card tier (DEBIT / STANDARD / REWARDS / PREMIUM)| 0.50% – 2.40% + $0.10 | Issuer        |
| Assessment    | Card network (VISA / MC / AMEX / DISCOVER)      | 0.13% – 0.15%         | Card network  |
| Processor     | Acquirer markup                                 | 0.15% + $0.05         | Acquirer      |
| Cross-border  | Applied when issuer country ≠ merchant country  | 1.00%                 | Network/scheme|
| FX            | Applied when txn currency ≠ settlement currency | 1.00%                 | Acquirer      |

> **Net merchant payout** = `gross_amount − Σ(fees)`, in the merchant's
> settlement currency (USD / GBP / EUR / PKR / JPY depending on country).

---

## Fraud model (`src/fraud_detection.py` + `model_training/`)

Fraud scoring is a **LightGBM binary classifier** trained on synthetic
CardRails transactions. The trained booster lives at
`model_training/models/fraud_model.txt` and is loaded once at SD startup. If
the file is absent (or `lightgbm` / `numpy` aren't installed), the SD
transparently falls back to a deterministic rule stub so the pipeline always
runs — the fraud envelope's `model` field reads `lightgbm` or `stub`
accordingly.

| Component                            | Where                                |
|--------------------------------------|--------------------------------------|
| Feature extraction (40 columns)      | `src/fraud_features.py`              |
| Booster load + stub fallback         | `src/fraud_model.py`                 |
| SD wiring + decision thresholds      | `src/fraud_detection.py`             |
| Synthetic dataset generator          | `model_training/build_dataset.py`    |
| Trainer                              | `model_training/train.py`            |

**Features (40)** — raw `amount`, `log_amount`, `hour_of_day`, and booleans
for `is_cross_border` / `is_card_not_present` / `is_high_amount`, plus
one-hots for card network, card tier, currency, issuer country, merchant
country, MCC category, and channel.

**Decision thresholds** (`src/fraud_detection.py`)

| `score` (= round(probability × 100)) | Decision   |
|--------------------------------------|------------|
| ≥ 75                                 | `BLOCKED`  |
| ≥ 70 and < 75                        | `REVIEW`   |
| < 70                                 | `CLEARED`  |

`BLOCKED` short-circuits Fee Pricing → Clearing → Capture (see
[Decision interaction](#decision-interaction)).

**Retrain** (the repo ships with a pre-trained model; this step is optional)

```bash
uv sync --group train
uv run --group train python model_training/build_dataset.py --n 50000
uv run --group train python model_training/train.py
```

The trainer rewrites `model_training/models/fraud_model.txt` in place; the
next SD restart picks it up automatically. The latent risk function in
`build_dataset.py` is tuned to ~3% positives, which yields a test AUC near
0.78. Retune intercept / signal weights there if you want a different
positive rate.

---

## Infrastructure

Managed by `docker-compose.yaml`:

```
┌──────────────────────────── kafka-net (bridge) ────────────────────────────┐
│                                                                            │
│   ┌──────────────────────────────┐        ┌────────────────────────────┐   │
│   │ kafka-cluster                │        │ kafka-ui                   │   │
│   │ lensesio/fast-data-dev       │◄───────│ provectuslabs/kafka-ui     │   │
│   │  • Zookeeper       :2181     │ :9092  │  DYNAMIC_CONFIG_ENABLED    │   │
│   │  • Kafka broker    :9092     │        │  bootstrap=kafka-cluster   │   │
│   │  • Schema Registry :8081     │        │   exposed: :7000 → :8080   │   │
│   │  • REST Proxy      :8082     │        └────────────────────────────┘   │
│   │  • Connect         :8083     │        ┌────────────────────────────┐   │
│   │  • Lenses UI       :3030     │◄───────│ kafdrop                    │   │
│   │  ADV_HOST=kafka-cluster      │ :9092  │ obsidiandynamics/kafdrop   │   │
│   └──────────────┬───────────────┘        │  KAFKA_BROKERCONNECT=      │   │
│                  │                        │    kafka-cluster:9092      │   │
│                  │                        │   exposed: :9000 → :9000   │   │
│                  │                        └────────────────────────────┘   │
│                  │ /data (broker logs persisted)                            │
└──────────────────┼─────────────────────────────────────────────────────────┘
                   ▼
           ┌──────────────────┐
           │ volume: kafka_data│   ← survives container restarts
           └──────────────────┘
```

| Host port | In-container | Service                                       |
|-----------|--------------|-----------------------------------------------|
| 2181      | 2181         | Zookeeper                                     |
| 9092      | 9092         | Kafka broker (clients connect here)           |
| 8081      | 8081         | Schema Registry                               |
| 8082      | 8082         | Kafka REST Proxy                              |
| 8083      | 8083         | Kafka Connect                                 |
| 3030      | 3030         | Lenses fast-data-dev UI                       |
| 7000      | 8080         | kafka-ui (Provectus)                          |
| 9000      | 9000         | Kafdrop (Obsidian Dynamics)                   |

---

## Quickstart

### 0. Prerequisites
- Docker + Docker Compose
- Python 3.14+
- `pip install confluent-kafka` (or `uv sync` if using uv)

### 1. Bring up the broker stack
```bash
docker compose up -d
# Wait ~30-45s for Kafka to fully boot inside fast-data-dev
```

### 2. Create the 6 BIAN topics
```bash
./scripts/create-topics.sh
```
Optional overrides:
```bash
PARTITIONS=6 REPLICATION=1 ./scripts/create-topics.sh
CONTAINER=kafka-cluster BOOTSTRAP=localhost:9092 ./scripts/create-topics.sh
```

### 3. Run the pipeline

Open 6 terminals from the repo root:

```bash
# Terminals 1-5: start each Service Domain
cd src
python authorization.py
python fraud_detection.py
python fee_pricing.py
python clearing.py
python tracker.py

# Terminal 6: produce a batch of card transactions
python main.py
```

You'll see each SD log its decisions in real time, with a per-merchant
running settlement total printed by the final stage.

---

## Operations

### Inspect consumer groups (offsets, lag, members)
```bash
./scripts/describe-groups.sh                       # all 5 pipeline groups
./scripts/describe-groups.sh --list                # every group in the cluster
./scripts/describe-groups.sh sd-fraud-detection    # one group
```

Equivalent raw command the script wraps:
```bash
docker exec kafka-cluster kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group sd-card-authorization
```

### Tail a topic
```bash
docker exec kafka-cluster kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic card.fee.calculated --from-beginning
```

### Reset a consumer group (replay from earliest)
```bash
docker exec kafka-cluster kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group sd-card-fee-pricing \
  --reset-offsets --to-earliest --all-topics --execute
```

### Web UIs
- **kafka-ui** → http://localhost:7000  (topic browser, message viewer, consumer-group monitor)
- **Kafdrop** → http://localhost:9000  (lightweight topic / partition / message inspector)
- **Lenses fast-data-dev** → http://localhost:3030  (built-in cluster dashboard)

### Tear down
```bash
docker compose down            # stop & remove containers + network
docker compose down -v         # also drop the kafka_data volume (wipes topics)
```

---

## Repository layout

```
.
├── README.md                    ← you are here
├── docker-compose.yaml          ← kafka-cluster + kafka-ui + kafka_data volume
├── pyproject.toml               ← Python 3.14+ · confluent-kafka · lightgbm · numpy
├── scripts/
│   ├── create-topics.sh         ← creates the 6 BIAN topics
│   └── describe-groups.sh       ← describes the 5 pipeline consumer groups
├── model_training/              ← offline LightGBM training kit
│   ├── build_dataset.py         ← synthetic labeled dataset generator
│   ├── train.py                 ← LightGBM trainer
│   └── models/fraud_model.txt   ← trained booster (loaded by src/fraud_model.py)
└── src/
    ├── config.py                ← bootstrap, topic names, card metadata
    ├── main.py                  ← Card Terminal (producer)
    ├── authorization.py         ← BIAN SD: Card Authorization
    ├── fraud_detection.py       ← BIAN SD: Fraud Detection (LightGBM)
    ├── fraud_features.py        ← feature extractor (shared with trainer)
    ├── fraud_model.py           ← booster loader + stub fallback
    ├── fee_pricing.py           ← BIAN SD: Card Fee Pricing
    ├── clearing.py              ← BIAN SD: Card Clearing
    └── tracker.py               ← BIAN SD: Capture + Merchant Settlement
```

---

## Tech stack

- **Apache Kafka** via [`lensesio/fast-data-dev`](https://hub.docker.com/r/lensesio/fast-data-dev) — single-container dev cluster (broker + Zookeeper + Schema Registry + Connect + Lenses UI)
- **[provectuslabs/kafka-ui](https://hub.docker.com/r/provectuslabs/kafka-ui)** — web UI for topic/group inspection
- **Python 3.14+** with **`confluent-kafka`** (librdkafka bindings)
- **LightGBM** for fraud scoring — in-process inference; offline training kit in `model_training/`
- **BIAN v12+ Service Domain taxonomy** — Card Authorization, Fraud/AML, Card Fee Pricing, Card Clearing, Card Transaction, Merchant Services

---

## License

For learning / demo purposes. No real card data is ever generated — PANs are
masked, BINs are test-range, and no PCI-scope data flows through the system.
