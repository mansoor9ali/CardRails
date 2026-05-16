#!/usr/bin/env bash
# Create all Kafka topics for the BIAN card-transaction pipeline.
#
# Usage:
#   ./scripts/create-topics.sh                  # uses defaults below
#   CONTAINER=kafka-cluster ./scripts/create-topics.sh
#   PARTITIONS=6 REPLICATION=1 ./scripts/create-topics.sh
set -euo pipefail

CONTAINER="${CONTAINER:-kafka-cluster}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
PARTITIONS="${PARTITIONS:-3}"
REPLICATION="${REPLICATION:-1}"

TOPICS=(
  "card.transaction.requested"
  "card.authorization.decided"
  "card.fraud.scored"
  "card.fee.calculated"
  "card.transaction.cleared"
  "merchant.settlement.posted"
)

echo "🛠  Creating ${#TOPICS[@]} topics on container '${CONTAINER}' (bootstrap=${BOOTSTRAP})"
echo "    partitions=${PARTITIONS}  replication=${REPLICATION}"
echo

for topic in "${TOPICS[@]}"; do
  printf "  → %-32s " "${topic}"
  if docker exec "${CONTAINER}" kafka-topics \
        --bootstrap-server "${BOOTSTRAP}" \
        --create \
        --if-not-exists \
        --topic "${topic}" \
        --partitions "${PARTITIONS}" \
        --replication-factor "${REPLICATION}" \
        > /dev/null 2>&1; then
    echo "✅ created (or already exists)"
  else
    echo "❌ failed"
    exit 1
  fi
done

echo
echo "📋 Final topic list:"
docker exec "${CONTAINER}" kafka-topics \
  --bootstrap-server "${BOOTSTRAP}" \
  --list | sed 's/^/    /'
