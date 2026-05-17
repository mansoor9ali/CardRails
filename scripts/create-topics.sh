#!/usr/bin/env bash
# Create all Kafka topics for the BIAN card-transaction pipeline.
#
# Auto-detects which broker container is running:
#   - docker-compose.yaml            → container "kafka-cluster"
#   - docker-compose-withKraft.yaml  → container "kafka"
# Override with CONTAINER=... if you have a non-standard setup.
#
# Usage:
#   ./scripts/create-topics.sh                  # auto-detect container
#   CONTAINER=kafka ./scripts/create-topics.sh  # force a specific one
#   PARTITIONS=6 REPLICATION=1 ./scripts/create-topics.sh
set -euo pipefail

container_running() {
  [[ -n "$(docker ps --quiet --filter "name=^${1}$" 2>/dev/null)" ]]
}

if [[ -z "${CONTAINER:-}" ]]; then
  if container_running kafka-cluster; then
    CONTAINER=kafka-cluster
  elif container_running kafka; then
    CONTAINER=kafka
  else
    echo "❌ No Kafka broker container found (looked for 'kafka-cluster' and 'kafka')." >&2
    echo "   Start one with 'docker compose up -d' or 'docker compose -f docker-compose-withKraft.yaml up -d'," >&2
    echo "   or set CONTAINER=<name> explicitly." >&2
    exit 1
  fi
fi

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
