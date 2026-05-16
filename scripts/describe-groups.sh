#!/usr/bin/env bash
# Describe Kafka consumer groups for the BIAN card-transaction pipeline.
#
# Usage:
#   ./scripts/describe-groups.sh                       # describe all pipeline groups
#   ./scripts/describe-groups.sh sd-fraud-detection    # describe a single group
#   ./scripts/describe-groups.sh --list                # just list groups in the cluster
#   CONTAINER=kafka-cluster ./scripts/describe-groups.sh
set -euo pipefail

CONTAINER="${CONTAINER:-kafka-cluster}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"

# Consumer group IDs, kept in sync with group.id values in src/*.py
GROUPS=(
  "sd-card-authorization"
  "sd-fraud-detection"
  "sd-card-fee-pricing"
  "sd-card-clearing"
  "sd-card-capture"
)

if [[ "${1:-}" == "--list" ]]; then
  echo "📋 All consumer groups in cluster:"
  docker exec "${CONTAINER}" kafka-consumer-groups \
    --bootstrap-server "${BOOTSTRAP}" \
    --list | sed 's/^/    /'
  exit 0
fi

if [[ $# -gt 0 ]]; then
  GROUPS=("$@")
fi

for group in "${GROUPS[@]}"; do
  echo
  echo "🔎 group: ${group}"
  echo "──────────────────────────────────────────────────────────────────────────────"
  docker exec "${CONTAINER}" kafka-consumer-groups \
    --bootstrap-server "${BOOTSTRAP}" \
    --describe \
    --group "${group}" \
    || echo "    (group not found or no committed offsets)"
done
