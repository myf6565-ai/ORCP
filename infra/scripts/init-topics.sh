#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPICS=(raw-events standard-events result-events dead-letter-events)

for t in "${TOPICS[@]}"; do
  kafka-topics --bootstrap-server "$BOOTSTRAP_SERVERS" --create --if-not-exists --topic "$t" --partitions 1 --replication-factor 1
  echo "topic ready: $t"
done
