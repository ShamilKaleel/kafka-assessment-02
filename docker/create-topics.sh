#!/bin/sh
# Runs once inside the kafka-init container after the broker is healthy.
# Creates the topics the pipeline uses so they exist (and show in Kafka UI)
# before any producer or consumer starts. --if-not-exists makes it safe to
# re-run on every "docker compose up".
set -e

BOOTSTRAP="kafka:29092"

for topic in orders orders-dlq; do
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1
done

echo "Topics ready:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
