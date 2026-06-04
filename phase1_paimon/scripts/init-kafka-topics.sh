#!/bin/bash
# Kafka topic initialization script
# Usage: ./scripts/init-kafka-topics.sh

echo ">>> Creating Kafka topic: orders..."
docker exec kafka kafka-topics.sh \
  --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo ">>> Verifying topics..."
docker exec kafka kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092

echo ">>> Done."
