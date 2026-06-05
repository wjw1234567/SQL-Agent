#!/bin/bash
# Kafka topic initialization script
# Usage: ./scripts/init-kafka-topics.sh

echo ">>> Waiting for Kafka to be ready..."
MAX_RETRIES=30
RETRY_COUNT=0
until docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo ">>> ERROR: Kafka not ready after $MAX_RETRIES attempts."
        exit 1
    fi
    echo ">>> Waiting... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 3
done

echo ">>> Creating Kafka topic: orders..."
docker exec kafka kafka-topics \
  --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists

echo ">>> Verifying topics..."
docker exec kafka kafka-topics \
  --list \
  --bootstrap-server localhost:9092

echo ">>> Done."
