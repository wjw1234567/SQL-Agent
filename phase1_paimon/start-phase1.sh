#!/bin/bash
# ================================================================
# Phase 1: Kafka + Flink + Paimon 环境启动脚本
# ================================================================
# 使用方式:
#   chmod +x start-phase1.sh
#   ./start-phase1.sh
# ================================================================

set -e

echo "================================================"
echo "Phase 1: Starting Kafka + Flink + Paimon"
echo "================================================"

# Step 1: 启动所有 Docker 容器
echo ""
echo "[Step 1/4] Starting Docker containers..."
docker compose -f docker-compose-phase1.yml up -d
echo "Waiting for services to be ready..."
sleep 10

# Step 2: 验证容器状态
echo ""
echo "[Step 2/4] Verifying containers..."
docker ps --format "table {{.Names}}\t{{.Status}}" | head -5

# Step 3: 创建 Kafka topic
echo ""
echo "[Step 3/4] Creating Kafka topic 'orders'..."
docker exec kafka kafka-topics.sh \
    --create \
    --topic orders \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

# Step 4: 显示访问信息
echo ""
echo "[Step 4/4] Environment Ready!"
echo ""
echo "  Flink WebUI:    http://localhost:8081"
echo "  Kafka (internal): kafka:9092"
echo "  Kafka (external): localhost:29092"
echo ""
echo "================================================"
echo "Next steps:"
echo "  1. Enter Flink SQL Client:"
echo "     docker exec -it flink-jm sql-client.sh"
echo "  2. Execute SQL scripts in order:"
echo "     - 01_create_catalog.sql"
echo "     - 02_create_tables.sql"
echo "     - 03_streaming_write.sql"
echo "     - 04_batch_write.sql"
echo "     - 05_query.sql"
echo "  3. Or run the Python producer:"
echo "     python flink-job/kafka_producer.py"
echo "================================================"
