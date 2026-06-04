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

# Step 2: 等待服务就绪（使用轮询替代固定 sleep）
echo ""
echo "[Step 2/4] Waiting for services to be ready..."
echo "  - Waiting for Flink WebUI..."
until curl -s http://localhost:8081 > /dev/null 2>&1; do
    echo "    Flink not ready yet, retrying in 5s..."
    sleep 5
done
echo "  - Flink WebUI ready!"

# Step 3: 创建 Kafka topic
echo ""
echo "[Step 3/4] Creating Kafka topics..."
scripts/init-kafka-topics.sh

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
echo ""
echo "Before running Flink jobs, download Paimon JAR:"
echo "  wget https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/0.7.0/paimon-flink-1.18-0.7.0.jar"
echo "  docker cp paimon-flink-1.18-0.7.0.jar flink-jm:/opt/flink/lib/"
echo "================================================"
