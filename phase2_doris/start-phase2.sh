#!/bin/bash
# ================================================================
# Phase 2: Kafka + Flink + Paimon + MinIO + Doris 环境启动脚本
# ================================================================
# 使用方式:
#   chmod +x start-phase2.sh
#   ./start-phase2.sh
# ================================================================

set -e

echo "================================================"
echo "Phase 2: Starting Kafka + Flink + Paimon + MinIO + Doris"
echo "================================================"

# Step 1: 启动所有容器
echo ""
echo "[Step 1/6] Starting Docker containers..."
docker compose -f docker-compose-phase2.yml up -d

# Step 2: 等待服务就绪
echo ""
echo "[Step 2/6] Waiting for services to be ready..."

echo "  - Waiting for Doris FE..."
until docker ps --filter "name=doris-fe" --filter "health=healthy" | grep -q healthy; do
    echo "    Doris FE not ready, retrying in 5s..."
    sleep 5
done
echo "  - Doris FE ready!"

echo "  - Waiting for Flink WebUI..."
until curl -s http://localhost:8081 > /dev/null 2>&1; do
    echo "    Flink not ready yet, retrying in 5s..."
    sleep 5
done
echo "  - Flink WebUI ready!"

echo "  - Waiting for MinIO..."
until curl -s http://localhost:9001 > /dev/null 2>&1; do
    echo "    MinIO not ready yet, retrying in 3s..."
    sleep 3
done
echo "  - MinIO ready!"

# Step 3: 创建 Kafka topic
echo ""
echo "[Step 3/6] Creating Kafka topics..."
../phase1_paimon/scripts/init-kafka-topics.sh

# Step 4: 验证 Doris
echo ""
echo "[Step 4/6] Verifying Doris..."
docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "SELECT 1 AS doris_ready;"

# Step 5: 显示访问信息
echo ""
echo "[Step 5/6] Environment Ready!"
echo ""
echo "  Flink WebUI:      http://localhost:8081"
echo "  Doris WebUI:      http://localhost:8030"
echo "  Doris MySQL:      mysql -h 127.0.0.1 -P 9030 -uroot"
echo "  MinIO Console:    http://localhost:9001  (minioadmin/minioadmin)"
echo "  Kafka (external): localhost:29092"
echo ""

# Step 6: 提示下一步
echo "[Step 6/6] Next steps:"
echo "  1. Enter Flink SQL Client:"
echo "     docker exec -it flink-jm sql-client.sh"
echo "  2. Execute Doris SQL:"
echo "     - 07_register_paimon.sql"
echo "     - 08_query_via_doris.sql"
echo "     - 09_flink_to_doris.sql"
echo "  3. Or run dual writer:"
echo "     docker exec flink-jm flink run -py /opt/flink/job/dual_writer.py"
echo ""
echo "Before running Flink jobs, download Paimon JAR:"
echo "  wget https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/0.7.0/paimon-flink-1.18-0.7.0.jar"
echo "  docker cp paimon-flink-1.18-0.7.0.jar flink-jm:/opt/flink/lib/"
echo "================================================"
