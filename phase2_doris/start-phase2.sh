#!/bin/bash
# ================================================================
# Phase 2: Kafka + Flink + Paimon + Doris 环境启动脚本
# ================================================================
# 使用方式:
#   chmod +x start-phase2.sh
#   ./start-phase2.sh
# ================================================================

set -e

echo "================================================"
echo "Phase 2: Starting Kafka + Flink + Paimon + Doris"
echo "================================================"

# Step 1: 启动所有容器
echo ""
echo "[Step 1/5] Starting Docker containers..."
docker compose -f docker-compose-phase2.yml up -d
echo "Waiting for services..."
sleep 20

# Step 2: 验证容器
echo ""
echo "[Step 2/5] Verifying containers..."
docker ps --format "table {{.Names}}\t{{.Status}}"

# Step 3: 创建 Kafka topic
echo ""
echo "[Step 3/5] Creating Kafka topic 'orders'..."
docker exec kafka kafka-topics.sh \
    --create \
    --topic orders \
    --bootstrap-server localhost:9092 \
    --partitions 3 \
    --replication-factor 1 \
    --if-not-exists

# Step 4: 验证 Doris 就绪
echo ""
echo "[Step 4/5] Verifying Doris is ready..."
docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "SELECT 1 AS doris_ready;"

# Step 5: 显示访问信息
echo ""
echo "[Step 5/5] Environment Ready!"
echo ""
echo "  Flink WebUI:      http://localhost:8081"
echo "  Doris WebUI:      http://localhost:8030"
echo "  Doris MySQL:      mysql -h 127.0.0.1 -P 9030 -uroot"
echo "  Kafka (external): localhost:29092"
echo ""
echo "================================================"
echo "Next steps:"
echo "  1. Enter Flink SQL Client:"
echo "     docker exec -it flink-jm sql-client.sh"
echo "  2. Execute Doris SQL:"
echo "     - 07_register_paimon.sql"
echo "     - 08_query_via_doris.sql"
echo "     - 09_flink_to_doris.sql"
echo "  3. Or run dual writer:"
echo "     docker exec flink-jm flink run -py /opt/flink/job/dual_writer.py"
echo "================================================"
