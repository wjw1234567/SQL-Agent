# Phase 2: 引入 Doris 查询加速 — 手动部署教程

## 概述

Phase 1 你已经学会了把数据写入 Paimon。但 Paimon 是基于文件的存储，查询速度有限。Phase 2 引入 **Doris** 作为查询加速层。

**学习目标：**
- 理解 Doris FE（前端）和 BE（后端）的角色
- 掌握两种查询方式：Paimon Catalog 零 ETL 查询 vs Flink Doris Connector 双写
- 学会用 MySQL 协议连接 Doris 查询数据
- 为后续 AI SQL-Agent 打好查询接口基础

**两种方式对比：**

| 方式 | 数据复制 | 查询性能 | 维护成本 | 适合场景 |
|---|---|---|---|---|
| Paimon Catalog | 无 | 中等，文件读取 | 低 | 零 ETL、数据探索 |
| Flink Connector | 有，双写 | 高，列存引擎 | 中 | 高并发、毫秒级查询 |

---

## 第一节：了解 Doris 架构

Doris 有两个核心组件：

```
Doris FE (Frontend)
  - 端口 9030: MySQL 协议（客户端连接）
  - 端口 8030: Web UI（http://localhost:8030）
  - 职责: 解析 SQL、生成执行计划、管理元数据

Doris BE (Backend)
  - 端口 9050: 数据存储和计算
  - 职责: 存储数据、执行查询计划、聚合计算
```

**架构关系：**

```
MySQL Client → FE(9030) → BE(9050) → 返回结果
     ↑
  你也可以通过 FE 的 Paimon Catalog 直接查询 Paimon 中的数据
```

---

## 第二节：启动完整环境（Phase 1 + 2）

Phase 2 的 `docker-compose-phase2.yml` 包含了 Phase 1 的全部服务 + Doris FE + Doris BE。

### 2.1 了解新增的 Doris 配置

```bash
cd phase2_doris

# 查看 docker-compose-phase2.yml 中新增的 Doris 部分
```

新增的 Doris 部分：

**Doris FE：**
- `image: apache/doris:2.0.3-fe` —— Doris 前端官方镜像
- 端口 `9030` —— MySQL 协议端口（用 mysql -h 127.0.0.1 -P 9030 -uroot 连接）
- 端口 `8030` —— Doris Web UI
- `healthcheck` —— 健康检查配置，等待 FE 就绪后再启动 BE

**Doris BE：**
- `image: apache/doris:2.0.3-be` —— Doris 后端官方镜像
- `depends_on: doris-fe: condition: service_healthy` —— 确保 FE 先启动

### 2.2 启动所有容器

```bash
# 启动 Kafka + Flink + Paimon + Doris
docker compose -f docker-compose-phase2.yml up -d
```

### 2.3 确认 Doris 就绪

Doris FE 启动比 Kafka 慢（约 30-60 秒），需要等待它健康：

```bash
# 查看所有容器状态
docker ps

# 等待 Doris FE 健康
while ! docker ps --filter "name=doris-fe" --filter "health=healthy" | grep -q healthy; do
    echo "Doris FE 还未就绪，等待 5 秒..."
    sleep 5
done
echo "Doris FE 就绪！"
```

### 2.4 验证 Doris 可用

通过 MySQL 协议连接 Doris：

```bash
docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "SELECT 1 AS test;"
```

如果返回一行 `test: 1`，说明 Doris 运行正常。

查看 BE 状态：

```bash
docker exec doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "SHOW PROC '/backends';"
```

应该能看到一个 `Alive: true` 的 BE。

---

## 第三节：创建 Kafka Topic（如果 Phase 1 清理了）

```bash
docker exec kafka kafka-topics \
  --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists
```

---

## 第四节：安装 Paimon JAR（如果 Phase 1 清理了）

```bash
# 确认 JAR 是否存在，没有的话重新安装
docker exec flink-jm ls /opt/flink/lib/ | grep paimon

# 如果不存在:
# wget https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/0.7.0/paimon-flink-1.18-0.7.0.jar
# docker cp paimon-flink-1.18-0.7.0.jar flink-jm:/opt/flink/lib/
# docker cp paimon-flink-1.18-0.7.0.jar flink-tm:/opt/flink/lib/
# docker restart flink-jm flink-tm
```

---

## 第五节：准备 Paimon 数据

如果你在 Phase 1 已经创建了 Paimon 表并写入了数据，Paimon 的数据文件在 `./data/paimon/warehouse/` 目录下，是持久化的，重启后仍然存在。

### 5.1 验证 Paimon 表和数据

进入 Flink SQL Client：

```bash
docker exec -it flink-jm sql-client.sh
```

在 Client 中：

```sql
-- 如果 Catalog 之前已创建，它会自动加载
-- 如果没有，重新创建:
CREATE CATALOG paimon_catalog WITH (
    'type' = 'filesystem',
    'warehouse' = 'file:///opt/paimon/data/warehouse'
);
USE CATALOG paimon_catalog;
USE demo;

-- 查看是否有表和数据的
SHOW TABLES;
SELECT COUNT(*) FROM orders;
```

如果数据为空，重新写入一些：

```sql
SET 'execution.runtime-mode' = 'batch';
CREATE TEMPORARY TABLE batch_source (... 跟 Phase 1 一样的建表语句 ...);
INSERT INTO orders SELECT * FROM batch_source;
SELECT COUNT(*) FROM orders;
```

### 5.2 退出 Flink SQL Client

```sql
quit;
```

---

## 第六节：方式一 — Doris Paimon Catalog 查询（零 ETL）

这是最直接的集成方式——Doris 直接读取 Paimon 的文件，不需要任何数据复制。

### 6.1 连接 Doris

```bash
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -uroot
```

这样你就进入了 Doris 的 MySQL 命令行（跟 MySQL 用法完全一样，提示符是 `mysql>`）。

### 6.2 注册 Paimon Catalog

在 Doris MySQL Client 中执行：

```sql
-- 注册 Paimon Catalog
-- 告诉 Doris 去哪里找 Paimon 的数据
CREATE CATALOG paimon_catalog PROPERTIES (
    'type' = 'paimon',
    'paimon.catalog.type' = 'filesystem',
    'paimon.catalog.warehouse' = 'file:///opt/paimon/data/warehouse'
);
```

**参数详解：**
- `'type' = 'paimon'` —— 固定为 'paimon'，告诉 Doris 这是 Paimon 类型的 Catalog
- `'paimon.catalog.type' = 'filesystem'` —— 与 Flink 中创建 Paimon Catalog 时一致
- `'paimon.catalog.warehouse' = '...'` —— 指向相同的 warehouse 路径

### 6.3 切换到 Paimon Catalog 并查询

```sql
-- 切换到 Paimon Catalog
SWITCH TO paimon_catalog;

-- 查看 Paimon 中的数据库和表
SHOW DATABASES;
USE demo;
SHOW TABLES;

-- 直接查询 Paimon 数据 —— 零 ETL！
-- Doris 会实时读取 Paimon 的文件并返回结果
SELECT category,
       COUNT(*) AS order_count,
       ROUND(SUM(total_amount), 2) AS total_revenue
FROM orders
GROUP BY category
ORDER BY total_revenue DESC;
```

**这就是"湖仓一体"的核心体验：** 数据在 Paimon 中存储（湖），Doris 直接查询（仓），不需要 ETL 搬运。

### 6.4 更复杂的分析查询

```sql
-- 按小时统计订单趋势
SELECT DATE_FORMAT(order_ts, '%Y-%m-%d %H:00:00') AS hour,
       COUNT(*) AS order_count,
       ROUND(SUM(total_amount), 2) AS revenue
FROM orders
GROUP BY hour
ORDER BY hour;

-- 热门商品 Top 10
SELECT product_name, category,
       SUM(quantity) AS total_sold,
       ROUND(SUM(total_amount), 2) AS revenue
FROM orders
GROUP BY product_name, category
ORDER BY total_sold DESC
LIMIT 10;
```

### 6.5 退出 Doris Client

```sql
exit;
```

---

## 第七节：方式二 — Flink Doris Connector（双写）

这种方式让 Flink 在写入 Paimon 的同时也写入 Doris，数据在 Doris 中独立存储一份，查询性能更优。

### 7.1 在 Doris 中创建目标表

```bash
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -uroot
```

```sql
-- 使用 lakehouse 数据库
USE lakehouse;

-- 创建 Doris 内部表（数据存储在 Doris BE 中）
-- DISTRIBUTED BY HASH 指定分桶方式，BUCKETS 4 设 4 个桶
-- replication_num = 1 因为只有一个 BE 节点
CREATE TABLE doris_orders (
    order_id      BIGINT,
    user_id       BIGINT,
    category      STRING,
    total_amount  DECIMAL(10,2),
    order_status  STRING,
    order_ts      DATETIME
) DISTRIBUTED BY HASH(order_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

-- 验证
SHOW TABLES;
exit;
```

### 7.2 用 Flink SQL 将数据写入 Doris

进入 Flink SQL Client：

```bash
docker exec -it flink-jm sql-client.sh
```

```sql
USE CATALOG paimon_catalog;
USE demo;

-- 创建 Doris 映射表
-- 这个表不是真的表，而是 Flink 和 Doris 之间的"桥梁"
CREATE TEMPORARY TABLE doris_sink (
    order_id      BIGINT,
    user_id       BIGINT,
    category      STRING,
    total_amount  DECIMAL(10,2),
    order_status  STRING,
    order_ts      TIMESTAMP(3)
) WITH (
    'connector' = 'doris',
    'fenodes' = 'doris-fe:8030',
    'table.identifier' = 'lakehouse.doris_orders',
    'username' = 'root',
    'password' = '',
    'sink.label-prefix' = 'flink_doris_sink',
    'sink.properties.format' = 'json',
    'sink.batch.size' = '1000',
    'sink.batch.interval' = '10s',
    'sink.max-retries' = '3'
);
```

**Doris Connector 参数详解：**
- `'connector' = 'doris'` —— 使用 Doris Flink 连接器
- `'fenodes' = 'doris-fe:8030'` —— Doris FE 地址（注意是 8030 不是 9030，Stream Load 协议用 8030）
- `'table.identifier' = 'lakehouse.doris_orders'` —— 目标表名 = 数据库.表名
- `'sink.label-prefix' = 'flink_doris_sink'` —— 标签前缀，用于去重（保证 exactly-once）
- `'sink.properties.format' = 'json'` —— 数据传输格式
- `'sink.batch.size' = '1000'` —— 攒够 1000 条再批量写入
- `'sink.batch.interval' = '10s'` —— 或每 10 秒写入一次

### 7.3 将 Paimon 的数据同步到 Doris

```sql
-- 切换到批式模式，一次性同步
SET 'execution.runtime-mode' = 'batch';

-- 从 Paimon 读 → 写入 Doris
INSERT INTO doris_sink
SELECT order_id, user_id, category, total_amount, order_status, order_ts
FROM orders;

quit;
```

### 7.4 验证 Doris 中的数据

```bash
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "
USE lakehouse;
SELECT COUNT(*) AS total_orders FROM doris_orders;
"
```

行数应该与 Paimon orders 表一致。

---

## 第八节：对比两种查询方式的性能

### 8.1 Paimon Catalog 查询

```bash
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "
SWITCH TO paimon_catalog.demo;
SELECT category, COUNT(*) AS cnt, ROUND(SUM(total_amount), 2) AS rev
FROM orders
GROUP BY category
ORDER BY rev DESC;
"
```

### 8.2 Doris 内部表查询（双写方式）

```bash
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "
USE lakehouse;
SELECT category, COUNT(*) AS cnt, ROUND(SUM(total_amount), 2) AS rev
FROM doris_orders
GROUP BY category
ORDER BY rev DESC;
"
```

两种查询结果应该一致。区别在于底层：
- Paimon Catalog：Doris 读取 Paimon 的列式文件
- Doris 内部表：Doris 读取自己的列式存储（更快）

---

## 第九节：运行双写 Flink 作业（DataStream API）

前面的步骤是手动分批同步。实际生产环境中，你会希望 Flink **实时**同时写入 Paimon 和 Doris。

### 9.1 了解 dual_writer.py

打开 `flink-job/dual_writer.py`：

```python
# 核心逻辑:
# 1. 注册 Paimon Catalog
# 2. 创建 Kafka Source 表（读取 orders topic）
# 3. 创建 Doris 映射表
# 4. 使用 StatementSet 同时提交两个 INSERT 到一个作业:
#    - INSERT INTO orders (Paimon)
#    - INSERT INTO doris_sink (Doris)
```

**关键点：** 使用 `StatementSet` 将两个 INSERT 合并为一个 Flink 作业，而不是两个独立作业。

### 9.2 提交双写作业

```bash
# 确保 Kafka 有数据
# 启动 Python producer 发送模拟数据
cd phase1_paimon
pip install kafka-python faker
python flink-job/kafka_producer.py &
```

新开一个终端提交 Flink 作业：

```bash
# 提交双写作业到 Flink
docker exec flink-jm flink run -py /opt/flink/job/dual_writer.py
```

### 9.3 验证双写结果

等待几秒后，分别查询 Paimon 和 Doris：

```bash
# 查询 Paimon
docker exec -it flink-jm sql-client.sh -e "
USE CATALOG paimon_catalog;
USE demo;
SELECT COUNT(*) AS paimon_count FROM orders;
"

# 查询 Doris
docker exec -it doris-fe mysql -h 127.0.0.1 -P 9030 -uroot -e "
USE lakehouse;
SELECT COUNT(*) AS doris_count FROM doris_orders;
"
```

两个数据量应该同步增长。

---

## 第十节：后续 — AI SQL-Agent 入口

Doris 提供 MySQL 协议接口（端口 9030），这意味着任何支持 MySQL 的客户端或 AI Agent，都可以这样连接：

```python
# Python 连接 Doris 示例（后续 SQL-Agent 的基础）
import pymysql

conn = pymysql.connect(
    host='127.0.0.1',
    port=9030,
    user='root',
    database='lakehouse'
)

cursor = conn.cursor()
cursor.execute("SELECT category, SUM(total_amount) FROM doris_orders GROUP BY category")
results = cursor.fetchall()
```

这就是 Phase 2 的最终成果——**一个可供 AI 查询的数据接口**。

---

## 清理环境

```bash
cd phase2_doris

# 停止所有容器（保留数据）
docker compose -f docker-compose-phase2.yml down

# 停止并删除数据
# docker compose -f docker-compose-phase2.yml down -v
```

---

## 总结：Phase 2 全链路

```
                      ┌──────────────────┐
                      │   MySQL Client    │
                      │  (端口 9030)      │
                      └────────┬─────────┘
                               │
                    ┌──────────▼──────────┐
                    │     Doris FE        │
                    │  两种查询方式:       │
                    └──────────┬──────────┘
                               │
               ┌───────────────┴───────────────┐
               ▼                               ▼
     ┌─────────────────┐             ┌─────────────────┐
     │  Paimon Catalog  │             │  Doris 内部表    │
     │  (零 ETL)        │             │  (双写数据)      │
     │  读 Paimon 文件  │             │  列存引擎更快    │
     └─────────────────┘             └─────────────────┘
               ▲                               ▲
               │                               │
     ┌─────────┴─────────┐          ┌──────────┴──────────┐
     │    Flink → Paimon │          │  Flink → Doris      │
     │    (实时写入)      │          │  (Doris Connector)  │
     └───────────────────┘          └─────────────────────┘
               ▲
               │
          ┌────┴────┐
          │  Kafka  │
          └─────────┘
```
