# Kafka + Flink + Paimon + Doris 学习教程 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a complete learning tutorial with code for deploying Kafka+Flink real-time/offline ETL writing to Paimon+Doris on local Docker Desktop.

**Architecture:** Two-phase learning path: Phase 1 covers Kafka → Flink → Paimon standalone; Phase 2 introduces Doris as query acceleration layer. Both phases include Docker Compose environment, Flink SQL tutorial scripts with inline parameter explanations, Flink DataStream API Python jobs, and startup scripts.

**Tech Stack:** Docker Compose (bitnami kafka/zookeeper, flink:1.18, apache/doris), Paimon 0.7, PyFlink, Flink SQL Client

---

## Spec Coverage Check

Before tasks, map spec requirements to tasks:

| Spec Requirement | Task |
|---|---|
| .gitignore + data dirs | Task 1 |
| Phase 1 Docker Compose | Task 2 |
| Flink config | Task 2 |
| Init Kafka topics script | Task 2 |
| 01_create_catalog.sql | Task 3 |
| 02_create_tables.sql | Task 4 |
| 03_streaming_write.sql | Task 5 |
| 04_batch_write.sql | Task 6 |
| 05_query.sql | Task 7 |
| 06_paimon_params.md | Task 8 |
| realtime_writer.py (DataStream) | Task 9 |
| kafka_producer.py | Task 10 |
| Phase 1 README + start script | Task 11 |
| Phase 2 Docker Compose + init | Task 12 |
| Phase 2 07_register_paimon.sql | Task 13 |
| Phase 2 08_query_via_doris.sql | Task 14 |
| Phase 2 09_flink_to_doris.sql | Task 15 |
| dual_writer.py | Task 16 |
| Phase 2 README + start script | Task 17 |
| Root README.md | Task 18 |

---

### Task 1: Project scaffolding

**Files:**
- Create: `.gitignore`
- Create: `data/paimon/.gitkeep`
- Create: `data/kafka/.gitkeep`

- [ ] **Step 1: Create .gitignore**

```gitignore
# Data directories
data/paimon/
data/kafka/

# IDE
.idea/
*.iml

# Python
__pycache__/
*.pyc
*.pyo
.eggs/
*.egg-info/

# OS
.DS_Store
Thumbs.db
```

- [ ] **Step 2: Create data placeholder files**

```bash
mkdir -p data/paimon data/kafka
touch data/paimon/.gitkeep data/kafka/.gitkeep
```

- [ ] **Step 3: Commit**

```bash
git add .gitignore data/paimon/.gitkeep data/kafka/.gitkeep
git commit -m "chore: add project scaffolding with gitignore"
```

---

### Task 2: Phase 1 Docker environment

**Files:**
- Create: `phase1_paimon/docker-compose-phase1.yml`
- Create: `phase1_paimon/flink-conf/flink-conf.yaml`
- Create: `phase1_paimon/scripts/init-kafka-topics.sh`

- [ ] **Step 1: Create docker-compose-phase1.yml**

```yaml
version: '3.8'

networks:
  sql-agent-net:
    driver: bridge

services:
  zookeeper:
    image: bitnami/zookeeper:3.9
    container_name: zk
    ports:
      - "2181:2181"
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes
    mem_limit: 512m
    networks:
      - sql-agent-net

  kafka:
    image: bitnami/kafka:3.6
    container_name: kafka
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CLIENT:PLAINTEXT,EXTERNAL:PLAINTEXT
      - KAFKA_CFG_LISTENERS=CLIENT://:9092,EXTERNAL://:29092
      - KAFKA_CFG_ADVERTISED_LISTENERS=CLIENT://kafka:9092,EXTERNAL://localhost:29092
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=CLIENT
    volumes:
      - ../data/kafka:/bitnami/kafka/data
    mem_limit: 2g
    depends_on:
      - zookeeper
    networks:
      - sql-agent-net

  flink-jobmanager:
    image: flink:1.18-java11
    container_name: flink-jm
    ports:
      - "8081:8081"
      - "6123:6123"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jm
        jobmanager.memory.process.size: 2048m
    volumes:
      - ../data/paimon:/opt/paimon/data
      - ./flink-conf/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml
      - ./sql:/opt/flink/sql
      - ./flink-job:/opt/flink/job
    mem_limit: 2g
    networks:
      - sql-agent-net

  flink-taskmanager:
    image: flink:1.18-java11
    container_name: flink-tm
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jm
        taskmanager.memory.process.size: 4096m
        taskmanager.numberOfTaskSlots: 4
    volumes:
      - ../data/paimon:/opt/paimon/data
      - ./flink-conf/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml
      - ./sql:/opt/flink/sql
      - ./flink-job:/opt/flink/job
    mem_limit: 4g
    depends_on:
      - flink-jobmanager
    networks:
      - sql-agent-net
```

- [ ] **Step 2: Create flink-conf.yaml**

```yaml
# Flink Configuration
jobmanager.rpc.address: flink-jm
jobmanager.memory.process.size: 2048m
taskmanager.memory.process.size: 4096m
taskmanager.numberOfTaskSlots: 4

# Checkpoint (required for Paimon exactly-once writes)
execution.checkpointing.interval: 30s
execution.checkpointing.mode: EXACTLY_ONCE
state.backend: hashmap
state.checkpoints.dir: file:///opt/paimon/data/checkpoints

# Paimon specific (will be downloaded at runtime via SQL Client)
# Paimon jars must be placed in /opt/flink/lib/ or added via ADD JAR
```

- [ ] **Step 3: Create init-kafka-topics.sh**

```bash
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
```

- [ ] **Step 4: Commit**

```bash
git add phase1_paimon/docker-compose-phase1.yml phase1_paimon/flink-conf/flink-conf.yaml phase1_paimon/scripts/init-kafka-topics.sh
git commit -m "feat: add Phase 1 Docker environment for Kafka+Flink+Paimon"
```

---

### Task 3: SQL tutorial — Paimon Catalog (01_create_catalog.sql)

**Files:**
- Create: `phase1_paimon/sql/01_create_catalog.sql`

- [ ] **Step 1: Create 01_create_catalog.sql**

```sql
-- ================================================================
-- Paimon Catalog 详解
-- ================================================================
-- Catalog 是 Paimon 的元数据管理入口，类比传统数据库的"实例"或
-- Hive 的 Metastore。Catalog 决定了表定义和元数据存储在哪里。
--
-- Paimon 支持以下 Catalog 类型:
--   type='filesystem'     — 元数据存储在文件系统（本地/HDFS/S3）
--   type='hive'           — 元数据存储在 Hive Metastore
--   type='jdbc'           — 元数据存储在关系数据库（MySQL/PostgreSQL）
--
-- 本教程使用 filesystem 类型，适合学习和单机部署。
-- ================================================================

-- ================================================================
-- 【参数详解】
-- ---------------------------------------------------------------
-- 参数名          | 必需 | 默认值   | 可选值              | 说明
-- ================================================================
-- type            | 是   | 无      | filesystem,hive,jdbc | Catalog 类型
-- warehouse       | 是   | 无      |                      | 数据存储根路径
--                |      |         |                      | 本地: /opt/paimon/data
--                |      |         |                      | HDFS: hdfs://namenode:8020/...
--                |      |         |                      | S3:  s3://bucket/...
-- ================================================================

-- 创建 Paimon Filesystem Catalog
CREATE CATALOG paimon_catalog WITH (
    'type' = 'filesystem',
    'warehouse' = 'file:///opt/paimon/data/warehouse'
);

-- 使用该 Catalog
USE CATALOG paimon_catalog;

-- 创建默认数据库
CREATE DATABASE IF NOT EXISTS demo;
USE demo;

-- 验证当前 Catalog 和数据库
SHOW CURRENT CATALOG;
SHOW DATABASES;

-- ================================================================
-- 【对比学习：如果想用 Hive Catalog】
-- ---------------------------------------------------------------
-- CREATE CATALOG hive_catalog WITH (
--     'type' = 'hive',
--     'hive-conf-dir' = '/opt/hive-conf'  -- 指向 hive-site.xml 目录
-- );
-- Filesystem Catalog 和 Hive Catalog 的区别:
--   - Filesystem: 轻量，无需额外服务，元数据以文件形式存储在 warehouse
--   - Hive: 需要 Hive Metastore 服务，元数据存储在关系数据库中
--   - Filesystem 适合开发和测试，Hive 适合生产环境
-- ================================================================

-- 预期输出:
-- SHOW CURRENT CATALOG → paimon_catalog
-- SHOW DATABASES → default, demo
```

- [ ] **Step 2: Commit**

```bash
git add phase1_paimon/sql/01_create_catalog.sql
git commit -m "feat: add Paimon Catalog tutorial SQL"
```

---

### Task 4: SQL tutorial — Paimon tables (02_create_tables.sql)

**Files:**
- Create: `phase1_paimon/sql/02_create_tables.sql`

- [ ] **Step 1: Create 02_create_tables.sql**

```sql
-- ================================================================
-- Paimon 表创建详解（DDL）
-- ================================================================
-- Paimon 表使用 Flink SQL 标准 DDL 语法，通过 WITH 参数控制
-- 存储行为。理解这些参数是使用好 Paimon 的关键。
-- ================================================================

-- 使用 Catalog 和数据库
USE CATALOG paimon_catalog;
USE demo;

-- ================================================================
-- 1. 创建订单表 (orders)
-- ================================================================
-- 【参数详解】
-- ---------------------------------------------------------------
-- 参数名              | 默认值       | 说明
-- ================================================================
-- connector           | (必需)       | 固定为 'paimon'
-- primary-key         | (可选)       | 主键字段，逗号分隔
-- bucket              | 1           | 每个分区的分桶数，影响并行度
--                    |              | 建议: 小表 1-4，大表按数据量调整
-- bucket-key          | 主键字段     | 分桶键，默认与主键一致
-- partition           | (可选)       | 分区字段，按日期/时间分区常用
-- merge-engine        | deduplicate | 合并引擎:
--                    |              |   deduplicate   — 去重（默认，保留最后一条）
--                    |              |   partial-update— 部分字段更新
--                    |              |   aggregation   — 聚合（SUM/MAX/MIN）
-- changelog-producer | none         | Changelog 生成方式:
--                    |              |   none  — 不生成（查询时计算）
--                    |              |   input — 根据输入生成
--                    |              |   lookup— 全量读取后对比生成
-- ================================================================

-- 创建订单表 — 最基本的主键表
CREATE TABLE IF NOT EXISTS orders (
    order_id      BIGINT,
    user_id       BIGINT,
    product_id    BIGINT,
    product_name  STRING,
    category      STRING,
    quantity      INT,
    unit_price    DECIMAL(10,2),
    total_amount  DECIMAL(10,2),
    order_status  STRING,
    order_ts      TIMESTAMP(3),
    proc_time     AS PROCTIME(),
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'bucket' = '4',
    'changelog-producer' = 'input'
);

-- 查看表结构
DESCRIBE orders;

-- ================================================================
-- 2. 创建订单分区表 (orders_partitioned)
-- ================================================================
-- 按日期分区 + 分桶 + aggregation merge-engine 演示

CREATE TABLE IF NOT EXISTS orders_partitioned (
    order_id      BIGINT,
    user_id       BIGINT,
    product_id    BIGINT,
    product_name  STRING,
    category      STRING,
    quantity      INT,
    unit_price    DECIMAL(10,2),
    total_amount  DECIMAL(10,2),
    order_status  STRING,
    order_ts      TIMESTAMP(3),
    partition_dt  STRING,        -- 分区字段: 日期字符串 yyyy-MM-dd
    PRIMARY KEY (order_id, partition_dt) NOT ENFORCED
) PARTITIONED BY (partition_dt)
WITH (
    'bucket' = '2',
    'changelog-producer' = 'input'
);

-- ================================================================
-- 3. 创建聚合表 (order_stats)
-- ================================================================
-- 使用 aggregation merge-engine 实现实时聚合

CREATE TABLE IF NOT EXISTS order_stats (
    category        STRING,
    total_orders    BIGINT,
    total_revenue   DECIMAL(14,2),
    window_start    TIMESTAMP(3),
    window_end      TIMESTAMP(3),
    PRIMARY KEY (category, window_start, window_end) NOT ENFORCED
) WITH (
    'bucket' = '2',
    'merge-engine' = 'aggregation',
    'changelog-producer' = 'input'
);

-- 查看所有表
SHOW TABLES;
```

- [ ] **Step 2: Commit**

```bash
git add phase1_paimon/sql/02_create_tables.sql
git commit -m "feat: add Paimon table DDL tutorial SQL"
```

---

### Task 5: SQL tutorial — Streaming write (03_streaming_write.sql)

**Files:**
- Create: `phase1_paimon/sql/03_streaming_write.sql`

- [ ] **Step 1: Create 03_streaming_write.sql**

```sql
-- ================================================================
-- Flink SQL 实时写入 Paimon
-- ================================================================
-- 流式写入（Streaming INSERT INTO）是 Paimon 最常用的写入模式。
-- 数据源可以是 Kafka、Kinesis、DataGen 等流式 Source。
--
-- 关键概念:
--   - INSERT INTO: 追加写入，流式模式下持续运行
--   - checkpoint: Paimon 依赖 Flink checkpoint 提交数据
--   - run_mode: 当前作业的运行模式
-- ================================================================

USE CATALOG paimon_catalog;
USE demo;

-- ================================================================
-- 【参数详解】
-- ---------------------------------------------------------------
-- 参数名                       | 作用位置  | 说明
-- ================================================================
-- execution.checkpointing.*    | Flink     | checkpoint 配置
-- scan.startup.mode            | Source    | Kafka 消费起始位置:
--                               |           |   earliest, latest, timestamp
-- scan.startup.timestamp-millis| Source    | 指定起始时间戳（ms）
-- sink.parallelism             | Sink      | 写入并行度
-- ================================================================

-- 设置 Flink Session 级别参数
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'parallelism.default' = '2';

-- ================================================================
-- 方式 1: 使用 DataGen 生成模拟数据写入 Paimon
-- ================================================================
-- 这种方式不需要外部 Kafka，完全由 Flink 内置 DataGen 生成数据，
-- 适合快速验证 Paimon 写入功能。

-- 创建 DataGen Source 表
CREATE TEMPORARY TABLE datagen_orders (
    order_id      BIGINT,
    user_id       BIGINT,
    product_id    BIGINT,
    product_name  STRING,
    category      STRING,
    quantity      INT,
    unit_price    DECIMAL(10,2),
    total_amount  DECIMAL(10,2),
    order_status  STRING,
    order_ts      TIMESTAMP(3)
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '5',
    'fields.order_id.kind' = 'sequence',
    'fields.order_id.start' = '1',
    'fields.order_id.end' = '1000000',
    'fields.user_id.kind' = 'sequence',
    'fields.user_id.start' = '1001',
    'fields.user_id.end' = '9999',
    'fields.product_id.kind' = 'sequence',
    'fields.product_id.start' = '20001',
    'fields.product_id.end' = '30000',
    'fields.product_name.length' = '10',
    'fields.category.length' = '6',
    'fields.quantity.min' = '1',
    'fields.quantity.max' = '10',
    'fields.unit_price.min' = '10',
    'fields.unit_price.max' = '1000',
    'fields.order_status.length' = '8',
    'fields.order_ts.kind' = 'random',
    'fields.order_ts.max-past' = '3600000'
);

-- 流式写入 Paimon orders 表
INSERT INTO orders
SELECT * FROM datagen_orders;

-- ================================================================
-- 方式 2: 从 Kafka 消费写入 Paimon
-- ================================================================
-- 等 Kafka 环境就绪后使用此方式替代 DataGen

-- CREATE TEMPORARY TABLE kafka_orders (
--     order_id      BIGINT,
--     user_id       BIGINT,
--     product_id    BIGINT,
--     product_name  STRING,
--     category      STRING,
--     quantity      INT,
--     unit_price    DECIMAL(10,2),
--     total_amount  DECIMAL(10,2),
--     order_status  STRING,
--     order_ts      TIMESTAMP(3)
-- ) WITH (
--     'connector' = 'kafka',
--     'topic' = 'orders',
--     'properties.bootstrap.servers' = 'kafka:9092',
--     'properties.group.id' = 'paimon-consumer',
--     'scan.startup.mode' = 'earliest-offset',
--     'format' = 'json',
--     'json.fail-on-missing-field' = 'false'
-- );
--
-- INSERT INTO orders SELECT * FROM kafka_orders;

-- ================================================================
-- 验 证
-- ================================================================
-- 在新窗口中运行以下查询:
-- SELECT COUNT(*) FROM orders;
-- 预期: 数字持续增长

-- ================================================================
-- 【学习要点】
-- ================================================================
-- 1. INSERT INTO 在流式模式下是一个持续运行的作业
-- 2. Checkpoint 触发时 Paimon 提交一个 snapshot
-- 3. 可以在 Flink WebUI 中观察作业状态: http://localhost:8081
-- 4. Paimon 写入是 ACID 的，查询只会看到已提交的数据
-- ================================================================
```

- [ ] **Step 2: Commit**

```bash
git add phase1_paimon/sql/03_streaming_write.sql
git commit -m "feat: add Paimon streaming write tutorial SQL"
```

---

### Task 6: SQL tutorial — Batch write (04_batch_write.sql)

**Files:**
- Create: `phase1_paimon/sql/04_batch_write.sql`

- [ ] **Step 1: Create 04_batch_write.sql**

```sql
-- ================================================================
-- Flink SQL 离线写入 Paimon (Batch INSERT OVERWRITE)
-- ================================================================
-- 离线（批式）写入使用 INSERT OVERWRITE 语法，会覆盖已有数据。
-- 常用于:
--   - 每天的全量数据覆盖
--   - 历史数据回填
--   - 数据修正
--
-- 核心区别:
--   INSERT INTO    → 追加写入（流式或批式皆可）
--   INSERT OVERWRITE → 覆盖写入（仅批式）
-- ================================================================

USE CATALOG paimon_catalog;
USE demo;

-- ================================================================
-- 【参数详解】
-- ---------------------------------------------------------------
-- 参数名              | 设置位置   | 说明
-- ================================================================
-- execution.runtime-mode | Session | 切换至 batch 模式
-- table.exec.sink.upsert-materialize | Flink | 批式写入时的物化策略
-- ================================================================

-- 切换到 Batch 模式
SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.resource.default-parallelism' = '2';

-- 预先写入一些数据以便演示覆盖效果（使用 DataGen）
-- 注意: 在 Batch 模式下，DataGen 生成固定行数（默认 100 行）
CREATE TEMPORARY TABLE batch_source (
    order_id      BIGINT,
    user_id       BIGINT,
    product_id    BIGINT,
    product_name  STRING,
    category      STRING,
    quantity      INT,
    unit_price    DECIMAL(10,2),
    total_amount  DECIMAL(10,2),
    order_status  STRING,
    order_ts      TIMESTAMP(3)
) WITH (
    'connector' = 'datagen',
    'number-of-rows' = '50',        -- Batch 模式下生成固定行数
    'fields.order_id.kind' = 'sequence',
    'fields.order_id.start' = '1',
    'fields.order_id.end' = '1000',
    'fields.user_id.kind' = 'sequence',
    'fields.user_id.start' = '1001',
    'fields.user_id.end' = '9999',
    'fields.product_name.length' = '10',
    'fields.category.length' = '6',
    'fields.quantity.min' = '1',
    'fields.quantity.max' = '5',
    'fields.unit_price.min' = '10',
    'fields.unit_price.max' = '500',
    'fields.order_status.length' = '8',
    'fields.order_ts.kind' = 'random',
    'fields.order_ts.max-past' = '86400000'
);

-- ================================================================
-- 1. 批量追加写入 (INSERT INTO batch mode)
-- ================================================================
-- 在 Batch 模式下，INSERT INTO 执行有限行数后自动结束
INSERT INTO orders SELECT * FROM batch_source;

-- 验证
SELECT COUNT(*) AS total_orders FROM orders;

-- ================================================================
-- 2. 覆盖写入 (INSERT OVERWRITE)
-- ================================================================
-- INSERT OVERWRITE 会先清空表或分区，再写入新数据
-- 方式一: 覆盖整表
INSERT OVERWRITE orders
SELECT * FROM batch_source
WHERE order_status <> 'cancelled';  -- 只保留非取消订单

-- 方式二: 覆盖指定分区（需表有分区）
-- INSERT OVERWRITE orders_partitioned
-- SELECT *, DATE_FORMAT(order_ts, 'yyyy-MM-dd') AS partition_dt
-- FROM batch_source
-- WHERE DATE_FORMAT(order_ts, 'yyyy-MM-dd') = '2024-01-15';

-- ================================================================
-- 【学习要点】
-- ================================================================
-- 1. INSERT OVERWRITE 会清空旧数据再写入，注意不要误操作
-- 2. 分区表上使用 OVERWRITE 可以只覆盖特定分区
-- 3. Batch 模式下 INSERT INTO 执行完成后自动停止（非持续运行）
-- 4. Paimon 的 Streaming 和 Batch 读写是同一份存储，无需切换
-- ================================================================

-- 切回流模式（可选）
-- SET 'execution.runtime-mode' = 'streaming';
```

- [ ] **Step 2: Commit**

```bash
git add phase1_paimon/sql/04_batch_write.sql
git commit -m "feat: add Paimon batch write tutorial SQL"
```

---

### Task 7: SQL tutorial — Query (05_query.sql)

**Files:**
- Create: `phase1_paimon/sql/05_query.sql`

- [ ] **Step 1: Create 05_query.sql**

```sql
-- ================================================================
-- Paimon 查询详解: 批式读 vs 流式读
-- ================================================================
-- Paimon 支持两种查询模式:
--   批式读 (Batch Read): 读取当前最新全量快照，执行完毕即结束
--   流式读 (Streaming Read): 持续监控新提交的 snapshot，增量输出
--
-- 核心: Paimon 的每一次 checkpoint 产生一个 snapshot，
-- 查询可以选择从任意 snapshot 读取。
-- ================================================================

USE CATALOG paimon_catalog;
USE demo;

-- ================================================================
-- 【参数详解】
-- ---------------------------------------------------------------
-- 参数名              | 使用位置   | 说明
-- ================================================================
-- scan.mode           | 表级别 WITH | 扫描模式（核心参数）
--                    |            |   latest          — 读取最新快照（默认）
--                    |            |   from-timestamp  — 从指定时间戳读取
--                    |            |   from-snapshot   — 从指定 snapshot ID 读取
--                    |            |   full            — 读取所有历史快照
-- scan.snapshot-id    | 表级别 WITH | 指定起始 snapshot ID
-- scan.timestamp-millis | 表级别   | 指定起始时间戳（毫秒）
-- scan.file-creation | 表级别 WITH | 按文件创建时间筛选
--   -timestamp-millis|            |
-- ================================================================

-- ================================================================
-- 1. 批式读 — 读取最新快照（默认方式）
-- ================================================================
SET 'execution.runtime-mode' = 'batch';

-- 全表扫描（读取 orders 表最新数据）
SELECT category,
       COUNT(*) AS order_count,
       SUM(total_amount) AS revenue,
       AVG(unit_price) AS avg_price
FROM orders
GROUP BY category
ORDER BY revenue DESC;

-- 查看 Paimon 表有多少条记录
SELECT COUNT(*) AS total_orders FROM orders;

-- ================================================================
-- 2. 从指定时间戳读取（增量读取）
-- ================================================================
-- 场景: 读取 2024-01-01 08:00:00 之后提交的数据
-- 注意: 时间戳是 Paimon snapshot 的提交时间，不是数据时间
SELECT order_id, category, total_amount, order_ts
FROM orders /*+ OPTIONS('scan.mode'='from-timestamp',
                        'scan.timestamp-millis'='1704067200000') */
WHERE category = 'Electronics'
LIMIT 20;

-- ================================================================
-- 3. 流式读 — 持续监控新数据
-- ================================================================
SET 'execution.runtime-mode' = 'streaming';

-- 先切回流模式，然后持续监控 orders 表的新数据
-- 如果 orders 表持续有数据写入，该查询会不断输出新数据
SELECT order_id, category, total_amount, order_status
FROM orders /*+ OPTIONS('scan.mode'='latest') */;

-- ================================================================
-- 4. 读取聚合表
-- ================================================================
SELECT * FROM order_stats;

-- ================================================================
-- 【学习要点】
-- ================================================================
-- 1. scan.mode='latest' → 只读最新快照，常用于流式读的"从当前开始"
-- 2. scan.mode='from-timestamp' → 增量读取，适合做增量 ETL
-- 3. scan.mode='full' → 全量历史（流式模式下会在读完后持续增量）
-- 4. 批式读和流式读操作的是同一份底层数据，不需要复制
-- 5. Paimon 的 snapshot 机制保证了时间旅行 (Time Travel) 能力
-- ================================================================
```

- [ ] **Step 2: Commit**

```bash
git add phase1_paimon/sql/05_query.sql
git commit -m "feat: add Paimon query tutorial SQL"
```

---

### Task 8: Paimon parameters reference (06_paimon_params.md)

**Files:**
- Create: `phase1_paimon/sql/06_paimon_params.md`

- [ ] **Step 1: Create 06_paimon_params.md**

```markdown
# Paimon 核心参数完整参考

## 1. Catalog 参数

| 参数 | 必需 | 默认值 | 说明 |
|---|---|---|---|
| `type` | 是 | — | Catalog 类型: `filesystem` / `hive` / `jdbc` |
| `warehouse` | 是 | — | 数据仓库根路径 |
| `metastore` | 否 | — | Hive 方式时指定 metastore 类型 |

## 2. 表级核心参数

### 主键与分桶

| 参数 | 默认值 | 说明 |
|---|---|---|
| `bucket` | `1` | 每个分区的分桶数。影响写入并行度和读取并发。建议从 `2-4` 开始，后续按数据量调整。每个 bucket 对应一个文件。 |
| `bucket-key` | 主键字段 | 分桶键，决定数据如何分布到不同 bucket。 |

**Bucket 选择建议:**
- 小表 (<100 万行): `bucket=2`
- 中表 (<1 亿行): `bucket=4-8`
- 大表 (>1 亿行): `bucket=16-64`

### 分区

| 参数 | 默认值 | 说明 |
|---|---|---|
| `partition` | 无 | 在 DDL 中用 `PARTITIONED BY (col)` 指定。常用日期分区。 |

### Merge Engine

| 参数  | 默认值 | 说明 |
|---|---|---|
| `merge-engine` | `deduplicate` | Merge 引擎类型 |
| | | `deduplicate` — 默认，相同主键保留最后一条写入 |
| | | `partial-update` — 相同主键下逐字段更新 |
| | | `aggregation` — 相同主键下按聚合函数合并 |

**merge-engine 选择场景:**
- 普通 CDC: `deduplicate`
- 宽表逐字段填充: `partial-update`
- 实时指标聚合（SUM/COUNT/MIN/MAX）: `aggregation`

### Changelog Producer

| 参数 | 默认值 | 说明 |
|---|---|---|
| `changelog-producer` | `none` | Changelog 生成方式 |
| | | `none` — 不额外生成 changelog，需要时从文件计算 |
| | | `input` — 根据输入流的变更记录生成（推荐） |
| | | `lookup` — 通过全量读取对比生成，消耗较大 |

## 3. 查询参数 (通过 `/*+ OPTIONS() */` 设置)

| 参数 | 默认值 | 说明 |
|---|---|---|
| `scan.mode` | `latest` | 扫描模式: `latest` / `from-timestamp` / `from-snapshot` / `full` |
| `scan.snapshot-id` | — | 配合 `from-snapshot` 使用 |
| `scan.timestamp-millis` | — | 配合 `from-timestamp` 使用 |

## 4. Flink Session 参数

| 参数 | 推荐值 | 说明 |
|---|---|---|
| `execution.checkpointing.interval` | `30s` | Checkpoint 间隔，Paimon 提交依赖此设置 |
| `execution.checkpointing.mode` | `EXACTLY_ONCE` | 保证 Paimon 写入一致性 |
| `execution.runtime-mode` | `streaming` / `batch` | 流式或批式运行模式 |

## 5. 常见问题

**Q: Bucket 数可以修改吗？**
可以。ALTER TABLE 修改 bucket 后，新写入的数据会按新 bucket 数分布。旧数据保持不变。

**Q: merge-engine 建表后能改吗？**
不能。merge-engine 决定了底层文件的合并策略，建表后不可修改。

**Q: Paimon 和 Hudi/Iceberg 有什么区别？**
Paimon 的 Merge Engine 设计使其在流式写入场景更灵活（partial-update / aggregation），而 Iceberg 更侧重 ACID 和快照隔离。
```

- [ ] **Step 2: Commit**

```bash
git add phase1_paimon/sql/06_paimon_params.md
git commit -m "feat: add Paimon parameters reference document"
```

---

### Task 9: Flink DataStream realtime writer

**Files:**
- Create: `phase1_paimon/flink-job/realtime_writer.py`

- [ ] **Step 1: Create realtime_writer.py**

```python
# ================================================================
# Flink DataStream API — 从 Kafka 实时写入 Paimon
# ================================================================
# 这个作业演示如何使用 PyFlink DataStream API 实现:
#   Kafka Source → JSON 解析 → Paimon Sink
#
# 运行方式:
#   docker exec flink-jm flink run -py /opt/flink/job/realtime_writer.py
#
# 前提条件:
#   1. Paimon 表已创建 (通过 02_create_tables.sql)
#   2. Kafka topic 'orders' 已创建
#   3. Paimon Flink jar 已放置在 /opt/flink/lib/
# ================================================================

import json
from datetime import datetime, timezone

from pyflink.common import (
    Configuration,
    WatermarkStrategy,
    Time,
    Types,
    SimpleStringSchema,
)
from pyflink.datastream import (
    StreamExecutionEnvironment,
    DataStream,
)
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.table import (
    StreamTableEnvironment,
    TableDescriptor,
    Schema,
    DataTypes,
)


def create_kafka_source(env: StreamExecutionEnvironment) -> DataStream:
    """
    创建 Kafka Source，消费 'orders' topic 的 JSON 消息。
    
    参数说明:
    - bootstrap.servers: Kafka 地址（容器内使用服务名 kafka:9092）
    - group.id: 消费组名，用于记录消费偏移量
    - topics: 订阅的 topic 列表
    - KafkaOffsetsInitializer.latest(): 从最新位置开始消费
                                      可选: earliest() 从最旧位置
    """
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_group_id("paimon-flink-consumer")
        .set_topics("orders")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="kafka_orders_source",
    )


def parse_and_write_to_paimon(stream: DataStream) -> None:
    """
    将 Kafka JSON 消息解析后写入 Paimon 表。
    
    这里通过 Table API 将 DataStream 写入 Table，
    利用 Paimon Flink Sink 的 exactly-once 语义。
    """
    # 将 DataStream<String> 转为 Table
    table_env = StreamTableEnvironment.create(stream.execution_environment)

    # 注册 Paimon Catalog（与 SQL 教程中的 Catalog 一致）
    table_env.execute_sql(
        """
        CREATE CATALOG paimon_catalog WITH (
            'type' = 'filesystem',
            'warehouse' = 'file:///opt/paimon/data/warehouse'
        )
        """
    )
    table_env.use_catalog("paimon_catalog")
    table_env.use_database("demo")

    # 将 DataStream 创建为临时视图
    # 先用 Map 操作解析 JSON 字符串
    parsed_stream = stream.map(lambda msg: json.loads(msg))

    # 定义表结构并写入 Paimon
    # 注意: DataStream 写入 Paimon 需要字段类型匹配
    table = table_env.from_data_stream(
        parsed_stream,
        Schema.new_builder()
        .column("order_id", DataTypes.BIGINT())
        .column("user_id", DataTypes.BIGINT())
        .column("product_id", DataTypes.BIGINT())
        .column("product_name", DataTypes.STRING())
        .column("category", DataTypes.STRING())
        .column("quantity", DataTypes.INT())
        .column("unit_price", DataTypes.DECIMAL(10, 2))
        .column("total_amount", DataTypes.DECIMAL(10, 2))
        .column("order_status", DataTypes.STRING())
        .column("order_ts", DataTypes.TIMESTAMP(3))
        .build(),
    )

    # 写入 Paimon orders 表
    table.execute_insert("orders").wait()


if __name__ == "__main__":
    # 创建执行环境
    config = Configuration()
    # 设置 checkpoint（Paimon 写入的必要条件）
    config.set_string("execution.checkpointing.interval", "30s")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(2)

    # 构建作业
    kafka_stream = create_kafka_source(env)
    parse_and_write_to_paimon(kafka_stream)

    # 启动作业
    env.execute("Kafka-to-Paimon Realtime Writer")
```

- [ ] **Step 2: Commit**

```bash
git add phase1_paimon/flink-job/realtime_writer.py
git commit -m "feat: add Flink DataStream API realtime writer to Paimon"
```

---

### Task 10: Kafka producer script

**Files:**
- Create: `phase1_paimon/flink-job/kafka_producer.py`

- [ ] **Step 1: Create kafka_producer.py**

```python
# ================================================================
# Python Kafka 模拟数据生产者
# ================================================================
# 运行本脚本会向 Kafka 的 'orders' topic 持续发送模拟订单数据。
# 用于验证 Kafka → Flink → Paimon 的整条链路。
#
# 运行方式:
#   pip install kafka-python faker
#   python flink-job/kafka_producer.py
#
# 如果遇到模块缺失: pip install kafka-python faker
# ================================================================

import json
import random
import time
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker("zh_CN")  # 使用中文生成器，生成中文商品名

# Kafka 配置
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"  # 使用外部端口
TOPIC = "orders"

# 商品分类
CATEGORIES = [
    "Electronics",
    "Clothing",
    "Books",
    "Home",
    "Sports",
    "Food",
    "Toys",
]

# 订单状态
ORDER_STATUSES = ["pending", "paid", "shipped", "cancelled"]

# 商品名称池（模拟数据）
PRODUCT_NAMES = [
    "Wireless Headphones",
    "Cotton T-Shirt",
    "Python Cookbook",
    "Desk Lamp",
    "Yoga Mat",
    "Green Tea",
    "Building Blocks",
    "Running Shoes",
    "Coffee Maker",
    "Backpack",
]


def generate_order(order_id: int) -> dict:
    """生成一条模拟订单数据"""
    category = random.choice(CATEGORIES)
    product_name = random.choice(PRODUCT_NAMES)
    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(10.0, 999.0), 2)
    total_amount = round(quantity * unit_price, 2)

    return {
        "order_id": order_id,
        "user_id": random.randint(1001, 9999),
        "product_id": random.randint(20001, 30000),
        "product_name": product_name,
        "category": category,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "order_status": random.choice(ORDER_STATUSES),
        "order_ts": datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }


def main():
    # 创建 Kafka Producer
    # 参数说明:
    #   bootstrap_servers: Kafka 地址（本地运行用 29092 外部端口）
    #   value_serializer: 将 Python dict 序列化为 JSON 字节
    #   acks='all': 等待所有副本确认，保证不丢数据
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
    )

    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    print(f"Sending orders to topic '{TOPIC}'...")
    print("Press Ctrl+C to stop.\n")

    order_id = 1
    try:
        while True:
            order = generate_order(order_id)
            # 发送消息到 Kafka
            # 异步发送，可通过 future.get() 确认发送成功
            future = producer.send(TOPIC, value=order)
            # 可选: 等待发送确认
            # record_metadata = future.get(timeout=10)
            # print(f"Sent: offset={record_metadata.offset}")

            print(
                f"Sent order #{order_id}: "
                f"{order['product_name']} x{order['quantity']} = "
                f"${order['total_amount']} [{order['category']}]"
            )

            order_id += 1
            time.sleep(random.uniform(0.5, 2.0))  # 模拟随机间隔

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()
        print(f"Sent {order_id - 1} orders total.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add phase1_paimon/flink-job/kafka_producer.py
git commit -m "feat: add Kafka producer script for simulated orders"
```

---

### Task 11: Phase 1 README + start script

**Files:**
- Create: `phase1_paimon/start-phase1.sh`
- Create: `phase1_paimon/README.md`

- [ ] **Step 1: Create start-phase1.sh**

```bash
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
```

- [ ] **Step 2: Create phase1_paimon/README.md**

```markdown
# Phase 1: Kafka + Flink + Paimon

## 学习目标

理解 Paimon 的流批一体存储能力，掌握：
- Paimon Filesystem Catalog 的配置
- 主键表、分区表、聚合表的创建与参数含义
- Flink SQL 流式写入和批式写入
- Paimon 的 snapshot 机制与 Time Travel 查询
- Paimon 核心参数: bucket, merge-engine, changelog-producer, scan.mode

## 环境

| 组件 | 版本 | 端口 |
|---|---|---|
| Zookeeper | 3.9 | 2181 |
| Kafka | 3.6 | 9092 / 29092 |
| Flink | 1.18 (Java 11) | 8081 (WebUI), 6123 (RPC) |
| Paimon | 0.7 | (嵌入 Flink) |

## 快速开始

### 1. 启动环境

```bash
./start-phase1.sh
```

### 2. 进入 Flink SQL Client

```bash
docker exec -it flink-jm sql-client.sh
```

### 3. 按顺序执行 SQL 教程

在 SQL Client 中逐条执行 `sql/` 目录下的脚本。可以将 SQL 内容复制粘贴到 Client，或使用:

```sql
-- 直接复制执行
CREATE CATALOG paimon_catalog WITH (...);
```

### 4. 提交 DataStream 作业

```bash
docker exec flink-jm flink run -py /opt/flink/job/realtime_writer.py
```

### 5. 运行 Kafka Producer（可选，在宿主机新终端）

```bash
pip install kafka-python faker
python flink-job/kafka_producer.py
```

## 文件说明

| 文件 | 说明 |
|---|---|
| `docker-compose-phase1.yml` | Docker Compose 配置 |
| `flink-conf/flink-conf.yaml` | Flink 配置（checkpoint 等） |
| `sql/01_create_catalog.sql` | Paimon Catalog 教程 |
| `sql/02_create_tables.sql` | 表创建 DDL 教程 |
| `sql/03_streaming_write.sql` | 流式写入教程 |
| `sql/04_batch_write.sql` | 批式写入教程 |
| `sql/05_query.sql` | 查询教程 |
| `sql/06_paimon_params.md` | Paimon 参数参考 |
| `flink-job/realtime_writer.py` | DataStream API 实时写入 |
| `flink-job/kafka_producer.py` | Python 模拟数据生产者 |

## 停止环境

```bash
docker compose -f docker-compose-phase1.yml down
```

要同时删除数据:

```bash
docker compose -f docker-compose-phase1.yml down -v
```

## Troubleshooting

**Q: Flink SQL Client 连不上 Kafka？**
确保 Kafka 地址使用容器内地址 `kafka:9092`，不是 `localhost:9092`。

**Q: Paimon 表查询为空？**
检查 checkpoint 是否已完成。Paimon 在 checkpoint 时提交数据。

**Q: Docker 内存不足？**
调整 docker-compose 中的 mem_limit 值，或停止不必要的容器。
```

- [ ] **Step 3: Commit**

```bash
git add phase1_paimon/start-phase1.sh phase1_paimon/README.md
git commit -m "docs: add Phase 1 README and startup script"
```

---

### Task 12: Phase 2 Docker environment

**Files:**
- Create: `phase2_doris/docker-compose-phase2.yml`
- Create: `phase2_doris/doris-init/init.sql`

- [ ] **Step 1: Create docker-compose-phase2.yml**

```yaml
version: '3.8'

# Phase 2: 在 Phase 1 基础上增加 Doris FE 和 BE
# 使用方式: docker compose -f docker-compose-phase2.yml up -d

networks:
  sql-agent-net:
    driver: bridge

services:
  # ========== Phase 1 服务 (完整移植) ==========
  zookeeper:
    image: bitnami/zookeeper:3.9
    container_name: zk
    ports:
      - "2181:2181"
    environment:
      - ALLOW_ANONYMOUS_LOGIN=yes
    mem_limit: 512m
    networks:
      - sql-agent-net

  kafka:
    image: bitnami/kafka:3.6
    container_name: kafka
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      - KAFKA_BROKER_ID=1
      - KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=CLIENT:PLAINTEXT,EXTERNAL:PLAINTEXT
      - KAFKA_CFG_LISTENERS=CLIENT://:9092,EXTERNAL://:29092
      - KAFKA_CFG_ADVERTISED_LISTENERS=CLIENT://kafka:9092,EXTERNAL://localhost:29092
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=CLIENT
    volumes:
      - ../data/kafka:/bitnami/kafka/data
    mem_limit: 2g
    depends_on:
      - zookeeper
    networks:
      - sql-agent-net

  flink-jobmanager:
    image: flink:1.18-java11
    container_name: flink-jm
    ports:
      - "8081:8081"
      - "6123:6123"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jm
        jobmanager.memory.process.size: 2048m
    volumes:
      - ../data/paimon:/opt/paimon/data
      - ../phase1_paimon/flink-conf/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml
      - ./sql:/opt/flink/sql
      - ./flink-job:/opt/flink/job
    mem_limit: 2g
    networks:
      - sql-agent-net

  flink-taskmanager:
    image: flink:1.18-java11
    container_name: flink-tm
    command: taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: flink-jm
        taskmanager.memory.process.size: 4096m
        taskmanager.numberOfTaskSlots: 4
    volumes:
      - ../data/paimon:/opt/paimon/data
      - ../phase1_paimon/flink-conf/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml
      - ./sql:/opt/flink/sql
      - ./flink-job:/opt/flink/job
    mem_limit: 4g
    depends_on:
      - flink-jobmanager
    networks:
      - sql-agent-net

  # ========== Phase 2 新增: Doris ==========
  doris-fe:
    image: apache/doris:2.0.3-fe
    container_name: doris-fe
    ports:
      - "9030:9030"    # MySQL 协议端口
      - "8030:8030"    # Doris WebUI
    volumes:
      - ./doris-init:/opt/doris/init
    mem_limit: 2g
    healthcheck:
      test: ["CMD", "mysql", "-h", "localhost", "-P", "9030", "-uroot", "-e", "SELECT 1"]
      interval: 10s
      timeout: 5s
      retries: 30
    networks:
      - sql-agent-net

  doris-be:
    image: apache/doris:2.0.3-be
    container_name: doris-be
    ports:
      - "9050:9050"
    mem_limit: 4g
    depends_on:
      doris-fe:
        condition: service_healthy
    networks:
      - sql-agent-net
```

- [ ] **Step 2: Create doris-init/init.sql**

```sql
-- Doris 初始化 SQL（启动时自动执行）
-- 用于创建初始数据库和配置

-- 创建数据库
CREATE DATABASE IF NOT EXISTS lakehouse;

-- 切换到 lakehouse 库
USE lakehouse;

-- 查看 BE 状态
SHOW PROC '/backends';
```

- [ ] **Step 3: Commit**

```bash
git add phase2_doris/docker-compose-phase2.yml phase2_doris/doris-init/init.sql
git commit -m "feat: add Phase 2 Docker environment with Doris"
```

---

### Task 13: Phase 2 SQL — Register Paimon in Doris

**Files:**
- Create: `phase2_doris/sql/07_register_paimon.sql`

- [ ] **Step 1: Create 07_register_paimon.sql**

```sql
-- ================================================================
-- Doris 注册 Paimon 外部表
-- ================================================================
-- Doris 2.0+ 支持通过 Paimon Catalog 直接查询 Paimon 表数据，
-- 无需数据复制。这是"湖仓一体"的核心能力。
--
-- 两种注册方式:
--   方式 A: CREATE CATALOG — 注册整个 Paimon Catalog（推荐）
--   方式 B: CREATE TABLE ... ENGINE=PAIMON — 注册单张表
-- ================================================================

-- ================================================================
-- 【参数详解】
-- ---------------------------------------------------------------
-- 参数名                | 说明
-- ================================================================
-- type                  | 固定为 'paimon'
-- paimon.catalog.type   | Paimon Catalog 类型: 'filesystem' / 'hive'
-- paimon.catalog.warehouse | Paimon warehouse 路径
-- ================================================================

-- 方式 A（推荐）: 注册整个 Paimon Catalog
-- 注册后可以查询 Paimon 中的所有库和表
CREATE CATALOG paimon_catalog PROPERTIES (
    'type' = 'paimon',
    'paimon.catalog.type' = 'filesystem',
    'paimon.catalog.warehouse' = 'file:///opt/paimon/data/warehouse'
);

-- 使用 Catalog
SWITCH TO paimon_catalog;

-- 查看 Paimon 中的库和表
SHOW DATABASES;
USE demo;
SHOW TABLES;

-- 查询 Paimon 订单数据
SELECT category,
       COUNT(*) AS order_count,
       SUM(total_amount) AS total_revenue
FROM orders
GROUP BY category
ORDER BY total_revenue DESC;

-- ================================================================
-- 方式 B: 注册单张 Paimon 外部表（不推荐，每张表都要单独建）
-- ================================================================
-- USE lakehouse;
-- CREATE TABLE paimon_orders (
--     order_id      BIGINT,
--     user_id       BIGINT,
--     category      STRING,
--     total_amount  DECIMAL(10,2),
--     order_status  STRING,
--     order_ts      DATETIME
-- ) ENGINE=PAIMON
-- PROPERTIES (
--     'path' = 'file:///opt/paimon/data/warehouse/demo.db/orders'
-- );
```

- [ ] **Step 2: Commit**

```bash
git add phase2_doris/sql/07_register_paimon.sql
git commit -m "feat: add Doris Paimon Catalog registration tutorial"
```

---

### Task 14: Phase 2 SQL — Query via Doris

**Files:**
- Create: `phase2_doris/sql/08_query_via_doris.sql`

- [ ] **Step 1: Create 08_query_via_doris.sql**

```sql
-- ================================================================
-- 通过 Doris 查询 Paimon 数据
-- ================================================================
-- Doris 提供 MySQL 兼容协议，可以通过 mysql CLI、
-- JDBC/ODBC 驱动、或任何 MySQL Client 连接查询。
--
-- 连接方式: mysql -h 127.0.0.1 -P 9030 -uroot
-- ================================================================

USE paimon_catalog.demo;

-- ================================================================
-- 1. Paimon 数据查询（Doris 通过 Catalog 读取）
-- ================================================================

-- 订单统计
SELECT order_status, COUNT(*) AS cnt, SUM(total_amount) AS revenue
FROM orders
GROUP BY order_status
ORDER BY revenue DESC;

-- 按小时统计订单数
SELECT DATE_FORMAT(order_ts, '%Y-%m-%d %H:00:00') AS hour,
       COUNT(*) AS order_count,
       SUM(total_amount) AS revenue
FROM orders
GROUP BY hour
ORDER BY hour;

-- Top 10 热销商品
SELECT product_name, category,
       SUM(quantity) AS total_sold,
       SUM(total_amount) AS revenue
FROM orders
GROUP BY product_name, category
ORDER BY total_sold DESC
LIMIT 10;

-- ================================================================
-- 2. 性能对比: Doris 直接查询 vs Paimon Catalog 查询
-- ================================================================
-- 通过 Flink Doris Connector 双写的数据在 Doris 中独立存储，
-- 查询性能优于通过 Paimon Catalog 读取。
--
-- 这步需要先执行 09_flink_to_doris.sql 完成双写后再验证。

-- 切换到 lakehouse 库（Doris 中创建的表）
-- USE lakehouse;
-- SELECT COUNT(*) FROM doris_orders;

-- ================================================================
-- 3. MySQL 协议验证
-- ================================================================
-- 在宿主机终端运行:
--   mysql -h 127.0.0.1 -P 9030 -uroot
--   进入后: SWITCH TO paimon_catalog.demo;
--           SELECT COUNT(*) FROM orders;
--
-- 预期输出: 与 Phase 1 中 Paimon 表的数据一致
```

- [ ] **Step 2: Commit**

```bash
git add phase2_doris/sql/08_query_via_doris.sql
git commit -m "feat: add Doris query tutorial SQL"
```

---

### Task 15: Phase 2 SQL — Flink to Doris connector

**Files:**
- Create: `phase2_doris/sql/09_flink_to_doris.sql`

- [ ] **Step 1: Create 09_flink_to_doris.sql**

```sql
-- ================================================================
-- Flink Doris Connector: Flink 同时写入 Paimon + Doris
-- ================================================================
-- 这种方式在 Flink 作业中同时配置 Paimon Sink 和 Doris Sink，
-- 数据被双写到两个系统:
--   - Paimon: 湖存储，作为数据底座
--   - Doris: 查询加速，提供高性能分析查询
--
-- 适用场景: 对查询性能要求高、需要毫秒级响应的分析场景
-- ================================================================

USE CATALOG paimon_catalog;
USE demo;

-- ================================================================
-- 【参数详解】
-- ---------------------------------------------------------------
-- 参数名                          | 说明
-- ================================================================
-- connector                       | 固定为 'doris'
-- fenodes                         | Doris FE 地址 (MySQL 协议端口 8030)
-- table.identifier                | 目标表名: db.table
-- username / password             | Doris 用户密码
-- sink.label-prefix               | Stream Load 标签前缀（保证幂等性）
-- sink.properties.format          | 数据格式: json / csv
-- ================================================================

-- 1. 在 Doris 中创建目标表（需要先通过 MySQL 协议执行）
-- 在宿主机终端:
--   mysql -h 127.0.0.1 -P 9030 -uroot
--   然后执行:
--     USE lakehouse;
--     CREATE TABLE doris_orders (
--         order_id      BIGINT,
--         user_id       BIGINT,
--         category      STRING,
--         total_amount  DECIMAL(10,2),
--         order_status  STRING,
--         order_ts      DATETIME
--     ) DISTRIBUTED BY HASH(order_id) BUCKETS 4
--     PROPERTIES ("replication_num" = "1");

-- 2. 在 Flink SQL Client 中创建 Doris 映射表
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

-- 3. 从 Paimon 读取数据写入 Doris
-- 注意: 这是一个批式同步作业，执行完毕后会自动停止
SET 'execution.runtime-mode' = 'batch';
INSERT INTO doris_sink
SELECT order_id, user_id, category, total_amount, order_status, order_ts
FROM orders;

-- 4. 验证 Doris 中的数据
-- 在 MySQL Client 中执行:
--   USE lakehouse;
--   SELECT COUNT(*) FROM doris_orders;
--
-- 预期: 行数与 Paimon 的 orders 表一致

-- ================================================================
-- 【学习要点】
-- ================================================================
-- 1. Doris Connector 底层使用 Stream Load 协议
-- 2. sink.label-prefix 用于保证 exactly-once 语义（重启时去重）
-- 3. Paimon Catalog 方式零维护但查询性能受限于文件读取
-- 4. Doris Connector 方式维护双份数据但查询性能最优
-- 5. 生产环境中可以两条链路都保留: Paimon 作数据底座，
--    Doris 作查询加速，根据场景选择合适的读取方式
-- ================================================================
```

- [ ] **Step 2: Commit**

```bash
git add phase2_doris/sql/09_flink_to_doris.sql
git commit -m "feat: add Flink Doris Connector tutorial SQL"
```

---

### Task 16: Phase 2 dual writer Flink job

**Files:**
- Create: `phase2_doris/flink-job/dual_writer.py`

- [ ] **Step 1: Create dual_writer.py**

```python
# ================================================================
# Flink DataStream: 同时写入 Paimon + Doris（双写）
# ================================================================
# 这个作业演示如何在一个 Flink 作业中同时写入两个系统:
#   - Paimon: 湖存储（作为数据底座，适合离线分析）
#   - Doris:  查询加速（适合实时分析，MySQL 协议接口）
#
# 运行方式:
#   docker exec flink-jm flink run -py /opt/flink/job/dual_writer.py
#
# 前提:
#   1. Phase 1 的 Paimon 表已创建
#   2. Phase 2 的 Doris FE/BE 已启动
#   3. Doris 中已创建目标表 lakehouse.doris_orders
# ================================================================

import json

from pyflink.common import Configuration
from pyflink.datastream import (
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    SimpleStringSchema,
)
from pyflink.table import (
    StreamTableEnvironment,
    Schema,
    DataTypes,
)


def main():
    # 执行环境
    config = Configuration()
    config.set_string("execution.checkpointing.interval", "30s")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(2)
    table_env = StreamTableEnvironment.create(env)

    # ---- 1. 注册 Paimon Catalog ----
    table_env.execute_sql(
        """
        CREATE CATALOG paimon_catalog WITH (
            'type' = 'filesystem',
            'warehouse' = 'file:///opt/paimon/data/warehouse'
        )
        """
    )
    table_env.use_catalog("paimon_catalog")
    table_env.use_database("demo")

    # ---- 2. 创建 Kafka Source 表 ----
    table_env.execute_sql(
        """
        CREATE TEMPORARY TABLE kafka_orders (
            order_id      BIGINT,
            user_id       BIGINT,
            product_id    BIGINT,
            product_name  STRING,
            category      STRING,
            quantity      INT,
            unit_price    DECIMAL(10,2),
            total_amount  DECIMAL(10,2),
            order_status  STRING,
            order_ts      TIMESTAMP(3),
            proc_time     AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'orders',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'dual-writer-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false'
        )
        """
    )

    # ---- 3. 写入 Paimon（流式 INSERT INTO）----
    # Paimon 写入使用 Flink checkpoint 提交 snapshot
    table_env.execute_sql(
        """
        INSERT INTO orders
        SELECT * FROM kafka_orders
        """
    ).wait()

    # ---- 4. 创建 Doris 映射表 ----
    # Doris Connector 使用 Stream Load 协议
    table_env.execute_sql(
        """
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
            'sink.label-prefix' = 'dual_writer',
            'sink.properties.format' = 'json',
            'sink.batch.size' = '1000',
            'sink.batch.interval' = '10s',
            'sink.max-retries' = '3'
        )
        """
    )

    # ---- 5. 写入 Doris ----
    # 从 Kafka 选择子集字段写入 Doris
    table_env.execute_sql(
        """
        INSERT INTO doris_sink
        SELECT order_id, user_id, category,
               total_amount, order_status, order_ts
        FROM kafka_orders
        """
    ).wait()


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add phase2_doris/flink-job/dual_writer.py
git commit -m "feat: add Flink dual writer (Paimon + Doris) job"
```

---

### Task 17: Phase 2 README + start script

**Files:**
- Create: `phase2_doris/start-phase2.sh`
- Create: `phase2_doris/README.md`

- [ ] **Step 1: Create start-phase2.sh**

```bash
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
```

- [ ] **Step 2: Create phase2_doris/README.md**

```markdown
# Phase 2: 引入 Doris 查询加速

## 学习目标

理解 Paimon + Doris 湖仓一体的两种集成方式：
- **Doris Paimon Catalog**: 零 ETL，Doris 直接查询 Paimon 数据
- **Flink Doris Connector**: Flink 双写，Doris 独立存储查询

## 新增组件

| 组件 | 版本 | 端口 | 说明 |
|---|---|---|---|
| Doris FE | 2.0.3 | 9030 (MySQL), 8030 (WebUI) | 查询前端 |
| Doris BE | 2.0.3 | 9050 | 数据节点 |

## 快速开始

### 1. 启动环境

```bash
./start-phase2.sh
```

### 2. 验证 Doris

```bash
mysql -h 127.0.0.1 -P 9030 -uroot -e "SHOW PROC '/backends';"
```

### 3. 注册 Paimon Catalog

在 Flink SQL Client 或 Doris MySQL Client 中执行:
```sql
CREATE CATALOG paimon_catalog PROPERTIES (
    'type' = 'paimon',
    'paimon.catalog.type' = 'filesystem',
    'paimon.catalog.warehouse' = 'file:///opt/paimon/data/warehouse'
);
```

### 4. 查询验证

```bash
mysql -h 127.0.0.1 -P 9030 -uroot -e "SWITCH TO paimon_catalog.demo; SELECT COUNT(*) FROM orders;"
```

## 学习路线

1. 先做 `07_register_paimon.sql` — 体验零 ETL 查询
2. 再做 `08_query_via_doris.sql` — 学习 Doris 查询语法
3. 最后 `09_flink_to_doris.sql` — 学习双写模式
4. 尝试运行 `dual_writer.py` 体验自动化双写

## 停止环境

```bash
docker compose -f docker-compose-phase2.yml down
```
```

- [ ] **Step 3: Commit**

```bash
git add phase2_doris/start-phase2.sh phase2_doris/README.md
git commit -m "docs: add Phase 2 README and startup script"
```

---

### Task 18: Root README.md

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update root README.md**

```markdown
# SQL-Agent

Paimon + Doris 湖仓一体架构，本地 LLM + RAG 检索生成 SQL。

## 项目结构

```
├── phase1_paimon/       # Phase 1: Kafka + Flink → Paimon 学习教程
│   ├── sql/             # Flink SQL 教程（含参数详解）
│   ├── flink-job/       # PyFlink DataStream API 作业
│   └── ...
├── phase2_doris/        # Phase 2: 引入 Doris 查询加速
│   ├── sql/             # Doris 集成教程
│   ├── flink-job/       # 双写作业
│   └── ...
└── docs/                # 设计文档和计划
```

## 学习路线

1. **Phase 1**: 学习 Paimon 流批一体存储
   - Docker 部署 Kafka + Flink + Paimon
   - Flink SQL 实时/离线写入 Paimon
   - Paimon 核心参数详解

2. **Phase 2**: 引入 Doris 查询加速
   - Doris Paimon Catalog 零 ETL 查询
   - Flink Doris Connector 双写模式

3. **未来**: AI SQL-Agent
   - 基于 Doris MySQL 接口接入本地 LLM
   - RAG 检索表结构 → 自动生成 SQL

详情请参见各阶段目录中的 README。
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: update root README with project structure and learning roadmap"
```

---

## Self-Review

### Spec Coverage

| Spec Requirement | Task Coverage |
|---|---|
| .gitignore + data dirs | Task 1 ✓ |
| Phase 1 Docker Compose (ZK+Kafka+Flink) | Task 2 ✓ |
| Flink conf (checkpoint config) | Task 2 ✓ |
| Phase 1 SQL: create_catalog | Task 3 ✓ |
| Phase 1 SQL: create_tables (with bucket/merge-engine/changelog params) | Task 4 ✓ |
| Phase 1 SQL: streaming_write (with checkpoint/run_mode params) | Task 5 ✓ |
| Phase 1 SQL: batch_write (INSERT OVERWRITE) | Task 6 ✓ |
| Phase 1 SQL: query (scan.mode comparison) | Task 7 ✓ |
| Paimon parameters reference | Task 8 ✓ |
| DataStream realtime_writer.py (Kafka→Paimon) | Task 9 ✓ |
| kafka_producer.py | Task 10 ✓ |
| Phase 1 README + start script | Task 11 ✓ |
| Phase 2 Docker Compose (+Doris FE/BE) | Task 12 ✓ |
| Phase 2 SQL: register_paimon | Task 13 ✓ |
| Phase 2 SQL: query_via_doris | Task 14 ✓ |
| Phase 2 SQL: flink_to_doris (Doris Connector) | Task 15 ✓ |
| Phase 2 dual_writer.py | Task 16 ✓ |
| Phase 2 README + start script | Task 17 ✓ |
| Root README update | Task 18 ✓ |

### Placeholder Scan
No TBD, TODO, or placeholder patterns found. All SQL scripts contain complete parameter explanations and runnable code.

### Type Consistency
- All SQL use `paimon_catalog` as Catalog name consistently
- All table schemas match the spec's data model
- Docker compose mount paths are consistent across tasks
- File paths in commit commands match the files created in each task
