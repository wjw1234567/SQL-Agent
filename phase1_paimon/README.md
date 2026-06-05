# Phase 1: Kafka + Flink + Paimon 手动部署教程

## 概述

本教程将手动搭建 Kafka → Flink → Paimon 数据管道，包含实时写入和离线写入两种方式。

**学习目标：**
- 理解 Kafka、Flink、Paimon 三个组件的关系
- 掌握 Paimon Filesystem Catalog 的配置
- 理解 Paimon 主键、分区、Bucket、Merge Engine 等核心概念
- 区分 Flink SQL 流式写入和批式写入

---

## 第一节：环境准备

### 1.1 检查环境

```bash
# 确认 Docker 已安装
docker --version

# 确认 Docker Compose 已安装
docker compose version

# 确认 Python 已安装
python --version
```

### 1.2 了解 docker-compose-phase1.yml

这个文件定义了 4 个服务，它们的关系如下：

```
Zookeeper (协调服务)
    │
    ▼
Kafka (消息队列)      ← Flink 从这里消费数据
    │
    ▼
Flink JobManager (作业调度) ──→ Flink TaskManager (作业执行)
                                   │
                              Paimon (嵌入 Flink 的表存储)
```

**关键点：** Paimon 没有独立容器——它作为 Flink 的连接器 JAR 嵌入在 Flink 进程中运行。

打开 `docker-compose-phase1.yml`，我们来理解每个服务：

**Zookeeper (端口 2181)：**
- Kafka 依赖 Zookeeper 管理集群元数据（broker 列表、topic 配置、选举 leader）
- `bitnami/zookeeper:3.9` 是 Bitnami 维护的轻量镜像

**Kafka (端口 9092/29092)：**
- `bitnami/kafka:3.6` —— Kafka 消息队列
- 9092 = 容器内通信端口，29092 = 宿主机外部访问端口
- `ADVERTISED_LISTENERS` 告诉客户端应该连接哪个地址：容器内用 `kafka:9092`，宿主机用 `localhost:29092`
- `../data/kafka:/bitnami/kafka/data` —— Kafka 数据持久化到宿主机

**Flink JobManager (端口 8081/6123)：**
- 8081 = Web UI 管理界面，6123 = RPC 通信端口
- `jobmanager.memory.process.size: 2048m` —— 分配 2GB 内存
- 挂载了卷：`./sql`、`./flink-job` 让 Flink 容器能访问我们的 SQL 和 Python 文件

**Flink TaskManager：**
- `taskmanager.memory.process.size: 4096m` —— 分配 4GB 内存（核心计算资源）
- `taskmanager.numberOfTaskSlots: 4` —— 每个 TaskManager 可以同时运行 4 个任务

---

## 第二节：启动基础环境

### 2.1 启动所有容器

**不要用 start-phase1.sh，我们一步步来：**

```bash
cd phase1_paimon

# 启动所有服务
docker compose -f docker-compose-phase1.yml up -d
```

参数说明：
- `-f` 指定 compose 文件
- `-d` 后台运行（detached mode），不加的话会在前台打印日志

### 2.2 验证容器是否正常启动

```bash
# 查看所有运行中的容器
docker ps

# 你应该看到 4 个容器:
# - zk (Zookeeper)
# - kafka
# - flink-jm (Flink JobManager)
# - flink-tm (Flink TaskManager)

# 查看各容器日志（确认没有报错）
docker logs zk --tail 20
docker logs kafka --tail 20
docker logs flink-jm --tail 20
```

### 2.3 验证 Flink Web UI

打开浏览器访问：http://localhost:8081

你应该能看到 Flink Dashboard，在 Task Managers 页面可以看到 1 个 TaskManager 在线。

### 2.4 验证 Kafka 可用

```bash
# 检查 Kafka 是否就绪（轮询直到成功）
docker exec kafka kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

如果返回空列表（没有 topic），说明 Kafka 正常运行。

**命令解析：**
- `docker exec kafka` —— 在 kafka 容器内执行命令
- `kafka-topics.sh` —— Kafka 自带的 topic 管理工具
- `--bootstrap-server localhost:9092` —— 连接 Kafka（容器内用 localhost:9092）
- `--list` —— 列出所有 topic

### 2.5 创建订单 topic

```bash
docker exec kafka kafka-topics.sh \
  --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1 \
  --if-not-exists
```

**参数含义：**
- `--topic orders` —— topic 名称，我们的订单数据会发到这个 topic
- `--partitions 3` —— 3 个分区，Flink 可以并行消费
- `--replication-factor 1` —— 副本数 1（单节点 Kafka 只能设为 1）

---

## 第三节：安装 Paimon JAR 到 Flink

### 3.1 为什么需要这一步？

Paimon 不是独立服务，它是一个 **Flink 连接器**（connector）。Flink 需要加载 Paimon 的 JAR 包才能理解 `'connector' = 'paimon'` 这样的建表语句。

### 3.2 下载并安装 Paimon JAR

```bash
# 1. 从 Maven 中央仓库下载 Paimon Flink 连接器 JAR
#    flink-1.18 对应 paimon-flink-1.18 版本
wget https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/0.7.0/paimon-flink-1.18-0.7.0.jar

# 如果 wget 没有，用 curl 也可以:
# curl -O https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/0.7.0/paimon-flink-1.18-0.7.0.jar

# 2. 复制到 Flink JobManager 容器的 lib 目录
#    Flink 启动时会自动加载 /opt/flink/lib/ 下的所有 JAR
docker cp paimon-flink-1.18-0.7.0.jar flink-jm:/opt/flink/lib/

# 3. 同样复制到 TaskManager
docker cp paimon-flink-1.18-0.7.0.jar flink-tm:/opt/flink/lib/

# 4. 重启 Flink 容器使 JAR 生效
docker restart flink-jm flink-tm

# 5. 等待重启完成
sleep 10
docker ps | grep flink
```

### 3.3 验证 JAR 已加载

```bash
docker exec flink-jm ls /opt/flink/lib/ | grep paimon
```

应该输出：`paimon-flink-1.18-0.7.0.jar`

---

## 第四节：进入 Flink SQL Client

```bash
docker exec -it flink-jm sql-client.sh
```

**命令解析：**
- `docker exec -it` —— 交互模式（interactive + tty），让我们能输入命令
- `flink-jm` —— 在 JobManager 容器中执行
- `sql-client.sh` —— Flink 自带的 SQL 命令行工具

你应当看到 Flink SQL 提示符：

```
Flink SQL>
```

现在你已经进入了 Flink SQL Client。后续的 SQL 操作都在这个终端中执行。

---

## 第五节：注册 Paimon Catalog

### 5.1 执行创建 Catalog 的 SQL

在 Flink SQL Client 中粘贴以下 SQL：

```sql
CREATE CATALOG paimon_catalog WITH (
    'type' = 'filesystem',
    'warehouse' = 'file:///opt/paimon/data/warehouse'
);
```

**参数详解：**
- `type = 'filesystem'` —— 元数据存储在文件系统（适合学习）
  - 其他选项：`hive`（对接 Hive Metastore）、`jdbc`（对接数据库）
- `warehouse = 'file:///opt/paimon/data/warehouse'` —— 数据存储根路径
  - `file://` 表示本地文件系统
  - 对应的就是 docker-compose 中挂载的 `../data/paimon:/opt/paimon/data`

### 5.2 使用 Catalog 并创建数据库

```sql
-- 切换到 paimon_catalog
USE CATALOG paimon_catalog;

-- 创建演示数据库
CREATE DATABASE IF NOT EXISTS demo;
USE demo;

-- 验证
SHOW CURRENT CATALOG;
SHOW DATABASES;
```

**Catalog 是什么？**
Catalog 是 Paimon 的元数据管理入口，类比传统数据库的"实例名"。Filesystem Catalog 是最简单的形式——在 `warehouse` 目录下创建文件夹来存储表定义和元数据。

---

## 第六节：创建 Paimon 表

### 6.1 创建订单表（主键表）

```sql
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
```

**核心参数解析：**

| 参数 | 值 | 含义 |
|---|---|---|
| `PRIMARY KEY (order_id)` | 主键 | Paimon 按主键去重——相同 order_id 的数据，后到的覆盖先到的 |
| `'bucket' = '4'` | 4 个分桶 | 相当于 4 个并行写入文件，提高吞吐。小表设 4 即可 |
| `'changelog-producer' = 'input'` | 按输入生成 | 让 Paimon 记录数据变更日志，支持流式读取 |

**NOT ENFORCED 是什么意思？**
Flink SQL 中的主键约束默认是 NOT ENFORCED——Flink 不会检查数据是否真的唯一，由你保证。Paimon 按这个主键做 upsert（去重）。

### 6.2 验证表已创建

```sql
DESCRIBE orders;
SHOW TABLES;
```

### 6.3 创建分区表（了解即可）

```sql
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
    partition_dt  STRING,
    PRIMARY KEY (order_id, partition_dt) NOT ENFORCED
) PARTITIONED BY (partition_dt)
WITH (
    'bucket' = '2',
    'changelog-producer' = 'input'
);
```

**分区的作用：** 按日期分区后，查询特定日期的数据只需要扫描对应分区目录，速度更快。`PARTITIONED BY (partition_dt)` 告诉 Paimon 按这个字段做物理分区。

### 6.4 创建聚合表（演示 merge-engine）

```sql
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
```

**merge-engine = 'aggregation' 的作用：**
当多条数据的主键相同时，Paimon 不会"后到覆盖先到"，而是**做聚合**。比如 category='Electronics' 的数据来了两次：
- 第一次：total_orders = 5
- 第二次：total_orders = 3
- 最终结果：total_orders = 8（SUM 聚合）

---

## 第七节：实时写入数据（Flink SQL Streaming）

### 7.1 使用 DataGen 生成模拟数据

Flink 内置了 DataGen 连接器，无需外部系统即可生成测试数据：

```sql
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'parallelism.default' = '2';

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
```

**DataGen 参数的含义：**
- `'connector' = 'datagen'` —— 使用 Flink 内置的数据生成器
- `'rows-per-second' = '5'` —— 每秒生成 5 条数据
- `'fields.order_id.kind' = 'sequence'` —— order_id 按顺序递增
- `'fields.quantity.min' = '1'`、`'fields.quantity.max' = '10'` —— quantity 取值在 1~10 之间随机
- `'fields.order_ts.max-past' = '3600000'` —— 时间戳在最近 1 小时内随机

### 7.2 启动流式写入

```sql
-- 将模拟数据实时写入 Paimon orders 表
INSERT INTO orders
SELECT * FROM datagen_orders;
```

**注意：** 这个 INSERT 语句会**持续运行**不会结束，因为它是流式作业。DataGen 每秒生成 5 条数据，Paimon 每 30 秒（checkpoint 间隔）提交一次数据。

### 7.3 打开一个新窗口验证数据

**不要关闭当前 Flink SQL Client！** 打开一个新终端：

```bash
# 进入另一个 Flink SQL Client 会话
docker exec -it flink-jm sql-client.sh
```

在新 Client 中：

```sql
USE CATALOG paimon_catalog;
USE demo;

-- 切换到批式模式查询
SET 'execution.runtime-mode' = 'batch';

-- 查看 orders 表中有多少数据
SELECT COUNT(*) AS total_orders FROM orders;

-- 查看各分类的订单统计
SELECT category,
       COUNT(*) AS order_count,
       ROUND(SUM(total_amount), 2) AS revenue
FROM orders
GROUP BY category
ORDER BY revenue DESC;
```

你会发现数据在不断增长。这是因为流式写入作业持续写入，而批式查询读取最新的 snapshot。

### 7.4 查看 Flink WebUI

打开 http://localhost:8081，你会看到正在运行的 DataGen→Paimon 作业。可以在这里：
- 查看作业的 DAG（算子图）
- 查看作业的吞吐量指标
- 查看 Checkpoint 状态

---

## 第八节：离线批量写入（Batch Mode）

### 8.1 切换到批式模式

在 Flink SQL Client 中：

```sql
-- 切换到批式模式
SET 'execution.runtime-mode' = 'batch';
SET 'table.exec.resource.default-parallelism' = '2';
```

批式模式下，INSERT 语句执行完数据后自动结束，不会持续运行。

### 8.2 批量追加写入

```sql
-- 创建 DataGen Source（批式模式只生成固定行数）
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
    'number-of-rows' = '50',
    'fields.order_id.kind' = 'sequence',
    'fields.order_id.start' = '1001',
    'fields.order_id.end' = '2000',
    'fields.user_id.kind' = 'sequence',
    'fields.user_id.start' = '2001',
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

-- 批量追加写入（执行完毕后自动停止）
INSERT INTO orders SELECT * FROM batch_source;
```

注意 `'number-of-rows' = '50'` —— 批式模式下 DataGen 生成指定行数后结束。

### 8.3 覆盖写入（INSERT OVERWRITE）

```sql
-- 清空 orders 表并重新写入（只保留非取消订单）
INSERT OVERWRITE orders
SELECT * FROM batch_source
WHERE order_status <> 'cancelled';

-- 验证覆盖后的数据量
SELECT COUNT(*) FROM orders;
```

**INSERT INTO vs INSERT OVERWRITE 的区别：**
- `INSERT INTO`：追加数据，原有数据保留
- `INSERT OVERWRITE`：先清空表（或分区），再写入新数据

---

## 第九节：查询 Paimon 数据

### 9.1 批式查询（读取最新快照）

```sql
SET 'execution.runtime-mode' = 'batch';

-- 按状态统计订单
SELECT order_status,
       COUNT(*) AS cnt,
       ROUND(SUM(total_amount), 2) AS revenue
FROM orders
GROUP BY order_status;

-- Top 5 商品分类
SELECT category,
       SUM(quantity) AS items_sold,
       ROUND(SUM(total_amount), 2) AS revenue
FROM orders
GROUP BY category
ORDER BY revenue DESC
LIMIT 5;
```

### 9.2 时间旅行查询

Paimon 的每次 checkpoint 都会产生一个 snapshot。你可以读取任意历史 snapshot：

```sql
-- 从指定时间戳之后的数据
-- 1704067200000 对应 2024-01-01 08:00:00 UTC
SELECT order_id, category, total_amount, order_ts
FROM orders /*+ OPTIONS('scan.mode'='from-timestamp',
                        'scan.timestamp-millis'='1704067200000') */
LIMIT 20;
```

### 9.3 流式查询（持续监控新数据）

```sql
SET 'execution.runtime-mode' = 'streaming';

-- 持续读取最新到达的数据
SELECT order_id, category, total_amount, order_status
FROM orders /*+ OPTIONS('scan.mode'='latest') */;
```

这个查询会持续输出新写入的数据，直到你按 Ctrl+C 停止。

**不同 scan.mode 选项：**

| 选项 | 含义 | 适用场景 |
|---|---|---|
| `latest` | 只读取最新的 snapshot | 流式读，从当前开始 |
| `from-timestamp` | 从指定时间戳开始 | 增量 ETL |
| `from-snapshot` | 从指定 snapshot ID 开始 | 精准恢复 |
| `full` | 读取全部历史 | 全量+增量 |

---

## 第十节：清理环境

### 10.1 退出 Flink SQL Client

在每个 Client 中输入：
```sql
quit;
```

### 10.2 停止并删除容器

```bash
cd phase1_paimon

# 停止容器（保留数据）
docker compose -f docker-compose-phase1.yml down

# 停止容器并删除数据卷
# docker compose -f docker-compose-phase1.yml down -v
```

### 10.3 验证清理

```bash
docker ps | grep -E "(zk|kafka|flink)"
```

不应有任何相关容器在运行。

---

## 总结：Phase 1 全链路

```
┌────────────────────────────────────────────────────────────┐
│ 你手动完成了以下链路：                                      │
│                                                            │
│ 步骤1: docker compose up -d        → 启动 ZK+Kafka+Flink  │
│ 步骤2: docker cp paimon JAR        → 安装 Paimon 到 Flink │
│ 步骤3: Flink SQL Client            → 注册 Paimon Catalog  │
│ 步骤4: CREATE TABLE orders         → 创建 Paimon 表       │
│ 步骤5: INSERT INTO (streaming)     → 流式写入，持续运行    │
│ 步骤6: SELECT (另一个窗口)          → 查询验证，确认写入    │
│ 步骤7: INSERT OVERWRITE (batch)    → 批量覆盖写入          │
│ 步骤8: 各种查询、时间旅行           → 理解 snapshot 机制    │
└────────────────────────────────────────────────────────────┘
```

准备好后，进入 Phase 2 学习 Doris 集成。
