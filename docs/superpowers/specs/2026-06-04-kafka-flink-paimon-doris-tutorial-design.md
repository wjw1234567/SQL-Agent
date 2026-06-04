# Kafka + Flink 实时/离线 ETL 写入 Paimon + Doris 湖仓一体学习教程 — 设计文档

## 概述

本教程面向有 Flink 基础、正在学习 Paimon + Doris 集成的开发者，通过 Docker Desktop 在本机搭建完整的湖仓一体学习环境。教程采用**先 Paimon 独立、后引入 Doris** 的两阶段设计，配合模拟数据，涵盖实时写入和离线写入两条路径。

## 两阶段架构

### Phase 1：Kafka → Flink → Paimon

```
模拟数据 (Flink DataGenerator / Python Producer)
       │
       ▼
    Kafka ──→ Flink (DataStream API / SQL) ──→ Paimon (文件存储)
                                                   │
                                              Flink SQL 查询验证
```

目标：理解 Paimon 的流批一体存储、主键表、分区、Merge Engine 等核心概念。

### Phase 2：引入 Doris 作为查询加速层

```
模拟数据 ──→ Kafka ──→ Flink ──→ Paimon ──→ Doris (Paimon Catalog)
                              │                     │
                              └──→ Doris (Flink Connector) ──→ MySQL 协议查询
```

目标：理解 Doris 与 Paimon 的两种集成方式，为后续 AI SQL-Agent 提供查询接口。

## 环境要求

| 项目 | 要求 |
|---|---|
| 操作系统 | Windows 11 (本机) |
| Docker Desktop | 已安装 |
| Python | 已安装 |
| 可用内存 | ~20GB |
| 项目目录 | `D:\Pycharm_Project\SQL-Agent` |

## 容器资源分配

| 容器 | 分配内存 | 说明 |
|---|---|---|
| Zookeeper | 512MB | Kafka 依赖 |
| Kafka (KRaft) | 2GB | 消息队列 |
| Flink JobManager | 2GB | 作业调度 |
| Flink TaskManager | 4GB | 作业执行 (可扩展) |
| Doris FE | 2GB | 查询前端 |
| Doris BE | 4GB | 数据存储与计算 |

Phase 1 不包含 Doris，仅占用 ~8.5GB；Phase 2 全量 ~14.5GB，均在 20GB 预算内。

## 模拟数据模型

### 订单表 (orders)

```sql
order_id      BIGINT        -- 订单ID
user_id       BIGINT        -- 用户ID
product_id    BIGINT        -- 商品ID
product_name  STRING        -- 商品名称
category      STRING        -- 商品分类
quantity      INT           -- 数量
unit_price    DECIMAL(10,2) -- 单价
total_amount  DECIMAL(10,2) -- 总金额
order_status  STRING        -- 状态: pending/paid/shipped/cancelled
order_ts      TIMESTAMP(3)  -- 订单时间（事件时间）
proc_time     AS PROCTIME() -- 处理时间（Flink 计算列）
```

### 订单聚合表 (order_stats)

```sql
category        STRING PRIMARY KEY    -- 商品分类
total_orders    BIGINT                -- 订单总数
total_revenue   DECIMAL(14,2)         -- 总销售额
window_start    TIMESTAMP(3)          -- 窗口开始
window_end      TIMESTAMP(3)          -- 窗口结束
```

## Phase 1 详细设计

### Docker Compose 服务

- **Zookeeper**: `bitnami/zookeeper:latest`，端口 2181
- **Kafka**: `bitnami/kafka:latest`，端口 9092（内网）、29092（外网）
- **Flink JobManager**: `flink:1.18-java11`，端口 8081（WebUI）、6123（RPC）
- **Flink TaskManager**: `flink:1.18-java11`，连接到 JM

卷挂载：
- `./data/paimon:/opt/paimon/data` — Paimon 表数据
- `./data/kafka:/bitnami/kafka/data` — Kafka 数据持久化
- `./flink-conf/flink-conf.yaml:/opt/flink/conf/flink-conf.yaml` — Flink 配置

网络：`sql-agent-net`，bridge 模式

### Flink SQL 教程文件

每个 `.sql` 文件包含：**参数说明表格 + 可执行 SQL + 执行后预期输出示例**。

| 编号 | 文件 | 教程内容 |
|---|---|---|
| 01 | `create_catalog.sql` | Paimon Filesystem Catalog 详解：`type`、`warehouse`、不同 Catalog 对比 |
| 02 | `create_tables.sql` | 主键策略、`bucket` 含义与选择、分区策略、`merge-engine`、`changelog-producer` |
| 03 | `streaming_write.sql` | Flink SQL `INSERT INTO` 流式语义、checkpoint 配置、消费起始位置、`run_mode` |
| 04 | `batch_write.sql` | `INSERT OVERWRITE` 静态/动态分区、batch 模式下的参数差异 |
| 05 | `query.sql` | 批式读 (`scan.mode=latest`) vs 流式读 (`scan.mode=from-timestamp`) |
| 06 | `paimon_params.md` | Paimon 核心参数专题：bucket、merge-engine、changelog-producer、scan.mode、run_mode 完整对照 |

### Flink DataStream API 作业

`realtime_writer.py` — 使用 PyFlink Table API + DataStream API 混合写法，逐行注释：

1. **Kafka Source**：`KafkaSource` 构建，配置 bootstrap.servers、group.id、起始偏移量
2. **Deserialization**：JSON 反序列化，将 Kafka 消息转为 DataStream `Row`
3. **Transformation**：可选的数据清洗/转换逻辑
4. **Paimon Sink**：通过 Table API 将 DataStream 写入 Paimon 表

`kafka_producer.py` — 可选组件，Python 脚本发送模拟 JSON 数据到 Kafka Topic `orders`，用于不依赖 DataGenerator 的场景。

### 启动流程

```
Step 1: docker compose -f docker-compose-phase1.yml up -d
Step 2: 验证容器 → docker ps
Step 3: 验证 Kafka → 创建 topic orders
Step 4: 验证 Flink → 访问 http://localhost:8081
Step 5: 通过 Flink SQL Client 执行 01-05 号脚本
Step 6: 打包提交 realtime_writer.py 作业
Step 7: 查询验证
```

## Phase 2 详细设计

### 新增 Docker Compose 服务

- **Doris FE**: `apache/doris:2.0-alpine`，端口 9030（MySQL协议）、8030（WebUI）
- **Doris BE**: `apache/doris:2.0-alpine`，端口 9050

### Doris Paimon Catalog 方式（零 ETL）

1. Doris 中通过 `CREATE CATALOG` 或 `CREATE TABLE ... ENGINE=PAIMON` 注册 Paimon 表
2. 直接通过 MySQL 协议查询：`mysql -h 127.0.0.1 -P 9030 -uroot`
3. 示例：`SELECT category, SUM(total_amount) FROM paimon_orders GROUP BY category;`

### Flink Doris Connector 方式（双写）

1. Flink 作业中增加 DorisStreamSink，数据同时写入 Paimon + Doris
2. Doris 侧得到独立的数据副本，查询性能最优
3. 适合对查询延迟要求高的场景

### 教程文件

| 编号 | 文件 | 教程内容 |
|---|---|---|
| 07 | `register_paimon.sql` | Doris Catalog 注册方式对比、Paimon 外部表配置 |
| 08 | `query_via_doris.sql` | Doris 查询优化、MySQL 协议使用、JDBC 连接方式 |
| 09 | `flink_to_doris.sql` | Flink Doris Connector 参数、Stream Load 原理 |

## 后续集成

教程覆盖 ETL 写入和查询层搭建。后续 SQL-Agent 阶段将基于本教程产出的 Doris MySQL 接口，接入本地 LLM + RAG 实现自然语言查询。

## 项目文件结构

```
D:\Pycharm_Project\SQL-Agent\
├── .gitignore
├── README.md
├── data/
│   ├── paimon/            (gitignored)
│   └── kafka/             (gitignored)
├── phase1_paimon/
│   ├── docker-compose-phase1.yml
│   ├── flink-conf/
│   │   └── flink-conf.yaml
│   ├── sql/
│   │   ├── 01_create_catalog.sql
│   │   ├── 02_create_tables.sql
│   │   ├── 03_streaming_write.sql
│   │   ├── 04_batch_write.sql
│   │   ├── 05_query.sql
│   │   └── 06_paimon_params.md
│   ├── flink-job/
│   │   ├── realtime_writer.py
│   │   └── kafka_producer.py
│   ├── scripts/
│   │   └── init-kafka-topics.sh
│   ├── start-phase1.sh
│   └── README.md
├── phase2_doris/
│   ├── docker-compose-phase2.yml
│   ├── doris-init/
│   │   └── init.sql
│   ├── sql/
│   │   ├── 07_register_paimon.sql
│   │   ├── 08_query_via_doris.sql
│   │   └── 09_flink_to_doris.sql
│   ├── flink-job/
│   │   └── dual_writer.py
│   ├── start-phase2.sh
│   └── README.md
```

## 关键技术决策记录

1. **Flink 1.18 + Paimon 0.7**：当前稳定组合，社区活跃，文档齐全
2. **分两阶段**：先 Paimon 后 Doris，降低初期学习负担，内存友好
3. **教程注释嵌入 SQL 文件**：无需跳转查阅，边操作边理解
4. **同时覆盖 DataStream API 和 Flink SQL**：兼顾理解底层原理与生产效率
5. **模拟数据用订单场景**：字段丰富（数值、字符串、时间、枚举），适合做各种维度的聚合分析
