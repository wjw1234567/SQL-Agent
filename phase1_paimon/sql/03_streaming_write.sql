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
