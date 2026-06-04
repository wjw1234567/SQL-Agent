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
