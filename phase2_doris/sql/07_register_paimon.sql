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
--     'type' = 'paimon',
--     'path' = 'file:///opt/paimon/data/warehouse/demo.db/orders'
-- );
