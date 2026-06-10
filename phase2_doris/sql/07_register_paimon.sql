-- ================================================================
-- Doris 注册 Paimon 外部表（S3 存储）
-- ================================================================
-- Doris 2.0+ 支持通过 Paimon Catalog 直接查询 Paimon 表数据。
-- 本环境使用 MinIO (S3) 作为 Paimon 的存储后端。
-- ================================================================

-- ================================================================
-- 【参数详解】
-- ---------------------------------------------------------------
-- 参数名                            | 说明
-- ================================================================
-- type                              | 固定为 'paimon'
-- paimon.catalog.type               | Paimon Catalog 类型: 'filesystem'
-- paimon.catalog.warehouse          | Paimon warehouse S3 路径
-- paimon.catalog.s3.endpoint        | MinIO S3 服务地址
-- paimon.catalog.s3.access-key      | S3 访问密钥
-- paimon.catalog.s3.secret-key      | S3 安全密钥
-- paimon.catalog.s3.path.style.access | 使用路径样式（MinIO 设为 true）
-- ================================================================

-- 注册整个 Paimon Catalog（S3 存储）
CREATE CATALOG paimon_catalog PROPERTIES (
    'type' = 'paimon',
    'paimon.catalog.type' = 'filesystem',
    'paimon.catalog.warehouse' = 's3://paimon-bucket/warehouse',
    'paimon.catalog.s3.endpoint' = 'http://minio:9000',
    'paimon.catalog.s3.access-key' = 'minioadmin',
    'paimon.catalog.s3.secret-key' = 'minioadmin',
    'paimon.catalog.s3.path.style.access' = 'true'
);

-- ================================================================
-- 【备用方案：本地文件系统】
-- ---------------------------------------------------------------
-- CREATE CATALOG paimon_catalog PROPERTIES (
--     'type' = 'paimon',
--     'paimon.catalog.type' = 'filesystem',
--     'paimon.catalog.warehouse' = 'file:///opt/paimon/data/warehouse'
-- );
-- ================================================================

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
