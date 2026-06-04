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
