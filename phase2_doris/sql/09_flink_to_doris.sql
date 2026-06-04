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
