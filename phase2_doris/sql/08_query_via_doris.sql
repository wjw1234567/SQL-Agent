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
