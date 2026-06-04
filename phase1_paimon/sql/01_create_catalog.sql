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
