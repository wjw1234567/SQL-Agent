-- Doris 初始化 SQL
-- 用于创建初始数据库和配置

-- 创建数据库
CREATE DATABASE IF NOT EXISTS lakehouse;

-- 切换到 lakehouse 库
USE lakehouse;

-- 查看 BE 状态
SHOW PROC '/backends';
