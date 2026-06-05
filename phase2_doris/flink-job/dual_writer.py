# ================================================================
# Flink DataStream: 同时写入 Paimon + Doris（双写）
# ================================================================
# 这个作业演示如何在一个 Flink 作业中同时写入两个系统:
#   - Paimon: 湖存储（作为数据底座，适合离线分析）
#   - Doris:  查询加速（适合实时分析，MySQL 协议接口）
#
# 运行方式:
#   docker exec flink-jm flink run -py /opt/flink/job/dual_writer.py
#
# 前提:
#   1. Phase 1 的 Paimon 表已创建
#   2. Phase 2 的 Doris FE/BE 已启动
#   3. Doris 中已创建目标表 lakehouse.doris_orders
# ================================================================

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StatementSet, StreamTableEnvironment


def main():
    # 执行环境
    config = Configuration()
    config.set_string("execution.checkpointing.interval", "30s")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(2)
    table_env = StreamTableEnvironment.create(env)

    # ---- 1. 注册 Paimon Catalog ----
    table_env.execute_sql(
        """
        CREATE CATALOG paimon_catalog WITH (
            'type' = 'paimon',
            'warehouse' = 'file:///opt/paimon/data/warehouse'
        )
        """
    )
    table_env.use_catalog("paimon_catalog")
    table_env.use_database("demo")

    # ---- 2. 创建 Kafka Source ----
    table_env.execute_sql(
        """
        CREATE TEMPORARY TABLE kafka_orders (
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
            proc_time     AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'orders',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'dual-writer-group',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false'
        )
        """
    )

    # ---- 3. 创建 Doris 映射表 ----
    table_env.execute_sql(
        """
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
            'sink.label-prefix' = 'dual_writer',
            'sink.properties.format' = 'json',
            'sink.batch.size' = '1000',
            'sink.batch.interval' = '10s',
            'sink.max-retries' = '3'
        )
        """
    )

    # ---- 4. 使用 StatementSet 同时提交两个写入作业 ----
    # 关键: StatementSet 将多个 INSERT 合并为一个 Flink 作业，
    # 避免逐个 .wait() 导致后续代码死锁
    stmt_set = table_env.create_statement_set()
    stmt_set.add_insert_sql("INSERT INTO orders SELECT * FROM kafka_orders")
    stmt_set.add_insert_sql(
        "INSERT INTO doris_sink "
        "SELECT order_id, user_id, category, "
        "       total_amount, order_status, order_ts "
        "FROM kafka_orders"
    )
    stmt_set.execute().wait()


if __name__ == "__main__":
    main()
