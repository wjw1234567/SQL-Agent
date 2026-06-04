# ================================================================
# Flink DataStream API — 从 Kafka 实时写入 Paimon
# ================================================================
# 这个作业演示如何使用 PyFlink DataStream API 实现:
#   Kafka Source → JSON 解析 → Paimon Sink
#
# 运行方式:
#   docker exec flink-jm flink run -py /opt/flink/job/realtime_writer.py
#
# 前提条件:
#   1. Paimon 表已创建 (通过 02_create_tables.sql)
#   2. Kafka topic 'orders' 已创建
#   3. Paimon Flink jar 已放置在 /opt/flink/lib/
# ================================================================

import json
from datetime import datetime, timezone

from pyflink.common import (
    Configuration,
    WatermarkStrategy,
    Time,
    Types,
    SimpleStringSchema,
)
from pyflink.datastream import (
    StreamExecutionEnvironment,
    DataStream,
)
from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
)
from pyflink.table import (
    StreamTableEnvironment,
    TableDescriptor,
    Schema,
    DataTypes,
)


def create_kafka_source(env: StreamExecutionEnvironment) -> DataStream:
    """
    创建 Kafka Source，消费 'orders' topic 的 JSON 消息。

    参数说明:
    - bootstrap.servers: Kafka 地址（容器内使用服务名 kafka:9092）
    - group.id: 消费组名，用于记录消费偏移量
    - topics: 订阅的 topic 列表
    - KafkaOffsetsInitializer.latest(): 从最新位置开始消费
                                      可选: earliest() 从最旧位置
    """
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers("kafka:9092")
        .set_group_id("paimon-flink-consumer")
        .set_topics("orders")
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )
    return env.from_source(
        source=source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="kafka_orders_source",
    )


def parse_and_write_to_paimon(stream: DataStream) -> None:
    """
    将 Kafka JSON 消息解析后写入 Paimon 表。

    这里通过 Table API 将 DataStream 写入 Table，
    利用 Paimon Flink Sink 的 exactly-once 语义。
    """
    # 将 DataStream<String> 转为 Table
    table_env = StreamTableEnvironment.create(stream.execution_environment)

    # 注册 Paimon Catalog（与 SQL 教程中的 Catalog 一致）
    table_env.execute_sql(
        """
        CREATE CATALOG paimon_catalog WITH (
            'type' = 'filesystem',
            'warehouse' = 'file:///opt/paimon/data/warehouse'
        )
        """
    )
    table_env.use_catalog("paimon_catalog")
    table_env.use_database("demo")

    # 将 DataStream 创建为临时视图
    # 先用 Map 操作解析 JSON 字符串
    parsed_stream = stream.map(lambda msg: json.loads(msg))

    # 定义表结构并写入 Paimon
    # 注意: DataStream 写入 Paimon 需要字段类型匹配
    table = table_env.from_data_stream(
        parsed_stream,
        Schema.new_builder()
        .column("order_id", DataTypes.BIGINT())
        .column("user_id", DataTypes.BIGINT())
        .column("product_id", DataTypes.BIGINT())
        .column("product_name", DataTypes.STRING())
        .column("category", DataTypes.STRING())
        .column("quantity", DataTypes.INT())
        .column("unit_price", DataTypes.DECIMAL(10, 2))
        .column("total_amount", DataTypes.DECIMAL(10, 2))
        .column("order_status", DataTypes.STRING())
        .column("order_ts", DataTypes.TIMESTAMP(3))
        .build(),
    )

    # 写入 Paimon orders 表
    table.execute_insert("orders").wait()


if __name__ == "__main__":
    # 创建执行环境
    config = Configuration()
    # 设置 checkpoint（Paimon 写入的必要条件）
    config.set_string("execution.checkpointing.interval", "30s")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(2)

    # 构建作业
    kafka_stream = create_kafka_source(env)
    parse_and_write_to_paimon(kafka_stream)

    # 启动作业
    env.execute("Kafka-to-Paimon Realtime Writer")
