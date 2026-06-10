# ================================================================
# Flink DataStream API — 从 Kafka 实时写入 Paimon
# ================================================================
# 这个作业演示如何使用 PyFlink 混合 API 实现:
#   Kafka Source → JSON 解析 → Paimon Sink
#
# 运行方式:
#   docker exec flink-jm flink run -py /opt/flink/job/realtime_writer.py
#
# 前提条件:
#   1. Paimon 表已创建 (通过 02_create_tables.sql)
#   2. Kafka topic 'orders' 已创建
#   3. MinIO S3 存储和 Paimon JAR 已配置
# ================================================================

import json

from pyflink.common import (
    Configuration,
    WatermarkStrategy,
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
    Schema,
    DataTypes,
)


def create_kafka_source(env: StreamExecutionEnvironment) -> DataStream:
    """
    创建 Kafka Source，消费 'orders' topic 的 JSON 消息。
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
    将 Kafka JSON 消息解析后写入 Paimon 表 (S3 存储)。
    """
    table_env = StreamTableEnvironment.create(stream.execution_environment)

    # 注册 Paimon Catalog (S3/MinIO 存储)
    # 备用: 去掉 S3 参数使用 'warehouse' = 'file:///opt/paimon/data/warehouse'
    table_env.execute_sql(
        """
        CREATE CATALOG paimon_catalog WITH (
            'type' = 'paimon',
            'warehouse' = 's3://paimon-bucket/warehouse',
            's3.endpoint' = 'http://minio:9000',
            's3.access-key' = 'minioadmin',
            's3.secret-key' = 'minioadmin',
            's3.path.style.access' = 'true'
        )
        """
    )
    table_env.use_catalog("paimon_catalog")
    table_env.use_database("demo")

    parsed_stream = stream.map(lambda msg: json.loads(msg))

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

    table_env.create_temporary_view("source_view", table)
    table_env.execute_sql("INSERT INTO orders SELECT * FROM source_view")


if __name__ == "__main__":
    config = Configuration()
    config.set_string("execution.checkpointing.interval", "30s")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(2)

    kafka_stream = create_kafka_source(env)
    parse_and_write_to_paimon(kafka_stream)

    env.execute("Kafka-to-Paimon Realtime Writer")
