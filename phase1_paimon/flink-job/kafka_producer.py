# ================================================================
# Python Kafka 模拟数据生产者
# ================================================================
# 运行本脚本会向 Kafka 的 'orders' topic 持续发送模拟订单数据。
# 用于验证 Kafka → Flink → Paimon 的整条链路。
#
# 运行方式:
#   pip install kafka-python faker
#   python flink-job/kafka_producer.py
# ================================================================

import json
import random
import time
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer

fake = Faker("zh_CN")

# Kafka 配置
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC = "orders"

# 商品分类
CATEGORIES = [
    "Electronics",
    "Clothing",
    "Books",
    "Home",
    "Sports",
    "Food",
    "Toys",
]

ORDER_STATUSES = ["pending", "paid", "shipped", "cancelled"]

PRODUCT_NAMES = [
    "Wireless Headphones",
    "Cotton T-Shirt",
    "Python Cookbook",
    "Desk Lamp",
    "Yoga Mat",
    "Green Tea",
    "Building Blocks",
    "Running Shoes",
    "Coffee Maker",
    "Backpack",
]


def generate_order(order_id: int) -> dict:
    """生成一条模拟订单数据"""
    category = random.choice(CATEGORIES)
    product_name = random.choice(PRODUCT_NAMES)
    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(10.0, 999.0), 2)
    total_amount = round(quantity * unit_price, 2)

    return {
        "order_id": order_id,
        "user_id": random.randint(1001, 9999),
        "product_id": random.randint(20001, 30000),
        "product_name": product_name,
        "category": category,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total_amount,
        "order_status": random.choice(ORDER_STATUSES),
        "order_ts": datetime.now(timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
    )

    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS}...")
    print(f"Sending orders to topic '{TOPIC}'...")
    print("Press Ctrl+C to stop.\n")

    order_id = 1
    try:
        while True:
            order = generate_order(order_id)
            future = producer.send(TOPIC, value=order)

            print(
                f"Sent order #{order_id}: "
                f"{order['product_name']} x{order['quantity']} = "
                f"${order['total_amount']} [{order['category']}]"
            )

            order_id += 1
            time.sleep(random.uniform(0.5, 2.0))

    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        producer.close()
        print(f"Sent {order_id - 1} orders total.")


if __name__ == "__main__":
    main()
