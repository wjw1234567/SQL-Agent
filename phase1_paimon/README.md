# Phase 1: Kafka + Flink + Paimon

## 学习目标

理解 Paimon 的流批一体存储能力，掌握：
- Paimon Filesystem Catalog 的配置
- 主键表、分区表、聚合表的创建与参数含义
- Flink SQL 流式写入和批式写入
- Paimon 的 snapshot 机制与 Time Travel 查询
- Paimon 核心参数: bucket, merge-engine, changelog-producer, scan.mode

## 环境

| 组件 | 版本 | 端口 |
|---|---|---|
| Zookeeper | 3.9 | 2181 |
| Kafka | 3.6 | 9092 / 29092 |
| Flink | 1.18 (Java 11) | 8081 (WebUI), 6123 (RPC) |
| Paimon | 0.7 | (嵌入 Flink) |

## 快速开始

### 1. 启动环境

```bash
./start-phase1.sh
```

### 2. 进入 Flink SQL Client

```bash
docker exec -it flink-jm sql-client.sh
```

### 3. 按顺序执行 SQL 教程

在 SQL Client 中逐条执行 `sql/` 目录下的脚本。可以将 SQL 内容复制粘贴到 Client：

```sql
-- 直接复制到 SQL Client 执行
CREATE CATALOG paimon_catalog WITH (
    'type' = 'filesystem',
    'warehouse' = 'file:///opt/paimon/data/warehouse'
);
```

### 4. 下载 Paimon JAR（首次运行必需）

Paimon Flink 连接器需要额外的 JAR 包。下载并复制到 Flink 容器:

```bash
# 下载 Paimon Flink 连接器 JAR
wget https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.18/0.7.0/paimon-flink-1.18-0.7.0.jar

# 复制到 Flink JobManager 容器
docker cp paimon-flink-1.18-0.7.0.jar flink-jm:/opt/flink/lib/

# 重启 Flink 以加载 JAR
docker restart flink-jm flink-tm
```

如果下载困难，也可以从阿里云 Maven 镜像下载:
```bash
wget https://maven.aliyun.com/repository/public/org/apache/paimon/paimon-flink-1.18/0.7.0/paimon-flink-1.18-0.7.0.jar
```

### 5. 提交 DataStream 作业

```bash
docker exec flink-jm flink run -py /opt/flink/job/realtime_writer.py
```

### 6. 运行 Kafka Producer（可选，在宿主机新终端）

```bash
pip install kafka-python faker
python flink-job/kafka_producer.py
```

## 文件说明

| 文件 | 说明 |
|---|---|
| `docker-compose-phase1.yml` | Docker Compose 配置 |
| `flink-conf/flink-conf.yaml` | Flink 配置（checkpoint 等） |
| `sql/01_create_catalog.sql` | Paimon Catalog 教程 |
| `sql/02_create_tables.sql` | 表创建 DDL 教程 |
| `sql/03_streaming_write.sql` | 流式写入教程 |
| `sql/04_batch_write.sql` | 批式写入教程 |
| `sql/05_query.sql` | 查询教程 |
| `sql/06_paimon_params.md` | Paimon 参数参考 |
| `flink-job/realtime_writer.py` | DataStream API 实时写入 |
| `flink-job/kafka_producer.py` | Python 模拟数据生产者 |

## 停止环境

```bash
docker compose -f docker-compose-phase1.yml down
```

要同时删除数据:

```bash
docker compose -f docker-compose-phase1.yml down -v
```

## Troubleshooting

**Q: Flink SQL Client 连不上 Kafka？**
确保 Kafka 地址使用容器内地址 `kafka:9092`，不是 `localhost:9092`。

**Q: Paimon 表查询为空？**
检查 checkpoint 是否已完成。Paimon 在 checkpoint 时提交数据。

**Q: Docker 内存不足？**
调整 docker-compose 中的 mem_limit 值，或停止不必要的容器。
