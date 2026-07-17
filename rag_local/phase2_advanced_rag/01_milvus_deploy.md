# Milvus 部署与基础操作教程

## Milvus 是什么？

Milvus 是一个**云原生向量数据库**。

和 ChromaDB 的核心区别：

| 对比维度 | ChromaDB | Milvus |
|---------|----------|--------|
| 架构 | 嵌入式 Python 库，进程内运行 | C/S 架构，独立进程运行 |
| 部署 | pip install 即用 | Docker Compose 部署 |
| 索引类型 | 仅 HNSW | IVF_FLAT, IVF_SQ8, HNSW, DISKANN, GPU_IVF 等 |
| 混合检索 | 不支持 | Dense + Sparse + BM25 混合 |
| 分布式 | 单机 | 支持分片和读写分离 |
| 管理界面 | 无 | Attu WebUI / Milvus Insight |
| 适用场景 | 学习/原型 | 生产级 |

## 架构回顾

详见 docker-compose.yml 的注释。三个服务各司其职：
- etcd: 元数据注册中心
- MinIO: 数据持久化层
- Milvus: 计算引擎

## 启动

```bash
cd rag_local
docker compose -f docker-compose.yml up -d
# 查看日志确认启动
docker compose -f docker-compose.yml logs -f milvus
```

## pymilvus 基础操作

本教程所有操作使用 pymilvus 客户端 SDK：

### 连接

```python
from pymilvus import connections

# 连接到 Milvus 服务端
connections.connect(
    alias="default",
    host="localhost",
    port="19530",
)
print("已连接到 Milvus")
```

### Collection（表）管理

Milvus 中的 Collection 类似于 MySQL 的表或 ChromaDB 的 Collection：

```python
from pymilvus import Collection, CollectionSchema, FieldSchema, DataType

# 定义字段
fields = [
    FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
    FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
    FieldSchema(name="source", dtype=DataType.VARCHAR, max_length=255),
    FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=1024),
]

# 创建 Schema
schema = CollectionSchema(fields, description="订单知识库")

# 创建 Collection（类比 MySQL 的 CREATE TABLE）
collection = Collection(
    name="order_knowledge",
    schema=schema,
    # IVF_FLAT 索引: 速度适中，精度高
    # nlist=1024: 聚类中心数，越大精度越高但速度越慢
    index_params={
        "index_type": "IVF_FLAT",
        "metric_type": "COSINE",
        "params": {"nlist": 1024},
    },
)
```

### 写入数据

```python
import numpy as np

# 模拟 10 条数据
texts = ["订单#1001：客户张三购买智能音箱", "订单#1002：客户李四购买摄像头"]
embeddings = np.random.random((len(texts), 1024)).tolist()  # 实际需调用 BGE-M3

collection.insert([
    [texts],           # text 字段
    ["orders.csv"],    # source 字段
    embeddings,        # embedding 字段
])

# 创建索引后需要加载才能搜索
collection.load()
print(f"总行数: {collection.num_entities}")
```

### 搜索

```python
search_params = {
    "metric_type": "COSINE",
    "params": {"nprobe": 10},  # nprobe: 搜索时的聚类数，越大精度越高
}

results = collection.search(
    data=[query_embedding],   # 查询向量
    anns_field="embedding",   # 向量字段名
    param=search_params,
    limit=5,                  # 返回 Top 5
    output_fields=["text", "source"],
)

for hits in results:
    for hit in hits:
        print(f"ID: {hit.id}, 距离: {hit.score:.4f}, 文本: {hit.entity.get('text')}")
```

## 索引类型详解

详见同级目录 `docs/concepts/milvus_index.md`
