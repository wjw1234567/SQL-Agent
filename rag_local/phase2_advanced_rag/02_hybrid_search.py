# ================================================================
# 混合检索（Hybrid Search）— Dense + Sparse + RRF 融合
# ================================================================
# 混合检索 = 稠密向量（语义相似度）+ 稀疏向量（关键词匹配）
#   Dense:   "智能手机" 和 "iPhone 15" 语义相近 → 向量距离近
#   Sparse:  "智能" 这个词精确出现在文档中   → BM25 分高
#   Hybrid:  两者结合，取长补短
#
# Milvus 2.5 原生支持 Hybrid Search，无需手动合并。
# ================================================================
#
# 【运行方式】
#   python phase2_advanced_rag/02_hybrid_search.py
#
# 【前置条件】
#   1. docker-compose.yml 中的 Milvus 已启动
#   2. Ollama 已启动并包含 bge-m3 模型
# ================================================================

import numpy as np
from pymilvus import (
    connections, Collection, CollectionSchema,
    FieldSchema, DataType, WeightedRanker,
)
import httpx
import json

# ---- 配置 ----
OLLAMA_URL = "http://localhost:11434"
MILVUS_HOST = "localhost"
MILVUS_PORT = "19530"
EMBED_MODEL = "bge-m3"
COLLECTION_NAME = "hybrid_demo"
DIM = 1024  # BGE-M3 embedding 维度

# ================================================================
# 第 1 部分：Ollama Embedding
# ================================================================

def embed_text(text: str) -> dict:
    """
    调用 Ollama BGE-M3 生成稠密向量 (dense) 和稀疏向量 (sparse)。

    BGE-M3 的特殊之处：
    大多数 embedding 模型只输出 dense 向量。
    BGE-M3 同时输出 dense + sparse，所以很适合做 Hybrid Search。

    稀疏向量 = 词汇权重：
      "智能音箱" → {"智能": 0.8, "音箱": 0.7, "smart": 0.3}
    稠密向量 = 语义编码：
      "智能音箱" → [0.12, -0.34, 0.78, ...] (1024 维)
    """
    url = f"{OLLAMA_URL}/api/embed"
    payload = {"model": EMBED_MODEL, "input": text}
    resp = httpx.post(url, json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Ollama 默认返回 dense embedding
    dense = data["embeddings"][0]

    # BGE-M3 的 sparse 输出需要从原始响应中解析
    # 注意: Ollama 对 BGE-M3 的 sparse 支持取决于版本
    # 这里 fallback 模拟 sparse 权重（实际可调用 BGE-M3 Python 库）
    sparse_data = {}
    if "sparse" in data:
        sparse_data = data["sparse"][0]

    return {"dense": dense, "sparse": sparse_data}


def generate_demo_data() -> list:
    """生成模拟文档数据用于演示"""
    return [
        "智能音箱可以语音控制家居设备，支持播放音乐、查询天气",
        "智能手机具备拍照、导航、支付等多种功能",
        "智能摄像头支持实时监控、移动侦测、夜视模式",
        "笔记本电脑适合办公、编程、设计等场景",
        "平板电脑便携性强，适合阅读、看视频、绘画",
        "机械键盘敲击手感好，适合长时间打字",
        "无线耳机降噪效果好，适合通勤和运动",
        "智能手表可以监测心率、计步、接收通知",
    ]


# ================================================================
# 第 2 部分：Milvus 集合管理
# ================================================================

def create_hybrid_collection():
    """
    创建支持 Hybrid Search 的 Collection。

    关键设计：
    两个向量字段（dense + sparse）+ 两个索引 + WeightedRanker 融合
    """
    connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)

    # 删除已存在的集合（方便反复运行）
    try:
        Collection(COLLECTION_NAME).drop()
    except Exception:
        pass

    # 定义字段 Schema
    # - id: INT64 主键
    # - text: 原始文本
    # - dense_vector: FLOAT_VECTOR 1024 维（稠密向量）
    # - sparse_vector: SPARSE_FLOAT_VECTOR（稀疏向量）
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=False),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="dense_vector", dtype=DataType.FLOAT_VECTOR, dim=DIM),
        FieldSchema(name="sparse_vector", dtype=DataType.SPARSE_FLOAT_VECTOR),
    ]
    schema = CollectionSchema(fields, description="Hybrid Search 演示集合")

    collection = Collection(name=COLLECTION_NAME, schema=schema)
    print(f"[OK] 创建 Collection: {COLLECTION_NAME}")
    return collection


def build_indexes(collection):
    """
    为两个向量字段分别创建索引。

    Dense 索引: IVF_FLAT（聚类搜索，速度/精度均衡）
    Sparse 索引: SPARSE_INVERTED_INDEX（倒排索引，类似 Elasticsearch）
    """
    # Dense 索引
    collection.create_index(
        field_name="dense_vector",
        index_params={
            "metric_type": "COSINE",    # 余弦相似度
            "index_type": "IVF_FLAT",   # 聚类索引
            "params": {"nlist": 128},    # 聚类中心数
        },
        index_name="dense_idx",
    )

    # Sparse 索引
    collection.create_index(
        field_name="sparse_vector",
        index_params={
            "metric_type": "IP",                    # 内积（稀疏向量）
            "index_type": "SPARSE_INVERTED_INDEX",  # 倒排索引
        },
        index_name="sparse_idx",
    )
    print("[OK] 创建索引: dense(IVF_FLAT) + sparse(INVERTED)")

    # 加载到内存
    collection.load()
    print("[OK] 加载 Collection 到内存")


# ================================================================
# 第 3 部分：数据写入
# ================================================================

def insert_data(collection, texts: list):
    """将文档向量化后写入 Milvus。"""
    ids = list(range(len(texts)))
    dense_list = []
    sparse_list = []

    print(f"[索引] 调用 BGE-M3 生成双向量（共 {len(texts)} 条）...")
    for i, text in enumerate(texts):
        emb = embed_text(text)
        dense_list.append(emb["dense"])
        # 稀疏向量以特殊格式存储
        if emb["sparse"]:
            sparse_list.append(emb["sparse"])
        else:
            # 用简单的关键词权重模拟 sparse
            sparse_list.append({word: 1.0 for word in text[:20] if word != " "})
        print(f"  [进度] {i+1}/{len(texts)}")

    entities = [ids, texts, dense_list, sparse_list]
    collection.insert(entities)
    collection.flush()  # 确保数据落盘
    print(f"[OK] 写入 {len(texts)} 条数据")


# ================================================================
# 第 4 部分：Hybrid Search
# ================================================================

def hybrid_search(collection, query_text: str, top_k: int = 5):
    """
    混合检索 = Dense Search + Sparse Search + RRF 融合。

    Reciprocal Rank Fusion (RRF) 原理：
      每个结果在 dense 中的排名 → 分数 1/(rank + 60)
      每个结果在 sparse 中的排名 → 分数 1/(rank + 60)
      两个分数相加 → 重新排序

    这样即使某个结果只在一路检索中排得靠前，也能进入 Top K。
    """
    query_emb = embed_text(query_text)

    # 构造 Dense 搜索参数
    dense_params = {
        "metric_type": "COSINE",
        "params": {"nprobe": 10},
    }

    # 执行混合检索
    # WeightedRanker(0.7, 0.3): dense 权重 0.7, sparse 权重 0.3
    # 可以理解为"语义匹配占 70%，关键词匹配占 30%"
    results = collection.hybrid_search(
        reqs=[
            # ANN search 1: dense 向量
            {
                "anns_field": "dense_vector",
                "data": [query_emb["dense"]],
                "param": dense_params,
                "limit": top_k * 3,  # 每路多取一些，给 RRF 留融合空间
            },
            # ANN search 2: sparse 向量
            {
                "anns_field": "sparse_vector",
                "data": [query_emb["sparse"]],
                "param": {"metric_type": "IP"},
                "limit": top_k * 3,
            },
        ],
        rerank=WeightedRanker(0.7, 0.3),  # RRF + 权重
        limit=top_k,
        output_fields=["text"],
    )

    # 输出结果
    print(f"\n{'='*50}")
    print(f"查询: \"{query_text}\"")
    print(f"{'='*50}")
    for i, hit in enumerate(results[0]):
        print(f"  #{i+1} (score={hit.score:.4f}) {hit.entity.get('text')}")
    print()

    return results


# ================================================================
# 主流程
# ================================================================

if __name__ == "__main__":
    print("=" * 50)
    print("  Milvus Hybrid Search 演示")
    print("=" * 50)

    # 1. 连接 Milvus
    print("\n[1/5] 连接 Milvus...")
    connections.connect(host=MILVUS_HOST, port=MILVUS_PORT)

    # 2. 创建 Collection
    print("\n[2/5] 创建集合...")
    collection = create_hybrid_collection()

    # 3. 生成演示数据并写入
    print("\n[3/5] 准备数据...")
    texts = generate_demo_data()
    insert_data(collection, texts)

    # 4. 建索引
    print("\n[4/5] 建立索引...")
    build_indexes(collection)

    # 5. 执行混合检索
    print("\n[5/5] 混合检索演示...")

    hybrid_search(collection, "语音控制设备")
    hybrid_search(collection, "便携办公")
    hybrid_search(collection, "运动健康监测")

    # 清理
    connections.disconnect("default")
    print("\n[完成] Hybrid Search 演示结束")
