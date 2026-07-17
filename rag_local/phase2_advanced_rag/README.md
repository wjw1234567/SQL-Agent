# Phase 2: 进阶 RAG — Milvus + 混合检索 + CRAG

## 学习目标

掌握 Milvus 向量数据库部署和进阶 RAG 技术：
- Milvus Standalone 集群部署（etcd + MinIO + Milvus）
- Dense + Sparse 混合检索（Hybrid Search）
- Multi-Query / HyDE 查询改写
- BGE-Reranker Cross-Encoder 精排
- Corrective RAG 自适应重检索
- RAG 评估指标（Hit Rate / MRR / Faithfulness）

## 前置条件

| 依赖 | 检查方式 |
|---|---|
| Docker Desktop | `docker --version` |
| Ollama + bge-m3 | `ollama list` 包含 bge-m3 |
| Ollama + Qwen | `ollama list` 包含 qwen |
| Python 虚拟环境 | `.venv\Scripts\activate` |

## 学习路线

按编号顺序学习，每个文件独立运行：

```
01_milvus_deploy.md   → 阅读，理解 Milvus 架构
02_hybrid_search.py   → 运行，观察 Dense + Sparse + RRF 效果
03_multi_query.py     → 运行，对比单查询 vs 多路查询 vs HyDE
04_reranker.py        → 运行，观察 Rerank 前后的排序变化
05_corrective_rag.py  → 运行，体验 CRAG 三种路径的决策过程
06_evaluation.py      → 运行，看检索质量如何量化评估
```

## 快速启动

```powershell
# 1. 启动 Milvus
cd D:\Pycharm_Project\SQL-Agent\rag_local
docker compose -f docker-compose.yml up -d

# 2. 检查 Milvus 就绪
python -c "from pymilvus import connections; connections.connect(host='localhost', port=19530); print('OK')"

# 3. 运行教程代码
cd phase2_advanced_rag
python 02_hybrid_search.py
python 03_multi_query.py
python 04_reranker.py
python 05_corrective_rag.py
python 06_evaluation.py
```

## Troubleshooting

**Q: pymilvus 连不上 Milvus？**
```powershell
# 检查容器状态
docker ps | findstr milvus
# 检查日志
docker compose -f ../docker-compose.yml logs milvus
```

**Q: BGE-Reranker 下载慢？**
使用 HuggingFace 镜像：
```powershell
$env:HF_ENDPOINT = "https://hf-mirror.com"
```

**Q: 显存不足？**
BGE-Reranker 和 Qwen 同时加载可能超 16GB：
- 先跑完 04_reranker.py，关掉 Python 进程再跑其他
- 或使用 use_fp16=True（已默认开启）
