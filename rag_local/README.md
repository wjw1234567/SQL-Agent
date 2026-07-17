# RAG 三阶段进阶教程

从基础 RAG → 进阶 RAG (Milvus) → Agentic RAG (LangGraph)

## 项目结构

```
rag_local/
├── README.md                    ← 本文件
├── docker-compose.yml           ← Milvus 集群 (etcd+MinIO+Milvus)
├── requirements.txt             ← 所有依赖
├── start.bat                    ← 启动脚本
├── phase1_basic_rag/            ← 基础 RAG (ChromaDB)
├── phase2_advanced_rag/         ← 进阶 RAG (Milvus + CRAG)
│   ├── 01_milvus_deploy.md      ← Milvus 部署教程
│   ├── 02_hybrid_search.py      ← Dense+Sparse 混合检索
│   ├── 03_multi_query.py        ← Multi-Query + HyDE
│   ├── 04_reranker.py           ← BGE-Reranker 精排
│   ├── 05_corrective_rag.py     ← CRAG 自适应
│   └── 06_evaluation.py         ← Hit Rate / MRR / Faithfulness
├── phase3_agentic_rag/          ← Agentic RAG (LangGraph)
│   ├── 01_tool_definition.py    ← @tool 装饰器
│   ├── 02_simple_agent.py       ← StateGraph 条件路由
│   ├── 03_self_rag_graph.py     ← 自我校验循环
│   ├── 04_multi_agent.py        ← Supervisor + Worker
│   └── 05_trace_eval.py         ← 追踪 + 评估
└── docs/concepts/               ← 概念文档
    ├── rag_arch.md              ← RAG 架构演进
    ├── milvus_index.md          ← Milvus 索引对照
    └── langgraph_basics.md      ← LangGraph 核心概念
```

## 学习路径

1. 通读 `docs/concepts/` 下的概念文档
2. 按编号顺序完成 Phase 2 所有 6 个文件
3. 再按编号顺序完成 Phase 3 所有 5 个文件
