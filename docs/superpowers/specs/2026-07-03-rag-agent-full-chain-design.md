# RAG 系统三阶段进阶教程 — 设计文档

## 概述

对现有 `rag_local` 项目进行重构和扩展，构建从基础 RAG → 进阶 RAG → Agentic RAG 的三阶段学习体系，完整覆盖 AI Agent 开发全链路。

## 目标读者

有经验的大数据开发工程师，已具备基础 Python 和 Docker 能力，目标是学会本地部署 RAG + LangChain + LangGraph + Milvus，掌握 AI Agent 开发的完整链路。

## 三阶段架构

### Phase 1: 基础 RAG（现有代码保留+精简）

```
保留现有结构，作为"入门对照"。
向量库: ChromaDB（轻量、嵌入式，适合入门理解）
```

### Phase 2: 进阶 RAG（新增核心）

```
向量库: Milvus Standalone (Docker 部署)
新增能力:
  ├── Milvus 集群搭建（etcd + MinIO + Standalone）
  ├── 混合检索（Dense + Sparse/BMP）
  ├── 查询改写（Multi-Query / HyDE）
  ├── Rerank 精排（Cross-Encoder）
  ├── Corrective RAG（检索质量检测 + 自适应重检索）
  └── RAG 评估指标（Hit Rate / MRR / Faithfulness）
```

### Phase 3: Agentic RAG（高阶目标）

```
框架: LangGraph（有向状态图编排）
新增能力:
  ├── Tool 定义（检索 + SQL + 计算/API）
  ├── Agent 状态机（条件路由、循环、中断）
  ├── Self-RAG（生成 + 自我校验 + 修正）
  ├── 多 Agent 协作（Supervisor + Worker）
  └── Evaluate + Trace 全链路追踪
```

## 项目文件结构（最终目标）

```
rag_local/
├── README.md                     ← 入口文档（三阶段索引）
├── requirements.txt              ← 项目依赖
├── start.bat                     ← 一键启动
├── docker-compose.yml            ← Milvus + MinIO + etcd
│
├── phase1_basic_rag/             ← 现有代码（微调保留）
│   ├── app.py                    ← 原 app.py，向量库改为支持 ChromaDB/Milvus 可选
│   ├── chroma_db/                ← 向量库持久化目录
│   └── src/watch_folder/         ← 文档目录
│
├── phase2_advanced_rag/          ← 新增：进阶 RAG 教学
│   ├── 01_milvus_deploy.md       ← Milvus Docker 部署教程
│   ├── 02_hybrid_search.py       ← Dense + Sparse 混合检索
│   ├── 03_multi_query.py         ← 多路查询改写
│   ├── 04_reranker.py            ← BGE-Reranker 精排
│   ├── 05_corrective_rag.py      ← CRAG 自适应重检索
│   ├── 06_evaluation.py          ← 检索质量评估
│   └── README.md
│
├── phase3_agentic_rag/           ← 新增：LangGraph Agent
│   ├── 01_tool_definition.py     ← @tool 装饰器定义工具
│   ├── 02_simple_agent.py        ← 单 Agent + 检索工具
│   ├── 03_self_rag_graph.py      ← Self-RAG 状态图
│   ├── 04_multi_agent.py         ← Supervisor + Worker
│   ├── 05_trace_eval.py          ← LangSmith 追踪/评估
│   └── README.md
│
└── docs/
    └── concepts/                 ← 概念文档
        ├── rag_arch.md           ← RAG 架构详解
        ├── milvus_index.md       ← Milvus 索引类型对照
        └── langgraph_basics.md   ← LangGraph 核心概念
```

## 关键技术选型

| 组件 | Phase 1 | Phase 2 | Phase 3 |
|---|---|---|---|
| 向量库 | ChromaDB | Milvus 2.5 | Milvus 2.5 |
| Embedding | BGE-M3 (Ollama) | BGE-M3 (Ollama) | BGE-M3 (Ollama) |
| Reranker | — | BGE-Reranker-v2 | BGE-Reranker-v2 |
| LLM | Qwen (Ollama) | Qwen (Ollama) | Qwen (Ollama) |
| 框架 | — | LangChain | LangChain + LangGraph |
| 检索策略 | 单向量检索 | Hybrid + Multi-Query | Agent 编排 |
| 编排方式 | 线性代码 | 线性代码 | 有向状态图 |
| 追踪 | — | — | LangSmith |

## Milvus 部署架构

```
Docker Compose (docker-compose.yml):
┌─────────────────────┐
│  etcd                │ ← Milvus 元数据存储
│  (bitnami/etcd:3.5)  │
└─────────┬───────────┘
          │
┌─────────▼───────────┐
│  MinIO               │ ← Milvus 数据持久化（向量文件、日志）
│  (minio/minio)        │
└─────────┬───────────┘
          │
┌─────────▼───────────┐
│  Milvus Standalone   │ ← 向量检索服务
│  (milvusdb/milvus)   │ ← 端口: 19530 (gRPC), 9091 (HTTP)
│                      │ ← WebUI: localhost:9091/webui
└─────────────────────┘

资源预算:
  etcd: 512MB   (内存)
  MinIO: 512MB  (内存)
  Milvus: 4GB   (内存 + 部分显存用于索引)
  总计: ~5GB + Qwen(~10GB) + BGE-M3(~2GB) = ~17GB ← 64GB 内存够用
```

## Phase 2 关键设计

### 2.1 混合检索（Hybrid Search）

```
用户问题 → BGE-M3 Dense Embedding → Milvus dense 索引
       → BM25/BGE-M3 Sparse → Milvus sparse 索引
                 ↓
            Dense 结果 + Sparse 结果
                 ↓
            Reciprocal Rank Fusion (RRF) 合并排序
                 ↓
            Top K 最终结果
```

### 2.2 查询改写（Query Translation）

```python
@llm 将"上季度营收多少？" 改写成多条查询:
  ├─ "2024年Q4 营收"       → Dense 检索
  ├─ "2024年Q4 revenue"   → Dense 检索
  ├─ "quarterly earnings" → Sparse 检索
  └─ "收入 2024 第四季度" → BM25 检索
```

### 2.3 Corrective RAG (CRAG)

```
用户问题
    ↓
检索文档
    ↓
相关性判断（小模型 / LLM 判断）
    ├─ 相关度 > 阈值 → 直接生成
    ├─ 相关度 中等  → 知识图谱补充检索 → 融合生成
    └─ 相关度 < 阈值 → 网络搜索补充 → 生成 + 标注"外部来源"
```

## Phase 3 关键设计

### 3.1 LangGraph Self-RAG 状态图

```python
# LangGraph 有向图
# 节点:
#   ① retrieve(state) → 检索文档
#   ② generate(state) → LLM 生成回答
#   ③ check_hallucination(state) → 校验回答是否基于文档
#   ④ answer_grader(state) → 评估是否回答完整
#
# 边:
#   START → retrieve
#   retrieve → generate
#   generate → check_hallucination
#   check_hallucination → answer_grader  (通过校验)
#   check_hallucination → retrieve       (未通过 → 重新检索)
#   answer_grader → END                  (完整回答)
#   answer_grader → generate             (不完整 → 补充生成)
```

### 3.2 多 Agent 协作

```
用户问题
    ↓
Supervisor Agent（LangGraph）
    ├─ 判断问题类型 → 路由到:
    │   ├─ Research Agent   → 多轮检索 + 综合分析
    │   ├─ SQL Agent        → 查结构化数据库
    │   ├─ Calculator Agent → 数值计算
    │   └─ Q&A Agent        → 简单问答
    ↓
Worker Agent 执行 → 返回结果
    ↓
Supervisor Agent 收集结果 → 综合回答
```

## 迭代策略

每次更新只涉及 1 个文件，每完成一个文件可以独立验证：

| 步骤 | 文件 | 验证方法 |
|---|---|---|
| 1 | `docker-compose.yml` | `docker ps` 确认 Milvus 就绪 |
| 2 | `01_milvus_deploy.md` | 在 Attu WebUI 中看到空集合 |
| 3 | `02_hybrid_search.py` | `python 02_hybrid_search.py "测试查询"` 看返回结果 |
| 4 | `03_multi_query.py` | 对比单查询 vs 多路查询的召回率 |
| 5 | `04_reranker.py` | 对比 Rerank 前后 Top 5 的准确率 |
| 6 | `05_corrective_rag.py` | 输入低质量文档，验证系统是否触发重检索 |
| 7 | `06_evaluation.py` | `python 06_evaluation.py` 输出 HitRate/MRR |
| 8 | Phase 3 各文件 | 逐步运行，每个 Agent 可独立测试 |

## 技术决策记录

1. **Milvus 2.5**：最新稳定版，原生支持 Dense + Sparse 混合检索
2. **BGE-Reranker 替代 Cohere**：Cohere API 需要联网且收费，BGE 可本地部署
3. **LangGraph 替代纯 LangChain Agent**：LangGraph 有向图能表达更复杂的控制流
4. **Python 单文件教学代码**：每个概念一个独立 `.py` 文件，无需翻页查上下文
5. **保留 ChromaDB 作为对照**：让学习者体验"换向量库 = 只改 5 行代码"的 LangChain 抽象层优势
