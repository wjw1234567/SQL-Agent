# RAG 架构详解

## 从朴素 RAG 到 Agentic RAG

### 朴素 RAG (Naive RAG)

```
用户输入 → 向量检索 → Top K 拼接 → LLM 生成
         ↓
      问题: 检索质量不可控，一次失败没有补救
```

### 进阶 RAG (Advanced RAG) — Phase 2 内容

```
用户输入
    ↓
查询改写 (HyDE / Multi-Query)     ← 优化输入
    ↓
混合检索 (Dense + Sparse)         ← 优化检索
    ↓
重排序 (Reranker)                 ← 优化排序
    ↓
Context 压缩                       ← 优化上下文
    ↓
LLM 生成
    ↓
是否需补充检索? (Corrective RAG)   ← 自适应
    ├─ 是 → 重新检索
    └─ 否 → 输出
```

### Agentic RAG (Agent 驱动的 RAG) — Phase 3 内容

```
用户输入
    ↓
Agent 分析问题
    ├─ 需要事实性信息 → 检索工具
    ├─ 需要计算       → Python 工具
    ├─ 需要查数据库   → SQL 工具
    ├─ 需要实时信息   → 网络搜索工具
    └─ 需要多步推理   → 子 Agent 分解
    ↓
收集所有工具结果
    ↓
LLM 综合回答
    ↓
自我校验 (Self-RAG)                ← 检查幻觉
    ├─ 有幻觉 → 重新检索/修正
    └─ 无幻觉 → 最终回答
```

## RAG 三阶段递进图

看三阶段的变化：

Phase 1:  ChromaDB     │  固定流程    │  单向量检索    │  无校验
Phase 2:  Milvus      │  固定流程    │  混合检索      │  Corrective
Phase 3:  Milvus      │  Agent 编排  │  多工具调用    │  Self-RAG
