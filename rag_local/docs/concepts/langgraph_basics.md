# LangGraph 核心概念

## 为什么需要 LangGraph？

传统 LLM 应用是"线性"的:
```
输入 → LLM → 输出     ← 一次对话，无法纠错
```

RAG 是"分支"的:
```
输入 → 检索 → 判断质量 → (好 → 输出 | 差 → 重检索)
```

复杂 Agent 是"有向图"的:
```
输入 → 分析意图
    ├─ 需要信息 → 检索工具
    ├─ 需要计算 → 计算工具
    └─ 需要查询 → SQL 工具
```

LangGraph = 用"有向图"表达这些复杂流程。

## 核心概念

### State（状态）

Agent 的"工作记忆"。是一个 TypedDict:
```python
class AgentState(TypedDict):
    messages: List     # 对话历史
    context: str       # 检索到的文档
    iteration: int     # 当前是第几轮
```

### Node（节点）

一个 Node = 一个处理步骤 = 一个 Python 函数：
```python
def retrieve_node(state: AgentState) -> AgentState:
    docs = milvus_search(state["query"])
    state["context"] = docs
    return state
```

### Edge（边）

Edge 定义 Node 之间的流转：
```python
# 无条件边
graph.add_edge("retrieve", "generate")

# 条件边
graph.add_conditional_edges("check", router_function)
```

### Graph（图）

所有 Node + Edge 的集合：
```python
graph = StateGraph(AgentState)
graph.add_node("retrieve", retrieve_node)
graph.add_node("generate", generate_node)
graph.add_edge("retrieve", "generate")
app = graph.compile()
result = app.invoke({"query": "智能音箱价格"})
```

## 关键模式

### 1. 条件循环 (Retry)

```
retrieve → verify → (通过 → generate | 不通过 → retrieve)
```

### 2. 并行扇出 (Parallel)

```
分析
├─ 检索工具
├─ 计算工具
└─ 时间工具        ← 三路同时进行
    ↓
综合回答
```

### 3. Supervisor (管理 Agent)

```
Supervisor Agent (判断)
    ├─ → Research Agent
    ├─ → SQL Agent
    └─ 收集结果 → 最终回答
```
