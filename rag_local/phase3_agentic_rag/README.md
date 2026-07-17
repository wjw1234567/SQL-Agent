# Phase 3: Agentic RAG — LangGraph + 多 Agent

## 学习目标

掌握 LangGraph Agent 编排：
- @tool 装饰器定义可复用工具
- StateGraph 状态机
- 条件路由和循环（Self-RAG）
- Supervisor + Worker 多 Agent 协作
- 追踪评估

## 学习路线

```
01_tool_definition.py   → 理解 @tool 和工具接口
02_simple_agent.py      → 第一个 StateGraph 条件路由
03_self_rag_graph.py    → 自我校验循环
04_multi_agent.py       → Supervisor + Worker 协作
05_trace_eval.py        → 追踪 + 评估
```

## 概念

LangGraph 将代码组织为有向图:
- Node = 处理步骤
- Edge = 流转规则
- State = 共享数据
