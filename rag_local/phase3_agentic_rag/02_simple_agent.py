# ================================================================
# LangGraph 简单 Agent — 单 Agent + 检索工具
# ================================================================
# 本节从"手工编排"过渡到 LangGraph 状态机。
#
# LangGraph 的核心概念:
#   State:      Agent 的"工作记忆"（当前状态、已收集的信息）
#   Node:       Agent 可以执行的"动作"（检索、推理、回答）
#   Edge:       动作之间的流转（条件判断、顺序执行）
#   Graph:      所有 Node 和 Edge 组成的"流程图"
#
# 对比:
#   传统代码:    if/else 硬编码分支 → 难以修改
#   LangGraph:  有向图定义流程 → 灵活修改
# ================================================================
#
# 【运行方式】
#   python phase3_agentic_rag/02_simple_agent.py
# ================================================================

import httpx
from typing import TypedDict, Literal
from langgraph.graph import StateGraph, START, END

OLLAMA_URL = "http://localhost:11434"
LLM_MODEL = "qwen3.5:35b-a3b"
EMBED_MODEL = "bge-m3"

# ================================================================
# 第 1 部分: 定义 State（状态）
# ================================================================
# StateGraph 的状态是一个 TypedDict，定义了整个流程中共享的数据。
# 类比: 生产线上的"工件"——每个工位（Node）读取/修改它。

class AgentState(TypedDict):
    """Agent 的工作状态。"""
    query: str           # 用户原始问题
    context: str         # 检索到的文档内容
    answer: str          # 生成的回答
    needs_retrieval: bool  # 是否需要检索


# ================================================================
# 第 2 部分: 定义 Node（节点）
# ================================================================

def decide_action(state: AgentState) -> AgentState:
    """
    Node 1: 判断是否需要检索。
    简单规则: 如果问题涉及文档内容，就检索。
    """
    query = state["query"]
    retrieval_keywords = [
        "什么", "多少", "如何", "哪些", "介绍", "说明",
        "价格", "功能", "市场", "尺寸", "参数", "区别"
    ]
    state["needs_retrieval"] = any(kw in query for kw in retrieval_keywords)
    return state


def retrieve(state: AgentState) -> AgentState:
    """
    Node 2: 执行文档检索。
    """
    if not state.get("needs_retrieval"):
        state["context"] = "(无需检索)"
        return state

    documents = [
        "智能音箱价格从99元到599元不等，高端产品支持触屏和摄像头",
        "2025年中国智能音箱市场规模达到120亿元，同比增长15%",
        "智能音箱需要连接WiFi完成配置，支持2.4G和5G双频",
        "智能音箱支持语音控制灯光、空调、电视等设备",
    ]

    query = state["query"]
    matched = [d for d in documents if any(kw in d for kw in query.split())]
    state["context"] = "\n".join(matched[:2]) if matched else "(未找到匹配文档)"
    return state


def generate(state: AgentState) -> AgentState:
    """
    Node 3: LLM 基于检索结果生成回答。
    """
    context = state["context"]
    query = state["query"]

    prompt = f"""你是智能问答助手。

参考信息:
{context}

用户问题: {query}

请回答（如果参考信息为空，诚实地说明无法回答）:"""

    try:
        resp = httpx.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": LLM_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
            },
            timeout=30,
        )
        state["answer"] = resp.json()["message"]["content"]
    except Exception as e:
        state["answer"] = f"生成回答时出错: {e}"

    return state


# ================================================================
# 第 3 部分: 路由函数（条件边）
# ================================================================

def router(state: AgentState) -> Literal["retrieve", "generate"]:
    """根据是否检索决定走哪条路。"""
    return "retrieve" if state["needs_retrieval"] else "generate"


# ================================================================
# 第 4 部分: 构建图
# ================================================================

def build_agent():
    """用 StateGraph 构建 Agent 工作流。"""
    workflow = StateGraph(AgentState)

    workflow.add_node("decide", decide_action)
    workflow.add_node("retrieve", retrieve)
    workflow.add_node("generate", generate)

    workflow.add_edge(START, "decide")
    workflow.add_conditional_edges("decide", router)
    workflow.add_edge("retrieve", "generate")
    workflow.add_edge("generate", END)

    return workflow.compile()


if __name__ == "__main__":
    print("=" * 50)
    print("  LangGraph 简单 Agent 演示")
    print("=" * 50)
    print("\n状态机: START → decide → (retrieve?) → generate → END\n")

    agent = build_agent()

    query1 = "智能音箱有哪些功能"
    print(f"{'─'*40}")
    print(f"查询: \"{query1}\"")
    result = agent.invoke({"query": query1})
    print(f"  是否需要检索: {result['needs_retrieval']}")
    print(f"  检索到的内容: {result['context'][:60]}...")
    print(f"  AI 回答: {result['answer'][:80]}...")

    query2 = "你好，请介绍一下你自己"
    print(f"\n{'─'*40}")
    print(f"查询: \"{query2}\"")
    result = agent.invoke({"query": query2})
    print(f"  是否需要检索: {result['needs_retrieval']}")
    print(f"  AI 回答: {result['answer'][:80]}...")

    print(f"\n{'='*50}")
    print("什么是 StateGraph?")
    print("  - State = 共享数据（像 Redis 缓存）")
    print("  - Node  = 处理函数（像微服务）")
    print("  - Edge  = 路由逻辑（像 API 网关）")
