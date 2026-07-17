# ================================================================
# Self-RAG — 自我校验 + 修正循环
# ================================================================
# Self-RAG = 让 LLM 自己检查自己的回答。
#
# 传统 RAG:   检索 → 生成 → 结束
# Self-RAG:   检索 → 生成 → 校验质量 → (合格→结束 | 不合格→修正)
#
# LangGraph 实现 Self-RAG 的核心价值:
#   循环（retrieve → generate → check → generate → check...）
#   在传统代码中实现"循环到满意为止"很别扭，
#   在图中就是一个简单的条件边。
# ================================================================
#
# 【运行方式】
#   python phase3_agentic_rag/03_self_rag_graph.py
# ================================================================

import httpx
import json
import re
from typing import TypedDict, Literal
from langgraph.graph import StateGraph, START, END

OLLAMA_URL = "http://localhost:11434"
LLM_MODEL = "qwen3.5:35b-a3b"


class SelfRAGState(TypedDict):
    query: str
    contexts: list
    answer: str
    iteration: int
    max_iterations: int
    verdict: str


def retrieve(state: SelfRAGState) -> SelfRAGState:
    docs = [
        "智能音箱价格从99元到599元不等，高端产品支持触屏和摄像头",
        "2025年智能音箱市场规模120亿元，同比增长15%",
    ]
    if not state.get("contexts"):
        state["contexts"] = []
    state["contexts"].extend(docs)
    state["iteration"] = state.get("iteration", 0) + 1
    return state


def generate(state: SelfRAGState) -> SelfRAGState:
    context = "\n".join(state.get("contexts", []))
    prompt = f"基于以下内容回答问题:\n{context}\n\n问题: {state['query']}"
    resp = httpx.post(
        f"{OLLAMA_URL}/api/chat",
        json={"model": LLM_MODEL, "messages": [{"role": "user", "content": prompt}], "stream": False},
        timeout=30,
    )
    state["answer"] = resp.json()["message"]["content"]
    return state


def check_quality(state: SelfRAGState) -> SelfRAGState:
    prompt = f"""
检查回答质量。

问题: {state['query']}
参考: {' '.join(state.get('contexts', []))}
回答: {state['answer']}

返回JSON:
{{"verdict": "pass" | "needs_revision", "reason": "原因"}}
"""
    resp = httpx.post(
        f"{OLLAMA_URL}/api/chat",
        json={"model": LLM_MODEL, "messages": [{"role": "user", "content": prompt}], "stream": False},
        timeout=30,
    )
    content = resp.json()["message"]["content"]
    match = re.search(r'\{.*\}', content, re.DOTALL)
    state["verdict"] = json.loads(match.group())["verdict"] if match else "pass"
    return state


def should_continue(state: SelfRAGState) -> Literal["__end__", "retrieve"]:
    if state.get("verdict") == "pass" or state.get("iteration", 0) >= state.get("max_iterations", 3):
        return END
    return "retrieve"


def build_self_rag():
    workflow = StateGraph(SelfRAGState)
    workflow.add_node("retrieve", retrieve)
    workflow.add_node("generate", generate)
    workflow.add_node("check", check_quality)

    workflow.add_edge(START, "retrieve")
    workflow.add_edge("retrieve", "generate")
    workflow.add_edge("generate", "check")
    workflow.add_conditional_edges("check", should_continue)

    return workflow.compile()


if __name__ == "__main__":
    print("=" * 60)
    print("  Self-RAG: 自我校验 + 修正循环")
    print("=" * 60)

    agent = build_self_rag()
    result = agent.invoke({
        "query": "智能音箱多少钱？有什么功能？",
        "max_iterations": 3,
    })
    print(f"\n查询: \"{result['query']}\"")
    print(f"迭代轮次: {result['iteration']}")
    print(f"校验结果: {result.get('verdict', 'N/A')}")
    print(f"\nAI 回答:\n{result['answer']}")
