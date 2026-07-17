# ================================================================
# 多 Agent 协作 — Supervisor + Worker 模式
# ================================================================
# Supervisor Agent (管理者):
#   接收用户问题 → 分析 → 决定交给哪个 Worker → 汇总结果
#
# Worker Agents (执行者):
#   Research Worker:   知识检索和综合分析
#   Calculator Worker: 数值计算
#   General Worker:    通用问答
# ================================================================
#
# 【运行方式】
#   python phase3_agentic_rag/04_multi_agent.py
# ================================================================

import httpx
import json
import re
from typing import TypedDict, Literal
from langgraph.graph import StateGraph, START, END

OLLAMA_URL = "http://localhost:11434"
LLM_MODEL = "qwen3.5:35b-a3b"


class MultiAgentState(TypedDict):
    query: str
    assigned_worker: str
    research_result: str
    calculation_result: str
    final_answer: str


# ================================================================
# Supervisor Agent
# ================================================================

def supervisor(state: MultiAgentState) -> MultiAgentState:
    prompt = f"""判断问题类别:
- research: 询问信息、知识、价格、功能
- calculator: 数值计算、比较
- general: 问候、闲聊

问题: {state['query']}
类别:"""
    try:
        resp = httpx.post(
            f"{OLLAMA_URL}/api/chat",
            json={"model": LLM_MODEL, "messages": [{"role": "user", "content": prompt}], "stream": False, "options": {"temperature": 0.1}},
            timeout=30,
        )
        category = resp.json()["message"]["content"].strip().lower()
        for c in ["research", "calculator", "general"]:
            if c in category:
                state["assigned_worker"] = c; break
        else:
            state["assigned_worker"] = "general"
    except Exception:
        state["assigned_worker"] = "general"
    print(f"  [Supervisor] → {state['assigned_worker']}")
    return state


def research_worker(state: MultiAgentState) -> MultiAgentState:
    doc = "智能音箱价格99-599元，市场120亿元增长15%"
    prompt = f"根据信息回答问题:\n{doc}\n\n问题: {state['query']}"
    resp = httpx.post(
        f"{OLLAMA_URL}/api/chat",
        json={"model": LLM_MODEL, "messages": [{"role": "user", "content": prompt}], "stream": False},
        timeout=30,
    )
    state["research_result"] = resp.json()["message"]["content"]
    return state


def calculator_worker(state: MultiAgentState) -> MultiAgentState:
    prompt = f"提取数字并计算。返回JSON: {{'result':'结果', 'steps':'步骤'}}\n问题: {state['query']}"
    resp = httpx.post(
        f"{OLLAMA_URL}/api/chat",
        json={"model": LLM_MODEL, "messages": [{"role": "user", "content": prompt}], "stream": False},
        timeout=30,
    )
    content = resp.json()["message"]["content"]
    match = re.search(r'\{.*\}', content, re.DOTALL)
    state["calculation_result"] = json.loads(match.group())["result"] if match else content
    return state


def general_worker(state: MultiAgentState) -> MultiAgentState:
    prompt = f"简短回答: {state['query']}"
    resp = httpx.post(
        f"{OLLAMA_URL}/api/chat",
        json={"model": LLM_MODEL, "messages": [{"role": "user", "content": prompt}], "stream": False},
        timeout=30,
    )
    state["research_result"] = resp.json()["message"]["content"]
    return state


def synthesize(state: MultiAgentState) -> MultiAgentState:
    parts = []
    if state.get("research_result"): parts.append(f"📖 {state['research_result']}")
    if state.get("calculation_result"): parts.append(f"🧮 {state['calculation_result']}")
    state["final_answer"] = "\n\n".join(parts) if parts else "无法回答"
    return state


def route(state: MultiAgentState) -> str:
    return {"research": "research_worker", "calculator": "calculator_worker", "general": "general_worker"}.get(state["assigned_worker"], "general_worker")


def build_multi_agent():
    wf = StateGraph(MultiAgentState)
    wf.add_node("supervisor", supervisor)
    wf.add_node("research_worker", research_worker)
    wf.add_node("calculator_worker", calculator_worker)
    wf.add_node("general_worker", general_worker)
    wf.add_node("synthesize", synthesize)
    wf.add_edge(START, "supervisor")
    wf.add_conditional_edges("supervisor", route)
    wf.add_edge("research_worker", "synthesize")
    wf.add_edge("calculator_worker", "synthesize")
    wf.add_edge("general_worker", "synthesize")
    wf.add_edge("synthesize", END)
    return wf.compile()


if __name__ == "__main__":
    print("=" * 60)
    print("  多 Agent: Supervisor + Worker")
    print("=" * 60)
    agent = build_multi_agent()

    for q in ["智能音箱多少钱？", "299*3打8折是多少？", "你好"]:
        print(f"\n{'─'*40}")
        print(f"用户: \"{q}\"")
        r = agent.invoke({"query": q})
        print(f"分配: {r['assigned_worker']} → {r['final_answer'][:80]}")
