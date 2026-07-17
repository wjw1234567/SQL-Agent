# ================================================================
# LangChain Tool 定义 — Agent 的工具箱
# ================================================================
# LangChain Agent 像是一个"智能助手"，
# 它自己不能做所有事，但是可以调用工具。
# 工具 = Agent 可以调用的函数。
#
# 类比:
#   你是一个 AI Agent，你的工具包括:
#   - 浏览器（搜信息）  → web_search tool
#   - 计算器（算数字）  → calculator tool
#   - 笔记本（记东西）  → memory tool
#
# 本节: 自定义三个工具（检索 + 计算 + 时间），
# 为后面的 Agent 做准备。
# ================================================================
#
# 【运行方式】
#   python phase3_agentic_rag/01_tool_definition.py
# ================================================================

import httpx
import json
from datetime import datetime
from typing import Any

from langchain_core.tools import tool

OLLAMA_URL = "http://localhost:11434"
EMBED_MODEL = "bge-m3"

# ================================================================
# 模拟文档库
# ================================================================

DOCUMENTS = [
    "智能音箱价格从99元到599元不等，支持语音控制家居设备",
    "2025年中国智能音箱市场规模达到120亿元，同比增长15%",
    "智能音箱需要连接WiFi，通过APP完成初始配置",
    "智能音箱支持蓝牙5.0，可以连接手机播放音乐",
]


# ================================================================
# 工具 1: 知识检索
# ================================================================

@tool
def knowledge_retrieval(query: str) -> str:
    """
    从本地知识库中检索与查询相关的文档内容。
    当用户询问产品信息、价格、功能介绍时使用。
    """
    def embed_text(text: str):
        resp = httpx.post(
            f"{OLLAMA_URL}/api/embed",
            json={"model": EMBED_MODEL, "input": text},
            timeout=30,
        )
        return resp.json()["embeddings"][0]

    def similarity(a, b):
        dot = sum(x*y for x, y in zip(a, b))
        na = sum(x*x for x in a) ** 0.5
        nb = sum(x*x for x in b) ** 0.5
        return dot / (na * nb) if na * nb > 0 else 0

    qv = embed_text(query)
    doc_vecs = [embed_text(d) for d in DOCUMENTS]
    scores = [(similarity(qv, dv), d) for dv, d in zip(doc_vecs, DOCUMENTS)]
    scores.sort(key=lambda x: -x[0])

    results = "\n".join([d for _, d in scores[:2]])
    return f"检索结果:\n{results}"


# ================================================================
# 工具 2: 计算器
# ================================================================

@tool
def calculator(expression: str) -> str:
    """
    执行数学计算。当用户询问数值计算时使用。
    输入应该是数学表达式，如 "100 * 0.15" 或 "(299 + 399) / 2"
    """
    SAFE_OPS = {'+', '-', '*', '/', '(', ')', ' ', '.', '0', '1', '2',
                '3', '4', '5', '6', '7', '8', '9'}
    for ch in expression:
        if ch not in SAFE_OPS:
            return f"错误: 表达式包含非法字符 '{ch}'"

    try:
        result = eval(expression, {"__builtins__": {}}, {})
        return f"计算结果: {expression} = {result}"
    except Exception as e:
        return f"计算错误: {e}"


# ================================================================
# 工具 3: 当前时间
# ================================================================

@tool
def get_current_time(format_str: str = "%Y-%m-%d %H:%M:%S") -> str:
    """获取当前日期和时间。不需要额外参数时使用默认格式。"""
    return f"当前时间: {datetime.now().strftime(format_str)}"


# ================================================================
# 工具 4: 文档总结
# ================================================================

@tool
def summarize_text(text: str) -> str:
    """
    用 LLM 总结一段文本的核心内容。
    当用户要求"总结""概括""提炼"时使用。
    """
    prompt = f"请用一两句话总结以下内容的核心要点:\n\n{text}"
    try:
        resp = httpx.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": "qwen3.5:35b-a3b",
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
            },
            timeout=30,
        )
        return resp.json()["message"]["content"]
    except Exception as e:
        return f"总结失败: {e}"


# ================================================================
# 演示：查看工具定义
# ================================================================

if __name__ == "__main__":
    print("=" * 50)
    print("  LangChain Tool 定义演示")
    print("=" * 50)

    # @tool 装饰器会把函数转为 Tool 对象
    tools = [knowledge_retrieval, calculator, get_current_time, summarize_text]

    print(f"\n共定义 {len(tools)} 个工具:\n")

    for t in tools:
        print(f"  ┌─ {t.name}")
        print(f"  │  描述: {t.description[:60]}...")
        print(f"  │  参数: {t.args}")
        print()

    # 演示调用
    print(f"{'─'*40}")
    print("工具调用演示:\n")

    print(">>> knowledge_retrieval('智能音箱价格')")
    print(knowledge_retrieval.invoke({"query": "智能音箱价格"}))

    print(f"\n>>> calculator('(299+399)*0.85')")
    print(calculator.invoke({"expression": "(299+399)*0.85"}))

    print(f"\n>>> get_current_time()")
    print(get_current_time.invoke({"format_str": "%Y-%m-%d"}))

    print(f"\n>>> summarize_text('智能音箱市场2025年达到120亿元...')")
    text = "智能音箱市场2025年达到120亿元同比增长15%百度阿里小米占85%份额"
    print(summarize_text.invoke({"text": text}))

    print(f"\n{'='*50}")
    print("关键概念:")
    print("  - @tool 装饰器: 将函数标记为工具")
    print("  - tool.name: Agent 通过名称选择工具")
    print("  - tool.description: 描述告诉 Agent 什么时候用")
    print("  - tool.args: 参数 Schema，Agent 知道要传什么参数")
