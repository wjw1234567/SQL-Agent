# ================================================================
# Corrective RAG (CRAG) — 自适应重检索
# ================================================================
# CRAG 的核心思想：让 LLM 自己判断检索结果的质量，决定对策。
#
# 传统 RAG:  检索 → 生成（一次过，好坏不论）
# CRAG:      检索 → 评估相关性 →
#             ├─ 相关度 > 阈值 → 直接生成
#             ├─ 相关度 中等   → 扩展检索（换角度）→ 融合生成
#             └─ 相关度 < 阈值 → 先用 LLM 自身知识 + 标注"无相关文档"
#
# 为什么需要 CRAG？
#   检索质量不稳定（短文本/噪声/文档缺失）。
#   如果检索到不相关的内容，LLM 反而会被误导。
#   CRAG 加了一层"质检员"。
# ================================================================
#
# 【运行方式】
#   python phase2_advanced_rag/05_corrective_rag.py
# ================================================================

import httpx
import json
import re
from typing import List, Tuple

OLLAMA_URL = "http://localhost:11434"
EMBED_MODEL = "bge-m3"
LLM_MODEL = "qwen3.5:35b-a3b"

# ================================================================
# 文档库
# ================================================================

DOCUMENTS = [
    "智能音箱可以通过语音控制家中的灯光、空调、电视等设备，"
    "支持定时开关和场景模式设置，让生活更加便利。",

    "智能音箱的价格从99元到599元不等，入门级产品功能简单，"
    "高端产品支持触屏、摄像头和智能家居网关功能。",

    "2025年中国智能音箱市场规模达到120亿元，同比增长15%，"
    "其中百度、阿里、小米三家占据85%市场份额。",
]


def embed_text(text: str) -> List[float]:
    """BGE-M3 向量化。"""
    resp = httpx.post(
        f"{OLLAMA_URL}/api/embed",
        json={"model": EMBED_MODEL, "input": text},
        timeout=30,
    )
    return resp.json()["embeddings"][0]


def cosine_similarity(a, b):
    dot = sum(x*y for x, y in zip(a, b))
    na = sum(x*x for x in a) ** 0.5
    nb = sum(x*x for x in b) ** 0.5
    return dot / (na * nb) if na * nb > 0 else 0


def retrieve(query: str, top_k: int = 2) -> List[Tuple[str, float]]:
    """向量检索（模拟，实际使用 Milvus）。"""
    qv = embed_text(query)
    doc_vecs = [embed_text(d) for d in DOCUMENTS]
    scored = [(cosine_similarity(qv, dv), d) for dv, d in zip(doc_vecs, DOCUMENTS)]
    scored.sort(key=lambda x: -x[0])
    return [(d, s) for s, d in scored[:top_k]]


# ================================================================
# CRAG 核心：相关性评估
# ================================================================

def evaluate_relevance(query: str, documents: List[str]) -> List[Tuple[str, float, str]]:
    """
    让 LLM 评估每个检索结果的相关性。

    评估标准：
      relevant:    明确回答了问题，信息直接可用
      partially:   部分相关，信息不完整或角度不够直接
      irrelevant:  不相关或无关

    返回值: [(文档内容, 相似度分数, 评级), ...]
    """
    evaluations = []
    for doc, score in documents:
        prompt = f"""
你是一个检索质量评估专家。请判断以下文档是否与用户问题相关。

用户问题: {query}

文档内容: {doc}

请只返回 JSON:
{{"relevance": "relevant | partially | irrelevant", "reason": "简短原因"}}
"""
        try:
            resp = httpx.post(
                f"{OLLAMA_URL}/api/chat",
                json={
                    "model": LLM_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "stream": False,
                    "options": {"temperature": 0.1},  # 低温度，稳定判断
                },
                timeout=30,
            )
            content = resp.json()["message"]["content"]
            match = re.search(r'\{.*\}', content, re.DOTALL)
            if match:
                result = json.loads(match.group())
                evaluations.append((doc, result["relevance"], result["reason"]))
            else:
                evaluations.append((doc, "partially", "无法解析评估结果"))
        except Exception:
            evaluations.append((doc, "partially", "评估异常"))

    return evaluations


def generate_answer(query: str, context: str, source_note: str = "") -> str:
    """LLM 基于生成回答。"""
    prompt = f"""请基于以下参考内容回答问题。

{source_note}

参考内容:
{context}

问题: {query}

请给出简洁、准确的回答。"""
    resp = httpx.post(
        f"{OLLAMA_URL}/api/chat",
        json={
            "model": LLM_MODEL,
            "messages": [{"role": "user", "content": prompt}],
            "stream": False,
        },
        timeout=60,
    )
    return resp.json()["message"]["content"]


# ================================================================
# 主流程：演示 CRAG 三种路径
# ================================================================

def crag_pipeline(query: str, top_k: int = 2):
    """CRAG 全流程。"""
    print(f"\n{'='*50}")
    print(f"用户问题: \"{query}\"")

    # 1. 检索
    print("\n[Step 1] 检索文档...")
    retrieved = retrieve(query, top_k)
    for doc, score in retrieved:
        print(f"  相关度 {score:.3f} | {doc[:40]}...")

    # 2. 评估
    print("\n[Step 2] LLM 评估相关性...")
    evaluations = evaluate_relevance(query, retrieved)

    relevant_count = sum(1 for _, rel, _ in evaluations if rel == "relevant")
    partial_count = sum(1 for _, rel, _ in evaluations if rel == "partially")

    # 3. 决策
    print("\n[Step 3] CRAG 决策...")

    if relevant_count >= 1:
        # 路径 A: 有足够的相关文档 → 直接生成
        print("  → 路径 A: 相关文档充足，直接生成")
        context = "\n".join([doc for doc, _, _ in evaluations if doc])
        answer = generate_answer(query, context)
        print(f"\n[回答] {answer}")

    elif partial_count >= 1:
        # 路径 B: 仅有部分相关 → 扩展检索
        print("  → 路径 B: 文档部分相关，扩展检索...")
        # 用 LLM 生成补充查询
        ext_prompt = f"根据'{query}'和现有文档，生成一个更精确的搜索词："
        resp = httpx.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": LLM_MODEL,
                "messages": [{"role": "user", "content": ext_prompt}],
                "stream": False,
                "options": {"temperature": 0.3},
            },
            timeout=30,
        )
        new_query = resp.json()["message"]["content"].strip().split("\n")[0]
        print(f"  扩展查询: \"{new_query}\"")

        # 用原文档 + LLM 知识生成
        context = "\n".join([doc for doc, _, _ in evaluations if doc])
        answer = generate_answer(query, context, source_note="注意：部分参考内容可能与问题不完全相关，请结合自身知识回答。")
        print(f"\n[回答] {answer}")

    else:
        # 路径 C: 无相关文档 → 靠 LLM 自身知识
        print("  → 路径 C: 无相关文档，使用 LLM 自身知识")
        answer = generate_answer(query, "没有找到直接相关的参考文档。",
                                 source_note="没有找到直接相关的参考文档。请基于自身知识回答，并在末尾注明'以上回答基于模型自身知识，非文档来源'。")
        print(f"\n[回答] {answer}\n\n(以上回答基于模型自身知识，非文档来源)")

    print(f"{'='*50}\n")


if __name__ == "__main__":
    print("=" * 50)
    print("  Corrective RAG (CRAG) 演示")
    print("=" * 50)

    # 演示三种路径
    crag_pipeline("智能音箱有哪些功能")       # 路径 A: 直接有相关文档
    crag_pipeline("智能家居发展趋势")          # 路径 B: 部分相关
    crag_pipeline("火星移民计划方案")          # 路径 C: 完全不相关
