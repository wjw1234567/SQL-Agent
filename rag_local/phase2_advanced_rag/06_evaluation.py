# ================================================================
# RAG 检索质量评估 — Hit Rate + MRR + Faithfulness
# ================================================================
# 评估指标说明：
#
# Hit Rate (命中率):
#   在 Top-K 结果中，有多少次包含了正确答案
#   衡量"有没有找到"——不是第一名也行，在前 K 名就算
#   公式: HR@K = 命中次数 / 总查询次数
#
# MRR (Mean Reciprocal Rank，平均倒数排名):
#   第一个正确答案出现的位置的倒数的平均值
#   衡量"是不是第一名就找到了"
#   公式: MRR@K = (1/rank1 + 1/rank2 + ...) / N
#   如果没找到，倒数算 0
#   举例: 第一次查到第 1 名就是答案 → MRR 贡献 1/1 = 1
#         第一次查到第 3 名才是答案 → MRR 贡献 1/3 = 0.33
#
# Faithfulness (忠实度):
#   回答是否完全基于检索到的文档，没有编造
#   用 LLM 判断"回答中的每句话都能在文档中找到依据"
# ================================================================
#
# 【运行方式】
#   python phase2_advanced_rag/06_evaluation.py
# ================================================================

import httpx
import json
import re
from typing import List

OLLAMA_URL = "http://localhost:11434"
EMBED_MODEL = "bge-m3"
LLM_MODEL = "qwen3.5:35b-a3b"

# ================================================================
# 测试集
# ================================================================
# 每条测试数据包含:
#   - query: 用户问题
#   - relevant_doc: 期望被检索到的文档（正确答案）
#   - doc_pool: 文档池（系统从中检索）

TEST_SET = [
    {
        "query": "智能音箱价格范围是多少？",
        "relevant_doc": "智能音箱的价格从99元到599元不等",
        "doc_pool": [
            "智能音箱的价格从99元到599元不等",
            "智能音箱可以语音控制家居设备",
            "笔记本电脑适合办公使用",
            "智能音箱市场年增长15%",
            "智能音箱支持WiFi和蓝牙连接",
        ]
    },
    {
        "query": "2025年智能音箱市场规模",
        "relevant_doc": "2025年中国智能音箱市场规模达到120亿元",
        "doc_pool": [
            "智能音箱的价格从99元到599元不等",
            "智能音箱可以语音控制家居设备",
            "2025年中国智能音箱市场规模达到120亿元",
            "笔记本电脑年出货量2.6亿台",
            "智能家居套装售价1599元",
        ]
    },
]


def embed_text(text: str) -> list:
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


def vector_search(query: str, docs: List[str], top_k: int = 3) -> List[int]:
    """向量检索，返回排序后的文档索引。"""
    qv = embed_text(query)
    doc_vecs = [embed_text(d) for d in docs]
    scores = [(cosine_similarity(qv, dv), i) for i, dv in enumerate(doc_vecs)]
    scores.sort(key=lambda x: -x[0])
    return [i for _, i in scores[:top_k]]


# ================================================================
# 计算 Hit Rate @ K
# ================================================================

def hit_rate(results: list, relevant_idx: int, k: int = 3) -> bool:
    """Hit@K: 正确答案是否在 Top-K 中。"""
    return relevant_idx in results[:k]


def mean_reciprocal_rank(all_results: list, all_relevant_idx: list, k: int = 3) -> float:
    """MRR@K: 平均倒数排名。"""
    total = 0.0
    for results, relevant_idx in zip(all_results, all_relevant_idx):
        for rank, idx in enumerate(results[:k], 1):
            if idx == relevant_idx:
                total += 1.0 / rank
                break
    return total / len(all_results) if all_results else 0.0


# ================================================================
# Faithfulness 评估
# ================================================================

def evaluate_faithfulness(query: str, answer: str, context: str) -> float:
    """
    用 LLM 评估回答是否忠实于上下文。

    让 LLM 逐个检查回答中的关键陈述能否在上下文中找到依据。
    返回 0-1 的忠实度分数。
    """
    prompt = f"""
你是一个回答忠实度评估专家。

请判断以下"回答"中的每个关键陈述是否都能在"参考内容"中找到依据。

参考内容:
{context}

回答:
{answer}

评分规则:
- 1.0: 回答中所有陈述都能在参考内容中找到直接依据
- 0.7: 大部分有依据，少量基于模型自身知识
- 0.3: 大部分是模型自身知识，少量有依据
- 0.0: 回答完全与参考内容无关

请只返回 JSON:
{{"faithfulness": 0.0-1.0, "reason": "简短说明"}}
"""
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
        content = resp.json()["message"]["content"]
        match = re.search(r'\{.*\}', content, re.DOTALL)
        if match:
            result = json.loads(match.group())
            return float(result["faithfulness"])
    except Exception:
        pass
    return 0.0


def generate_answer(query: str, context: str) -> str:
    """LLM 生成回答。"""
    prompt = f"基于以下内容回答问题:\n{context}\n\n问题: {query}"
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


if __name__ == "__main__":
    print("=" * 50)
    print("  RAG 检索质量评估")
    print("=" * 50)

    print(f"\n测试集大小: {len(TEST_SET)} 条查询")

    # ---- 1. Hit Rate & MRR ----
    print(f"\n{'─'*40}")
    print("[1] 检索质量: Hit Rate & MRR")

    all_results = []
    all_relevant = []

    for item in TEST_SET:
        results = vector_search(item["query"], item["doc_pool"], top_k=3)
        relevant_idx = item["doc_pool"].index(item["relevant_doc"])
        all_results.append(results)
        all_relevant.append(relevant_idx)

        hits = "✓" if hit_rate(results, relevant_idx, 3) else "✗"
        print(f"  查询: \"{item['query']}\"")
        print(f"    检索结果 Top3: {[item['doc_pool'][i][:20]+'...' for i in results]}")
        print(f"    期望文档 #{relevant_idx+1} → {'命中' if hits == '✓' else '未命中'}")

    hr_3 = sum(hit_rate(r, ri, 3) for r, ri in zip(all_results, all_relevant)) / len(TEST_SET)
    mrr_3 = mean_reciprocal_rank(all_results, all_relevant, 3)

    print(f"\n  指标:")
    print(f"    Hit Rate@3 = {hr_3:.1%}   ({sum(1 for r, ri in zip(all_results, all_relevant) if hit_rate(r, ri, 3))}/{len(TEST_SET)})")
    print(f"    MRR@3      = {mrr_3:.3f}")

    # ---- 2. Faithfulness ----
    print(f"\n{'─'*40}")
    print("[2] 生成质量: Faithfulness 忠实度评估")

    total_faith = 0.0
    for item in TEST_SET:
        # 用最相关的文档作为上下文
        results = vector_search(item["query"], item["doc_pool"], top_k=2)
        context = "\n".join([item["doc_pool"][i] for i in results])

        answer = generate_answer(item["query"], context)
        faith = evaluate_faithfulness(item["query"], answer, context)

        total_faith += faith
        print(f"  查询: \"{item['query']}\"")
        print(f"    回答: {answer[:50]}...")
        print(f"    忠实度: {faith:.1%}")

    avg_faith = total_faith / len(TEST_SET)

    # ---- 3. 汇总 ----
    print(f"\n{'='*50}")
    print("  评估汇总")
    print(f"{'='*50}")
    print(f"  Hit Rate@3 (检索命中率):  {hr_3:.1%}")
    print(f"  MRR@3 (排名质量):         {mrr_3:.3f}")
    print(f"  Faithfulness (忠实度):    {avg_faith:.1%}")
    print(f"{'='*50}")
    print("\n说明:")
    print("  - Hit Rate 衡量"有没有找到正确的"")
    print("  - MRR 衡量"是不是第一个就找到了"")
    print("  - Faithfulness 衡量"回答有没有瞎编"")
    print()
    print("  改进方向:")
    print("  提高 Hit Rate → 查询改写/多路检索/扩大 Top K")
    print("  提高 MRR      → Rerank 精排")
    print("  提高 Faithfulness → CRAG/更好的 prompt")
