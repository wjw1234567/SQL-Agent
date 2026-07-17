# ================================================================
# 查询改写（Query Translation）— Multi-Query + HyDE
# ================================================================
# Multi-Query: 让 LLM 把 1 个问题改写成 N 个不同角度的查询
# HyDE: 先生成一个假设性文档，用文档的向量去检索
#
# 为什么有效？
#   用户的问题往往很短（3-10 个字），
#   但被检索的文档通常很长（几百字）。
#   短问题 → 短向量 → 在长文档的向量空间中"找不到方向"
#   改写后 → 多个角度 → 覆盖更多语义空间
# ================================================================
#
# 【运行方式】
#   python phase2_advanced_rag/03_multi_query.py
# ================================================================

import httpx
import json
import re
from typing import List

OLLAMA_URL = "http://localhost:11434"
EMBED_MODEL = "bge-m3"
LLM_MODEL = "qwen3.5:35b-a3b"

# ================================================================
# 第 1 部分：调用 LLM 改写查询
# ================================================================

def generate_multi_queries(original_query: str, n: int = 4) -> List[str]:
    """
    让 LLM 将一个问题改写成 N 个不同角度的查询。

    例如:
      原始: "智能音箱多少钱"
      改写:
        - "智能音箱价格 售价"
        - "smart speaker price"
        - "智能音箱各型号价格对比"
        - "智能音箱性价比推荐"

    为什么这样做？
    用户的一句话问法可能和文档中的表述方式完全不同。
    多路查询覆盖了"中文/英文/对比/推荐"四种表述方式，
    更大概率命中文档中的相关段落。
    """
    prompt = f"""
你是一个查询改写专家。请将用户的问题改写成 {n} 个不同角度的搜索查询。

要求:
- 从不同角度/同义词/中英文改写
- 每条查询 5-15 个字
- 保持原意，不要扩展出原问题没有的信息
- 用 JSON 数组格式返回

用户问题: {original_query}

请只返回 JSON 数组，不要其他内容:
```json
["改写1", "改写2", "改写3", "改写4"]
```"""
    try:
        resp = httpx.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": LLM_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
                "options": {"temperature": 0.3},
            },
            timeout=30,
        )
        content = resp.json()["message"]["content"]
        # 提取 JSON 数组
        match = re.search(r'\[.*?\]', content, re.DOTALL)
        if match:
            queries = json.loads(match.group())
            return [q.strip() for q in queries if q.strip()][:n]
        return [original_query]
    except Exception as e:
        print(f"  [WARN] 查询改写失败: {e}")
        return [original_query]


def generate_hyde_document(query: str) -> str:
    """
    HyDE (Hypothetical Document Embedding) 的核心思想:

    传统检索: 用"问题"的向量去匹配"文档"的向量
             问题短 → 向量信息量少 → 匹配精度低

    HyDE:     让 LLM 先"假装回答"这个问题 → 生成一个假设文档
             用"假设文档"的向量去匹配"真实文档"的向量
             文档 vs 文档 → 向量信息量相当 → 匹配精度高

    直觉理解:
      找书:       用户说"我想看一本关于…的书"
      传统检索:   用这句 10 个字的描述去匹配整本书 → 很难
      HyDE:       图书馆员根据描述脑补出一本书的样子
                 用脑补的书去找真书 → 更容易命中
    """
    prompt = f"""
请根据以下问题，写一段假设性的文档内容来回答它。
这段文档不需要是真实的，只需要看起来像"文档中的一个段落"。

风格要求:
- 使用客观陈述语气
- 包含具体的数据和事实（可以编造）
- 长度为 100-200 字
- 像是从技术文档/手册中摘录的

问题: {query}

假设文档:"""

    try:
        resp = httpx.post(
            f"{OLLAMA_URL}/api/chat",
            json={
                "model": LLM_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "stream": False,
                "options": {"temperature": 0.5},
            },
            timeout=30,
        )
        return resp.json()["message"]["content"]
    except Exception as e:
        print(f"  [WARN] HyDE 生成失败: {e}")
        return query


# ================================================================
# 第 2 部分：向量检索演示
# ================================================================

def embed_text(text: str) -> List[float]:
    """调用 BGE-M3 生成向量。"""
    resp = httpx.post(
        f"{OLLAMA_URL}/api/embed",
        json={"model": EMBED_MODEL, "input": text},
        timeout=30,
    )
    return resp.json()["embeddings"][0]


def cosine_similarity(a: List[float], b: List[float]) -> float:
    """计算余弦相似度。"""
    dot = sum(x*y for x, y in zip(a, b))
    na = sum(x*x for x in a) ** 0.5
    nb = sum(x*x for x in b) ** 0.5
    return dot / (na * nb) if na * nb > 0 else 0


def demo_documents() -> List[str]:
    """演示用的文档库。"""
    return [
        "本店智能音箱产品线包括：小爱触屏版(299元)、天猫精灵方糖(89元)、"
        "华为Sound(399元)，支持语音控制、音乐播放、智能家居联动等功能。",

        "笔记本电脑选购指南：办公本推荐联想ThinkBook(4999元起)、"
        "华为MateBook(5999元起)；游戏本推荐拯救者(7999元起)、"
        "ROG玩家国度(9999元起)。",

        "智能家居套装包含：智能音箱+智能灯+智能插座+门窗传感器，"
        "全套售价1599元。支持手机APP远程控制和语音控制。",

        "穿戴设备推荐：Apple Watch S9(2999元)支持心率/血氧/ECG监测，"
        "小米手环8(249元)支持心率/睡眠/计步，华为GT4(1499元)支持14天续航。",
    ]


if __name__ == "__main__":
    print("=" * 60)
    print("  查询改写演示: Multi-Query + HyDE")
    print("=" * 60)

    docs = demo_documents()
    doc_vecs = [embed_text(d) for d in docs]

    query = "智能家居设备多少钱"
    print(f"\n原始问题: \"{query}\"")

    # ---- 1. 传统单查询检索 ----
    print(f"\n{'─'*50}")
    print("[方法 1] 传统单查询检索")
    q_vec = embed_text(query)
    scores = [cosine_similarity(q_vec, dv) for dv in doc_vecs]
    for i, (s, d) in sorted(enumerate(zip(scores, docs)), key=lambda x: -x[1][0]):
        print(f"  得分 {s:.3f} | {d[:50]}...")

    # ---- 2. Multi-Query 多路查询 ----
    print(f"\n{'─'*50}")
    print("[方法 2] Multi-Query 多路查询改写")
    queries = generate_multi_queries(query, 4)
    print(f"  改写为 {len(queries)} 个查询:")
    for q in queries:
        print(f"    · {q}")

    # 取多路查询中最好的结果
    all_scores = []
    for q in queries:
        qv = embed_text(q)
        for di, dv in enumerate(doc_vecs):
            all_scores.append((cosine_similarity(qv, dv), di))

    all_scores.sort(key=lambda x: -x[0])
    seen = set()
    print("\n  Multi-Query 综合结果:")
    for score, di in all_scores:
        if di not in seen:
            seen.add(di)
            print(f"  得分 {score:.3f} | {docs[di][:50]}...")

    # ---- 3. HyDE 假设文档检索 ----
    print(f"\n{'─'*50}")
    print("[方法 3] HyDE 假设文档检索")
    hyde_doc = generate_hyde_document(query)
    print(f"  生成的假设文档 ({len(hyde_doc)} 字):")
    print(f"  \"{hyde_doc[:80]}...\"")
    hyde_vec = embed_text(hyde_doc)
    hyde_scores = [cosine_similarity(hyde_vec, dv) for dv in doc_vecs]
    for i, (s, d) in sorted(enumerate(zip(hyde_scores, docs)), key=lambda x: -x[1][0]):
        print(f"  得分 {s:.3f} | {d[:50]}...")

    # ---- 对比总结 ----
    print(f"\n{'='*50}")
    print("结论:")
    print("  方法1 (单查询): 问题短 → 向量信息少 → 匹配泛泛")
    print("  方法2 (多查询): 覆盖不同表述 → 命中更相关")
    print("  方法3 (HyDE):   假设文档和真实文档空间更接近")
    print(f"{'='*50}")
