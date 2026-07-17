# ================================================================
# 重排序（Rerank）— Cross-Encoder 精排
# ================================================================
# 为什么需要 Rerank？
#
#   BGE-M3 等 Embedding 模型（Bi-Encoder）:
#     问题 → 向量, 文档 → 向量, 然后计算距离
#     速度快（一次推理处理所有文档），但精度有限
#
#   Reranker 模型（Cross-Encoder）:
#     把"问题 + 文档"拼接起来直接输入模型
#     模型能看到问题和文档之间的交互（attention）
#     速度慢（每对都要单独推理），但精度高
#
# 典型策略 = Bi-Encoder 初筛（快）→ Cross-Encoder 精排（准）
#   Top 100 → BGE-Reranker → Top 5
#   相比只做 Bi-Encoder Top 5，召回率提升 10-20%
# ================================================================
#
# 【运行方式】
#   python phase2_advanced_rag/04_reranker.py
#
# 【前置条件】
#   pip install FlagEmbedding
#   首次运行会自动下载 BAAI/bge-reranker-v2-m3 (约 1.2GB)
# ================================================================

import time
from typing import List

# ================================================================
# BGE-Reranker 加载
# ================================================================
# FlagEmbedding 是 BAAI 官方的 Embedding/Reranker 库。
# bge-reranker-v2-m3: 轻量级 Cross-Encoder，支持多语言，约 1.2GB
#
# 第一次运行会自动从 HuggingFace 下载模型，
# 下载后缓存在 C:\Users\<用户名>\.cache\huggingface\hub\

print("[加载 BGE-Reranker 模型...]")
print("  (首次运行需下载 ~1.2GB，请耐心等待)")
from FlagEmbedding import FlagReranker
reranker = FlagReranker(
    'BAAI/bge-reranker-v2-m3',
    use_fp16=True,  # 半精度推理，省一半显存
)
print("[OK] Reranker 加载完成")


# ================================================================
# 演示数据
# ================================================================

QUERY = "智能音箱怎么连接WiFi"

DOCUMENTS = [
    "笔记本电脑开机后进入系统桌面，点击右下角网络图标选择WiFi连接",
    "智能音箱首次使用需要下载APP，通过APP引导完成WiFi配置和网络连接",
    "手机蓝牙连接耳机：打开设置→蓝牙→搜索设备→点击配对",
    "智能音箱支持2.4G和5G双频WiFi，建议使用2.4G信号更稳定",
    "智能家居设备连接教程：网关配对→添加子设备→配置自动化场景",
    "平板电脑可以通过HDMI线连接电视，实现屏幕镜像功能",
    "智能音箱的语音助手可以查询天气、播放音乐、控制家居设备",
    "WiFi路由器如果信号不稳定，可以尝试切换信道或重启路由器",
]


# ================================================================
# 向量检索模拟（Bi-Encoder 初筛）
# ================================================================

def bi_encoder_search(query: str, docs: List[str], top_k: int = 5) -> List[str]:
    """
    模拟 Bi-Encoder 初筛（实际会调用 BGE-M3，这里用关键词匹配模拟）。
    目的是演示 Rerank 对比，不是重复 Hybrid Search。
    """
    # 简化模拟：用关键词匹配
    keywords = query.lower().split()
    scored = []
    for doc in docs:
        score = sum(1 for kw in keywords if kw in doc.lower())
        scored.append((score, doc))
    scored.sort(key=lambda x: -x[0])
    return [doc for _, doc in scored[:top_k]]


def rerank_results(query: str, candidates: List[str]) -> List[tuple]:
    """
    BGE-Reranker 精排。

    reranker.compute_score() 接受 (query, doc) 对，
    返回相关性分数（0-1，越高越相关）。

    和 Bi-Encoder 的区别：
      Bi-Encoder:  "智能音箱" → 向量A, "说明书第3页" → 向量B, cos(A,B) → 0.85
      Cross-Encoder: 输入 [CLS] 智能音箱 [SEP] 说明书第3页 [SEP] → 直接打分 → 0.92

    Cross-Encoder 的 attention 能看到问题和文档每个词之间的交互关系，
    所以精度更高，但不能预计算文档向量（每对都要重新过模型）。
    """
    pairs = [(query, doc) for doc in candidates]
    scores = reranker.compute_score(pairs)

    # scores 可能是单个 float 或列表
    if isinstance(scores, (int, float)):
        scores = [scores]

    # 按分数从高到低排序
    result = list(zip(scores, candidates))
    result.sort(key=lambda x: -x[0])
    return result


def print_result(method: str, results: List[tuple]):
    """格式化输出结果。"""
    print(f"\n{'─'*40}")
    print(f"[{method}]")
    for i, (score, doc) in enumerate(results, 1):
        bar = "█" * int(score * 20) + "░" * (20 - int(score * 20))
        print(f"  #{i} {bar} {score:.3f}")
        print(f"     {doc[:50]}...")


if __name__ == "__main__":
    print("=" * 50)
    print("  BGE-Reranker 精排演示")
    print("=" * 50)
    print(f"\n查询: \"{QUERY}\"")

    # ---- 1. 初筛 ----
    t0 = time.time()
    candidates = bi_encoder_search(QUERY, DOCUMENTS, top_k=5)
    t1 = time.time()
    print(f"\n[初筛] 从 {len(DOCUMENTS)} 篇中选出 {len(candidates)} 篇 (耗时 {t1-t0:.3f}s)")
    for i, doc in enumerate(candidates, 1):
        print(f"  #{i} {doc}")

    # ---- 2. Rerank 精排 ----
    t2 = time.time()
    reranked = rerank_results(QUERY, candidates)
    t3 = time.time()
    print(f"\n[Rerank] 对 {len(candidates)} 个候选精排 (耗时 {t3-t2:.3f}s)")

    print_result("重排序结果", reranked)

    # ---- 3. 对比分析 ----
    print(f"\n{'='*50}")
    print("分析:")
    best_before = candidates[0] if candidates else "N/A"
    best_after = reranked[0][1] if reranked else "N/A"
    print(f"  初筛 Top1: {best_before}")
    print(f"  精排 Top1: {best_after}")
    print(f"  结论: Rerank 将最相关的文档提升到首位，")
    print(f"         不相关的被降权或移出 TopK")
    print(f"{'='*50}")
