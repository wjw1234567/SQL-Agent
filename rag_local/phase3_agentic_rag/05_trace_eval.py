# ================================================================
# 追踪与评估 — LangSmith + 自定义指标
# ================================================================
# 一个完整的 AI Agent 系统需要三件事:
#   1. Tracing (追踪): 每次调用花了多久，调了什么工具
#   2. Evaluation (评估): 回答质量如何
#   3. Monitoring (监控): 运行时告警
#
# 这里用 Python 装饰器实现轻量级追踪。
# ================================================================
#
# 【运行方式】
#   python phase3_agentic_rag/05_trace_eval.py
# ================================================================

import time
import json
import httpx
import re
from functools import wraps
from datetime import datetime
from typing import List, Dict

OLLAMA_URL = "http://localhost:11434"
LLM_MODEL = "qwen3.5:35b-a3b"


class TraceCollector:
    """追踪信息收集器。"""

    def __init__(self):
        self.runs: List[Dict] = []
        self.current_run_id = 0

    def start_run(self, name: str, input_data: str) -> int:
        self.current_run_id += 1
        self.runs.append({
            "run_id": self.current_run_id, "name": name,
            "input": input_data[:100],
            "start_time": datetime.now().isoformat(),
            "duration_ms": None, "output": None, "status": "running",
        })
        return self.current_run_id

    def end_run(self, run_id: int, output: str, duration_ms: float):
        for run in self.runs:
            if run["run_id"] == run_id:
                run["output"] = output[:100]
                run["duration_ms"] = round(duration_ms, 1)
                run["status"] = "completed"
                break

    def print_report(self):
        print(f"\n{'='*60}")
        print("  追踪报告")
        print(f"{'='*60}")
        total = 0
        for run in self.runs:
            dt = run["duration_ms"] or 0
            total += dt
            s = "✓" if run["status"] == "completed" else "✗"
            print(f"  [{s}] {run['name']:25s} | {dt:>8.1f}ms | {run['input'][:40]}")
        print(f"  {'─'*60}")
        print(f"  [*] 总计: {len(self.runs)} 次调用, {total:.0f}ms")
        print(f"{'='*60}")

    def to_dict(self):
        return {"total_calls": len(self.runs), "total_duration_ms": sum(r["duration_ms"] or 0 for r in self.runs), "runs": self.runs}


trace = TraceCollector()


def traced(name: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            rid = trace.start_run(name, str(args[:2]))
            t0 = time.time()
            try:
                r = func(*args, **kwargs)
                trace.end_run(rid, str(r)[:100], (time.time() - t0) * 1000)
                return r
            except Exception as e:
                trace.end_run(rid, f"ERROR: {e}", (time.time() - t0) * 1000)
                raise
        return wrapper
    return decorator


@traced("知识检索")
def retrieve(query: str) -> str:
    time.sleep(0.3)
    return "智能音箱价格99-599元\n市场规模120亿元增长15%"


@traced("LLM调用")
def call_llm(prompt: str) -> str:
    time.sleep(0.5)
    resp = httpx.post(f"{OLLAMA_URL}/api/chat", json={"model": LLM_MODEL, "messages": [{"role": "user", "content": prompt}], "stream": False}, timeout=30)
    return resp.json()["message"]["content"]


@traced("质量评估")
def evaluate(query: str, answer: str) -> dict:
    prompt = f"评分1-5: 相关、完整、准确\n问题:{query}\n回答:{answer}\n返回JSON: {{'relevance':N, 'completeness':N, 'accuracy':N, 'overall':N}}"
    try:
        resp = httpx.post(f"{OLLAMA_URL}/api/chat", json={"model": LLM_MODEL, "messages": [{"role": "user", "content": prompt}], "stream": False}, timeout=30)
        m = re.search(r'\{.*\}', resp.json()["message"]["content"], re.DOTALL)
        return json.loads(m.group()) if m else {"overall": 3}
    except: return {"overall": 3}


def rag(query: str):
    print(f"\n处理: \"{query}\"")
    ctx = retrieve(query)
    ans = call_llm(f"基于以下内容回答问题:\n{ctx}\n\n问题: {query}")
    sc = evaluate(query, ans)
    print(f"回答: {ans[:60]}...")
    print(f"评分: 综合{sc.get('overall',0)}/5")
    return ans


if __name__ == "__main__":
    print("=" * 60)
    print("  AI Agent 追踪与评估")
    print("=" * 60)
    for q in ["智能音箱有什么功能？", "智能音箱市场规模？"]:
        rag(q)
    trace.print_report()
