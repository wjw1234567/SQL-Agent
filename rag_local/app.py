# ================================================================
#  本地 RAG 智能问答系统 — 主程序
#  版本: v1.0
#  功能: 读取文件夹中的文档，构建知识库，通过自然语言问答
# ================================================================
#
#  ╔══════════════════════════════════════════════════════════╗
#  ║  学习路线：按 SECTION 编号从上往下读                      ║
#  ║  Section 1: 配置区 —— 先看有哪些参数可以调                ║
#  ║  Section 2: Ollama API 调用层 —— 理解 Ollama 怎么用     ║
#  ║  Section 3: 文档解析 —— 各格式怎么转成纯文本             ║
#  ║  Section 4: 文本分块 —— 为什么切块，怎么切               ║
#  ║  Section 5: 向量存储与检索 —— ChromaDB 怎么用            ║
#  ║  Section 6: RAG 问答引擎 —— 检索+生成怎么串联           ║
#  ║  Section 7: FastAPI Web 服务 —— HTTP 接口怎么暴露        ║
#  ║  入口（末尾）—— 程序从哪里开始                           ║
#  ╚══════════════════════════════════════════════════════════╝
#
#  前置知识:
#   - 你已经在 Ollama 中部署了 Qwen3.5-35B-A3B 和 BGE-M3
#   - Ollama API 在 http://localhost:11434
#   - 文档放在 src/watch_folder/ 目录下
# ================================================================

import os
import json
import glob
from typing import List

# ================================================================
# Web 框架相关
# ================================================================
from fastapi import FastAPI, HTTPException
from fastapi.responses import (
    HTMLResponse,
    StreamingResponse,
    JSONResponse,
)
from fastapi.staticfiles import StaticFiles

# ================================================================
# HTTP 客户端 — 用来调用 Ollama API
# ================================================================
# httpx 比 Python 自带的 requests 更现代，支持异步（async）。
# Ollama API 基于 HTTP，所以我们用 httpx 发送请求。
#
# Ollama API 地址：http://localhost:11434
#   /api/embed  -> 文本转向量 (BGE-M3)
#   /api/chat   -> 对话生成 (Qwen)
import httpx

# ================================================================
# 文档解析 — LangChain 的文档加载器
# ================================================================
# LangChain 的文档加载器是一个"统一接口"：
#   不管输入是 PDF、DOCX 还是 TXT，
#   都输出 List[Document(page_content="...", metadata={...})]
#
# 这样我们后续的代码就不需要关心"原始文件是什么格式"了，
# 只需要处理 Document 对象。
from langchain_community.document_loaders import (
    PyMuPDFLoader,     # PDF 加载器（基于 PyMuPDF/fitz）
    CSVLoader,         # CSV 加载器
    UnstructuredMarkdownLoader,  # MD 加载器
    TextLoader,        # TXT 加载器
)
from langchain_community.document_loaders import UnstructuredWordDocumentLoader

# ================================================================
# 文本分割 — 把长文档切成小块
# ================================================================
# RecursiveCharacterTextSplitter 是"递归字符文本分割器"。
# 它会递归地尝试用不同分隔符（\n\n → \n → 句号 → 逗号 → 字符）
# 切分文本，尽量保持段落/句子的完整性。
from langchain_text_splitters import RecursiveCharacterTextSplitter

# ================================================================
# ================================================================
#  ╔════════════════════════════════════════════════════════╗
#  ║  SECTION 1: 配 置 区                                  ║
#  ║  所有可调参数集中在这里，方便修改和调试                   ║
#  ╚════════════════════════════════════════════════════════╝
# ================================================================
# ================================================================

# --- Ollama 配置 ---
OLLAMA_BASE_URL = "http://localhost:11434"   # Ollama 服务地址
EMBED_MODEL = "bge-m3"                       # 嵌入模型（文本→向量）
LLM_MODEL = "qwen3.5:35b-a3b"               # 问答模型（LLM）

# --- 文档目录 ---
# 把需要查询的文档放在这个目录下
WATCH_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "src", "watch_folder"
)

# --- ChromaDB 持久化目录 ---
# 向量数据库存在这里，重启后不丢失
CHROMA_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "chroma_db"
)

# --- 文本分块参数 ---
# 为什么需要这些参数？
# LLM 有上下文窗口限制（Qwen 约 128K tokens），但长文档可能有几十万字。
# 我们把文档切成小块，只检索最相关的几块发给 LLM。
CHUNK_SIZE = 500      # 每块最大字符数（大约 150-200 个中文字）
CHUNK_OVERLAP = 100   # 块之间重叠字符数（防止关键信息被切散）

# --- 检索参数 ---
TOP_K = 4             # 每次检索返回多少个相关块

# --- 系统提示词（System Prompt）---
# 这个提示词会在每次问答时发给 LLM，
# 告诉 LLM 它的角色和行为方式。
SYSTEM_PROMPT = """你是一个基于本地知识库的智能问答助手。

你的任务是根据提供的参考文档内容回答用户问题。

回答规则：
1. 严格基于参考内容回答，不要编造信息
2. 如果参考内容不足以回答问题，如实说"文档中没有相关记载"
3. 引用参考内容中的具体信息来支撑你的回答
4. 使用中文回答
5. 保持回答简洁、准确"""

# ================================================================
# ================================================================
#  ╔════════════════════════════════════════════════════════╗
#  ║  SECTION 2: Ollama API 调用层                          ║
#  ║  封装与 Ollama 的 HTTP 通信                            ║
#  ╚════════════════════════════════════════════════════════╝
# ================================================================
# ================================================================


class OllamaClient:
    """
    Ollama HTTP API 的 Python 封装。

    为什么需要这个类？
    Ollama 提供的是 HTTP API，我们可以直接用 httpx 发送请求。
    但这个类把"调用 API"的逻辑封装起来，让其他代码只需要调用
    embed_text() 或 chat_stream()，不用关心 HTTP 细节。

    Ollama API 文档参考:
      https://github.com/ollama/ollama/blob/main/docs/api.md
    """

    def __init__(self, base_url: str = OLLAMA_BASE_URL):
        """初始化客户端，指定 Ollama 服务地址"""
        self.base_url = base_url.rstrip("/")

    # -----------------------------------------------------------
    #  2.1 检查模型是否可用
    # -----------------------------------------------------------
    def check_model(self, model_name: str) -> bool:
        """
        检查指定模型是否已被 Ollama 下载。

        原理：
        GET /api/tags 返回已下载的模型列表。
        我们在初始化时用这个函数验证用户是否已经 pull 了模型。

        参数:
            model_name: 模型名称（如 "bge-m3"）

        返回:
            True: 模型存在，可以调用
            False: 模型不存在，需要 ollama pull
        """
        try:
            resp = httpx.get(f"{self.base_url}/api/tags", timeout=5)
            if resp.status_code == 200:
                models = resp.json().get("models", [])
                # 检查模型名是否在列表中
                # 注意：Ollama 返回的模型名可能是 "bge-m3:latest" 格式
                return any(
                    model_name in m.get("name", "")
                    for m in models
                )
            return False
        except Exception:
            return False

    # -----------------------------------------------------------
    #  2.2 文本 → 向量（Embedding）
    # -----------------------------------------------------------
    def embed_text(self, text: str) -> List[float]:
        """
        将一段文本转为向量（embedding）。

        这是 RAG 中最关键的步骤之一。
        BGE-M3 模型将文本映射到一个高维空间（通常 1024 维），
        语义相似的文本在这个空间中距离也近。

        API 调用:
          POST /api/embed
          {
            "model": "bge-m3",
            "input": "要转成向量的文本"
          }
          返回: {"embeddings": [[0.123, -0.456, ...]]}

        参数:
            text: 要向量化的文本

        返回:
            一个浮点数列表，表示文本的向量
        """
        url = f"{self.base_url}/api/embed"
        payload = {"model": EMBED_MODEL, "input": text}

        try:
            resp = httpx.post(url, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            # Ollama 返回的 embeddings 是二维数组
            # 外层对应多个输入，内层是向量
            return data["embeddings"][0]
        except Exception as e:
            print(f"  [ERROR] Embedding 失败: {e}")
            # 返回零向量（长度为 1024，BGE-M3 的默认维度）
            # 零向量虽然不能正确匹配，但至少不会让程序崩溃
            return [0.0] * 1024

    # -----------------------------------------------------------
    #  2.3 流式对话（Qwen 生成回答）
    # -----------------------------------------------------------
    def chat_stream(self, messages: List[dict]):
        """
        流式调用 Qwen 生成回答。

        【什么是流式？】
        普通的 API 调用要等 LLM 生成完整回答后才返回，
        大段文字可能需要等 5-10 秒。
        流式（streaming）则是 LLM 生成一个字就返回一个字，
        用户可以实时看到 AI 打字的效果，体验更好。

        API 调用:
          POST /api/chat
          {
            "model": "qwen3.5:35b-a3b",
            "messages": [{"role": "user", "content": "你好"}],
            "stream": true
          }
          返回: SSE (Server-Sent Events) 流

        参数:
            messages: OpenAI 格式的消息列表
              [
                {"role": "system", "content": "系统提示词"},
                {"role": "user",   "content": "用户问题"}
              ]

        生成器(yield):
            每次 yield 一个文本片段（如 "我"、"是"、"Q"、"wen"）
        """
        url = f"{self.base_url}/api/chat"
        payload = {
            "model": LLM_MODEL,
            "messages": messages,
            "stream": True,          # 启用流式输出
            "options": {
                "temperature": 0.7,   # 生成随机性 0-2，越大越有创意
                "top_p": 0.9,         # 核采样参数
            }
        }

        try:
            # stream=True 表示 httpx 也使用流式传输
            # 这样我们可以在 Ollama 生成的过程中逐行读取
            with httpx.stream("POST", url, json=payload, timeout=120) as resp:
                # Ollama 流式 API 使用 JSON Lines 格式
                # 每行是一个 JSON 对象，包含当前生成的内容
                for line in resp.iter_lines():
                    if line:
                        try:
                            data = json.loads(line)
                            content = data.get("message", {}).get("content", "")
                            if content:
                                yield content
                            # done=True 表示生成完毕
                            if data.get("done"):
                                break
                        except json.JSONDecodeError:
                            continue
        except Exception as e:
            yield f"\n\n[ERROR] 调用 Qwen 失败: {str(e)}"


# ================================================================
# ================================================================
#  ╔════════════════════════════════════════════════════════╗
#  ║  SECTION 3: 文档解析                                    ║
#  ║  把各种格式的文件变成统一的文本内容                       ║
#  ╚════════════════════════════════════════════════════════╝
# ================================================================
# ================================================================

# -----------------------------------------------------------
#  3.1 LangChain 文档加载器（自动根据扩展名选择解析方式）
# -----------------------------------------------------------
#
# 【什么是 Document 对象？】
# LangChain 定义了一个统一的数据结构：
#   Document {
#       page_content: str,     ← 文档的文本内容
#       metadata: dict         ← 文档的元信息（来源、页码等）
#   }
#
# 不管源文件是什么格式，解析后都变成 Document 对象列表。
# 后续代码只处理 Document，不需要关心原始格式。
#


def load_file(file_path: str) -> List:
    """
    根据文件扩展名选择对应的解析器加载文档。

    解析策略：
      .pdf  → PyMuPDFLoader  (基于 PyMuPDF/fitz 库)
      .docx → UnstructuredWordDocumentLoader (基于 python-docx)
      .xlsx → 自定义解析 (用 openpyxl 逐行读取)
      .csv  → CSVLoader
      .md   → UnstructuredMarkdownLoader
      .txt  → TextLoader
      其他   → 尝试用 TextLoader 读取

    为什么 PDF 用 PyMuPDFLoader 而不是别的？
    PyMuPDF（fitz）是目前最快的 PDF 解析库之一，
    而且对中文支持好，提取效果比 pdfminer 和 pdfplumber 好。

    参数:
        file_path: 文件完整路径

    返回:
        List[Document] — 可能返回多个 Document（多页 PDF）
    """
    ext = os.path.splitext(file_path)[1].lower()

    try:
        if ext == ".pdf":
            # PyMuPDFLoader: 自动将 PDF 每页转为 1 个 Document
            loader = PyMuPDFLoader(file_path)
            docs = loader.load()
            print(f"  [OK] PDF 解析完成: {os.path.basename(file_path)} ({len(docs)} 页)")

        elif ext == ".docx":
            # 注意: 某些 Windows 系统上 UnstructuredWordDocumentLoader
            # 需要安装 libreoffice。如果报错，下面有纯 python-docx 替代方案。
            try:
                loader = UnstructuredWordDocumentLoader(file_path)
                docs = loader.load()
            except Exception:
                docs = _parse_docx_fallback(file_path)
            print(f"  [OK] DOCX 解析完成: {os.path.basename(file_path)} ({len(docs)} 块)")

        elif ext == ".xlsx":
            docs = _parse_xlsx(file_path)
            print(f"  [OK] XLSX 解析完成: {os.path.basename(file_path)} ({len(docs)} 行)")

        elif ext == ".csv":
            loader = CSVLoader(file_path)
            docs = loader.load()
            print(f"  [OK] CSV 解析完成: {os.path.basename(file_path)} ({len(docs)} 行)")

        elif ext == ".md":
            loader = UnstructuredMarkdownLoader(file_path)
            docs = loader.load()
            print(f"  [OK] MD 解析完成: {os.path.basename(file_path)}")

        elif ext == ".txt":
            loader = TextLoader(file_path, encoding="utf-8")
            docs = loader.load()
            print(f"  [OK] TXT 解析完成: {os.path.basename(file_path)}")

        else:
            # 未知格式：尝试按文本读取
            try:
                loader = TextLoader(file_path, encoding="utf-8")
                docs = loader.load()
                print(f"  [OK] 作为文本读取: {os.path.basename(file_path)}")
            except Exception:
                print(f"  [SKIP] 不支持格式: {os.path.basename(file_path)}")
                return []

    except Exception as e:
        print(f"  [ERROR] 解析失败 {os.path.basename(file_path)}: {e}")
        return []

    # 为每个 Document 添加来源信息（文件名）
    for doc in docs:
        doc.metadata["source"] = os.path.basename(file_path)

    return docs


# -----------------------------------------------------------
#  3.2 DOCX 解析备用方案（不用 LangChain，直接 python-docx）
# -----------------------------------------------------------
#
# 如果 UnstructuredWordDocumentLoader 无法工作（比如缺少依赖），
# 这个备用函数直接用 python-docx 库手动解析 DOCX 文件。
#

def _parse_docx_fallback(file_path: str) -> List:
    """
    使用 python-docx 直接解析 Word 文档。

    python-docx 的工作原理：
      1. 打开 .docx 文件（本质上是个 ZIP 包）
      2. 遍历所有段落（Paragraph）对象
      3. 提取每段的文本内容
      4. 拼成完整的文档文本

    参数:
        file_path: .docx 文件路径

    返回:
        List[Document] — 一个 Document 包含整篇文档
    """
    from docx import Document as DocxDocument

    doc = DocxDocument(file_path)
    # 提取所有段落的文本
    paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
    full_text = "\n\n".join(paragraphs)

    # LangChain 的 Document 对象（用于统一接口）
    from langchain_core.documents import Document
    return [Document(page_content=full_text, metadata={"source": os.path.basename(file_path)})]


# -----------------------------------------------------------
#  3.3 XLSX 解析（用 openpyxl 逐行读取）
# -----------------------------------------------------------
#
# Excel 文件的特殊之处：
#   Excel 是表格式数据，每一行都是独立的记录。
# 我们不把整个表格当作文本串，而是每行作为一个独立的文档块。
#

def _parse_xlsx(file_path: str) -> List:
    """
    使用 openpyxl 解析 Excel 文件。

    openpyxl 的工作原理：
      1. 打开 .xlsx 文件（也是 ZIP 包）
      2. 遍历每个工作表（Sheet）
      3. 遍历每行，提取单元格内容
      4. 将每行格式化为"列名: 值, 列名: 值, ..."的文本

    参数:
        file_path: .xlsx 文件路径

    返回:
        List[Document] — 每行一个 Document
    """
    from openpyxl import load_workbook
    from langchain_core.documents import Document

    wb = load_workbook(file_path, read_only=True, data_only=True)
    docs = []

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]

        # 读取表头（第一行）
        headers = []
        for row in ws.iter_rows(max_row=1, values_only=True):
            headers = [str(c) if c is not None else "" for c in row]
            break

        # 逐行读取数据
        for row_idx, row in enumerate(ws.iter_rows(min_row=2, values_only=True), start=2):
            # 跳过全空行
            if all(c is None or str(c).strip() == "" for c in row):
                continue

            # 构建 "列名: 值" 格式的文本
            cells = []
            for i, val in enumerate(row):
                if i < len(headers) and val is not None:
                    cells.append(f"{headers[i]}: {val}")

            text = ", ".join(cells)
            docs.append(Document(
                page_content=text,
                metadata={
                    "source": os.path.basename(file_path),
                    "sheet": sheet_name,
                    "row": row_idx,
                }
            ))

    wb.close()
    return docs


# -----------------------------------------------------------
#  3.4 批量加载目录下所有文档
# -----------------------------------------------------------

def load_all_documents(watch_dir: str) -> List:
    """
    扫描 watch_folder 目录，加载所有支持的文档文件。

    支持的扩展名: .pdf, .docx, .xlsx, .csv, .md, .txt

    参数:
        watch_dir: 要扫描的目录路径

    返回:
        List[Document] — 所有文档合并后的 Document 列表

    处理流程:
        1. glob 扫描目录下所有文件
        2. 按扩展名过滤支持的文件类型
        3. 逐个调用 load_file() 解析
        4. 合并所有 Document 列表
    """
    # 如果目录不存在，自动创建
    os.makedirs(watch_dir, exist_ok=True)

    # 支持的扩展名（全小写）
    SUPPORTED_EXTS = {".pdf", ".docx", ".xlsx", ".csv", ".md", ".txt"}

    all_docs = []

    # glob 递归搜索所有文件
    files = glob.glob(os.path.join(watch_dir, "**", "*"), recursive=True)

    for file_path in files:
        if not os.path.isfile(file_path):
            continue

        ext = os.path.splitext(file_path)[1].lower()
        if ext not in SUPPORTED_EXTS:
            continue

        docs = load_file(file_path)
        all_docs.extend(docs)

    return all_docs


# ================================================================
# ================================================================
#  ╔════════════════════════════════════════════════════════╗
#  ║  SECTION 4: 文本切分器                                 ║
#  ║  把长文档切成适合搜索的小块                              ║
#  ╚════════════════════════════════════════════════════════╝
# ================================================================
# ================================================================
#
# 【为什么需要切分？】
# 假设你有一本 300 页的技术手册（约 50 万字）。
# 如果整本发给 Qwen，会超出上下文窗口，而且用户问的往往只是
# 其中一个小问题。检索整本书也不现实——你不能把 50 万字都
# 转成一个向量去匹配。
#
# 解决方案：
#   把文档切成 ~500 字的小块，每块转一个向量。
# 用户提问时，找最相似的 3-5 个块，只把这几个块发给 LLM。
#
# 【递归切分的原理】
# RecursiveCharacterTextSplitter 按以下优先级切分：
#   1. 先按 "\n\n"（段落分隔）切
#   2. 如果块还太大，按 "\n"（行分隔）切
#   3. 如果还太大，按 "。"（句号）切
#   4. 如果还太大，按 " "（空格）切
#   5. 最后按单个字符硬切
#
# 这样最大程度保持了语义完整性。


def create_text_splitter(chunk_size: int = CHUNK_SIZE,
                         chunk_overlap: int = CHUNK_OVERLAP):
    """
    创建文本切分器。

    参数:
        chunk_size: 每块的最大字符数
        chunk_overlap: 相邻块之间重叠的字符数

    重叠（Overlap）的作用：
        假设 chunk_size=500，overlap=100：
        块 1: [字符 0-599]
        块 2: [字符 500-1099]
        块 3: [字符 1000-1599]

        如果没有 overlap，一个关键信息刚好在 500 字处断开，
        可能两个块都包含不完整的信息。overlap 确保这种
        "边界信息"至少在一个块中是完整的。

    返回:
        RecursiveCharacterTextSplitter 实例
    """
    return RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        length_function=len,             # 使用字符数（而不是 token 数）计算长度
        separators=["\n\n", "\n", "。", "！", "？", "，", " ", ""],
    )


# ================================================================
# ================================================================
#  ╔════════════════════════════════════════════════════════╗
#  ║  SECTION 5: 向量存储与检索                               ║
#  ║  ChromaDB 操作：建索引、搜索、统计                       ║
#  ╚════════════════════════════════════════════════════════╝
# ================================================================
# ================================================================
#
# 【ChromaDB 是什么？】
# ChromaDB 是一个"向量数据库"。它专门存储"向量 + 原始文本"。
#
# 传统数据库（MySQL）:
#   存: {"title": "用户手册", "content": "第1章 安装..."}
#   查: SELECT * FROM docs WHERE title LIKE '%用户%'
#       → 精确匹配，无法理解语义
#
# 向量数据库（ChromaDB）:
#   存: {"向量": [0.12, -0.45, ...], "文本": "第1章 安装..."}
#   查: "如何安装软件？" → 转成向量 → 找最近的 5 个向量
#       → 语义匹配，能理解"安装"和"setup"是相近的意思


def build_vector_store(documents: List, chunk_size: int = CHUNK_SIZE,
                       chunk_overlap: int = CHUNK_OVERLAP):
    """
    构建向量数据库索引。

    这是 RAG 系统的"建库"阶段，运行一次后，
    下次启动不用重建（如果文档没变）。

    处理流程（理解这个流程 = 理解了 RAG 的核心）:
      原始文档                                   ← 用户放入 watch_folder
          ↓
      ┌──────────────────────────────────────┐
      │  1. 切分文本块                         │  ← SECTION 4
      │     "第1章 安装\n\n首先下载..." → 3 块  │
      └──────────────────────────────────────┘
          ↓
      ┌──────────────────────────────────────┐
      │  2. 每个块 → BGE-M3 → 向量            │  ← SECTION 2.2
      │     "第1章 安装..." → [0.12, -0.45, .] │
      │     "2.1 配置..."  → [0.33, -0.12, .] │
      │     "2.2 启动..."  → [0.28, -0.55, .] │
      └──────────────────────────────────────┘
          ↓
      ┌──────────────────────────────────────┐
      │  3. 向量 + 原文 存入 ChromaDB          │  ← 本节
      │     Collection("my_docs")              │
      │       ID  |  向量          |  文本      │
      │       a1  | [0.12, ...]   | "第1章..." │
      │       a2  | [0.33, ...]   | "2.1..."   │
      └──────────────────────────────────────┘

    参数:
        documents: Document 对象列表（来自 SECTION 3）
        chunk_size: 切分块大小
        chunk_overlap: 切分块重叠

    返回:
        chromadb.Collection — 向量集合，用于后续检索
    """
    import chromadb

    # ------ 第 1 步：创建 ChromaDB 客户端 ------
    #
    # ChromaDB 有两种模式：
    #   EphemeralClient(): 内存模式，重启数据丢失
    #   PersistentClient(path): 持久化模式，数据存磁盘
    #
    # 我们使用持久化模式，这样建好索引后重启系统无需重建。
    client = chromadb.PersistentClient(path=CHROMA_DIR)

    # ------ 第 2 步：创建/获取集合（Collection）------
    #
    # Collection 类似于 MySQL 中的"表"。
    # 同一个数据库中可以有多个 Collection（多套知识库）。
    collection_name = "my_knowledge_base"

    # 如果集合已存在，删除重建（确保索引是最新的）
    # 如果想增量添加（不删除旧的），这里可以改为 get_or_create
    try:
        client.delete_collection(collection_name)
    except Exception:
        pass  # 首次运行时集合还不存在，忽略删除错误

    collection = client.create_collection(
        name=collection_name,
        # metadata={"hnsw:space": "cosine"}  # 使用余弦相似度（默认）
    )

    # ------ 第 3 步：切分文档 ------
    splitter = create_text_splitter(chunk_size, chunk_overlap)
    chunks = splitter.split_documents(documents)

    if not chunks:
        print("  [WARN] 没有可索引的文档内容")
        return collection

    print(f"  [索引] 文档被切分为 {len(chunks)} 个文本块")

    # ------ 第 4 步：生成向量并存入 ChromaDB ------
    #
    # 这里逐块调用 BGE-M3 生成向量。
    # 对于大量文档，可以考虑批量 embedding 以提高性能，
    # 但为保持代码清晰易懂，我们逐块处理。
    #
    # ChromaDB 的 add() 方法需要三个参数：
    #   ids:      每个向量的唯一 ID（字符串）
    #   embeddings: 向量列表（二维浮点数数组）
    #   documents: 原始文本列表
    #   metadatas: 元信息列表（来源文件名等）

    ollama = OllamaClient()

    texts = [chunk.page_content for chunk in chunks]
    metadatas = [{
        "source": chunk.metadata.get("source", "unknown"),
    } for chunk in chunks]
    ids = [f"doc_{i}" for i in range(len(chunks))]

    # 分批处理，每批 5 个（减少 API 调用次数）
    batch_size = 5
    all_embeddings = []

    print(f"  [索引] 正在调用 BGE-M3 生成向量（共 {len(texts)} 块）...")

    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i:i + batch_size]
        for text in batch_texts:
            emb = ollama.embed_text(text)
            all_embeddings.append(emb)
        print(f"  [进度] {min(i+batch_size, len(texts))}/{len(texts)}")

    # 存入 ChromaDB
    collection.add(
        ids=ids,
        embeddings=all_embeddings,
        documents=texts,
        metadatas=metadatas,
    )

    print(f"  [OK] 索引完成！共 {len(chunks)} 个文本块已存入 ChromaDB")
    return collection


# -----------------------------------------------------------
#  5.1 搜索：问题 → 向量 → 检索相关块
# -----------------------------------------------------------

def search_similar(query: str, collection, top_k: int = TOP_K) -> List[dict]:
    """
    搜索与问题最相似的文本块。

    这就是 RAG 中的"检索"（Retrieval）步骤。

    处理流程：
        用户问题 → BGE-M3 向量化 → ChromaDB 相似度搜索 → 返回 TOP_K 块

    ChromaDB 的查询原理：
        1. 把问题向量和库中所有向量计算距离
        2. 按距离从小到大排序（距离越小越相似）
        3. 返回前 top_k 个

    参数:
        query: 用户的问题字符串
        collection: ChromaDB 集合
        top_k: 返回多少个最相似的块

    返回:
        [{"text": "文本内容", "source": "文件名", "score": 0.85}, ...]
        score 是余弦相似度，1.0 表示完全相同，0 表示无关
    """
    if collection.count() == 0:
        return []

    ollama = OllamaClient()

    # 第 1 步：把问题转成向量
    query_embedding = ollama.embed_text(query)

    # 第 2 步：在 ChromaDB 中搜索
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=top_k,
        include=["documents", "metadatas", "distances"],
    )

    # 第 3 步：整理结果
    # ChromaDB 返回的格式有点复杂：
    #   results["documents"]  = [["块1", "块2", ...]]
    #   results["metadatas"]  = [[{...}, {...}, ...]]
    #   results["distances"]  = [[0.15, 0.23, ...]]
    # 注意：distances 越小表示越相似（余弦距离 = 1 - 余弦相似度）
    formatted = []
    documents = results.get("documents", [[]])[0]
    metadatas = results.get("metadatas", [[]])[0]
    distances = results.get("distances", [[]])[0]

    for i in range(len(documents)):
        # 将距离转为相似度分数（0-1 范围，越高越好）
        similarity = 1.0 - distances[i] if i < len(distances) else 0.0
        formatted.append({
            "text": documents[i],
            "source": metadatas[i].get("source", "unknown") if i < len(metadatas) else "unknown",
            "score": round(similarity, 3),
        })

    return formatted


# -----------------------------------------------------------
#  5.2 获取索引统计
# -----------------------------------------------------------

def get_index_stats(collection) -> dict:
    """
    获取索引的统计信息。

    返回:
        {
            "total_chunks": 总文本块数,
            "sources": [文件1, 文件2, ...],
        }
    """
    if collection is None:
        return {"total_chunks": 0, "sources": []}

    count = collection.count()
    sources = set()

    try:
        # 获取所有唯一来源
        results = collection.get(include=["metadatas"])
        for meta in results.get("metadatas", []):
            src = meta.get("source", "unknown")
            if src:
                sources.add(src)
    except Exception:
        pass

    return {
        "total_chunks": count,
        "sources": sorted(list(sources)),
    }


# ================================================================
# ================================================================
#  ╔════════════════════════════════════════════════════════╗
#  ║  SECTION 6: RAG 问答引擎                                ║
#  ║  串联"检索"和"生成"两个步骤                              ║
#  ╚════════════════════════════════════════════════════════╝
# ================================================================
# ================================================================
#
# 【RAG 的核心逻辑】
#
# 用户提问 → 检索相关文档 → 构建 Prompt → LLM 生成回答
#
# 这一步之所以叫"问答引擎"，是因为它把前面所有的模块串联起来：
#   - OllamaClient (SECTION 2): 调 BGE-M3 和 Qwen
#   - 文档解析 (SECTION 3): 已经完成（在 build_index 中）
#   - ChromaDB (SECTION 5): 检索相似块
#
# 【Prompt 的结构】
# 发给 LLM 的消息由三部分组成：
#   1. System: 设定角色和行为规则（不可见）
#   2. Context: 从 ChromaDB 检索到的相关文档内容
#   3. User: 用户的问题
#
# System:
#   "你是一个基于本地知识库的智能问答助手..."
#
# Context:
#   "参考文档 1（来自 report.pdf）:
#    2024年第一季度营收为1.2亿元...
#
#    参考文档 2（来自 data.xlsx）:
#    销售总额: 1.2亿..."
#
# User:
#   "上季度公司营收多少？"
#
# LLM 基于 Context 和 User 生成回答，不会凭"记忆"编造。


def build_rag_prompt(query: str, context_chunks: List[dict]) -> List[dict]:
    """
    构建 RAG 的 Prompt 消息列表。

    这个函数体现了 RAG 的核心设计思想：
    把"检索到的资料"和"用户的问题"打包在一起，
    让 LLM 基于资料来回答。

    参数:
        query: 用户的问题
        context_chunks: search_similar 返回的相关文本块列表
          格式: [{"text": "...", "source": "...", "score": 0.85}, ...]

    返回:
        OpenAI 格式的消息列表：
        [
            {"role": "system", "content": "..."},   ← 系统提示词
            {"role": "user",   "content": "..."}    ← 上下文 + 问题
        ]
    """
    # 构建上下文文本
    if context_chunks:
        context_parts = []
        for i, chunk in enumerate(context_chunks, 1):
            context_parts.append(
                f"[参考文档 {i}（来自 {chunk['source']}）]:\n{chunk['text']}"
            )
        context_text = "\n\n" + "\n\n".join(context_parts)

        user_message = (
            f"请根据以下参考文档内容回答我的问题。\n"
            f"如果参考文档中没有相关内容，请如实说明。\n"
            f"{context_text}\n\n"
            f"我的问题是: {query}"
        )
    else:
        # 没有检索到相关内容
        user_message = (
            f"知识库中没有找到与问题相关的文档内容。\n"
            f"我的问题是: {query}\n"
            f"请如实告知知识库中没有相关信息。"
        )

    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_message},
    ]


# ================================================================
# ================================================================
#  ╔════════════════════════════════════════════════════════╗
#  ║  SECTION 7: FastAPI Web 服务                            ║
#  ║  提供 HTTP API 和 Web 聊天界面                          ║
#  ╚════════════════════════════════════════════════════════╝
# ================================================================
# ================================================================
#
# FastAPI 是一个 Python Web 框架。
# 我们用 FastAPI 来做两件事：
#   1. 提供 REST API（供程序调用）
#   2. 托管 Web 聊天界面（供人使用）


# ------ 创建 FastAPI 应用 ------
app = FastAPI(title="本地 RAG 问答系统")

# ------ 全局状态 ------
# 在内存中保存向量集合，避免每次请求都重新加载
_collection = None
_ollama = OllamaClient()


# -----------------------------------------------------------
#  7.1 启动事件：初始化索引
# -----------------------------------------------------------
#
# @app.on_event("startup") 是 FastAPI 的启动钩子。
# 服务器启动时自动执行 init_index()，扫描文档并建立索引。

@app.on_event("startup")
async def init_index():
    """
    系统启动时的初始化。

    执行顺序:
    1. 检查 Ollama 是否可用
    2. 检查两个模型是否已下载
    3. 扫描 watch_folder 并加载文档
    4. 构建向量索引
    """
    global _collection

    print("=" * 60)
    print("  本地 RAG 智能问答系统 v1.0")
    print("=" * 60)
    print(f"\n  [初始化] Ollama 地址: {OLLAMA_BASE_URL}")

    # 检查 Ollama 连接
    if not _ollama.check_model(EMBED_MODEL):
        print(f"  [ERROR] 模型 '{EMBED_MODEL}' 未找到！")
        print(f"  [ERROR] 请运行: ollama pull {EMBED_MODEL}")
        print("  [ERROR] 系统将使用降级模式（零向量）")
    else:
        print(f"  [OK] 嵌入模型: {EMBED_MODEL}")

    if not _ollama.check_model(LLM_MODEL):
        print(f"  [WARN] 模型 '{LLM_MODEL}' 未找到！")
        print(f"  [WARN] 请运行: ollama pull {LLM_MODEL}")
        print(f"  [WARN] 首次下载约需下载 ~10GB 文件")
    else:
        print(f"  [OK] 问答模型: {LLM_MODEL}")

    # 扫描文档
    print(f"\n  [索引] 扫描目录: {WATCH_DIR}")
    docs = load_all_documents(WATCH_DIR)

    if not docs:
        print(f"  [WARN] watch_folder 为空或没有支持的文档")
        print(f"  [WARN] 请将 PDF/DOCX/XLSX/CSV/MD/TXT 文件放入:")
        print(f"  [WARN]   {WATCH_DIR}")
        print(f"  [WARN] 或者重启系统后通过 API 添加")
    else:
        print(f"  [索引] 共加载 {len(docs)} 个文档片段")

        # 建立向量索引
        import chromadb
        _collection = build_vector_store(docs)
        stats = get_index_stats(_collection)
        print(f"  [OK] 索引完成: {stats['total_chunks']} 个文本块")
        for src in stats["sources"]:
            print(f"       └─ {src}")

    print("\n  [服务] 启动完成！浏览器打开: http://localhost:8000")
    print("=" * 60)


# -----------------------------------------------------------
#  7.2 GET / — 返回 Web 聊天界面
# -----------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def get_index():
    """返回聊天界面的 HTML。"""
    html_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "src", "static", "index.html"
    )
    try:
        with open(html_path, "r", encoding="utf-8") as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>index.html 未找到</h1><p>请确保 src/static/index.html 存在</p>")


# -----------------------------------------------------------
#  7.3 GET /stats — 查看系统状态
# -----------------------------------------------------------

@app.get("/stats")
async def get_stats():
    """返回系统状态和索引信息。"""
    stats = get_index_stats(_collection)
    return JSONResponse({
        "ollama_url": OLLAMA_BASE_URL,
        "embed_model": EMBED_MODEL,
        "llm_model": LLM_MODEL,
        "watch_dir": WATCH_DIR,
        "index": stats,
    })


# -----------------------------------------------------------
#  7.4 POST /ask — 问答（非流式）
# -----------------------------------------------------------

@app.post("/ask")
async def ask(query: dict):
    """
    问答接口（非流式）。

    请求体:
        {"query": "上季度营收多少？"}

    返回:
        {
            "answer": "根据文档记载，上季度营收为1.2亿元...",
            "sources": [{"text": "...", "source": "report.pdf", "score": 0.85}, ...]
        }
    """
    q = query.get("query", "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="query 不能为空")

    # 检索
    chunks = []
    if _collection:
        chunks = search_similar(q, _collection, TOP_K)

    # 构建 Prompt
    messages = build_rag_prompt(q, chunks)

    # 调用 LLM（非流式 - 完整返回）
    ollama = OllamaClient()
    full_response = ""
    for token in ollama.chat_stream(messages):
        full_response += token

    return JSONResponse({
        "answer": full_response,
        "sources": chunks,
    })


# -----------------------------------------------------------
#  7.5 GET /ask/stream — 问答（流式，SSE 协议）
# -----------------------------------------------------------
#
# 【什么是 SSE?】
# SSE = Server-Sent Events（服务器推送事件）。
# 它是一种让服务器主动向浏览器推送数据的技术。
#
# 在 RAG 中的用途：
#   LLM 是一个字一个字生成的。如果用非流式 API，
#   用户需要等 5-10 秒才能看到完整回答。
#   用 SSE，LLM 每生成一个字就推送给浏览器，
#   用户看到的就是"AI 正在打字"的效果。
#
# SSE 数据格式：
#   data: {"type": "token", "content": "我"}
#   data: {"type": "token", "content": "是"}
#   data: {"type": "token", "content": "Qwen"}
#   data: {"type": "sources", "content": [...]}   ← 最后发送来源
#   data: [DONE]                                   ← 结束标志


@app.get("/ask/stream")
async def ask_stream(query: str):
    """
    问答接口（流式，使用 SSE 协议）。

    请求:
        GET /ask/stream?query=上季度营收多少？

    返回（SSE 流）:
        data: {"type": "token", "content": "根据"}
        data: {"type": "token", "content": "文档"}
        data: {"type": "sources", "content": [{"text":"...", "source":"report.pdf", ...}]}
        data: [DONE]
    """
    if not query.strip():
        return JSONResponse({"error": "query 不能为空"}, status_code=400)

    async def event_generator():
        """异步生成 SSE 事件流。"""
        # 检索相关文档
        chunks = []
        if _collection:
            chunks = search_similar(query, _collection, TOP_K)

        # 构建 Prompt
        messages = build_rag_prompt(query, chunks)

        # 调用 Qwen 流式生成
        ollama = OllamaClient()
        for token in ollama.chat_stream(messages):
            yield f"data: {json.dumps({'type': 'token', 'content': token})}\n\n"

        # 发送来源信息（供前端展示）
        yield f"data: {json.dumps({'type': 'sources', 'content': chunks})}\n\n"

        # 结束标志
        yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


# -----------------------------------------------------------
#  7.6 POST /reindex — 手动重新建索引
# -----------------------------------------------------------

@app.post("/reindex")
async def reindex():
    """
    重新扫描文档目录并重建索引。

    当你往 watch_folder 中添加了新文件，调用此接口
    即可让系统识别新文档，无需重启服务。
    """
    global _collection

    docs = load_all_documents(WATCH_DIR)
    if not docs:
        return JSONResponse({"status": "error", "message": "没有找到文档"})

    _collection = build_vector_store(docs)
    stats = get_index_stats(_collection)

    return JSONResponse({
        "status": "ok",
        "message": f"索引重建完成，共 {stats['total_chunks']} 个文本块",
        "sources": stats["sources"],
    })


# ================================================================
# ================================================================
#  程 序 入 口
# ================================================================
# ================================================================

if __name__ == "__main__":
    """
    Python 程序的入口点。

    __name__ == "__main__" 的含义：
    当直接运行 `python app.py` 时，__name__ 的值是 "__main__"，
    所以会执行下面的代码块。

    当这个文件被其他文件 `import` 时（如 `import app`），
    __name__ 的值就不是 "__main__"了，不会执行启动逻辑。

    这就是 Python 标准的主函数入口模式。
    """
    import uvicorn

    print()
    print("  启动 FastAPI 服务器...")
    print()

    uvicorn.run(
        app,
        host="127.0.0.1",   # 只监听本机，安全
        port=8000,            # Web 服务端口
        log_level="info",     # 日志级别
    )
