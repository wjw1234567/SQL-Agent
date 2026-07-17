@echo off
chcp 65001 >nul
echo ============================================================
echo   本地 RAG 智能问答系统 — 一键启动
echo ============================================================
echo.

REM 检查虚拟环境是否存在
if exist .venv\Scripts\activate.bat (
    echo [*] 激活虚拟环境...
    call .venv\Scripts\activate
) else (
    echo [*] 未检测到虚拟环境，直接使用系统 Python
)

echo [*] 启动 RAG 系统...
echo [*] 浏览器打开: http://localhost:8000
echo.
python app.py

pause
