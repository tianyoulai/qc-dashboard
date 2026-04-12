#!/bin/bash
# 质培运营看板 - 启动脚本
# 使用方法: bash start.sh
# 局域网其他设备通过 http://<本机IP>:8501 访问

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

VENV_PYTHON=".venv/bin/python"
PORT="${PORT:-8501}"
ADDRESS="${ADDRESS:-0.0.0.0}"

echo "============================================"
echo "  质培运营看板"
echo "  监听: $ADDRESS:$PORT"
echo "  局域网访问: http://$(ipconfig getifaddr en0 2>/dev/null || hostname -I | awk '{print $1}'):$PORT"
echo "============================================"

# 检查依赖
if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ 未找到 .venv，请先创建虚拟环境"
    exit 1
fi

# 启动 Streamlit
"$VENV_PYTHON" -m streamlit run app.py \
    --server.port "$PORT" \
    --server.address "$ADDRESS" \
    --server.headless true \
    --browser.gatherUsageStats false
