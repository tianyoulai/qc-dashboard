#!/bin/bash
# ============================================================
# 启动带远程调试端口的 Chrome（保留已有登录态）
# ============================================================
# 用法: sh start_chrome_debug.sh [port]
# 默认端口 9222
#
# 注意：如果 Chrome 已经在运行，会先关闭再重新启动。
# 所有标签页和窗口会在重启后恢复（Chrome 自动恢复）。
# ============================================================

PORT=${1:-9222}
CHROME="/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
PROFILE_DIR="$HOME/Library/Application Support/Google/Chrome"

echo "╔══════════════════════════════════════╗"
echo "║  🌐 启动 Chrome 远程调试模式        ║"
echo "║  端口: $PORT                            ║"
echo "╚══════════════════════════════════════╝"
echo ""

# 检查端口是否已被占用
if lsof -i :$PORT >/dev/null 2>&1; then
    echo "✅ 端口 $PORT 已在监听，Chrome 调试模式可能已启动"
    echo ""
    echo "当前连接信息："
    curl -s http://localhost:$PORT/json/version 2>/dev/null | python3 -m json.tool 2>/dev/null || echo "  无法获取版本信息"
    exit 0
fi

# 检查是否有 Chrome 进程在运行
if pgrep -f "Google Chrome" > /dev/null 2>&1; then
    echo "⚠️  Chrome 正在运行中..."
    echo "   需要先关闭 Chrome 以启用调试模式"
    echo "   （Chrome 会自动恢复所有标签页）"
    echo ""
    read -p "   是否关闭并重启 Chrome? (y/n) " -n 1 -r
    echo ""
    
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "已取消操作。如需手动开启调试模式，请运行："
        echo "  '$CHROME' --remote-debugging-port=$PORT &"
        exit 0
    fi
    
    echo ""
    echo "🔄 正在关闭 Chrome..."
    osascript -e 'quit app "Google Chrome"' 2>/dev/null
    
    # 等待进程退出（最多等15秒）
    for i in $(seq 1 15); do
        if ! pgrep -f "Google Chrome" > /dev/null 2>&1; then
            break
        fi
        sleep 1
    done
    
    # 如果还没退出，强制 kill 主进程
    if pgrep -f "Google Chrome" > /dev/null 2>&1; then
        echo "⚠️  正常关闭超时，强制结束..."
        pkill -f "Google Chrome" 2>/dev/null
        sleep 2
    fi
fi

# 启动带调试端口的 Chrome
echo "🚀 启动 Chrome (debug port=$PORT)..."
"$CHROME" \
    --remote-debugging-port="$PORT" \
    --user-data-dir="$PROFILE_DIR" \
    &>/dev/null &

# 等待端口就绪
for i in $(seq 1 20); do
    if lsof -i :$PORT >/dev/null 2>&1; then
        echo ""
        echo "✅ Chrome 已启动！调试信息："
        curl -s http://localhost:$PORT/json/version | python3 -m json.tool
        echo ""
        echo "现在可以运行:"
        echo "  python3 src/auto_fetch.py              # 下载全部队列"
        echo "  python3 src/auto_fetch.py --dry-run     # 测试模式"
        exit 0
    fi
    sleep 1
done

echo "❌ 启动失败或超时。请检查 Chrome 是否正常安装。"
exit 1
