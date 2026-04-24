#!/bin/bash
# ============================================================
# QC Dashboard — 定时刷新管理工具
# ============================================================
# 用法:
#   bash scripts/refresh_manager.sh install    # 安装定时任务（每天 18:25）
#   bash scripts/refresh_manager.sh uninstall  # 卸载定时任务
#   bash scripts/refresh_manager.sh status     # 查看定时任务状态
#   bash scripts/refresh_manager.sh run        # 手动执行一次刷新
#   bash scripts/refresh_manager.sh dry-run    # 检查模式（不实际执行）
#   bash scripts/refresh_manager.sh log        # 查看最近日志
# ============================================================

set -e
cd "$(dirname "$0")/.."
PROJECT_ROOT="$(pwd)"
PLIST_SRC="scripts/com.qc-dashboard.auto-refresh.plist"
LAUNCHD_PLIST="$HOME/Library/LaunchAgents/com.qc-dashboard.auto-refresh.plist"
LOG_DIR="data/logs"
PYTHON_CMD="${PYTHON_CMD:-python3}"

echo "============================================="
echo "  📋 QC Dashboard 定时刷新管理"
echo "  项目目录: $PROJECT_ROOT"
echo "============================================="

case "${1:-help}" in
    install)
        echo ""
        echo "🔧 安装定时任务..."
        
        # 确保 log 目录存在
        mkdir -p "$LOG_DIR"
        
        # 复制 plist 到 LaunchAgents
        cp "$PLIST_SRC" "$LAUNCHD_PLIST"
        
        # 加载 launchd 服务
        launchctl load "$LAUNCHD_PLIST" 2>/dev/null || {
            # 如果已经加载过，先卸载再加载
            launchctl unload "$LAUNCHD_PLIST" 2>/dev/null || true
            sleep 1
            launchctl load "$LAUNCHD_PLIST"
        }
        
        echo "✅ 定时任务已安装！"
        echo "   ⏰ 执行时间: 每天 18:25"
        echo "   📝 日志位置: $PROJECT_ROOT/$LOG_DIR/launchd-auto-refresh.*.log"
        echo ""
        echo "💡 提示:"
        echo "   - 如果电脑在 18:25 关机，开机后会自动补偿执行"
        echo "   - 周末没开电脑的话，周一开电脑后自动补拉缺失日期的数据"
        echo ""
        echo "📌 管理命令:"
        echo "   bash scripts/refresh_manager.sh status   # 查看状态"
        echo "   bash scripts/refresh_manager.sh run      # 手动执行"
        echo "   bash scripts/refresh_manager.sh log      # 查看日志"
        ;;

    uninstall)
        echo ""
        echo "🗑️ 卸载定时任务..."
        if [ -f "$LAUNCHD_PLIST" ]; then
            launchctl unload "$LAUNCHD_PLIST" 2>/dev/null || true
            rm -f "$LAUNCHD_PLIST"
            echo "✅ 已卸载并删除配置文件"
        else
            echo "⚠️ 未找到已安装的定时任务"
        fi
        ;;

    status)
        echo ""
        echo "📊 定时任务状态:"
        echo ""
        if launchctl list | grep -q "com.qc-dashboard"; then
            echo "   ✅ 已运行 (Loaded)"
            echo "   📄 配置: $LAUNCHD_PLIST"
            launchctl list | grep "qc-dashboard"
        else
            echo "   ⏹️ 未运行 / 未安装"
            if [ -f "$LAUNCHD_PLIST" ]; then
                echo "   📄 配置文件存在但未加载: $LAUNCHD_PLIST"
                echo "   💡 运行 'bash scripts/refresh_manager.sh install' 加载"
            fi
        fi
        
        echo ""
        echo "📅 数据库最新日期:"
        $PYTHON_CMD -c "
import sys
sys.path.insert(0, 'src')
from collector import get_db_connection
from pathlib import Path
db = Path('data/metrics.db')
if db.exists():
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute(\"SELECT MAX(date), COUNT(DISTINCT date) FROM daily_metrics\")
        row = c.fetchone()
        print(f'   最新日期: {row[0] or \"无数据\"}')
        print(f'   总记录数: {row[1] or 0} 天')
        conn.close()
    except Exception as e:
        print(f'   ⚠️ 数据库连接失败（可能需要设置 QC_DB_PASSWORD）: {e}')
else:
    print('   ❌ 数据库不存在')
" 2>/dev/null
        
        # 最近运行状态
        STATUS_FILE="data/logs/last_run.json"
        if [ -f "$STATUS_FILE" ]; then
            echo ""
            echo "🕐 最近一次运行:"
            cat "$STATUS_FILE" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(f\"   时间: {d.get('timestamp', '?')}\")
print(f\"   结果: {'✅ 成功' if d.get('success') else '❌ 失败'}\")
print(f\"   新增记录: {d.get('imported', 0)} 条\")
print(f\"   缺口天数: {d.get('gap_days', 0)} 天\")
" 2>/dev/null
        fi
        ;;

    run|exec)
        echo ""
        echo "▶️ 手动执行刷新..."
        $PYTHON_CMD scripts/auto_refresh.py
        exit_code=$?
        if [ $exit_code -eq 0 ]; then
            echo "✅ 刷新成功！"
        else
            echo "❌ 刷新失败 (exit code: $exit_code)"
        fi
        exit $exit_code
        ;;

    dry-run|--dry-run|-n)
        echo ""
        echo "🔍 DRY-RUN 模式（只检查不执行）..."
        $PYTHON_CMD scripts/auto_refresh.py --dry-run
        ;;

    log|logs)
        echo ""
        echo "📜 最近日志:"
        echo ""
        
        # 今天的日志
        TODAY_LOG="data/logs/refresh_$(date +%Y%m%d).log"
        if [ -f "$TODAY_LOG" ]; then
            echo "=== 今日日志 ($(date +%Y-%m-%d)) ==="
            tail -30 "$TODAY_LOG"
        else
            echo "今日暂无日志"
        fi
        
        echo ""
        # launchd 日志
        for f in data/logs/launchd-auto-refresh.stderr.log; do
            if [ -f "$f" ] && [ -s "$f" ]; then
                echo "=== launchd 错误日志 (最后20行) ==="
                tail -20 "$f"
            fi
        done
        
        echo ""
        echo "📂 所有日志文件:"
        ls -la data/logs/*.log 2>/dev/null | tail -5 || echo "  （无日志文件）"
        ;;

    help|*)
        echo ""
        echo "用法: bash $0 <命令>"
        echo ""
        echo "命令:"
        echo "  install      安装定时任务（每天 18:25 自动执行）"
        echo "  uninstall    卸载定时任务"
        echo "  status       查看定时任务状态和数据情况"
        echo "  run          手动执行一次刷新"
        echo "  dry-run      检查模式（只检查数据缺口，不实际拉取）"
        echo "  log          查看最近日志"
        echo "  help         显示此帮助"
        echo ""
esac
