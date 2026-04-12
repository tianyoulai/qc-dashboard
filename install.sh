#!/bin/bash
# ============================================================
# QC Dashboard — 一键安装脚本
# ============================================================
# 用法: sh install.sh
# 环境要求: macOS / Linux, Python 3.9+
# ============================================================

set -e

echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║     📊 质检数据统一看板 — 安装向导              ║"
echo "║     QC Dashboard Installer                     ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""

# ── 1. 检测 Python ──
PYTHON_CMD=""
for cmd in python3 python; do
    if command -v $cmd &>/dev/null; then
        ver=$($cmd --version 2>&1 | grep -oE '[0-9]+\.[0-9]+' | head -1)
        major=$(echo "$ver" | cut -d. -f1)
        minor=$(echo "$ver" | cut -d. -f2)
        if [ "$major" -ge 3 ] && [ "$minor" -ge 8 ]; then
            PYTHON_CMD=$cmd
            break
        fi
    fi
done

if [ -z "$PYTHON_CMD" ]; then
    echo "❌ 需要 Python 3.8+，但未找到。请先安装 Python:"
    echo "   macOS: brew install python"
    echo "   Ubuntu: sudo apt install python3"
    exit 1
fi
echo "✅ Python: $($PYTHON_CMD --version) ($PYTHON_CMD)"

# ── 2. 安装 Python 依赖 ──
echo ""
echo "📦 安装 Python 依赖..."
$PYTHON_CMD -m pip install pyyaml openpyxl websockets -q 2>/dev/null || {
    echo "⚠️ pip 安装失败，尝试使用 --user..."
    $PYTHON_CMD -m pip install --user pyyaml openpyxl websockets -q
}
echo "✅ 依赖安装完成"

# ── 3. 检查/安装 wecom-cli（可选）──
echo ""
echo "🔧 检查 wecom-cli (企业微信命令行工具)..."
WECOM_CLI=""
for p in ~/.local/bin/wecom-cli /usr/local/bin/wecom-cli $(which wecom-cli 2>/dev/null); do
    [ -f "$p" ] && WECOM_CLI="$p" && break
done

if [ -z "$WECOM_CLI" ]; then
    echo "⚠️ wecom-cli 未安装（可选）"
    echo "   如需使用 API 自动采集模式，请运行:"
    echo "   npm install -g @wecom/cli --prefix ~/.local"
    echo ""
    read -p "   是否现在安装 wecom-cli? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        npm install -g @wecom/cli --prefix ~/.local 2>/dev/null || \
        npx @wecom/cli --version &>/dev/null || \
        echo "   ⚠️ npm 安装失败，请手动安装"
    fi
else
    echo "✅ wecom-cli: $WECOM_CLI"
fi

# ── 4. 创建目录结构 ──
echo ""
echo "📁 创建数据目录..."
mkdir -p data/uploads/data/processed
mkdir -p data/backups
echo "✅ 目录就绪"

# ── 5. 初始化数据库 ──
echo ""
echo "🗄️ 初始化 SQLite 数据库..."
$PYTHON_CMD -c "
import sqlite3, os
db_path = 'data/metrics.db'
conn = sqlite3.connect(db_path)
c = conn.cursor()
c.execute('''CREATE TABLE IF NOT EXISTS daily_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id TEXT NOT NULL,
    date TEXT NOT NULL,
    metric_key TEXT NOT NULL,
    metric_value REAL,
    raw_data TEXT,
    source TEXT DEFAULT 'manual',
    created_at TEXT DEFAULT (datetime('now','localtime')),
    updated_at TEXT DEFAULT (datetime('now','localtime')),
    UNIQUE(queue_id, date, metric_key)
)''')
c.execute('CREATE INDEX IF NOT EXISTS idx_qd ON daily_metrics(queue_id, date)')
conn.commit()
print(f'  📦 数据库: {os.path.abspath(db_path)}')
conn.close()
"

# ── 6. 配置检查 ──
echo ""
echo "⚙️  检查配置文件..."
if [ ! -f config.yaml ]; then
    echo "⚠️ config.yaml 不存在"
    if [ -f config.yaml.example ]; then
        cp config.yaml.example config.yaml
        echo "  ✅ 已从模板生成 config.yaml，请编辑填写队列信息"
    else
        echo "  ❌ 缺少配置文件模板，请确认 config.yaml 存在"
    fi
else
    echo "✅ config.yaml 已存在"
fi

# ── 7. 设置 macOS 定时任务（可选）──
if [[ "$OSTYPE" == "darwin"* ]]; then
    echo ""
    echo "⏰ 设置定时刷新任务?"
    PLIST_FILE=~/Library/LaunchAgents/com.qc-dashboard.refresh.plist
    SCRIPT_DIR="$(cd "$(dirname "$0")"; pwd)"
    
    if [ -f "$PLIST_FILE" ]; then
        echo "  ℹ️ 定时任务已存在: $PLIST_FILE"
    else
        read -p "  是否设置每日自动刷新? (y/n) " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            cat > "$PLIST_FILE" << EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.org/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.qc-dashboard.refresh</string>
    <key>ProgramArguments</key>
    <array>
        <string>$PYTHON_CMD</string>
        <string>$SCRIPT_DIR/src/collector.py</string>
        <string>fetch</string>
    </array>
    <key>StartCalendarInterval</key>
    <dict>
        <key>Hour</key>
        <integer>9</integer>
        <key>Minute</key>
        <integer>0</integer>
    </dict>
    <key>WorkingDirectory</key>
    <string>$SCRIPT_DIR</string>
    <key>StandardOutPath</key>
    <string>$SCRIPT_DIR/data/qc-dashboard.log</string>
    <key>StandardErrorPath</key>
    <string>$SCRIPT_DIR/data/qc-dashboard.err.log</string>
</dict>
</plist>
EOF
            launchctl load "$PLIST_FILE" 2>/dev/null && echo "  ✅ 定时任务已设置 (每天 09:00)" || echo "  ⚠️ 需要手动执行: launchctl load ~/Library/LaunchAgents/com.qc-dashboard.refresh.plist"
        fi
    fi
elif [[ "$OSTYPE" == "linux"* ]]; then
    echo ""
    echo "💡 Linux 用户可手动添加 crontab: crontab -e → 添加 '0 9 * * * cd $(pwd) && python3 src/collector.py fetch'"
fi

# ── 8. 完成 ──
echo ""
echo "╔══════════════════════════════════════════════════╗"
echo "║           ✅ 安装完成！                         ║"
echo "╠══════════════════════════════════════════════════╣"
echo "║                                                ║"
echo "║  快速开始:                                      ║"
echo "║  1. 编辑 config.yaml — 填写你的文档信息         ║"
echo "║  2. 导入 Excel: python3 src/collector.py import ║"
echo "║  3. 打开看板: open dashboard/index.html          ║"
echo "║                                                ║"
echo "║  其他命令:                                      ║"
echo "║  python3 src/collector.py status   # 查看状态    ║"
echo "║  python3 src/collector.py fetch    # 拉取数据    ║"
echo "║  python3 src/collector.py export-html  # 导出看板 ║"
echo "╚══════════════════════════════════════════════════╝"
echo ""
