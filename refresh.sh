#!/bin/bash
# ============================================================
# QC Dashboard 每日数据刷新
# ============================================================
# 用法:
#   ./refresh.sh                    # 全量刷新（所有队列）
#   ./refresh.sh --queue q3         # 只刷指定队列
#   ./refresh.sh --skip-fetch       # 跳过API拉取，只处理uploads里的xlsx
# ============================================================
set -e
cd "$(dirname "$0")"

echo "============================================="
echo "  🔄 QC Dashboard 数据刷新"
echo "  时间: $(date '+%Y-%m-%d %H:%M:%S')"
echo "  调度口径: launchd 18:25 自动刷新 / 09:15 独立推送"
echo "============================================="

MODE="excel"
SKIP_FETCH=0
QUEUE_ARG=""

for arg in "$@"; do
  case "$arg" in
    --skip-fetch) SKIP_FETCH=1 ;;
    --queue)      shift; QUEUE_ARG="--queue $1"; shift || true ;;
    *)            echo "未知参数: $arg"; exit 1 ;;
  esac
done

# Step 1: 拉取/导入数据
if [ "$SKIP_FETCH" -eq 0 ]; then
  if [ "$MODE" = "wcom-api" ]; then
    echo ""
    echo "📡 [1/3] 通过 API 拉取数据..."
    python3 src/collector.py fetch $QUEUE_ARG
  else
    # Excel 模式：检查 uploads 目录
    UPLOAD_COUNT=$(find data/uploads/*.xlsx data/uploads/*.xls 2>/dev/null | wc -l | tr -d ' ')
    if [ "$UPLOAD_COUNT" -gt 0 ]; then
      echo ""
      echo "📂 [1/3] 从 uploads 导入 Excel ($UPLOAD_COUNT 个文件)..."
      python3 src/collector.py import $QUEUE_ARG
    else
      echo ""
      echo "⚠️ [1/3] data/uploads/ 下没有 xlsx 文件"
      echo "   请先导出企微表格放到 data/uploads/ 目录"
      exit 1
    fi
  fi
fi

# Step 2: 清理未来日期（安全兜底）
echo ""
echo "🧹 [2/3] 清理未来日期占位行..."
python3 -c "
import sqlite3
from datetime import date, timedelta
conn = sqlite3.connect('data/metrics.db')
c = conn.cursor()
today = date.today().isoformat()
c.execute('DELETE FROM daily_metrics WHERE date > ?', (today,))
deleted = c.rowcount
if deleted > 0:
    print(f'   ✅ 删除 {deleted} 条未来日期记录 (> {today})')
else:
    print('   ✅ 无需清理')
conn.commit()
conn.close()
"

# Step 3: 重建看板
echo ""
echo "🔨 [3/3] 重建看板 HTML..."
python3 -c "
import sqlite3, json, re
conn = sqlite3.connect('data/metrics.db')
c = conn.cursor()

QUEUES = [
  'q1_toufang','q2_erjiansimple','q3_erjian_4qi_gt',
  'q3b_erjian_4qi_qiepian','q4_jubao_4qi','q5_lahei','q6_shangqiang'
]
queues = {}
total = 0
for qid in QUEUES:
    c.execute(\"SELECT date, metric_key, metric_value FROM daily_metrics WHERE queue_id=? ORDER BY date\", (qid,))
    rows = {}
    for d, k, v in c.fetchall():
        if d not in rows: rows[d] = {'date': d}
        rows[d][k] = v
    data = sorted(rows.values(), key=lambda x: x['date'])
    queues[qid] = data
    total += len(data)
    last_date = data[-1]['date'] if data else 'N/A'

with open('dashboard/index.html', 'r') as f:
    html = f.read()
new_db = json.dumps(queues, ensure_ascii=False)
html = re.sub(r'const DB_DATA = \{.*?\};', 'const DB_DATA = ' + new_db + ';', html, flags=re.DOTALL)
with open('dashboard/index.html', 'w') as f:
    f.write(html)
print(f'   ✅ {len(QUEUES)} 队列, 共 {total} 条记录')
conn.close()
"
python3 _rebuild.py

# 完成
echo ""
echo "============================================="
echo "  ✅ 刷新完成！打开 dashboard/index.html 查看"
echo "============================================="
