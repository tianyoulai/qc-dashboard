#!/usr/bin/env python3
"""
刷新 TiDB 中的 mart 聚合表。

功能：
1. 初始化 schema（建表/建视图）
2. 从 fact 层聚合刷新 mart 层（day/week/month × group/queue/auditor）
3. 支持 --target-date 刷新指定日期，--all 重跑全量

用法：
    # 刷新所有日期的 mart（默认）
    .venv/bin/python jobs/refresh_warehouse.py

    # 只刷新指定日期
    .venv/bin/python jobs/refresh_warehouse.py --target-date 2026-04-01

    # 只刷新今天
    .venv/bin/python jobs/refresh_warehouse.py --today
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage.repository import DashboardRepository


# ==========================================================
# mart 刷新 SQL（REPLACE INTO 支持 upsert）
# ==========================================================

REFRESH_MART_DAY_GROUP = """
REPLACE INTO mart_day_group (
    biz_date, group_name, mother_biz, sub_biz,
    qa_cnt,
    raw_correct_cnt, final_correct_cnt,
    raw_error_cnt, final_error_cnt,
    misjudge_cnt, missjudge_cnt,
    appeal_cnt, appeal_reversed_cnt,
    reviewer_cnt,
    raw_accuracy_rate, final_accuracy_rate,
    misjudge_rate, missjudge_rate,
    appeal_reverse_rate
)
SELECT
    biz_date,
    sub_biz AS group_name,
    MAX(mother_biz) AS mother_biz,
    sub_biz,
    COUNT(*) AS qa_cnt,
    SUM(is_raw_correct) AS raw_correct_cnt,
    SUM(is_final_correct) AS final_correct_cnt,
    SUM(1 - is_raw_correct) AS raw_error_cnt,
    SUM(1 - is_final_correct) AS final_error_cnt,
    SUM(is_misjudge) AS misjudge_cnt,
    SUM(is_missjudge) AS missjudge_cnt,
    SUM(is_appealed) AS appeal_cnt,
    SUM(is_appeal_reversed) AS appeal_reversed_cnt,
    COUNT(DISTINCT reviewer_name) AS reviewer_cnt,
    ROUND(SUM(is_raw_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS raw_accuracy_rate,
    ROUND(SUM(is_final_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS final_accuracy_rate,
    ROUND(SUM(is_misjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS misjudge_rate,
    ROUND(SUM(is_missjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS missjudge_rate,
    ROUND(SUM(is_appeal_reversed) * 100.0 / NULLIF(NULLIF(SUM(is_appealed), 0), 0), 2) AS appeal_reverse_rate
FROM fact_qa_event
WHERE {where_clause}
GROUP BY biz_date, sub_biz
"""

REFRESH_MART_DAY_QUEUE = """
REPLACE INTO mart_day_queue (
    biz_date, group_name, queue_name,
    qa_cnt,
    raw_correct_cnt, final_correct_cnt,
    raw_error_cnt, final_error_cnt,
    misjudge_cnt, missjudge_cnt,
    appeal_cnt, appeal_reversed_cnt,
    reviewer_cnt,
    raw_accuracy_rate, final_accuracy_rate,
    misjudge_rate, missjudge_rate,
    appeal_reverse_rate
)
SELECT
    biz_date,
    sub_biz AS group_name,
    COALESCE(queue_name, '未知') AS queue_name,
    COUNT(*) AS qa_cnt,
    SUM(is_raw_correct) AS raw_correct_cnt,
    SUM(is_final_correct) AS final_correct_cnt,
    COUNT(*) - SUM(is_raw_correct) AS raw_error_cnt,
    COUNT(*) - SUM(is_final_correct) AS final_error_cnt,
    SUM(is_misjudge) AS misjudge_cnt,
    SUM(is_missjudge) AS missjudge_cnt,
    SUM(is_appealed) AS appeal_cnt,
    SUM(is_appeal_reversed) AS appeal_reversed_cnt,
    COUNT(DISTINCT reviewer_name) AS reviewer_cnt,
    ROUND(SUM(is_raw_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS raw_accuracy_rate,
    ROUND(SUM(is_final_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS final_accuracy_rate,
    ROUND(SUM(is_misjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS misjudge_rate,
    ROUND(SUM(is_missjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS missjudge_rate,
    ROUND(SUM(is_appeal_reversed) * 100.0 / NULLIF(NULLIF(SUM(is_appealed), 0), 0), 2) AS appeal_reverse_rate
FROM fact_qa_event
WHERE {where_clause}
GROUP BY biz_date, sub_biz, COALESCE(queue_name, '未知')
"""

REFRESH_MART_DAY_AUDITOR = """
REPLACE INTO mart_day_auditor (
    biz_date, group_name, queue_name, reviewer_name,
    qa_cnt,
    raw_correct_cnt, final_correct_cnt,
    misjudge_cnt, missjudge_cnt,
    raw_accuracy_rate, final_accuracy_rate,
    misjudge_rate, missjudge_rate,
    appeal_reverse_rate
)
SELECT
    biz_date,
    sub_biz AS group_name,
    COALESCE(queue_name, '未知') AS queue_name,
    COALESCE(reviewer_name, '未知') AS reviewer_name,
    COUNT(*) AS qa_cnt,
    SUM(is_raw_correct) AS raw_correct_cnt,
    SUM(is_final_correct) AS final_correct_cnt,
    SUM(is_misjudge) AS misjudge_cnt,
    SUM(is_missjudge) AS missjudge_cnt,
    ROUND(SUM(is_raw_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS raw_accuracy_rate,
    ROUND(SUM(is_final_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS final_accuracy_rate,
    ROUND(SUM(is_misjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS misjudge_rate,
    ROUND(SUM(is_missjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS missjudge_rate,
    ROUND(SUM(is_appeal_reversed) * 100.0 / NULLIF(NULLIF(SUM(is_appealed), 0), 0), 2) AS appeal_reverse_rate
FROM fact_qa_event
WHERE {where_clause}
GROUP BY biz_date, sub_biz, COALESCE(queue_name, '未知'), COALESCE(reviewer_name, '未知')
"""

REFRESH_MART_WEEK_GROUP = """
REPLACE INTO mart_week_group (
    week_begin_date, group_name, mother_biz, sub_biz,
    qa_cnt, active_days,
    raw_accuracy_rate, final_accuracy_rate,
    misjudge_rate, missjudge_rate,
    appeal_reverse_rate
)
SELECT
    DATE_SUB(biz_date, INTERVAL WEEKDAY(biz_date) DAY) AS week_begin_date,
    sub_biz AS group_name,
    MAX(mother_biz) AS mother_biz,
    sub_biz,
    COUNT(*) AS qa_cnt,
    COUNT(DISTINCT biz_date) AS active_days,
    ROUND(SUM(is_raw_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS raw_accuracy_rate,
    ROUND(SUM(is_final_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS final_accuracy_rate,
    ROUND(SUM(is_misjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS misjudge_rate,
    ROUND(SUM(is_missjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS missjudge_rate,
    ROUND(SUM(is_appeal_reversed) * 100.0 / NULLIF(NULLIF(SUM(is_appealed), 0), 0), 2) AS appeal_reverse_rate
FROM fact_qa_event
WHERE {where_clause}
GROUP BY DATE_SUB(biz_date, INTERVAL WEEKDAY(biz_date) DAY), sub_biz
"""

REFRESH_MART_WEEK_QUEUE = """
REPLACE INTO mart_week_queue (
    week_begin_date, group_name, queue_name,
    qa_cnt, active_days,
    raw_accuracy_rate, final_accuracy_rate,
    misjudge_rate, missjudge_rate,
    appeal_reverse_rate
)
SELECT
    DATE_SUB(biz_date, INTERVAL WEEKDAY(biz_date) DAY) AS week_begin_date,
    sub_biz AS group_name,
    COALESCE(queue_name, '未知') AS queue_name,
    COUNT(*) AS qa_cnt,
    COUNT(DISTINCT biz_date) AS active_days,
    ROUND(SUM(is_raw_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS raw_accuracy_rate,
    ROUND(SUM(is_final_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS final_accuracy_rate,
    ROUND(SUM(is_misjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS misjudge_rate,
    ROUND(SUM(is_missjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS missjudge_rate,
    ROUND(SUM(is_appeal_reversed) * 100.0 / NULLIF(NULLIF(SUM(is_appealed), 0), 0), 2) AS appeal_reverse_rate
FROM fact_qa_event
WHERE {where_clause}
GROUP BY DATE_SUB(biz_date, INTERVAL WEEKDAY(biz_date) DAY), sub_biz, COALESCE(queue_name, '未知')
"""

REFRESH_MART_MONTH_GROUP = """
REPLACE INTO mart_month_group (
    month_begin_date, group_name, mother_biz, sub_biz,
    qa_cnt, active_days,
    raw_accuracy_rate, final_accuracy_rate,
    misjudge_rate, missjudge_rate,
    appeal_reverse_rate
)
SELECT
    LAST_DAY(biz_date - INTERVAL 1 MONTH) + INTERVAL 1 DAY AS month_begin_date,
    sub_biz AS group_name,
    MAX(mother_biz) AS mother_biz,
    sub_biz,
    COUNT(*) AS qa_cnt,
    COUNT(DISTINCT biz_date) AS active_days,
    ROUND(SUM(is_raw_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS raw_accuracy_rate,
    ROUND(SUM(is_final_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS final_accuracy_rate,
    ROUND(SUM(is_misjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS misjudge_rate,
    ROUND(SUM(is_missjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS missjudge_rate,
    ROUND(SUM(is_appeal_reversed) * 100.0 / NULLIF(NULLIF(SUM(is_appealed), 0), 0), 2) AS appeal_reverse_rate
FROM fact_qa_event
WHERE {where_clause}
GROUP BY LAST_DAY(biz_date - INTERVAL 1 MONTH) + INTERVAL 1 DAY, sub_biz
"""

REFRESH_MART_MONTH_QUEUE = """
REPLACE INTO mart_month_queue (
    month_begin_date, group_name, queue_name,
    qa_cnt, reviewer_cnt,
    raw_accuracy_rate, final_accuracy_rate,
    misjudge_rate, missjudge_rate,
    appeal_reverse_rate
)
SELECT
    LAST_DAY(biz_date - INTERVAL 1 MONTH) + INTERVAL 1 DAY AS month_begin_date,
    sub_biz AS group_name,
    COALESCE(queue_name, '未知') AS queue_name,
    COUNT(*) AS qa_cnt,
    COUNT(DISTINCT reviewer_name) AS reviewer_cnt,
    ROUND(SUM(is_raw_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS raw_accuracy_rate,
    ROUND(SUM(is_final_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS final_accuracy_rate,
    ROUND(SUM(is_misjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS misjudge_rate,
    ROUND(SUM(is_missjudge) * 100.0 / NULLIF(COUNT(*), 0), 2) AS missjudge_rate,
    ROUND(SUM(is_appeal_reversed) * 100.0 / NULLIF(NULLIF(SUM(is_appealed), 0), 0), 2) AS appeal_reverse_rate
FROM fact_qa_event
WHERE {where_clause}
GROUP BY LAST_DAY(biz_date - INTERVAL 1 MONTH) + INTERVAL 1 DAY, sub_biz, COALESCE(queue_name, '未知')
"""

REFRESH_MART_DAY_ERROR_TOPIC = """
REPLACE INTO mart_day_error_topic (
    biz_date, group_name, queue_name, error_type, error_reason,
    issue_cnt, affected_reviewer_cnt
)
SELECT
    biz_date,
    sub_biz AS group_name,
    COALESCE(queue_name, '未知') AS queue_name,
    COALESCE(NULLIF(TRIM(error_type), ''), '未分类') AS error_type,
    COALESCE(NULLIF(TRIM(error_reason), ''), '') AS error_reason,
    COUNT(*) AS issue_cnt,
    COUNT(DISTINCT reviewer_name) AS affected_reviewer_cnt
FROM fact_qa_event
WHERE {where_clause}
  AND COALESCE(error_type, '') != ''
GROUP BY biz_date, sub_biz, COALESCE(queue_name, '未知'),
         COALESCE(NULLIF(TRIM(error_type), ''), '未分类'),
         COALESCE(NULLIF(TRIM(error_reason), ''), '')
"""

REFRESH_MART_DAY_CONTENT_TYPE = """
REPLACE INTO mart_day_content_type (
    biz_date, group_name, content_type,
    qa_cnt, raw_correct_cnt, final_correct_cnt,
    raw_error_cnt, final_error_cnt,
    raw_accuracy_rate, final_accuracy_rate
)
SELECT
    biz_date,
    sub_biz AS group_name,
    COALESCE(NULLIF(TRIM(content_type), ''), '未知') AS content_type,
    COUNT(*) AS qa_cnt,
    SUM(is_raw_correct) AS raw_correct_cnt,
    SUM(is_final_correct) AS final_correct_cnt,
    COUNT(*) - SUM(is_raw_correct) AS raw_error_cnt,
    COUNT(*) - SUM(is_final_correct) AS final_error_cnt,
    ROUND(SUM(is_raw_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS raw_accuracy_rate,
    ROUND(SUM(is_final_correct) * 100.0 / NULLIF(COUNT(*), 0), 2) AS final_accuracy_rate
FROM fact_qa_event
WHERE {where_clause}
GROUP BY biz_date, sub_biz, COALESCE(NULLIF(TRIM(content_type), ''), '未知')
"""

# 所有 mart 刷新 SQL（按执行顺序）
ALL_MART_REFRESH = [
    ("mart_day_group", REFRESH_MART_DAY_GROUP),
    ("mart_day_queue", REFRESH_MART_DAY_QUEUE),
    ("mart_day_auditor", REFRESH_MART_DAY_AUDITOR),
    ("mart_day_error_topic", REFRESH_MART_DAY_ERROR_TOPIC),
    ("mart_day_content_type", REFRESH_MART_DAY_CONTENT_TYPE),
    ("mart_week_group", REFRESH_MART_WEEK_GROUP),
    ("mart_week_queue", REFRESH_MART_WEEK_QUEUE),
    ("mart_month_group", REFRESH_MART_MONTH_GROUP),
    ("mart_month_queue", REFRESH_MART_MONTH_QUEUE),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="刷新 TiDB mart 聚合表 + schema")
    parser.add_argument("--db-path", default=None, help="已废弃，保留兼容性")
    parser.add_argument("--schema-path", default=None, help="可选，自定义 schema.sql 路径")
    parser.add_argument("--target-date", type=str, default=None, help="目标日期 (YYYY-MM-DD)，只刷新该日期")
    parser.add_argument("--today", action="store_true", help="只刷新今天")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    repo = DashboardRepository()

    # 1. 初始化 schema（建表/建视图）
    print("🔄 初始化 schema...")
    repo.initialize_schema(args.schema_path)

    # 2. 确定 WHERE 子句
    target_date = None
    if args.today:
        target_date = date.today()
    elif args.target_date:
        try:
            target_date = date.fromisoformat(args.target_date)
        except ValueError:
            print(f"⚠️ 无效日期: {args.target_date}，将刷新全量")

    if target_date:
        where_clause = f"biz_date = '{target_date.isoformat()}'"
        print(f"📅 刷新目标日期: {target_date.isoformat()}")
    else:
        where_clause = "1=1"
        print("📅 刷新全量数据")

    # 3. 逐表刷新 mart
    print("\n📊 刷新 mart 聚合表...")
    results = {}
    for table_name, sql_template in ALL_MART_REFRESH:
        sql = sql_template.format(where_clause=where_clause)
        try:
            repo.execute(sql)
            count_result = repo.fetch_one(f"SELECT COUNT(*) AS cnt FROM {table_name}")
            cnt = int(list(count_result.values())[0]) if count_result else 0
            results[table_name] = {"status": "ok", "rows": cnt}
            print(f"  ✅ {table_name}: {cnt} 行")
        except Exception as e:
            results[table_name] = {"status": "error", "msg": str(e)}
            print(f"  ❌ {table_name}: {e}")

    # 4. 输出统计
    stats = {
        "db_type": "tidb",
        "target_date": target_date.isoformat() if target_date else "all",
        "mart_results": results,
    }

    # 补充 fact 行数统计
    for table_name in ["fact_qa_event", "fact_appeal_event"]:
        try:
            r = repo.fetch_one(f"SELECT COUNT(*) AS cnt FROM {table_name}")
            stats[f"{table_name}_rows"] = int(list(r.values())[0]) if r else 0
        except Exception:
            stats[f"{table_name}_rows"] = 0

    print(f"\n{'='*60}")
    print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))
    print("=" * 60)

    # 如果有错误，退出码 1
    has_error = any(v["status"] == "error" for v in results.values())
    sys.exit(1 if has_error else 0)


if __name__ == "__main__":
    main()
