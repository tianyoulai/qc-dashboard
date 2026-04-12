from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage.repository import DashboardRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="校验线下质检与线上申诉的 join_key 命中率、缺失样本和冲突样本。")
    parser.add_argument("--db-path", default="", help="已废弃，保留兼容性")
    parser.add_argument("--top", type=int, default=20, help="异常样本输出条数")
    parser.add_argument("--output", default=None, help="可选，把结果写到指定 JSON 文件")
    return parser.parse_args()


def safe_divide(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator * 100.0 / denominator, 4)


def df_to_records(df) -> list[dict[str, Any]]:
    if df.empty:
        return []
    cleaned = df.where(df.notna(), None)
    return cleaned.to_dict(orient="records")


def safe_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        if value != value:
            return 0
    except Exception:
        pass
    return int(value)


def main() -> None:
    args = parse_args()
    repo = DashboardRepository(args.db_path)

    # repo.initialize_schema()  # 视图已由 refresh_warehouse.py 初始化，避免重复创建导致的问题

    overall = repo.fetch_one(
        """
        SELECT
            COUNT(*) AS qa_total_rows,
            SUM(CASE WHEN COALESCE(join_key, '') <> '' THEN 1 ELSE 0 END) AS qa_with_join_key,
            SUM(CASE WHEN join_status = 'matched' THEN 1 ELSE 0 END) AS qa_matched_rows,
            SUM(CASE WHEN join_status = 'unmatched' THEN 1 ELSE 0 END) AS qa_unmatched_rows,
            SUM(CASE WHEN join_status = 'missing_join_key' THEN 1 ELSE 0 END) AS qa_missing_join_key_rows,
            SUM(CASE WHEN matched_with_result THEN 1 ELSE 0 END) AS qa_backfilled_rows
        FROM vw_join_quality_detail
        """
    ) or {}

    appeal = repo.fetch_one(
        """
        SELECT
            COUNT(*) AS appeal_total_rows,
            SUM(CASE WHEN COALESCE(join_key, '') <> '' THEN 1 ELSE 0 END) AS appeal_with_join_key,
            COUNT(DISTINCT CASE WHEN COALESCE(join_key, '') <> '' THEN join_key ELSE NULL END) AS appeal_distinct_join_keys,
            SUM(CASE WHEN COALESCE(TRIM(appeal_result), '') <> '' THEN 1 ELSE 0 END) AS appeal_rows_with_result
        FROM fact_appeal_event
        """
    ) or {}

    match_by_key_type_df = repo.fetch_df(
        """
        SELECT
            join_key_type,
            COUNT(*) AS qa_cnt,
            SUM(CASE WHEN join_status = 'matched' THEN 1 ELSE 0 END) AS matched_cnt,
            SUM(CASE WHEN join_status = 'unmatched' THEN 1 ELSE 0 END) AS unmatched_cnt,
            SUM(CASE WHEN join_status = 'missing_join_key' THEN 1 ELSE 0 END) AS missing_join_key_cnt,
            ROUND(SUM(CASE WHEN join_status = 'matched' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 4) AS match_rate,
            ROUND(SUM(CASE WHEN matched_with_result THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 4) AS result_backfill_rate
        FROM vw_join_quality_detail
        GROUP BY 1
        ORDER BY qa_cnt DESC, join_key_type
        """
    )

    daily_match_df = repo.fetch_df(
        """
        SELECT biz_date, join_key_type, qa_cnt, matched_cnt, unmatched_cnt, missing_join_key_cnt, match_rate, result_backfill_rate
        FROM vw_join_quality_daily
        ORDER BY biz_date DESC, qa_cnt DESC, join_key_type
        LIMIT %s
        """,
        [max(args.top * 5, 30)],
    )

    unmatched_samples_df = repo.fetch_df(
        """
        SELECT
            biz_date,
            group_name,
            queue_name,
            reviewer_name,
            join_key_type,
            source_record_id,
            comment_id,
            dynamic_id,
            account_id,
            join_key
        FROM vw_join_quality_detail
        WHERE join_status = 'unmatched'
        ORDER BY biz_date DESC, group_name, queue_name
        LIMIT %s
        """,
        [args.top],
    )

    missing_key_samples_df = repo.fetch_df(
        """
        SELECT
            biz_date,
            group_name,
            queue_name,
            reviewer_name,
            source_record_id,
            comment_id,
            dynamic_id,
            account_id
        FROM vw_join_quality_detail
        WHERE join_status = 'missing_join_key'
        ORDER BY biz_date DESC, group_name, queue_name
        LIMIT %s
        """,
        [args.top],
    )

    orphan_appeal_samples_df = repo.fetch_df(
        """
        SELECT
            a.biz_date,
            a.group_name,
            a.queue_name,
            a.reviewer_name,
            a.source_record_id,
            a.comment_id,
            a.dynamic_id,
            a.account_id,
            a.join_key,
            a.appeal_result
        FROM vw_appeal_latest a
        LEFT JOIN fact_qa_event q
          ON a.join_key = q.join_key
        WHERE q.join_key IS NULL
        ORDER BY a.biz_date DESC, a.group_name, a.queue_name
        LIMIT %s
        """,
        [args.top],
    )

    conflicting_appeal_keys_df = repo.fetch_df(
        """
        SELECT
            join_key,
            COUNT(*) AS appeal_row_cnt,
            COUNT(DISTINCT COALESCE(TRIM(appeal_result), '')) AS distinct_appeal_result_cnt,
            MIN(biz_date) AS first_biz_date,
            MAX(biz_date) AS last_biz_date
        FROM fact_appeal_event
        WHERE COALESCE(join_key, '') <> ''
        GROUP BY 1
        HAVING COUNT(*) > 1
        ORDER BY distinct_appeal_result_cnt DESC, appeal_row_cnt DESC, last_biz_date DESC
        LIMIT %s
        """,
        [args.top],
    )

    qa_total_rows = safe_int(overall.get("qa_total_rows"))
    qa_with_join_key = safe_int(overall.get("qa_with_join_key"))
    qa_matched_rows = safe_int(overall.get("qa_matched_rows"))
    qa_unmatched_rows = safe_int(overall.get("qa_unmatched_rows"))
    qa_missing_join_key_rows = safe_int(overall.get("qa_missing_join_key_rows"))
    qa_backfilled_rows = safe_int(overall.get("qa_backfilled_rows"))

    report = {
        "db_path": "tidb",
        "overall": {
            "qa_total_rows": qa_total_rows,
            "qa_with_join_key": qa_with_join_key,
            "qa_matched_rows": qa_matched_rows,
            "qa_unmatched_rows": qa_unmatched_rows,
            "qa_missing_join_key_rows": qa_missing_join_key_rows,
            "qa_backfilled_rows": qa_backfilled_rows,
            "qa_match_rate": safe_divide(qa_matched_rows, qa_total_rows),
            "qa_match_rate_when_key_present": safe_divide(qa_matched_rows, qa_with_join_key),
            "qa_result_backfill_rate": safe_divide(qa_backfilled_rows, qa_total_rows),
            "appeal_total_rows": safe_int(appeal.get("appeal_total_rows")),
            "appeal_with_join_key": safe_int(appeal.get("appeal_with_join_key")),
            "appeal_distinct_join_keys": safe_int(appeal.get("appeal_distinct_join_keys")),
            "appeal_rows_with_result": safe_int(appeal.get("appeal_rows_with_result")),
        },
        "match_by_join_key_type": df_to_records(match_by_key_type_df),
        "daily_match_snapshot": df_to_records(daily_match_df),
        "unmatched_qa_samples": df_to_records(unmatched_samples_df),
        "missing_join_key_samples": df_to_records(missing_key_samples_df),
        "orphan_appeal_samples": df_to_records(orphan_appeal_samples_df),
        "conflicting_appeal_keys": df_to_records(conflicting_appeal_keys_df),
    }

    output_text = json.dumps(report, ensure_ascii=False, indent=2, default=str)
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output_text, encoding="utf-8")

    print(output_text)


if __name__ == "__main__":
    main()
