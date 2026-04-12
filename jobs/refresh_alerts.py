from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage.repository import DashboardRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="根据 mart 与 join 校验结果重算 fact_alert_event 告警事件。")
    parser.add_argument("--db-path", default=None, help="已废弃，保留兼容性")
    parser.add_argument("--lookback-days", type=int, default=90, help="回刷最近多少天的告警，默认 90 天")
    return parser.parse_args()


def write_job_log(repo: DashboardRepository, run_id: str, source_rows: int, inserted_rows: int, run_status: str, error_message: str | None = None) -> None:
    repo.execute(
        """
        INSERT INTO etl_run_log (
            run_id,
            job_name,
            start_time,
            end_time,
            run_status,
            source_rows,
            inserted_rows,
            dedup_rows,
            warning_rows,
            error_message
        )
        VALUES (%s, 'refresh_alert_event', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s, %s, 0, 0, %s)
        """,
        [run_id, run_status, source_rows, inserted_rows, error_message],
    )


def fetch_scalar(repo: DashboardRepository, sql: str, params: list[Any] | None = None) -> Any:
    row = repo.fetch_one(sql, params or [])
    if row is None:
        return None
    return list(row.values())[0]


def insert_group_raw_accuracy_alerts(repo: DashboardRepository, cutoff_date) -> None:
    repo.execute(
        """
        INSERT INTO fact_alert_event (
            alert_id, alert_date, grain, target_level, target_key, rule_code,
            severity, metric_name, metric_value, threshold_value, is_resolved, alert_message
        )
        SELECT
            MD5(CONCAT(r.rule_code, '|', CAST(m.biz_date AS CHAR), '|', COALESCE(m.group_name, ''))) AS alert_id,
            m.biz_date AS alert_date,
            r.grain,
            r.target_level,
            COALESCE(m.group_name, '未分组') AS target_key,
            r.rule_code,
            r.severity,
            r.metric_name,
            m.raw_accuracy_rate AS metric_value,
            r.threshold_value,
            0 AS is_resolved,
            CONCAT('组别【', COALESCE(m.group_name, '未分组'), '】日原始正确率 ', CAST(ROUND(m.raw_accuracy_rate, 2) AS CHAR), '% 低于阈值 ', CAST(r.threshold_value AS CHAR), '%') AS alert_message
        FROM mart_day_group m
        JOIN dim_alert_rule r ON r.rule_code = 'RAW_ACC_LT_99_DAY' AND r.enabled = 1
        WHERE m.biz_date >= %s
          AND m.qa_cnt >= 20
          AND m.raw_accuracy_rate < r.threshold_value
        """,
        [cutoff_date],
    )


def insert_queue_final_accuracy_alerts(repo: DashboardRepository, cutoff_date) -> None:
    repo.execute(
        """
        INSERT INTO fact_alert_event (
            alert_id, alert_date, grain, target_level, target_key, rule_code,
            severity, metric_name, metric_value, threshold_value, is_resolved, alert_message
        )
        SELECT
            MD5(CONCAT(r.rule_code, '|', CAST(m.biz_date AS CHAR), '|', COALESCE(m.group_name, ''), '|', COALESCE(m.queue_name, ''))) AS alert_id,
            m.biz_date AS alert_date,
            r.grain,
            r.target_level,
            CONCAT(COALESCE(m.group_name, '未分组'), ' / ', COALESCE(m.queue_name, '未分配队列')) AS target_key,
            r.rule_code,
            r.severity,
            r.metric_name,
            m.final_accuracy_rate AS metric_value,
            r.threshold_value,
            0 AS is_resolved,
            CONCAT('队列【', COALESCE(m.group_name, '未分组'), ' / ', COALESCE(m.queue_name, '未分配队列'), '】日最终正确率 ', CAST(ROUND(m.final_accuracy_rate, 2) AS CHAR), '% 低于阈值 ', CAST(r.threshold_value AS CHAR), '%') AS alert_message
        FROM mart_day_queue m
        JOIN dim_alert_rule r ON r.rule_code = 'FINAL_ACC_LT_99_QUEUE_DAY' AND r.enabled = 1
        WHERE m.biz_date >= %s
          AND m.qa_cnt >= 20
          AND m.final_accuracy_rate < r.threshold_value
        """,
        [cutoff_date],
    )


def insert_queue_missjudge_alerts(repo: DashboardRepository, cutoff_date) -> None:
    repo.execute(
        """
        INSERT INTO fact_alert_event (
            alert_id, alert_date, grain, target_level, target_key, rule_code,
            severity, metric_name, metric_value, threshold_value, is_resolved, alert_message
        )
        SELECT
            MD5(CONCAT(r.rule_code, '|', CAST(m.biz_date AS CHAR), '|', COALESCE(m.group_name, ''), '|', COALESCE(m.queue_name, ''))) AS alert_id,
            m.biz_date AS alert_date,
            r.grain,
            r.target_level,
            CONCAT(COALESCE(m.group_name, '未分组'), ' / ', COALESCE(m.queue_name, '未分配队列')) AS target_key,
            r.rule_code,
            r.severity,
            r.metric_name,
            m.missjudge_rate AS metric_value,
            r.threshold_value,
            0 AS is_resolved,
            CONCAT('队列【', COALESCE(m.group_name, '未分组'), ' / ', COALESCE(m.queue_name, '未分配队列'), '】日漏判率 ', CAST(ROUND(m.missjudge_rate, 4) AS CHAR), '% 高于阈值 ', CAST(r.threshold_value AS CHAR), '%') AS alert_message
        FROM mart_day_queue m
        JOIN dim_alert_rule r ON r.rule_code = 'MISS_RATE_GT_035_DAY' AND r.enabled = 1
        WHERE m.biz_date >= %s
          AND m.qa_cnt >= 20
          AND m.missjudge_rate > r.threshold_value
        """,
        [cutoff_date],
    )


def insert_group_appeal_reverse_alerts(repo: DashboardRepository, cutoff_date) -> None:
    repo.execute(
        """
        INSERT INTO fact_alert_event (
            alert_id, alert_date, grain, target_level, target_key, rule_code,
            severity, metric_name, metric_value, threshold_value, is_resolved, alert_message
        )
        SELECT
            MD5(CONCAT(r.rule_code, '|', CAST(m.biz_date AS CHAR), '|', COALESCE(m.group_name, ''))) AS alert_id,
            m.biz_date AS alert_date,
            r.grain,
            r.target_level,
            COALESCE(m.group_name, '未分组') AS target_key,
            r.rule_code,
            r.severity,
            r.metric_name,
            m.appeal_reverse_rate AS metric_value,
            r.threshold_value,
            0 AS is_resolved,
            CONCAT('组别【', COALESCE(m.group_name, '未分组'), '】日申诉改判率 ', CAST(ROUND(m.appeal_reverse_rate, 2) AS CHAR), '% 高于阈值 ', CAST(r.threshold_value AS CHAR), '%') AS alert_message
        FROM mart_day_group m
        JOIN dim_alert_rule r ON r.rule_code = 'APPEAL_REV_GT_18_DAY' AND r.enabled = 1
        WHERE m.biz_date >= %s
          AND m.appeal_cnt >= 5
          AND m.appeal_reverse_rate > r.threshold_value
        """,
        [cutoff_date],
    )


def insert_system_join_match_alerts(conn: TiDBManager, cutoff_date) -> None:
    conn.execute(
        """
        INSERT INTO fact_alert_event (
            alert_id, alert_date, grain, target_level, target_key, rule_code,
            severity, metric_name, metric_value, threshold_value, is_resolved, alert_message
        )
        WITH daily AS (
            SELECT
                biz_date,
                SUM(CASE WHEN COALESCE(join_key, '') <> '' THEN 1 ELSE 0 END) AS with_join_key_cnt,
                SUM(CASE WHEN join_status = 'matched' THEN 1 ELSE 0 END) AS matched_cnt
            FROM vw_join_quality_detail
            WHERE biz_date >= %s
            GROUP BY 1
        )
        SELECT
            MD5(CONCAT(r.rule_code, '|', CAST(d.biz_date AS CHAR), '|system')) AS alert_id,
            d.biz_date AS alert_date,
            r.grain,
            r.target_level,
            '全局' AS target_key,
            r.rule_code,
            r.severity,
            r.metric_name,
            ROUND(d.matched_cnt * 100.0 / NULLIF(d.with_join_key_cnt, 0), 4) AS metric_value,
            r.threshold_value,
            0 AS is_resolved,
            CONCAT('全局日联表命中率（有主键样本）', CAST(ROUND(d.matched_cnt * 100.0 / NULLIF(d.with_join_key_cnt, 0), 2) AS CHAR), '% 低于阈值 ', CAST(r.threshold_value AS CHAR), '%') AS alert_message
        FROM daily d
        JOIN dim_alert_rule r ON r.rule_code = 'JOIN_MATCH_LT_85_DAY' AND r.enabled = 1
        WHERE d.with_join_key_cnt >= 20
          AND ROUND(d.matched_cnt * 100.0 / NULLIF(d.with_join_key_cnt, 0), 4) < r.threshold_value
        """,
        [cutoff_date],
    )


def insert_system_missing_key_alerts(repo: DashboardRepository, cutoff_date) -> None:
    repo.execute(
        """
        INSERT INTO fact_alert_event (
            alert_id, alert_date, grain, target_level, target_key, rule_code,
            severity, metric_name, metric_value, threshold_value, is_resolved, alert_message
        )
        WITH daily AS (
            SELECT
                biz_date,
                COUNT(*) AS qa_cnt,
                SUM(CASE WHEN join_status = 'missing_join_key' THEN 1 ELSE 0 END) AS missing_join_key_cnt
            FROM vw_join_quality_detail
            WHERE biz_date >= %s
            GROUP BY 1
        )
        SELECT
            MD5(CONCAT(r.rule_code, '|', CAST(d.biz_date AS CHAR), '|system')) AS alert_id,
            d.biz_date AS alert_date,
            r.grain,
            r.target_level,
            '全局' AS target_key,
            r.rule_code,
            r.severity,
            r.metric_name,
            ROUND(d.missing_join_key_cnt * 100.0 / NULLIF(d.qa_cnt, 0), 4) AS metric_value,
            r.threshold_value,
            0 AS is_resolved,
            CONCAT('全局日缺失关联主键占比 ', CAST(ROUND(d.missing_join_key_cnt * 100.0 / NULLIF(d.qa_cnt, 0), 2) AS CHAR), '% 高于阈值 ', CAST(r.threshold_value AS CHAR), '%') AS alert_message
        FROM daily d
        JOIN dim_alert_rule r ON r.rule_code = 'MISSING_JOIN_KEY_GT_10_DAY' AND r.enabled = 1
        WHERE d.qa_cnt >= 20
          AND ROUND(d.missing_join_key_cnt * 100.0 / NULLIF(d.qa_cnt, 0), 4) > r.threshold_value
        """,
        [cutoff_date],
    )


def insert_group_week_raw_drop_alerts(conn: TiDBManager, cutoff_date) -> None:
    conn.execute(
        """
        INSERT INTO fact_alert_event (
            alert_id, alert_date, grain, target_level, target_key, rule_code,
            severity, metric_name, metric_value, threshold_value, is_resolved, alert_message
        )
        WITH weekly AS (
            SELECT
                m.week_begin_date,
                m.group_name,
                m.qa_cnt,
                m.raw_accuracy_rate,
                p.qa_cnt AS prev_qa_cnt,
                p.raw_accuracy_rate AS prev_raw_accuracy_rate,
                ROUND(p.raw_accuracy_rate - m.raw_accuracy_rate, 4) AS raw_accuracy_drop_pp
            FROM mart_week_group m
            JOIN mart_week_group p
              ON m.group_name = p.group_name
             AND p.week_begin_date = m.week_begin_date - INTERVAL 7 DAY
            WHERE m.week_begin_date >= %s
        )
        SELECT
            MD5(CONCAT(r.rule_code, '|', CAST(w.week_begin_date AS CHAR), '|', COALESCE(w.group_name, ''))) AS alert_id,
            w.week_begin_date AS alert_date,
            r.grain,
            r.target_level,
            COALESCE(w.group_name, '未分组') AS target_key,
            r.rule_code,
            r.severity,
            r.metric_name,
            w.raw_accuracy_drop_pp AS metric_value,
            r.threshold_value,
            0 AS is_resolved,
            CONCAT('组别【', COALESCE(w.group_name, '未分组'), '】周原始正确率较上周下跌 ', CAST(ROUND(w.raw_accuracy_drop_pp, 2) AS CHAR), ' 个百分点（本周 ', CAST(ROUND(w.raw_accuracy_rate, 2) AS CHAR), '%，上周 ', CAST(ROUND(w.prev_raw_accuracy_rate, 2) AS CHAR), '%）') AS alert_message
        FROM weekly w
        JOIN dim_alert_rule r ON r.rule_code = 'RAW_ACC_DROP_GT_1P5_WEEK' AND r.enabled = 1
        WHERE w.qa_cnt >= 50
          AND COALESCE(w.prev_qa_cnt, 0) >= 50
          AND w.raw_accuracy_drop_pp > r.threshold_value
        """,
        [cutoff_date],
    )


def insert_queue_week_missjudge_spike_alerts(conn: TiDBManager, cutoff_date) -> None:
    conn.execute(
        """
        INSERT INTO fact_alert_event (
            alert_id, alert_date, grain, target_level, target_key, rule_code,
            severity, metric_name, metric_value, threshold_value, is_resolved, alert_message
        )
        WITH weekly AS (
            SELECT
                m.week_begin_date,
                m.group_name,
                m.queue_name,
                m.qa_cnt,
                m.missjudge_rate,
                p.qa_cnt AS prev_qa_cnt,
                p.missjudge_rate AS prev_missjudge_rate,
                ROUND(m.missjudge_rate - p.missjudge_rate, 4) AS missjudge_rate_spike_pp
            FROM mart_week_queue m
            JOIN mart_week_queue p
              ON m.group_name = p.group_name
             AND m.queue_name = p.queue_name
             AND p.week_begin_date = m.week_begin_date - INTERVAL 7 DAY
            WHERE m.week_begin_date >= %s
        )
        SELECT
            MD5(CONCAT(r.rule_code, '|', CAST(w.week_begin_date AS CHAR), '|', COALESCE(w.group_name, ''), '|', COALESCE(w.queue_name, ''))) AS alert_id,
            w.week_begin_date AS alert_date,
            r.grain,
            r.target_level,
            CONCAT(COALESCE(w.group_name, '未分组'), ' / ', COALESCE(w.queue_name, '未分配队列')) AS target_key,
            r.rule_code,
            r.severity,
            r.metric_name,
            w.missjudge_rate_spike_pp AS metric_value,
            r.threshold_value,
            0 AS is_resolved,
            CONCAT('队列【', COALESCE(w.group_name, '未分组'), ' / ', COALESCE(w.queue_name, '未分配队列'), '】周漏判率较上周上升 ', CAST(ROUND(w.missjudge_rate_spike_pp, 2) AS CHAR), ' 个百分点（本周 ', CAST(ROUND(w.missjudge_rate, 2) AS CHAR), '%，上周 ', CAST(ROUND(w.prev_missjudge_rate, 2) AS CHAR), '%）') AS alert_message
        FROM weekly w
        JOIN dim_alert_rule r ON r.rule_code = 'MISS_RATE_SPIKE_GT_0P2_QUEUE_WEEK' AND r.enabled = 1
        WHERE w.qa_cnt >= 50
          AND COALESCE(w.prev_qa_cnt, 0) >= 50
          AND w.missjudge_rate >= 0.35
          AND w.missjudge_rate_spike_pp > r.threshold_value
        """,
        [cutoff_date],
    )


def insert_queue_month_top_error_share_alerts(repo: DashboardRepository, cutoff_date) -> None:
    repo.execute(
        """
        INSERT INTO fact_alert_event (
            alert_id, alert_date, grain, target_level, target_key, rule_code,
            severity, metric_name, metric_value, threshold_value, is_resolved, alert_message
        )
        WITH ranked AS (
            SELECT
                e.month_begin_date,
                e.group_name,
                e.queue_name,
                e.error_type,
                e.issue_cnt,
                q.qa_cnt,
                ROUND(e.issue_cnt * 100.0 / NULLIF(q.qa_cnt, 0), 4) AS top_error_share,
                ROW_NUMBER() OVER (
                    PARTITION BY e.month_begin_date, e.group_name, e.queue_name
                    ORDER BY e.issue_cnt DESC, e.error_type
                ) AS rn
            FROM mart_month_error_topic e
            JOIN mart_month_queue q
              ON e.month_begin_date = q.month_begin_date
             AND e.group_name = q.group_name
             AND e.queue_name = q.queue_name
            WHERE e.month_begin_date >= %s
        )
        SELECT
            MD5(CONCAT(r.rule_code, '|', CAST(x.month_begin_date AS CHAR), '|', COALESCE(x.group_name, ''), '|', COALESCE(x.queue_name, ''))) AS alert_id,
            x.month_begin_date AS alert_date,
            r.grain,
            r.target_level,
            CONCAT(COALESCE(x.group_name, '未分组'), ' / ', COALESCE(x.queue_name, '未分配队列')) AS target_key,
            r.rule_code,
            r.severity,
            r.metric_name,
            x.top_error_share AS metric_value,
            r.threshold_value,
            0 AS is_resolved,
            CONCAT('队列【', COALESCE(x.group_name, '未分组'), ' / ', COALESCE(x.queue_name, '未分配队列'), '】月度问题结构过度集中，错误类型【', COALESCE(x.error_type, '未分类'), '】占全部质检量 ', CAST(ROUND(x.top_error_share, 2) AS CHAR), '%') AS alert_message
        FROM ranked x
        JOIN dim_alert_rule r ON r.rule_code = 'TOP_ERROR_SHARE_GT_35_QUEUE_MONTH' AND r.enabled = 1
        WHERE x.rn = 1
          AND COALESCE(x.qa_cnt, 0) >= 50
          AND COALESCE(x.issue_cnt, 0) >= 10
          AND x.top_error_share > r.threshold_value
        """,
        [cutoff_date],
    )


def insert_queue_week_error_repeat_alerts(conn: TiDBManager, cutoff_date) -> None:
    conn.execute(
        """
        INSERT INTO fact_alert_event (
            alert_id, alert_date, grain, target_level, target_key, rule_code,
            severity, metric_name, metric_value, threshold_value, is_resolved, alert_message
        )
        WITH weekly_error AS (
            SELECT
                DATE_SUB(biz_date, INTERVAL WEEKDAY(biz_date) DAY) AS week_begin_date,
                group_name,
                queue_name,
                error_type,
                COUNT(*) AS issue_cnt,
                COUNT(DISTINCT reviewer_name) AS affected_reviewer_cnt
            FROM vw_qa_base
            WHERE COALESCE(error_type, '') <> ''
            GROUP BY 1, 2, 3, 4
        ),
        weekly_compare AS (
            SELECT
                c.week_begin_date,
                c.group_name,
                c.queue_name,
                c.error_type,
                c.issue_cnt,
                c.affected_reviewer_cnt,
                q.qa_cnt,
                p.issue_cnt AS prev_issue_cnt,
                p.affected_reviewer_cnt AS prev_affected_reviewer_cnt,
                pq.qa_cnt AS prev_qa_cnt,
                ROUND(c.issue_cnt * 100.0 / NULLIF(q.qa_cnt, 0), 4) AS issue_share,
                ROUND(p.issue_cnt * 100.0 / NULLIF(pq.qa_cnt, 0), 4) AS prev_issue_share,
                ROUND(
                    ROUND(c.issue_cnt * 100.0 / NULLIF(q.qa_cnt, 0), 4)
                    - ROUND(p.issue_cnt * 100.0 / NULLIF(pq.qa_cnt, 0), 4),
                    4
                ) AS issue_share_change_pp
            FROM weekly_error c
            JOIN mart_week_queue q
              ON c.week_begin_date = q.week_begin_date
             AND c.group_name = q.group_name
             AND c.queue_name = q.queue_name
            JOIN weekly_error p
              ON c.group_name = p.group_name
             AND c.queue_name = p.queue_name
             AND c.error_type = p.error_type
             AND p.week_begin_date = c.week_begin_date - INTERVAL 7 DAY
            JOIN mart_week_queue pq
              ON p.week_begin_date = pq.week_begin_date
             AND p.group_name = pq.group_name
             AND p.queue_name = pq.queue_name
            WHERE c.week_begin_date >= %s
        )
        SELECT
            MD5(CONCAT(r.rule_code, '|', CAST(w.week_begin_date AS CHAR), '|', COALESCE(w.group_name, ''), '|', COALESCE(w.queue_name, ''), '|', COALESCE(w.error_type, ''))) AS alert_id,
            w.week_begin_date AS alert_date,
            r.grain,
            r.target_level,
            CONCAT(COALESCE(w.group_name, '未分组'), ' / ', COALESCE(w.queue_name, '未分配队列'), ' ｜ 错误类型=', COALESCE(w.error_type, '未分类')) AS target_key,
            r.rule_code,
            r.severity,
            r.metric_name,
            w.issue_share AS metric_value,
            r.threshold_value,
            0 AS is_resolved,
            CONCAT('队列【', COALESCE(w.group_name, '未分组'), ' / ', COALESCE(w.queue_name, '未分配队列'), '】错误类型【', COALESCE(w.error_type, '未分类'), '】连续两周未收敛：本周占比 ', CAST(ROUND(w.issue_share, 2) AS CHAR), '%，上周占比 ', CAST(ROUND(w.prev_issue_share, 2) AS CHAR), '%；本周样本 ', CAST(COALESCE(w.issue_cnt, 0) AS CHAR), ' 条，影响审核人 ', CAST(COALESCE(w.affected_reviewer_cnt, 0) AS CHAR), ' 人。') AS alert_message
        FROM weekly_compare w
        JOIN dim_alert_rule r ON r.rule_code = 'ERROR_TYPE_SHARE_GT_15_QUEUE_WEEK' AND r.enabled = 1
        WHERE COALESCE(w.qa_cnt, 0) >= 50
          AND COALESCE(w.prev_qa_cnt, 0) >= 50
          AND COALESCE(w.issue_cnt, 0) >= 10
          AND COALESCE(w.prev_issue_cnt, 0) >= 10
          AND COALESCE(w.affected_reviewer_cnt, 0) >= 2
          AND w.issue_share > r.threshold_value
          AND w.prev_issue_share > r.threshold_value
          AND w.issue_share_change_pp >= -2.00
        """,
        [cutoff_date],
    )


def main() -> None:
    args = parse_args()
    repo = DashboardRepository()
    repo.initialize_schema()

    run_id = uuid.uuid4().hex
    try:
        max_biz_date = fetch_scalar(repo, "SELECT MAX(biz_date) FROM fact_qa_event")
        if max_biz_date is None:
            write_job_log(repo, run_id, 0, 0, "success")
            print(json.dumps({"db_type": "tidb", "alert_rows": 0, "message": "fact_qa_event 为空，未生成告警。"}, ensure_ascii=False, indent=2))
            return

        cutoff_date = max_biz_date - timedelta(days=max(args.lookback_days - 1, 0))
        source_rows = int(fetch_scalar(repo, "SELECT COUNT(*) FROM fact_qa_event WHERE biz_date >= %s", [cutoff_date]) or 0)

        repo.execute("DELETE FROM fact_alert_event WHERE alert_date >= %s", [cutoff_date])

        insert_group_raw_accuracy_alerts(repo, cutoff_date)
        insert_queue_final_accuracy_alerts(repo, cutoff_date)
        insert_queue_missjudge_alerts(repo, cutoff_date)
        insert_group_appeal_reverse_alerts(repo, cutoff_date)
        insert_system_join_match_alerts(repo, cutoff_date)
        insert_system_missing_key_alerts(repo, cutoff_date)
        insert_group_week_raw_drop_alerts(repo, cutoff_date)
        insert_queue_week_missjudge_spike_alerts(repo, cutoff_date)
        insert_queue_month_top_error_share_alerts(repo, cutoff_date)
        insert_queue_week_error_repeat_alerts(repo, cutoff_date)

        inserted_rows = int(fetch_scalar(repo, "SELECT COUNT(*) FROM fact_alert_event WHERE alert_date >= %s", [cutoff_date]) or 0)
        severity_df = repo.fetch_df(
            """
            SELECT severity, COUNT(*) AS cnt
            FROM fact_alert_event
            WHERE alert_date >= %s
            GROUP BY 1
            ORDER BY severity
            """,
            [cutoff_date],
        )
        write_job_log(repo, run_id, source_rows, inserted_rows, "success")
    except Exception as exc:
        write_job_log(repo, run_id, 0, 0, "failed", str(exc))
        raise

    report = {
        "db_type": "tidb",
        "lookback_days": args.lookback_days,
        "cutoff_date": str(cutoff_date),
        "source_rows": source_rows,
        "alert_rows": inserted_rows,
        "severity_breakdown": severity_df.where(severity_df.notna(), None).to_dict(orient="records"),
        "rules": [
            "RAW_ACC_LT_99_DAY",
            "FINAL_ACC_LT_99_QUEUE_DAY",
            "MISS_RATE_GT_035_DAY",
            "APPEAL_REV_GT_18_DAY",
            "JOIN_MATCH_LT_85_DAY",
            "MISSING_JOIN_KEY_GT_10_DAY",
            "RAW_ACC_DROP_GT_1P5_WEEK",
            "MISS_RATE_SPIKE_GT_0P2_QUEUE_WEEK",
            "TOP_ERROR_SHARE_GT_35_QUEUE_MONTH",
            "ERROR_TYPE_SHARE_GT_15_QUEUE_WEEK",
        ],
    }
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
