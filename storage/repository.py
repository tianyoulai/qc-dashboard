"""质培运营看板 — 数据访问层（TiDB 版）。"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from storage.tidb_manager import TiDBManager

PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass
class DashboardRepository:
    """数据仓库访问入口（TiDB）。"""

    db_path: str = field(default="")

    def __post_init__(self) -> None:
        self._manager = TiDBManager()

    def connect(self) -> TiDBManager:
        """返回 TiDB 管理器实例（兼容旧代码）。"""
        return self._manager

    def database_exists(self) -> bool:
        return True  # TiDB 始终可用

    def initialize_schema(self, schema_path: str | Path | None = None) -> None:
        schema_file = Path(schema_path) if schema_path else Path(__file__).resolve().parent / "schema.sql"
        sql = schema_file.read_text(encoding="utf-8")
        statements = self._split_sql(sql)
        for stmt in statements:
            stmt = stmt.strip()
            if stmt:
                try:
                    self._manager.execute(stmt)
                except Exception:
                    pass  # 视图/表已存在等可忽略

    @staticmethod
    def _split_sql(sql: str) -> list[str]:
        """简单分割 SQL 语句（按分号，忽略注释和空行）。"""
        cleaned = re.sub(r'--[^\n]*', '', sql)
        parts = [s.strip() for s in cleaned.split(';')]
        return [p for p in parts if p and not p.startswith('--')]

    def fetch_df(self, sql: str, params: Iterable[Any] | None = None) -> pd.DataFrame:
        return self._manager.fetch_df(sql, params)

    def fetch_one(self, sql: str, params: Iterable[Any] | None = None) -> dict[str, Any] | None:
        return self._manager.fetch_one(sql, params)

    def execute(self, sql: str, params: Iterable[Any] | None = None) -> None:
        self._manager.execute(sql, params)

    def execute_in_transaction(self, sql_list: list[tuple[str, Iterable[Any] | None]]) -> None:
        self._manager.execute_in_transaction(sql_list)

    def insert_dataframe(self, table_name: str, df: pd.DataFrame) -> int:
        return self._manager.insert_dataframe(table_name, df)

    def truncate_table(self, table_name: str) -> None:
        self._manager.execute(f"DELETE FROM `{table_name}`")

    # ========== 告警管理 ==========

    def get_active_alerts(self, grain: str, anchor_date: date) -> pd.DataFrame:
        """获取待处理的活跃告警。"""
        sql = """
        SELECT
            e.alert_id, e.alert_date, e.severity, e.target_level, e.target_key,
            e.rule_code, e.metric_name, e.metric_value, e.threshold_value,
            e.alert_message,
            COALESCE(s.alert_status, CASE WHEN e.is_resolved = 1 THEN 'resolved' ELSE 'open' END) AS alert_status,
            COALESCE(s.owner_name, '') AS owner_name,
            COALESCE(s.handle_note, '') AS handle_note
        FROM fact_alert_event e
        LEFT JOIN (
            SELECT alert_id, alert_status, owner_name, handle_note,
                   ROW_NUMBER() OVER (PARTITION BY alert_id ORDER BY updated_at DESC) AS rn
            FROM fact_alert_status
        ) s ON e.alert_id = s.alert_id AND s.rn = 1
        WHERE e.grain = %s AND e.alert_date = %s
          AND COALESCE(s.alert_status, CASE WHEN e.is_resolved = 1 THEN 'resolved' ELSE 'open' END) != 'resolved'
        ORDER BY
            CASE e.severity WHEN 'P0' THEN 1 WHEN 'P1' THEN 2 ELSE 3 END,
            e.target_level, e.target_key
        """
        return self.fetch_df(sql, [grain, anchor_date])

    def upsert_alert_status(self, alert_id: str, alert_status: str, owner_name: str | None, handle_note: str | None) -> None:
        self.batch_upsert_alert_status([alert_id], alert_status, owner_name, handle_note)

    def batch_upsert_alert_status(
        self,
        alert_ids: Iterable[str],
        alert_status: str,
        owner_name: str | None,
        handle_note: str | None,
    ) -> None:
        normalized_alert_ids = []
        seen_ids: set[str] = set()
        for alert_id in alert_ids:
            normalized_id = str(alert_id).strip()
            if not normalized_id or normalized_id in seen_ids:
                continue
            seen_ids.add(normalized_id)
            normalized_alert_ids.append(normalized_id)

        if not normalized_alert_ids:
            return

        normalized_owner = (owner_name or "").strip() or None
        normalized_note = (handle_note or "").strip() or None
        is_resolved = 1 if alert_status == "resolved" else 0

        sql_list: list[tuple[str, Iterable[Any] | None]] = []
        for normalized_id in normalized_alert_ids:
            sql_list.append(("DELETE FROM fact_alert_status WHERE alert_id = %s", [normalized_id]))
            sql_list.append((
                """
                INSERT INTO fact_alert_status (alert_id, alert_status, owner_name, handle_note, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                """,
                [normalized_id, alert_status, normalized_owner, normalized_note],
            ))
            sql_list.append((
                """
                INSERT INTO fact_alert_status_history (alert_id, alert_status, owner_name, handle_note, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                """,
                [normalized_id, alert_status, normalized_owner, normalized_note],
            ))
            sql_list.append((
                "UPDATE fact_alert_event SET is_resolved = %s WHERE alert_id = %s",
                [is_resolved, normalized_id],
            ))

        self.execute_in_transaction(sql_list)

    def get_alert_history(self, alert_id: str, limit: int = 20) -> pd.DataFrame:
        sql = """
        SELECT alert_status, owner_name, handle_note, updated_at
        FROM fact_alert_status_history
        WHERE alert_id = %s
        ORDER BY updated_at DESC
        LIMIT %s
        """
        return self.fetch_df(sql, [alert_id, int(limit)])

    def get_alerts(self, grain: str, anchor_date: date) -> pd.DataFrame:
        sql = """
        WITH latest_status AS (
            SELECT alert_id, alert_status, owner_name, handle_note, updated_at
            FROM (
                SELECT
                    alert_id,
                    alert_status,
                    owner_name,
                    handle_note,
                    updated_at,
                    ROW_NUMBER() OVER (PARTITION BY alert_id ORDER BY updated_at DESC) AS rn
                FROM fact_alert_status
            ) t
            WHERE rn = 1
        )
        SELECT
            e.alert_id,
            e.alert_date,
            e.severity,
            e.target_level,
            e.target_key,
            e.rule_code,
            COALESCE(r.rule_name, e.rule_code) AS rule_name,
            COALESCE(r.rule_desc, '') AS rule_desc,
            e.metric_name,
            e.metric_value,
            e.threshold_value,
            COALESCE(s.alert_status, CASE WHEN e.is_resolved = 1 THEN 'resolved' ELSE 'open' END) AS alert_status,
            COALESCE(s.owner_name, r.owner_name, '待分配') AS owner_name,
            COALESCE(s.handle_note, '') AS handle_note,
            s.updated_at AS status_updated_at,
            e.created_at AS alert_created_at,
            e.alert_message
        FROM fact_alert_event e
        LEFT JOIN dim_alert_rule r
          ON e.rule_code = r.rule_code
        LEFT JOIN latest_status s
          ON e.alert_id = s.alert_id
        WHERE e.grain = %s AND e.alert_date = %s
        ORDER BY
            CASE e.severity WHEN 'P0' THEN 1 WHEN 'P1' THEN 2 WHEN 'P2' THEN 3 ELSE 4 END,
            CASE COALESCE(s.alert_status, CASE WHEN e.is_resolved = 1 THEN 'resolved' ELSE 'open' END)
                WHEN 'open' THEN 1
                WHEN 'claimed' THEN 2
                WHEN 'ignored' THEN 3
                WHEN 'resolved' THEN 4
                ELSE 5
            END,
            ABS(COALESCE(e.metric_value, 0) - COALESCE(e.threshold_value, 0)) DESC,
            e.target_level,
            e.target_key
        """
        return self.fetch_df(sql, [grain, anchor_date])

    # ==================== 组别/队列概览 ====================

    def get_group_overview(self, grain: str, anchor_date: date) -> pd.DataFrame:
        table = self._summary_table(grain, "group")
        anchor_column = self._anchor_column(grain)
        sql = f"""
        SELECT *
        FROM {table}
        WHERE {anchor_column} = %s
        ORDER BY
            CASE group_name
                WHEN 'A组-评论' THEN 1
                WHEN 'B组-评论' THEN 2
                WHEN 'B组-账号' THEN 3
                ELSE 4
            END,
            final_accuracy_rate ASC,
            qa_cnt DESC
        """
        return self.fetch_df(sql, [anchor_date])

    def get_queue_breakdown(self, grain: str, anchor_date: date, group_name: str) -> pd.DataFrame:
        table = self._summary_table(grain, "queue")
        anchor_column = self._anchor_column(grain)
        sql = f"""
        SELECT *
        FROM {table}
        WHERE {anchor_column} = %s
          AND group_name = %s
        ORDER BY final_accuracy_rate ASC, qa_cnt DESC
        """
        return self.fetch_df(sql, [anchor_date, group_name])

    def get_auditor_breakdown(
        self,
        grain: str,
        anchor_date: date,
        group_name: str,
        queue_name: str | None = None,
        reviewer_name: str | None = None,
    ) -> pd.DataFrame:
        grain_column = self._grain_column(grain)
        conditions = [f"{grain_column} = %s", "sub_biz = %s", "reviewer_name IS NOT NULL"]
        params: list[Any] = [anchor_date, group_name]
        if queue_name:
            conditions.append("queue_name = %s")
            params.append(queue_name)
        if reviewer_name:
            conditions.append("reviewer_name = %s")
            params.append(reviewer_name)
        where_sql = " AND ".join(conditions)
        sql = f"""
        SELECT
            reviewer_name,
            COUNT(*) AS qa_cnt,
            SUM(CASE WHEN is_raw_correct = 1 THEN 1 ELSE 0 END) AS raw_correct_cnt,
            SUM(CASE WHEN is_final_correct = 1 THEN 1 ELSE 0 END) AS final_correct_cnt,
            ROUND(SUM(CASE WHEN is_raw_correct = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 4) AS raw_accuracy_rate,
            ROUND(SUM(CASE WHEN is_final_correct = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 4) AS final_accuracy_rate,
            SUM(CASE WHEN is_misjudge = 1 THEN 1 ELSE 0 END) AS misjudge_cnt,
            SUM(CASE WHEN is_missjudge = 1 THEN 1 ELSE 0 END) AS missjudge_cnt,
            ROUND(SUM(CASE WHEN is_misjudge = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 4) AS misjudge_rate,
            ROUND(SUM(CASE WHEN is_missjudge = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 4) AS missjudge_rate,
            ROUND(SUM(CASE WHEN is_appeal_reversed = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(SUM(CASE WHEN is_appealed = 1 THEN 1 ELSE 0 END), 0), 4) AS appeal_reverse_rate
        FROM vw_qa_base
        WHERE {where_sql}
        GROUP BY 1
        ORDER BY final_accuracy_rate ASC, qa_cnt DESC
        """
        return self.fetch_df(sql, params)

    # ==================== 明细查询 ====================

    def get_issue_samples(
        self,
        grain: str,
        anchor_date: date,
        group_name: str,
        queue_name: str | None = None,
        reviewer_name: str | None = None,
        issue_mode: str | None = None,
        error_type: str | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        grain_column = self._grain_column(grain)
        conditions = [f"{grain_column} = %s", "sub_biz = %s"]
        params: list[Any] = [anchor_date, group_name]

        if queue_name:
            conditions.append("queue_name = %s")
            params.append(queue_name)
        if reviewer_name:
            conditions.append("reviewer_name = %s")
            params.append(reviewer_name)
        if error_type:
            conditions.append("COALESCE(error_type, '') = %s")
            params.append(str(error_type).strip())

        issue_condition_map = {
            "raw_incorrect": "COALESCE(is_raw_correct, 0) = 0",
            "final_incorrect": "COALESCE(is_final_correct, 0) = 0",
            "missjudge": "COALESCE(is_missjudge, 0) = 1",
            "misjudge": "COALESCE(is_misjudge, 0) = 1",
            "appeal_reversed": "COALESCE(is_appeal_reversed, 0) = 1",
        }
        issue_condition = issue_condition_map.get(issue_mode)
        if issue_condition:
            conditions.append(issue_condition)

        where_sql = " AND ".join(conditions)
        sql = f"""
        SELECT
            biz_date,
            qa_time,
            queue_name,
            reviewer_name,
            raw_judgement,
            final_review_result,
            appeal_status,
            appeal_result,
            CASE WHEN is_final_correct = 1 THEN '正确' ELSE '错误' END AS judge_result,
            COALESCE(error_type, '—') AS error_type,
            comment_text,
            qa_note,
            join_key
        FROM vw_qa_base
        WHERE {where_sql}
        ORDER BY qa_time IS NULL, qa_time DESC, biz_date DESC
        LIMIT %s
        """
        params.append(int(limit))
        return self.fetch_df(sql, params)

    def get_join_quality_samples(
        self,
        grain: str,
        anchor_date: date,
        group_name: str | None = None,
        queue_name: str | None = None,
        reviewer_name: str | None = None,
        join_status: str | None = None,
        limit: int = 100,
    ) -> pd.DataFrame:
        conditions = [self._biz_date_filter_sql("d.biz_date", grain)]
        params: list[Any] = [anchor_date]

        if group_name:
            conditions.append("d.group_name = %s")
            params.append(group_name)
        if queue_name:
            conditions.append("d.queue_name = %s")
            params.append(queue_name)
        if reviewer_name:
            conditions.append("d.reviewer_name = %s")
            params.append(reviewer_name)
        if join_status:
            conditions.append("d.join_status = %s")
            params.append(join_status)

        sql = f"""
        SELECT
            d.biz_date,
            b.qa_time,
            d.group_name,
            d.queue_name,
            d.reviewer_name,
            d.join_status,
            d.join_key_type,
            d.join_key,
            d.source_record_id,
            d.comment_id,
            d.dynamic_id,
            d.account_id,
            b.raw_judgement,
            b.final_review_result,
            d.appeal_status,
            d.appeal_result,
            b.comment_text,
            b.qa_note
        FROM vw_join_quality_detail d
        LEFT JOIN vw_qa_base b
          ON d.event_id = b.event_id
        WHERE {' AND '.join(conditions)}
        ORDER BY b.qa_time IS NULL, b.qa_time DESC, d.biz_date DESC
        LIMIT %s
        """
        params.append(int(limit))
        return self.fetch_df(sql, params)

    # ==================== 错误类型 TOP ====================

    def get_error_topics(
        self,
        grain: str,
        anchor_date: date,
        group_name: str,
        queue_name: str | None = None,
        limit: int = 20,
    ) -> pd.DataFrame:
        table = {
            "day": "mart_day_error_topic",
            "week": "mart_week_error_topic",
            "month": "mart_month_error_topic",
        }[grain]
        anchor_column = self._anchor_column(grain)
        conditions = [f"{anchor_column} = %s", "group_name = %s"]
        params: list[Any] = [anchor_date, group_name]
        if queue_name:
            conditions.append("queue_name = %s")
            params.append(queue_name)
        sql = f"""
        SELECT *
        FROM {table}
        WHERE {' AND '.join(conditions)}
        ORDER BY issue_cnt DESC, affected_reviewer_cnt DESC
        LIMIT %s
        """
        params.append(int(limit))
        return self.fetch_df(sql, params)

    # ==================== 培训整改闭环 ====================

    def get_training_action_recovery(
        self,
        selected_date: date,
        group_name: str,
        queue_name: str | None = None,
        error_type: str | None = None,
        limit: int = 20,
    ) -> pd.DataFrame:
        conditions = ["group_name = %s", "action_date <= %s"]
        params: list[Any] = [group_name, selected_date]
        if queue_name:
            conditions.append("queue_name = %s")
            params.append(queue_name)
        if error_type:
            conditions.append("error_type = %s")
            params.append(error_type)

        sql = f"""
        SELECT
            action_id,
            alert_id,
            rule_code,
            severity,
            alert_date,
            action_status,
            action_time,
            action_date,
            action_week_begin_date,
            group_name,
            queue_name,
            error_type,
            owner_name,
            handle_note,
            baseline_issue_cnt,
            baseline_qa_cnt,
            baseline_issue_share,
            week1_issue_cnt,
            week1_qa_cnt,
            week1_issue_share,
            week1_issue_share_change_pp,
            is_recovered_week1,
            week2_issue_cnt,
            week2_qa_cnt,
            week2_issue_share,
            week2_issue_share_change_pp,
            is_recovered_week2,
            recovery_status
        FROM mart_training_action_recovery
        WHERE {' AND '.join(conditions)}
        ORDER BY action_time DESC, action_id DESC
        LIMIT %s
        """
        params.append(int(limit))
        return self.fetch_df(sql, params)

    # ==================== 趋势数据 ====================

    def get_trend_series(self, grain: str, group_name: str, end_anchor_date: date) -> pd.DataFrame:
        if grain == "day":
            sql = """
            SELECT biz_date AS anchor_date, raw_accuracy_rate, final_accuracy_rate
            FROM mart_day_group
            WHERE group_name = %s AND biz_date <= %s
            ORDER BY biz_date DESC
            LIMIT 7
            """
            df = self.fetch_df(sql, [group_name, end_anchor_date])
            return df.sort_values("anchor_date")

        if grain == "week":
            sql = """
            SELECT week_begin_date AS anchor_date, raw_accuracy_rate, final_accuracy_rate
            FROM mart_week_group
            WHERE group_name = %s AND week_begin_date <= %s
            ORDER BY week_begin_date DESC
            LIMIT 8
            """
            df = self.fetch_df(sql, [group_name, end_anchor_date])
            return df.sort_values("anchor_date")

        sql = """
        SELECT month_begin_date AS anchor_date, raw_accuracy_rate, final_accuracy_rate
        FROM mart_month_group
        WHERE group_name = %s AND month_begin_date <= %s
        ORDER BY month_begin_date DESC
        LIMIT 6
        """
        df = self.fetch_df(sql, [group_name, end_anchor_date])
        return df.sort_values("anchor_date")

    # ==================== 维度分布 ====================

    def get_qa_label_distribution(
        self, grain: str, anchor_date: date, group_name: str | None = None, top_n: int = 10
    ) -> pd.DataFrame:
        """获取质检标签分布（qa_result 分布）。"""
        grain_column = self._grain_column(grain)
        conditions = [f"{grain_column} = %s"]
        params: list[Any] = [anchor_date]

        if group_name:
            conditions.append("sub_biz = %s")
            params.append(group_name)

        where_sql = " AND ".join(conditions)
        total_sql = f"SELECT COUNT(*) FROM vw_qa_base WHERE {where_sql}"
        sql = f"""
        SELECT
            qa_result AS label_name,
            COUNT(*) AS cnt,
            ROUND(COUNT(*) * 100.0 / NULLIF(({total_sql}), 0), 2) AS pct
        FROM vw_qa_base
        WHERE {where_sql}
          AND qa_result IS NOT NULL AND qa_result <> ''
        GROUP BY qa_result
        ORDER BY cnt DESC
        LIMIT %s
        """
        return self.fetch_df(sql, params + params + [top_n])

    def get_qa_owner_distribution(
        self, grain: str, anchor_date: date, group_name: str | None = None, top_n: int = 10
    ) -> pd.DataFrame:
        """获取质检员工作量分布。"""
        grain_column = self._grain_column(grain)
        conditions = [f"{grain_column} = %s"]
        params: list[Any] = [anchor_date]

        if group_name:
            conditions.append("sub_biz = %s")
            params.append(group_name)

        where_sql = " AND ".join(conditions)
        sql = f"""
        SELECT
            qa_owner_name AS owner_name,
            COUNT(*) AS qa_cnt,
            SUM(CASE WHEN COALESCE(is_raw_correct, 0) = 0 THEN 1 ELSE 0 END) AS error_cnt,
            ROUND(SUM(CASE WHEN is_raw_correct = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS accuracy_rate
        FROM vw_qa_base
        WHERE {where_sql}
          AND qa_owner_name IS NOT NULL AND qa_owner_name != ''
        GROUP BY qa_owner_name
        ORDER BY qa_cnt DESC
        LIMIT %s
        """
        params.append(top_n)
        return self.fetch_df(sql, params)

    # ==================== 内部辅助方法 ====================

    @staticmethod
    def _summary_table(grain: str, level: str) -> str:
        return {
            ("day", "group"): "mart_day_group",
            ("day", "queue"): "mart_day_queue",
            ("week", "group"): "mart_week_group",
            ("week", "queue"): "mart_week_queue",
            ("month", "group"): "mart_month_group",
            ("month", "queue"): "mart_month_queue",
        }[(grain, level)]

    @staticmethod
    def _anchor_column(grain: str) -> str:
        return {
            "day": "biz_date",
            "week": "week_begin_date",
            "month": "month_begin_date",
        }[grain]

    @staticmethod
    def _grain_column(grain: str) -> str:
        return {
            "day": "biz_date",
            "week": "week_begin_date",
            "month": "month_begin_date",
        }[grain]

    @staticmethod
    def _biz_date_filter_sql(column_name: str, grain: str) -> str:
        """TiDB 版：使用 DATE_SUB 计算周/月起始日。"""
        if grain == "day":
            return f"{column_name} = %s"
        if grain == "week":
            return f"DATE_SUB({column_name}, INTERVAL WEEKDAY({column_name}) DAY) = %s"
        return f"DATE_SUB({column_name}, INTERVAL DAYOFMONTH({column_name}) - 1 DAY) = %s"
