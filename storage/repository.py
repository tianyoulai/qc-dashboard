"""质培运营看板 — 数据访问层（TiDB 版）。"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
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
        errors: list[str] = []

        for idx, stmt in enumerate(statements, start=1):
            stmt = stmt.strip()
            if not stmt:
                continue
            try:
                self._manager.execute(stmt)
            except Exception as exc:
                preview = " ".join(stmt.split())[:120]
                errors.append(f"[{idx}] {preview} -> {exc}")

        if errors:
            details = "\n".join(errors[:10])
            raise RuntimeError(
                f"Schema 初始化失败，共 {len(errors)} 条 SQL 执行失败：\n{details}"
            )

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

    def get_qa_result_distribution(
        self, grain: str, anchor_date: date, group_name: str | None = None,
    ) -> pd.DataFrame:
        """质检结果分布：正确 / 错判 / 漏判 三分类汇总（百分比总和保证为100%）。

        关键设计：
        - 分母 = WHERE 条件后的总记录数（与分子同一数据集）
        - 每条记录有且仅有一种状态，三分类 pct 之和 = 100%
        """
        grain_column = self._grain_column(grain)
        conditions = [f"{grain_column} = %s"]
        params: list[Any] = [anchor_date]

        if group_name:
            conditions.append("sub_biz = %s")
            params.append(group_name)

        where_sql = " AND ".join(conditions)
        # 先查总记录数作为分母
        total_sql = f"SELECT COUNT(*) AS total FROM vw_qa_base WHERE {where_sql}"

        sql = f"""
        SELECT * FROM (
            SELECT
                '正确' AS result_label,
                SUM(CASE WHEN COALESCE(is_misjudge, 0) = 0
                         AND COALESCE(is_missjudge, 0) = 0
                    THEN 1 ELSE 0 END) AS cnt,
                ROUND(SUM(CASE WHEN COALESCE(is_misjudge, 0) = 0
                                 AND COALESCE(is_missjudge, 0) = 0
                            THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(({total_sql}), 0), 2) AS pct
            FROM vw_qa_base WHERE {where_sql}

            UNION ALL

            SELECT
                '错判' AS result_label,
                SUM(CASE WHEN COALESCE(is_misjudge, 0) = 1 THEN 1 ELSE 0 END) AS cnt,
                ROUND(SUM(CASE WHEN COALESCE(is_misjudge, 0) = 1 THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(({total_sql}), 0), 2) AS pct
            FROM vw_qa_base WHERE {where_sql}

            UNION ALL

            SELECT
                '漏判' AS result_label,
                SUM(CASE WHEN COALESCE(is_missjudge, 0) = 1 THEN 1 ELSE 0 END) AS cnt,
                ROUND(SUM(CASE WHEN COALESCE(is_missjudge, 0) = 1 THEN 1 ELSE 0 END) * 100.0
                    / NULLIF(({total_sql}), 0), 2) AS pct
            FROM vw_qa_base WHERE {where_sql}
        ) t
        ORDER BY CASE result_label
                     WHEN '正确' THEN 1
                     WHEN '错判' THEN 2
                     WHEN '漏判' THEN 3
                 END
        """
        # total_sql 需要被展开两次（UNION ALL 每个子查询各一次），params 也需要重复
        return self.fetch_df(sql, params + params + params + params)

    # ==================== Phase 1 新增维度查询 ====================

    def get_error_top5(
        self, grain: str, anchor_date: date, group_name: str | None = None, limit: int = 5
    ) -> pd.DataFrame:
        """高频错误 Top5（从 fact_qa_event 聚合，按错误量降序）。"""
        grain_column = self._grain_column(grain)
        conditions = [f"{grain_column} = %s", "COALESCE(error_type, '') != ''"]
        params: list[Any] = [anchor_date]
        if group_name:
            conditions.append("sub_biz = %s")
            params.append(group_name)
        where_sql = " AND ".join(conditions)
        sql = f"""
        SELECT
            COALESCE(error_type, '未分类') AS error_type,
            COUNT(*) AS error_cnt,
            COUNT(DISTINCT reviewer_name) AS affected_reviewers,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM vw_qa_base WHERE {where_sql}), 2) AS pct
        FROM vw_qa_base
        WHERE {where_sql}
        GROUP BY error_type
        ORDER BY error_cnt DESC
        LIMIT %s
        """
        params.append(limit)
        return self.fetch_df(sql, params)

    def get_label_accuracy(
        self, grain: str, anchor_date: date, group_name: str | None = None, min_cnt: int = 10, limit: int = 15
    ) -> pd.DataFrame:
        """标签准确率排行（回答"哪些标签最难判"）。按正确率升序排列。"""
        grain_column = self._grain_column(grain)
        conditions = [f"{grain_column} = %s", "COALESCE(final_label, '') != ''"]
        params: list[Any] = [anchor_date]
        if group_name:
            conditions.append("sub_biz = %s")
            params.append(group_name)
        where_sql = " AND ".join(conditions)
        sql = f"""
        SELECT
            COALESCE(final_label, '未分类') AS label_name,
            COUNT(*) AS qa_cnt,
            SUM(CASE WHEN COALESCE(is_final_correct, 0) = 1 THEN 1 ELSE 0 END) AS correct_cnt,
            ROUND(SUM(CASE WHEN COALESCE(is_final_correct, 0) = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS accuracy_rate
        FROM vw_qa_base
        WHERE {where_sql}
        GROUP BY final_label
        HAVING qa_cnt >= %s
        ORDER BY accuracy_rate ASC, qa_cnt DESC
        LIMIT %s
        """
        params.extend([min_cnt, limit])
        return self.fetch_df(sql, params)

    def get_content_type_distribution(
        self, grain: str, anchor_date: date, group_name: str | None = None
    ) -> pd.DataFrame:
        """内容类型分布 + 各类型正确率。"""
        grain_column = self._grain_column(grain)
        conditions = [f"{grain_column} = %s"]
        params: list[Any] = [anchor_date]
        if group_name:
            conditions.append("sub_biz = %s")
            params.append(group_name)
        where_sql = " AND ".join(conditions)
        sql = f"""
        SELECT
            COALESCE(NULLIF(TRIM(content_type), ''), '未知') AS content_type,
            COUNT(*) AS qa_cnt,
            SUM(CASE WHEN COALESCE(is_final_correct, 0) = 1 THEN 1 ELSE 0 END) AS correct_cnt,
            ROUND(AVG(CASE WHEN is_final_correct IS NOT NULL THEN is_final_correct ELSE NULL END) * 100, 2) AS accuracy_rate
        FROM vw_qa_base
        WHERE {where_sql}
        GROUP BY content_type
        ORDER BY qa_cnt DESC
        """
        return self.fetch_df(sql, params)

    def get_hourly_heatmap(
        self, grain: str, anchor_date: date, group_name: str | None = None
    ) -> pd.DataFrame:
        """时段质量热力图数据（每小时质检量+正确率）。"""
        grain_column = self._grain_column(grain)
        conditions = [f"{grain_column} = %s", "qa_time IS NOT NULL"]
        params: list[Any] = [anchor_date]
        if group_name:
            conditions.append("sub_biz = %s")
            params.append(group_name)
        where_sql = " AND ".join(conditions)
        sql = f"""
        SELECT
            HOUR(qa_time) AS hour,
            COUNT(*) AS qa_cnt,
            SUM(CASE WHEN COALESCE(is_final_correct, 0) = 1 THEN 1 ELSE 0 END) AS correct_cnt,
            ROUND(AVG(CASE WHEN is_final_correct IS NOT NULL THEN is_final_correct ELSE NULL END) * 100, 2) AS accuracy_rate
        FROM vw_qa_base
        WHERE {where_sql}
        GROUP BY HOUR(qa_time)
        ORDER BY hour
        """
        return self.fetch_df(sql, params)

    def get_appeal_analysis(
        self, grain: str, anchor_date: date, group_name: str | None = None
    ) -> dict:
        """申诉分析多维指标：申诉率、改判成功率、Top申诉理由。返回字典。"""
        grain_column = self._grain_column(grain)
        base_conditions = [f"{grain_column} = %s"]
        base_params: list[Any] = [anchor_date]
        if group_name:
            base_conditions.append("sub_biz = %s")
            base_params.append(group_name)
        base_where = " AND ".join(base_conditions)

        result = {}

        # 1. 申诉总体指标
        summary_sql = f"""
        SELECT
            COUNT(*) AS total_qa,
            SUM(CASE WHEN COALESCE(is_appealed, 0) = 1 THEN 1 ELSE 0 END) AS appeal_cnt,
            ROUND(SUM(CASE WHEN COALESCE(is_appealed, 0) = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS appeal_rate,
            SUM(CASE WHEN COALESCE(is_appeal_reversed, 0) = 1 THEN 1 ELSE 0 END) AS reversed_cnt,
            ROUND(
                CASE WHEN SUM(CASE WHEN COALESCE(is_appealed, 0) = 1 THEN 1 ELSE 0 END) > 0
                    THEN SUM(CASE WHEN COALESCE(is_appeal_reversed, 0) = 1 THEN 1 ELSE 0 END) * 100.0 /
                         SUM(CASE WHEN COALESCE(is_appealed, 0) = 1 THEN 1 ELSE 0 END)
                ELSE 0 END, 2
            ) AS reverse_success_rate
        FROM vw_qa_base WHERE {base_where}
        """
        summary_row = self.fetch_one(summary_sql, base_params)
        result["summary"] = summary_row if summary_row else {}

        # 2. Top 申诉理由（从 fact_appeal_event 取）
        reason_params = list(base_params)
        # 计算起始日期用于范围查询
        if grain == "day":
            start_date = anchor_date
        elif grain == "week":
            start_date = anchor_date
        else:
            start_date = anchor_date

        reason_sql = f"""
        SELECT
            COALESCE(NULLIF(TRIM(appeal_reason), ''), '未填写') AS appeal_reason,
            COUNT(*) AS cnt,
            ROUND(COUNT(*) * 100.0 / NULLIF((SELECT COUNT(*) FROM fact_appeal_event
                WHERE biz_date BETWEEN %s AND %s
                {'AND (' + ' OR '.join(f'group_name LIKE %s' for _ in [group_name]) + ')' if group_name else ''}
            ), 0), 2) AS pct
        FROM fact_appeal_event
        WHERE biz_date BETWEEN %s AND %s
        """
        reason_params = [start_date, anchor_date]
        if group_name:
            reason_sql += " AND group_name LIKE %s"
            reason_params.append(f"%{group_name.split('-')[0] if '-' in group_name else group_name}%")
        reason_params.extend([start_date, anchor_date])
        if group_name:
            reason_params.append(f"%{group_name.split('-')[0] if '-' in group_name else group_name}%")

        reason_sql += " GROUP BY appeal_reason ORDER BY cnt DESC LIMIT 8"
        try:
            result["reasons"] = self.fetch_df(reason_sql, reason_params)
        except Exception:
            result["reasons"] = pd.DataFrame()

        # 3. 按队列申诉率
        queue_appeal_sql = f"""
        SELECT
            queue_name,
            COUNT(*) AS qa_cnt,
            SUM(CASE WHEN COALESCE(is_appealed, 0) = 1 THEN 1 ELSE 0 END) AS appeal_cnt,
            ROUND(SUM(CASE WHEN COALESCE(is_appealed, 0) = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS appeal_rate,
            ROUND(
                CASE WHEN SUM(CASE WHEN COALESCE(is_appealed, 0) = 1 THEN 1 ELSE 0 END) > 0
                    THEN SUM(CASE WHEN COALESCE(is_appeal_reversed, 0) = 1 THEN 1 ELSE 0 END) * 100.0 /
                         SUM(CASE WHEN COALESCE(is_appealed, 0) = 1 THEN 1 ELSE 0 END)
                ELSE 0 END, 2
            ) AS reverse_rate
        FROM vw_qa_base
        WHERE {base_where} AND queue_name IS NOT NULL
        GROUP BY queue_name
        HAVING appeal_cnt >= 3
        ORDER BY appeal_rate DESC
        LIMIT 10
        """
        try:
            result["queue_appeal"] = self.fetch_df(queue_appeal_sql, list(base_params))
        except Exception:
            result["queue_appeal"] = pd.DataFrame()

        return result

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

    # ==================== Phase 2 新增维度查询 ====================

    def get_inspect_type_distribution(
        self, grain: str, anchor_date: date, group_name: str | None = None
    ) -> pd.DataFrame:
        """inspect_type 分布（外检/内部）+ 各类型正确率。"""
        grain_column = self._grain_column(grain)
        conditions = [f"{grain_column} = %s"]
        params: list[Any] = [anchor_date]
        if group_name:
            conditions.append("sub_biz = %s")
            params.append(group_name)
        where_sql = " AND ".join(conditions)
        sql = f"""
        SELECT
            COALESCE(NULLIF(TRIM(inspect_type), ''), '未知') AS inspect_type,
            COUNT(*) AS qa_cnt,
            SUM(CASE WHEN COALESCE(is_final_correct, 0) = 1 THEN 1 ELSE 0 END) AS correct_cnt,
            ROUND(AVG(CASE WHEN is_final_correct IS NOT NULL THEN is_final_correct ELSE NULL END) * 100, 2) AS accuracy_rate
        FROM vw_qa_base
        WHERE {where_sql}
        GROUP BY inspect_type
        ORDER BY qa_cnt DESC
        """
        return self.fetch_df(sql, params)

    def get_workforce_type_distribution(
        self, grain: str, anchor_date: date, group_name: str | None = None
    ) -> pd.DataFrame:
        """workforce_type 分布（正式/新人/复培/外检）+ 各类型正确率。"""
        grain_column = self._grain_column(grain)
        conditions = [f"{grain_column} = %s"]
        params: list[Any] = [anchor_date]
        if group_name:
            conditions.append("sub_biz = %s")
            params.append(group_name)
        where_sql = " AND ".join(conditions)
        sql = f"""
        SELECT
            COALESCE(NULLIF(TRIM(workforce_type), ''), '未知') AS workforce_type,
            COUNT(*) AS qa_cnt,
            SUM(CASE WHEN COALESCE(is_final_correct, 0) = 1 THEN 1 ELSE 0 END) AS correct_cnt,
            ROUND(AVG(CASE WHEN is_final_correct IS NOT NULL THEN is_final_correct ELSE NULL END) * 100, 2) AS accuracy_rate
        FROM vw_qa_base
        WHERE {where_sql}
        GROUP BY workforce_type
        ORDER BY qa_cnt DESC
        """
        return self.fetch_df(sql, params)

    def get_error_type_trend(
        self, anchor_date: date, group_name: str | None = None, days: int = 14, top_n: int = 3
    ) -> pd.DataFrame:
        """Top N 错误类型的日趋势（过去 days 天），用于面积图。

        返回列: biz_date, error_type, error_cnt
        """
        start_date = anchor_date - timedelta(days=days - 1)
        conditions = ["biz_date BETWEEN %s AND %s", "COALESCE(error_type, '') != ''"]
        params: list[Any] = [start_date, anchor_date]
        if group_name:
            conditions.append("sub_biz = %s")
            params.append(group_name)
        where_sql = " AND ".join(conditions)

        # 先找出 Top N 错误类型
        top_sql = f"""
        SELECT COALESCE(error_type, '未分类') AS error_type
        FROM vw_qa_base
        WHERE {where_sql}
        GROUP BY error_type
        ORDER BY COUNT(*) DESC
        LIMIT %s
        """
        params_copy = list(params)
        params_copy.append(top_n)
        top_df = self.fetch_df(top_sql, params_copy)
        if top_df.empty:
            return pd.DataFrame(columns=["biz_date", "error_type", "error_cnt"])

        top_types = top_df["error_type"].tolist()

        # 再拉趋势
        placeholders = ", ".join(["%s"] * len(top_types))
        trend_sql = f"""
        SELECT
            biz_date,
            COALESCE(error_type, '未分类') AS error_type,
            COUNT(*) AS error_cnt
        FROM vw_qa_base
        WHERE {where_sql}
          AND error_type IN ({placeholders})
        GROUP BY biz_date, error_type
        ORDER BY biz_date, error_type
        """
        trend_params = list(params) + top_types
        return self.fetch_df(trend_sql, trend_params)

    def get_error_affected_reviewers(
        self, grain: str, anchor_date: date, group_name: str | None = None, error_type: str | None = None, top_n: int = 10
    ) -> pd.DataFrame:
        """指定错误类型下受影响的审核人 Top N。用于下探链路。"""
        grain_column = self._grain_column(grain)
        conditions = [f"{grain_column} = %s", "COALESCE(error_type, '') != ''"]
        params: list[Any] = [anchor_date]
        if group_name:
            conditions.append("sub_biz = %s")
            params.append(group_name)
        if error_type:
            conditions.append("error_type = %s")
            params.append(error_type)
        where_sql = " AND ".join(conditions)

        sql = f"""
        SELECT
            reviewer_name,
            queue_name,
            error_type,
            COUNT(*) AS error_cnt,
            ROUND(COUNT(*) * 100.0 / NULLIF(
                (SELECT COUNT(*) FROM vw_qa_base WHERE {grain_column} = %s
                 {'AND sub_biz = %s' if group_name else ''}
                 AND reviewer_name = t.reviewer_name), 0), 2) AS error_rate
        FROM vw_qa_base t
        WHERE {where_sql}
          AND reviewer_name IS NOT NULL AND reviewer_name != ''
        GROUP BY reviewer_name, queue_name, error_type
        ORDER BY error_cnt DESC
        LIMIT %s
        """
        sub_params = [anchor_date]
        if group_name:
            sub_params.append(group_name)
        params.extend(sub_params)
        params.append(top_n)
        return self.fetch_df(sql, params)

    def get_data_health_indicators(
        self, anchor_date: date
    ) -> dict:
        """数据健康指标：关联率、缺失主键率、重复率。"""
        result = {}

        # 1. 质检-申诉关联率
        join_sql = """
        SELECT
            COUNT(*) AS total_qa,
            SUM(CASE WHEN is_appealed = 1 THEN 1 ELSE 0 END) AS has_appeal,
            ROUND(SUM(CASE WHEN is_appealed = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS appeal_match_rate
        FROM vw_qa_base
        WHERE biz_date = %s
        """
        join_row = self.fetch_one(join_sql, [anchor_date])
        result["appeal_match"] = join_row if join_row else {}

        # 2. 缺失主键率（join_key 为空的比例）
        missing_sql = """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN COALESCE(join_key, '') = '' THEN 1 ELSE 0 END) AS missing_cnt,
            ROUND(SUM(CASE WHEN COALESCE(join_key, '') = '' THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS missing_rate
        FROM fact_qa_event
        WHERE biz_date = %s
        """
        missing_row = self.fetch_one(missing_sql, [anchor_date])
        result["missing_key"] = missing_row if missing_row else {}

        # 3. 重复率（按 row_hash 去重检测）
        dup_sql = """
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT row_hash) AS unique_cnt,
            COUNT(*) - COUNT(DISTINCT row_hash) AS dup_cnt,
            ROUND((COUNT(*) - COUNT(DISTINCT row_hash)) * 100.0 / NULLIF(COUNT(*), 0), 2) AS dup_rate
        FROM fact_qa_event
        WHERE biz_date = %s
        """
        dup_row = self.fetch_one(dup_sql, [anchor_date])
        result["duplicate"] = dup_row if dup_row else {}

        # 4. 申诉表关联率（有 appeal 对应到 qa_event 的比例）
        appeal_link_sql = """
        SELECT
            COUNT(*) AS total_appeals,
            SUM(CASE WHEN qa_event_id IS NOT NULL THEN 1 ELSE 0 END) AS linked_cnt,
            ROUND(SUM(CASE WHEN qa_event_id IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS link_rate
        FROM fact_appeal_event
        WHERE biz_date = %s
        """
        appeal_row = self.fetch_one(appeal_link_sql, [anchor_date])
        result["appeal_link"] = appeal_row if appeal_row else {}

        return result

    def get_error_reason_wordcloud(
        self, grain: str, anchor_date: date, group_name: str | None = None, top_n: int = 20
    ) -> pd.DataFrame:
        """error_reason 简单词频统计（分词后的 Top N 高频词）。"""
        grain_column = self._grain_column(grain)
        conditions = [f"{grain_column} = %s", "COALESCE(error_reason, '') != ''"]
        params: list[Any] = [anchor_date]
        if group_name:
            conditions.append("sub_biz = %s")
            params.append(group_name)
        where_sql = " AND ".join(conditions)

        sql = f"""
        SELECT
            COALESCE(NULLIF(TRIM(error_reason), ''), '未填写') AS error_reason,
            COUNT(*) AS cnt
        FROM vw_qa_base
        WHERE {where_sql}
        GROUP BY error_reason
        ORDER BY cnt DESC
        LIMIT %s
        """
        params.append(top_n)
        return self.fetch_df(sql, params)

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
