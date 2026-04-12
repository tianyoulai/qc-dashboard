from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

import pandas as pd

from storage.repository import DashboardRepository


import numpy as np

SEVERITY_ORDER = {"P0": 0, "P1": 1, "P2": 2}
ALERT_STATUS_LABELS = {
    "open": "待处理",
    "claimed": "已认领",
    "ignored": "已忽略",
    "resolved": "已解决",
}
ALERT_STATUS_ORDER = {"open": 0, "claimed": 1, "ignored": 2, "resolved": 3}
ALERT_TARGET_LEVEL_LABELS = {"system": "全局", "group": "组别", "queue": "队列"}
ALERT_SLA_DEFAULT_HOURS = {
    "open": {"P0": 12.0, "P1": 24.0, "P2": 48.0},
    "claimed": {"P0": 24.0, "P1": 48.0, "P2": 72.0},
}
ALERT_SLA_RULE_HOURS = {
    "JOIN_MATCH_LT_85_DAY": {
        "open": {"P0": 4.0, "P1": 8.0},
        "claimed": {"P0": 8.0, "P1": 16.0},
    },
    "MISSING_JOIN_KEY_GT_10_DAY": {
        "open": {"P1": 8.0, "P2": 24.0},
        "claimed": {"P1": 24.0, "P2": 48.0},
    },
    "RAW_ACC_LT_99_DAY": {
        "open": {"P1": 12.0, "P2": 24.0},
        "claimed": {"P1": 24.0, "P2": 48.0},
    },
    "FINAL_ACC_LT_99_QUEUE_DAY": {
        "open": {"P1": 8.0, "P2": 24.0},
        "claimed": {"P1": 24.0, "P2": 48.0},
    },
    "MISS_RATE_GT_035_DAY": {
        "open": {"P1": 8.0, "P2": 24.0},
        "claimed": {"P1": 24.0, "P2": 48.0},
    },
    "APPEAL_REV_GT_18_DAY": {
        "open": {"P2": 24.0},
        "claimed": {"P2": 72.0},
    },
}


@dataclass
class DashboardService:
    db_path: str = ""

    def __post_init__(self) -> None:
        self.repo = DashboardRepository(self.db_path)

    def ensure_schema(self) -> None:
        self.repo.initialize_schema()

    def normalize_anchor_date(self, grain: str, selected_date: date) -> date:
        if grain == "day":
            return selected_date
        if grain == "week":
            return selected_date - timedelta(days=selected_date.weekday())
        return selected_date.replace(day=1)

    def has_any_data(self) -> bool:
        if not self.repo.database_exists():
            return False
        try:
            row = self.repo.fetch_one("SELECT COUNT(*) AS cnt FROM fact_qa_event")
            return bool(row and row.get("cnt", 0) > 0)
        except Exception:
            return False

    def load_dashboard_payload(self, grain: str, selected_date: date) -> dict[str, Any]:
        anchor_date = self.normalize_anchor_date(grain, selected_date)
        alerts_df = self.repo.get_alerts(grain, anchor_date)
        group_df = self.repo.get_group_overview(grain, anchor_date)
        if group_df.empty:
            return {
                "anchor_date": anchor_date,
                "group_df": group_df,
                "queue_df": pd.DataFrame(),
                "auditor_df": pd.DataFrame(),
                "sample_df": pd.DataFrame(),
                "alert_sample_df": pd.DataFrame(),
                "alert_sample_title": "告警关联样本",
                "error_df": pd.DataFrame(),
                "trend_df": pd.DataFrame(),
                "alerts_df": alerts_df,
                "alert_summary": self.summarize_alerts(alerts_df),
                "alert_actions": self.build_alert_actions(alerts_df),
                "actions": [],
            }

        selected_group = str(group_df.iloc[0]["group_name"])
        queue_df = self.repo.get_queue_breakdown(grain, anchor_date, selected_group)
        selected_queue = str(queue_df.iloc[0]["queue_name"]) if not queue_df.empty else None

        # 审核人查询不依赖队列选择，直接查全组
        auditor_df = self.repo.get_auditor_breakdown(grain, anchor_date, selected_group, queue_name=None)
        selected_auditor = str(auditor_df.iloc[0]["reviewer_name"]) if not auditor_df.empty else None

        sample_df = self.repo.get_issue_samples(
            grain=grain,
            anchor_date=anchor_date,
            group_name=selected_group,
            queue_name=selected_queue,
            reviewer_name=selected_auditor,
            limit=100,
        )
        error_df = self.repo.get_error_topics(grain, anchor_date, selected_group, selected_queue, limit=20)
        trend_df = self.repo.get_trend_series(grain, selected_group, anchor_date)

        return {
            "anchor_date": anchor_date,
            "group_df": group_df,
            "queue_df": queue_df,
            "auditor_df": auditor_df,
            "sample_df": sample_df,
            "alert_sample_df": pd.DataFrame(),
            "alert_sample_title": "告警关联样本",
            "error_df": error_df,
            "trend_df": trend_df,
            "alerts_df": alerts_df,
            "alert_summary": self.summarize_alerts(alerts_df),
            "alert_actions": self.build_alert_actions(alerts_df, selected_group, selected_queue),
            "actions": self.build_training_actions(queue_df, error_df, auditor_df),
        }

    def load_group_payload(
        self,
        grain: str,
        selected_date: date,
        group_name: str,
        queue_name: str | None,
        reviewer_name: str | None,
        focus_rule_code: str | None = None,
        focus_error_type: str | None = None,
    ) -> dict[str, Any]:
        anchor_date = self.normalize_anchor_date(grain, selected_date)
        alerts_df = self.repo.get_alerts(grain, anchor_date)
        queue_df = self.repo.get_queue_breakdown(grain, anchor_date, group_name)
        normalized_queue = queue_name or (str(queue_df.iloc[0]["queue_name"]) if not queue_df.empty else None)

        # 审核人查询：用户明确选择队列时按队列筛选，否则查全组
        # 如果用户选择了审核人，只返回该审核人的数据行
        auditor_df = self.repo.get_auditor_breakdown(
            grain, anchor_date, group_name, queue_name=queue_name, reviewer_name=reviewer_name
        )
        normalized_reviewer = reviewer_name or (str(auditor_df.iloc[0]["reviewer_name"]) if not auditor_df.empty else None)

        sample_df = self.repo.get_issue_samples(
            grain=grain,
            anchor_date=anchor_date,
            group_name=group_name,
            queue_name=normalized_queue,
            reviewer_name=normalized_reviewer,
            limit=100,
        )
        error_df = self.repo.get_error_topics(grain, anchor_date, group_name, normalized_queue, limit=20)
        trend_df = self.repo.get_trend_series(grain, group_name, anchor_date)
        training_recovery_df = self.repo.get_training_action_recovery(
            selected_date=selected_date,
            group_name=group_name,
            queue_name=normalized_queue,
            error_type=self.normalize_text(focus_error_type) or None,
            limit=20,
        )
        alert_sample_df, alert_sample_title = self.build_alert_sample_payload(
            grain=grain,
            anchor_date=anchor_date,
            focus_rule_code=focus_rule_code,
            focus_error_type=focus_error_type,
            group_name=group_name,
            queue_name=normalized_queue,
            reviewer_name=normalized_reviewer,
        )

        return {
            "anchor_date": anchor_date,
            "queue_df": queue_df,
            "auditor_df": auditor_df,
            "sample_df": sample_df,
            "alert_sample_df": alert_sample_df,
            "alert_sample_title": alert_sample_title,
            "error_df": error_df,
            "trend_df": trend_df,
            "training_recovery_df": training_recovery_df,
            "training_recovery_summary": self.summarize_training_recovery(training_recovery_df),
            "alerts_df": alerts_df,
            "alert_summary": self.summarize_alerts(alerts_df),
            "alert_actions": self.build_alert_actions(alerts_df, group_name, normalized_queue),
            "actions": self.build_training_actions(queue_df, error_df, auditor_df),
            "selected_queue": normalized_queue,
            "selected_reviewer": normalized_reviewer,
        }

    def build_alert_sample_payload(
        self,
        grain: str,
        anchor_date: date,
        focus_rule_code: str | None,
        focus_error_type: str | None,
        group_name: str,
        queue_name: str | None,
        reviewer_name: str | None,
    ) -> tuple[pd.DataFrame, str]:
        rule_code = self.normalize_text(focus_rule_code)
        if not rule_code:
            return pd.DataFrame(), "告警关联样本"

        if rule_code == "JOIN_MATCH_LT_85_DAY":
            df = self.repo.get_join_quality_samples(
                grain=grain,
                anchor_date=anchor_date,
                group_name=group_name,
                queue_name=queue_name,
                reviewer_name=reviewer_name,
                join_status="unmatched",
                limit=100,
            )
            return df, "告警关联样本 · 联表未命中"

        if rule_code == "MISSING_JOIN_KEY_GT_10_DAY":
            df = self.repo.get_join_quality_samples(
                grain=grain,
                anchor_date=anchor_date,
                group_name=group_name,
                queue_name=queue_name,
                reviewer_name=reviewer_name,
                join_status="missing_join_key",
                limit=100,
            )
            return df, "告警关联样本 · 缺失关联主键"

        issue_mode_map = {
            "RAW_ACC_LT_99_DAY": ("raw_incorrect", "告警关联样本 · 原始质检错误", None),
            "RAW_ACC_DROP_GT_1P5_WEEK": ("raw_incorrect", "告警关联样本 · 周原始质检错误样本", None),
            "FINAL_ACC_LT_99_QUEUE_DAY": ("final_incorrect", "告警关联样本 · 终审后仍错误", None),
            "MISS_RATE_GT_035_DAY": ("missjudge", "告警关联样本 · 漏判样本", None),
            "MISS_RATE_SPIKE_GT_0P2_QUEUE_WEEK": ("missjudge", "告警关联样本 · 周漏判上升样本", None),
            "APPEAL_REV_GT_18_DAY": ("appeal_reversed", "告警关联样本 · 申诉改判样本", None),
            "TOP_ERROR_SHARE_GT_35_QUEUE_MONTH": (None, "告警关联样本 · 月度结构问题样本", None),
            "ERROR_TYPE_SHARE_GT_15_QUEUE_WEEK": (
                None,
                f"告警关联样本 · 连续两周未收敛错误（{self.normalize_text(focus_error_type) or '目标错误类型'}）",
                self.normalize_text(focus_error_type) or None,
            ),
        }
        issue_mode, title, error_type = issue_mode_map.get(rule_code, (None, "告警关联样本", None))
        if rule_code not in issue_mode_map:
            return pd.DataFrame(), title

        df = self.repo.get_issue_samples(
            grain=grain,
            anchor_date=anchor_date,
            group_name=group_name,
            queue_name=queue_name,
            reviewer_name=reviewer_name,
            issue_mode=issue_mode,
            error_type=error_type,
            limit=100,
        )
        return df, title

    @staticmethod
    def summarize_alerts(alerts_df: pd.DataFrame) -> dict[str, int]:
        if alerts_df.empty or "severity" not in alerts_df.columns:
            return {"total": 0, "P0": 0, "P1": 0, "P2": 0}
        return {
            "total": int(len(alerts_df)),
            "P0": int((alerts_df["severity"] == "P0").sum()),
            "P1": int((alerts_df["severity"] == "P1").sum()),
            "P2": int((alerts_df["severity"] == "P2").sum()),
        }

    @staticmethod
    def summarize_training_recovery(training_recovery_df: pd.DataFrame) -> dict[str, int]:
        if training_recovery_df.empty:
            return {"total": 0, "week1_recovered": 0, "week2_recovered": 0, "week2_unrecovered": 0}

        recovery_status = training_recovery_df.get("recovery_status", pd.Series(dtype="string")).fillna("")
        week1_recovered = pd.Series(False, index=training_recovery_df.index)
        week2_recovered = pd.Series(False, index=training_recovery_df.index)
        if "is_recovered_week1" in training_recovery_df.columns:
            week1_recovered = training_recovery_df["is_recovered_week1"].fillna(False).astype(bool)
        if "is_recovered_week2" in training_recovery_df.columns:
            week2_recovered = training_recovery_df["is_recovered_week2"].fillna(False).astype(bool)

        return {
            "total": int(len(training_recovery_df)),
            "week1_recovered": int(week1_recovered.sum()),
            "week2_recovered": int(week2_recovered.sum()),
            "week2_unrecovered": int(recovery_status.eq("2周未回收").sum()),
        }

    @staticmethod
    def normalize_alert_status(value: Any) -> str:
        text = DashboardService.normalize_text(value).lower()
        return text if text in ALERT_STATUS_LABELS else "open"

    @staticmethod
    def get_alert_status_label(value: Any) -> str:
        return ALERT_STATUS_LABELS[DashboardService.normalize_alert_status(value)]

    @staticmethod
    def summarize_alert_status(alerts_df: pd.DataFrame) -> dict[str, int]:
        if alerts_df.empty or "alert_status" not in alerts_df.columns:
            return {key: 0 for key in ALERT_STATUS_LABELS}

        normalized = alerts_df["alert_status"].apply(DashboardService.normalize_alert_status)
        return {key: int((normalized == key).sum()) for key in ALERT_STATUS_LABELS}

    @staticmethod
    def get_alert_target_level_label(value: Any) -> str:
        text = DashboardService.normalize_text(value).lower()
        return ALERT_TARGET_LEVEL_LABELS.get(text, DashboardService.normalize_text(value) or "未知")

    @staticmethod
    def filter_alerts(
        alerts_df: pd.DataFrame,
        severity_filters: list[str] | None = None,
        status_filters: list[str] | None = None,
        target_levels: list[str] | None = None,
        keyword: str | None = None,
    ) -> pd.DataFrame:
        enriched = DashboardService.enrich_alerts(alerts_df)
        if enriched.empty:
            return enriched

        filtered = enriched.copy()
        if severity_filters:
            filtered = filtered[filtered["severity"].isin(severity_filters)]
        if status_filters:
            normalized_status_filters = [
                DashboardService.normalize_alert_status(value)
                for value in status_filters
                if DashboardService.normalize_text(value)
            ]
            filtered = filtered[filtered["alert_status"].isin(normalized_status_filters)]
        if target_levels:
            normalized_target_levels = [
                DashboardService.normalize_text(value).lower()
                for value in target_levels
                if DashboardService.normalize_text(value)
            ]
            filtered = filtered[filtered["target_level"].isin(normalized_target_levels)]
        normalized_keyword = DashboardService.normalize_text(keyword).lower()
        if normalized_keyword:
            keyword_series = (
                filtered[["rule_name", "target_key", "owner_name", "handle_note"]]
                .fillna("")
                .astype(str)
                .agg(" ".join, axis=1)
                .str.lower()
            )
            filtered = filtered[keyword_series.str.contains(normalized_keyword, na=False)]
        return filtered.reset_index(drop=True)

    def update_alert_status(self, alert_id: str, alert_status: str, owner_name: str | None, handle_note: str | None) -> None:
        normalized_status = self.normalize_alert_status(alert_status)
        self.repo.upsert_alert_status(alert_id, normalized_status, owner_name, handle_note)

    def bulk_update_alert_status(
        self,
        alert_ids: list[str],
        alert_status: str,
        owner_name: str | None,
        handle_note: str | None,
    ) -> int:
        normalized_ids: list[str] = []
        seen_ids: set[str] = set()
        for alert_id in alert_ids:
            normalized_id = self.normalize_text(alert_id)
            if not normalized_id or normalized_id in seen_ids:
                continue
            seen_ids.add(normalized_id)
            normalized_ids.append(normalized_id)

        if not normalized_ids:
            return 0

        normalized_status = self.normalize_alert_status(alert_status)
        self.repo.batch_upsert_alert_status(normalized_ids, normalized_status, owner_name, handle_note)
        return len(normalized_ids)

    def load_alert_history(self, alert_id: str, limit: int = 20) -> pd.DataFrame:
        history_df = self.repo.get_alert_history(alert_id, limit=limit)
        if history_df.empty:
            return history_df

        normalized = history_df.copy()
        normalized["alert_status"] = normalized["alert_status"].apply(self.normalize_alert_status)
        normalized["alert_status_label"] = normalized["alert_status"].apply(self.get_alert_status_label)
        normalized["owner_name"] = normalized["owner_name"].fillna("").astype(str).str.strip().replace("", "—")
        normalized["handle_note"] = normalized["handle_note"].fillna("").astype(str).str.strip().replace("", "—")
        normalized["updated_at"] = pd.to_datetime(normalized["updated_at"], errors="coerce")
        return normalized

    @staticmethod
    def get_alert_sla_limit_hours(rule_code: Any, severity: Any, alert_status: Any) -> float | None:
        normalized_status = DashboardService.normalize_alert_status(alert_status)
        if normalized_status not in {"open", "claimed"}:
            return None

        normalized_severity = DashboardService.normalize_text(severity).upper()
        normalized_rule_code = DashboardService.normalize_text(rule_code).upper()
        rule_config = ALERT_SLA_RULE_HOURS.get(normalized_rule_code, {})
        if normalized_severity in rule_config.get(normalized_status, {}):
            return float(rule_config[normalized_status][normalized_severity])
        if normalized_severity in ALERT_SLA_DEFAULT_HOURS.get(normalized_status, {}):
            return float(ALERT_SLA_DEFAULT_HOURS[normalized_status][normalized_severity])
        return None

    @staticmethod
    def get_alert_sla_snapshot(row: pd.Series | dict[str, Any]) -> dict[str, Any]:
        normalized_status = DashboardService.normalize_alert_status(row.get("alert_status"))
        status_label = DashboardService.get_alert_status_label(normalized_status)
        stage_started_at = row.get("status_updated_at") if normalized_status == "claimed" else row.get("alert_created_at")
        if stage_started_at is None and normalized_status == "claimed":
            stage_started_at = row.get("alert_created_at")
        if stage_started_at is None:
            stage_started_at = row.get("status_updated_at")
        stage_started_at = pd.to_datetime(stage_started_at, errors="coerce")

        if normalized_status in {"ignored", "resolved"}:
            return {
                "sla_limit_hours": None,
                "sla_elapsed_hours": None,
                "sla_remaining_hours": None,
                "sla_deadline_at": pd.NaT,
                "sla_stage_started_at": stage_started_at,
                "sla_is_overdue": False,
                "sla_label": f"{status_label}（不计 SLA）",
            }

        limit_hours = DashboardService.get_alert_sla_limit_hours(
            rule_code=row.get("rule_code"),
            severity=row.get("severity"),
            alert_status=normalized_status,
        )
        if limit_hours is None or pd.isna(stage_started_at):
            return {
                "sla_limit_hours": limit_hours,
                "sla_elapsed_hours": None,
                "sla_remaining_hours": None,
                "sla_deadline_at": pd.NaT,
                "sla_stage_started_at": stage_started_at,
                "sla_is_overdue": False,
                "sla_label": f"{status_label}（缺少 SLA 起点）",
            }

        now_ts = pd.Timestamp.now()
        elapsed_hours = max((now_ts - stage_started_at).total_seconds() / 3600.0, 0.0)
        remaining_hours = float(limit_hours) - elapsed_hours
        deadline_at = stage_started_at + pd.Timedelta(hours=float(limit_hours))
        is_overdue = remaining_hours < 0
        if is_overdue:
            sla_label = f"{status_label} SLA 已超时 {abs(remaining_hours):.1f}h"
        else:
            sla_label = f"{status_label} SLA 剩余 {remaining_hours:.1f}h"

        return {
            "sla_limit_hours": float(limit_hours),
            "sla_elapsed_hours": round(elapsed_hours, 2),
            "sla_remaining_hours": round(remaining_hours, 2),
            "sla_deadline_at": deadline_at,
            "sla_stage_started_at": stage_started_at,
            "sla_is_overdue": bool(is_overdue),
            "sla_label": sla_label,
        }

    @staticmethod
    def summarize_alert_sla(alerts_df: pd.DataFrame) -> dict[str, int]:
        enriched = DashboardService.enrich_alerts(alerts_df)
        if enriched.empty or "sla_is_overdue" not in enriched.columns:
            return {"total_overdue": 0, "open_overdue": 0, "claimed_overdue": 0}

        overdue_mask = enriched["sla_is_overdue"].fillna(False)
        return {
            "total_overdue": int(overdue_mask.sum()),
            "open_overdue": int((overdue_mask & enriched["alert_status"].eq("open")).sum()),
            "claimed_overdue": int((overdue_mask & enriched["alert_status"].eq("claimed")).sum()),
        }

    @staticmethod
    def get_sla_policy_text() -> str:
        return "SLA 规则：待处理按告警创建时间计时，已认领按最近状态更新时间计时；规则级时限优先，未配置时回落到 P0/P1/P2 默认时限；已忽略 / 已解决不再计时。"

    @staticmethod
    def parse_alert_target(target_level: Any, target_key: Any) -> tuple[str | None, str | None]:
        normalized_level = DashboardService.normalize_text(target_level)
        normalized_key = DashboardService.normalize_text(target_key)
        if not normalized_key:
            return None, None

        base_target = normalized_key.split("｜", 1)[0].strip()
        if normalized_level == "group":
            return base_target or None, None
        if normalized_level == "queue":
            if " / " in base_target:
                group_name, queue_name = base_target.split(" / ", 1)
                return group_name.strip() or None, queue_name.strip() or None
            return None, base_target or None
        return None, None

    @staticmethod
    def parse_alert_detail(target_key: Any) -> dict[str, str | None]:
        normalized_key = DashboardService.normalize_text(target_key)
        if "｜" not in normalized_key:
            return {"error_type": None}

        detail_text = normalized_key.split("｜", 1)[1]
        error_type: str | None = None
        for part in detail_text.split("｜"):
            detail_part = part.strip()
            if detail_part.startswith("错误类型="):
                error_type = detail_part.split("=", 1)[1].strip() or None
                break
        return {"error_type": error_type}

    @staticmethod
    def enrich_alerts(alerts_df: pd.DataFrame) -> pd.DataFrame:
        if alerts_df.empty:
            return alerts_df.copy()

        enriched = alerts_df.copy()
        groups: list[str | None] = []
        queues: list[str | None] = []
        for _, row in enriched.iterrows():
            group_name, queue_name = DashboardService.parse_alert_target(row.get("target_level"), row.get("target_key"))
            groups.append(group_name)
            queues.append(queue_name)

        enriched["alert_group_name"] = groups
        enriched["alert_queue_name"] = queues
        enriched["severity_rank"] = enriched["severity"].map(SEVERITY_ORDER).fillna(99)
        if "alert_status" in enriched.columns:
            status_series = enriched["alert_status"]
        else:
            status_series = pd.Series(["open"] * len(enriched), index=enriched.index)
        enriched["alert_status"] = status_series.apply(DashboardService.normalize_alert_status)
        enriched["status_rank"] = enriched["alert_status"].map(ALERT_STATUS_ORDER).fillna(99)
        enriched["alert_status_label"] = enriched["alert_status"].apply(DashboardService.get_alert_status_label)
        enriched["alert_created_at"] = pd.to_datetime(enriched.get("alert_created_at"), errors="coerce")
        enriched["status_updated_at"] = pd.to_datetime(enriched.get("status_updated_at"), errors="coerce")
        enriched["metric_gap"] = (
            pd.to_numeric(enriched.get("threshold_value"), errors="coerce")
            - pd.to_numeric(enriched.get("metric_value"), errors="coerce")
        ).abs().fillna(0)

        sla_snapshot_df = enriched.apply(
            lambda row: pd.Series(DashboardService.get_alert_sla_snapshot(row)),
            axis=1,
        )
        # 移除可能已存在的 SLA 列，避免 concat 时产生列冲突
        sla_cols = ["sla_limit_hours", "sla_elapsed_hours", "sla_remaining_hours", "sla_deadline_at",
                    "sla_stage_started_at", "sla_is_overdue", "sla_label"]
        for col in sla_cols:
            if col in enriched.columns:
                enriched = enriched.drop(columns=[col])
        enriched = pd.concat([enriched, sla_snapshot_df], axis=1)
        enriched["sla_overdue_rank"] = np.where(enriched["sla_is_overdue"].fillna(False), 0, 1)
        # 确保 sla_remaining_hours 是 Series 类型
        sla_remaining = enriched["sla_remaining_hours"]
        if isinstance(sla_remaining, pd.DataFrame):
            sla_remaining = sla_remaining.iloc[:, 0]
        enriched["sla_remaining_sort"] = pd.to_numeric(sla_remaining, errors="coerce").fillna(999999)
        return enriched.sort_values(
            ["severity_rank", "sla_overdue_rank", "status_rank", "sla_remaining_sort", "metric_gap", "target_level", "target_key"],
            ascending=[True, True, True, True, False, True, True],
        )

    @staticmethod
    def filter_alerts_for_view(alerts_df: pd.DataFrame, group_name: str | None, queue_name: str | None) -> pd.DataFrame:
        enriched = DashboardService.enrich_alerts(alerts_df)
        if enriched.empty:
            return enriched
        if not group_name:
            return enriched

        mask = enriched["target_level"].eq("system") | enriched["alert_group_name"].eq(group_name)
        if queue_name:
            mask = mask | (
                enriched["alert_group_name"].eq(group_name)
                & enriched["alert_queue_name"].eq(queue_name)
            )
        return enriched[mask]

    @staticmethod
    def format_metric_value(value: Any) -> str:
        if value is None:
            return "—"
        try:
            numeric = float(value)
            return f"{numeric:.2f}%"
        except (TypeError, ValueError):
            text = DashboardService.normalize_text(value)
            return text or "—"

    @staticmethod
    def normalize_text(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, float) and pd.isna(value):
            return ""
        text = str(value).strip()
        return "" if text.lower() == "nan" else text

    @staticmethod
    def suggest_alert_action(row: pd.Series | dict[str, Any]) -> str:
        rule_code = DashboardService.normalize_text(row.get("rule_code"))
        target_key = DashboardService.normalize_text(row.get("target_key")) or "当前对象"

        suggestions = {
            "JOIN_MATCH_LT_85_DAY": f"先跑联表质量校验，优先排查【{target_key}】未命中样本的 join_key 格式、主键空值和回退键是否一致。",
            "MISSING_JOIN_KEY_GT_10_DAY": f"优先补齐【{target_key}】的主键来源字段，先看缺失主键样本是否集中在固定文件或固定队列。",
            "RAW_ACC_LT_99_DAY": f"先组织【{target_key}】做原始质检复盘，重点抽查错判样本和近 3 天波动最大的审核人。",
            "RAW_ACC_DROP_GT_1P5_WEEK": f"重点复盘【{target_key}】近两周的原始质检错判样本，判断是规则口径变化、人员波动还是培训失效导致周度下跌。",
            "FINAL_ACC_LT_99_QUEUE_DAY": f"先下钻到【{target_key}】队列看终审样本，确认是线下质检问题还是线上申诉改判拉低了最终正确率。",
            "MISS_RATE_GT_035_DAY": f"优先抽查【{target_key}】的漏判样本，判断是否存在规则盲区或队列培训不到位。",
            "MISS_RATE_SPIKE_GT_0P2_QUEUE_WEEK": f"优先比较【{target_key}】本周与上周的漏判样本，确认是不是新场景、新标签或新审核人带来的结构性漏判。",
            "APPEAL_REV_GT_18_DAY": f"重点复盘【{target_key}】的申诉改判样本，确认是否是一审口径不稳或申诉策略发生变化。",
            "TOP_ERROR_SHARE_GT_35_QUEUE_MONTH": f"优先把【{target_key}】队列的高频错误专题拉出来，确认是否单一错误类型持续堆积，应该单独开训练专题。",
            "ERROR_TYPE_SHARE_GT_15_QUEUE_WEEK": f"优先对【{target_key}】连续两周高位的同类错误做专项复盘，确认规则理解、培训覆盖和重点审核人是否都还没收敛。",
        }
        return suggestions.get(rule_code, f"优先复盘【{target_key}】相关样本，确认这条告警是否属于一次性波动还是持续异常。")

    def build_alert_focus_options(self, alerts_df: pd.DataFrame) -> list[dict[str, Any]]:
        enriched = self.enrich_alerts(alerts_df)
        options: list[dict[str, Any]] = []
        for _, row in enriched.iterrows():
            rule_name = self.normalize_text(row.get("rule_name")) or self.normalize_text(row.get("rule_code")) or self.normalize_text(row.get("metric_name"))
            detail = self.parse_alert_detail(row.get("target_key"))
            options.append(
                {
                    "label": f"{row['severity']} · {rule_name} · {self.normalize_text(row.get('target_key'))}",
                    "severity": self.normalize_text(row.get("severity")),
                    "rule_name": rule_name,
                    "alert_id": self.normalize_text(row.get("alert_id")),
                    "rule_code": self.normalize_text(row.get("rule_code")),
                    "target_key": self.normalize_text(row.get("target_key")),
                    "target_level": self.normalize_text(row.get("target_level")),
                    "group_name": row.get("alert_group_name"),
                    "queue_name": row.get("alert_queue_name"),
                    "error_type": detail.get("error_type"),
                    "alert_status": self.normalize_alert_status(row.get("alert_status")),
                    "alert_status_label": self.get_alert_status_label(row.get("alert_status")),
                    "owner_name": self.normalize_text(row.get("owner_name")),
                    "handle_note": self.normalize_text(row.get("handle_note")),
                    "status_updated_at": row.get("status_updated_at"),
                    "alert_created_at": row.get("alert_created_at"),
                    "sla_stage_started_at": row.get("sla_stage_started_at"),
                    "sla_deadline_at": row.get("sla_deadline_at"),
                    "sla_label": self.normalize_text(row.get("sla_label")),
                    "sla_is_overdue": bool(row.get("sla_is_overdue", False)),
                    "alert_message": self.normalize_text(row.get("alert_message")),
                    "suggestion": self.suggest_alert_action(row),
                    "metric_value_display": self.format_metric_value(row.get("metric_value")),
                    "threshold_display": self.format_metric_value(row.get("threshold_value")),
                }
            )
        return options

    def build_alert_actions(self, alerts_df: pd.DataFrame, group_name: str | None = None, queue_name: str | None = None) -> list[str]:
        relevant_df = self.filter_alerts_for_view(alerts_df, group_name, queue_name)
        if relevant_df.empty:
            return ["当前没有触发告警，先看趋势与高频错误专题，确认是否只是样本量较小。"]

        actions: list[str] = []
        for _, row in relevant_df.head(5).iterrows():
            suggestion = self.suggest_alert_action(row)
            if suggestion not in actions:
                actions.append(suggestion)
            if len(actions) >= 3:
                break
        return actions or ["当前没有需要优先处理的异常。"]

    @staticmethod
    def build_training_actions(queue_df: pd.DataFrame, error_df: pd.DataFrame, auditor_df: pd.DataFrame) -> list[str]:
        actions: list[str] = []

        if not queue_df.empty:
            top_queue = queue_df.sort_values(["raw_accuracy_rate", "qa_cnt"], ascending=[True, False]).iloc[0]
            actions.append(
                f"优先复盘队列【{top_queue['queue_name']}】，当前原始正确率 {top_queue['raw_accuracy_rate']:.2f}% ，先看错判 / 漏判样本。"
            )

        if not error_df.empty:
            top_error = error_df.iloc[0]
            actions.append(
                f"把高频问题【{top_error['error_type']}】整理成训练专题，当前影响样本 {int(top_error['issue_cnt'])} 条。"
            )

        if not auditor_df.empty:
            weakest = auditor_df.sort_values(["raw_accuracy_rate", "qa_cnt"], ascending=[True, False]).iloc[0]
            actions.append(
                f"重点关注审核人【{weakest['reviewer_name']}】，当前原始正确率 {weakest['raw_accuracy_rate']:.2f}% ，建议做专项帮扶。"
            )

        if not actions:
            actions.append("当前没有可用数据，先完成 schema 初始化和 fact 数据导入。")

        return actions

    # ==================== 新增维度数据加载 ====================

    def load_qa_label_distribution(
        self, grain: str, selected_date: date, group_name: str | None = None, top_n: int = 10
    ) -> pd.DataFrame:
        """获取质检标签分布"""
        anchor_date = self.normalize_anchor_date(grain, selected_date)
        return self.repo.get_qa_label_distribution(grain, anchor_date, group_name, top_n)

    def load_qa_owner_distribution(
        self, grain: str, selected_date: date, group_name: str | None = None, top_n: int = 10
    ) -> pd.DataFrame:
        """获取质检员工作量分布"""
        anchor_date = self.normalize_anchor_date(grain, selected_date)
        return self.repo.get_qa_owner_distribution(grain, anchor_date, group_name, top_n)
