"""首页：告警驱动 + 组别经营 + 队列概览整合版。

设计原则：
- 一页看全：核心指标、告警、队列分布、趋势，一个页面都能看到
- 视觉舒适：留白充分、卡片圆角、颜色温和
- 交互自然：下探路径清晰、筛选便捷

布局结构：
- Hero 区：标题 + 模式切换 + 日期
- 第一行：核心指标卡片（4-5个关键指标）
- 第二行：告警区域（异常总览 + 告警列表）
- 第三行：组别卡片（横向滚动或网格）
- 第四行：队列概览（饼图 + 排名表）+ 趋势图
- 第五行：下探区域（队列表格 + 审核人表格）
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from services.dashboard_service import DashboardService
from storage.repository import DashboardRepository

st.set_page_config(page_title="质培运营看板-首页", page_icon="📊", layout="wide")

# ── 加载统一 CSS ──
import os as _os
_css_path = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "custom.css")
if _os.path.exists(_css_path):
    with open(_css_path, "r", encoding="utf-8") as _f:
        _CSS = _f.read()
    st.markdown(f'<style>{_CSS}</style>', unsafe_allow_html=True)

service = DashboardService()
repo = DashboardRepository()

GRAIN_LABELS = {
    "day": "日监控",
    "week": "周复盘",
    "month": "月管理",
}
ALERT_STATUS_OPTIONS = ["open", "claimed", "ignored", "resolved"]

# 颜色常量
COLOR_P0 = "#DC2626"
COLOR_P1 = "#F59E0B"
COLOR_P2 = "#3B82F6"
COLOR_SUCCESS = "#10B981"
COLOR_GOOD = "#10B981"
COLOR_BAD = "#EF4444"
COLOR_WARN = "#F59E0B"


@st.cache_data(show_spinner=False, ttl=300)
def get_data_date_range() -> tuple[date, date]:
    """获取数据库中的日期范围，用于设置默认日期"""
    row = repo.fetch_one("SELECT MIN(biz_date) AS min_d, MAX(biz_date) AS max_d FROM fact_qa_event")
    if not row or row.get("min_d") is None:
        return date.today(), date.today()
    min_val = row["min_d"]
    max_val = row["max_d"]
    if hasattr(min_val, "date"):
        min_val = min_val.date()
    if hasattr(max_val, "date"):
        max_val = max_val.date()
    return min_val, max_val


@st.cache_data(show_spinner=False, ttl=300)
def load_group_overview(grain: str, selected_date: date) -> dict:
    return service.load_dashboard_payload(grain, selected_date)


@st.cache_data(show_spinner=False, ttl=300)
def load_group_detail(
    grain: str,
    selected_date: date,
    group_name: str,
    queue_name: str | None,
    reviewer_name: str | None,
    focus_rule_code: str | None,
    focus_error_type: str | None,
) -> dict:
    return service.load_group_payload(
        grain,
        selected_date,
        group_name,
        queue_name,
        reviewer_name,
        focus_rule_code,
        focus_error_type,
    )


@st.cache_data(show_spinner=False, ttl=300)
def load_queue_overview_data(
    grain: str,
    start_date: date,
    end_date: date,
    group_name: str | None = None,
) -> dict:
    """加载队列概览数据"""
    if grain == "day":
        time_filter = "biz_date BETWEEN %s AND %s"
        anchor_col = "biz_date"
    elif grain == "week":
        time_filter = "week_begin_date BETWEEN %s AND %s"
        anchor_col = "week_begin_date"
    else:
        time_filter = "month_begin_date BETWEEN %s AND %s"
        anchor_col = "month_begin_date"
    
    # 选择对应的 mart 表
    if grain == "day":
        queue_table = "mart_day_queue"
        group_table = "mart_day_group"
    elif grain == "week":
        queue_table = "mart_week_queue"
        group_table = "mart_week_group"
    else:
        queue_table = "mart_month_queue"
        group_table = "mart_month_group"
    
    # 队列数据
    queue_sql = f"""
    SELECT
        group_name,
        queue_name,
        SUM(qa_cnt) AS total_qa_cnt,
        ROUND(SUM(raw_correct_cnt) * 100.0 / NULLIF(SUM(qa_cnt), 0), 2) AS raw_accuracy_rate,
        ROUND(SUM(final_correct_cnt) * 100.0 / NULLIF(SUM(qa_cnt), 0), 2) AS final_accuracy_rate,
        ROUND(SUM(misjudge_cnt) * 100.0 / NULLIF(SUM(qa_cnt), 0), 2) AS misjudge_rate,
        ROUND(SUM(missjudge_cnt) * 100.0 / NULLIF(SUM(qa_cnt), 0), 2) AS missjudge_rate
    FROM {queue_table}
    WHERE {time_filter}
    """
    params = [start_date, end_date]
    if group_name:
        if group_name == "B组":
            # B组整体：过滤所有 B组开头的队列
            queue_sql += " AND group_name LIKE %s"
            params.append("B组%")
        else:
            queue_sql += " AND group_name = %s"
            params.append(group_name)
    queue_sql += " GROUP BY group_name, queue_name ORDER BY total_qa_cnt DESC"
    queue_df = repo.fetch_df(queue_sql, params)
    
    # 趋势数据
    trend_sql = f"""
    SELECT 
        {anchor_col} AS anchor_date,
        SUM(qa_cnt) AS total_qa_cnt,
        ROUND(SUM(raw_correct_cnt) * 100.0 / NULLIF(SUM(qa_cnt), 0), 2) AS raw_accuracy_rate,
        ROUND(SUM(final_correct_cnt) * 100.0 / NULLIF(SUM(qa_cnt), 0), 2) AS final_accuracy_rate
    FROM {group_table}
    WHERE {time_filter}
    """
    params_trend = [start_date, end_date]
    if group_name:
        if group_name == "B组":
            # B组整体：过滤所有 B组开头的组
            trend_sql += " AND group_name LIKE %s"
            params_trend.append("B组%")
        else:
            trend_sql += " AND group_name = %s"
            params_trend.append(group_name)
    trend_sql += f" GROUP BY {anchor_col} ORDER BY {anchor_col}"
    trend_df = repo.fetch_df(trend_sql, params_trend)
    
    return {"queue_df": queue_df, "trend_df": trend_df}


@st.cache_data(show_spinner=False, ttl=300)
def load_alert_history(alert_id: str | None) -> pd.DataFrame:
    if not alert_id:
        return pd.DataFrame()
    return service.load_alert_history(alert_id)


@st.cache_data(show_spinner=False, ttl=300)
def load_qa_label_distribution_cached(grain: str, selected_date: date, group_name: str | None = None, top_n: int = 10) -> pd.DataFrame:
    """获取质检标签分布（缓存 5 分钟）"""
    return service.load_qa_label_distribution(grain, selected_date, group_name, top_n)


@st.cache_data(show_spinner=False, ttl=300)
def load_qa_owner_distribution_cached(grain: str, selected_date: date, group_name: str | None = None, top_n: int = 10) -> pd.DataFrame:
    """获取质检员工作量分布（缓存 5 分钟）"""
    return service.load_qa_owner_distribution(grain, selected_date, group_name, top_n)


@st.cache_data(show_spinner=False, ttl=300)
def load_qa_result_distribution_cached(grain: str, selected_date: date, group_name: str | None = None) -> pd.DataFrame:
    """获取质检结果分布：正确/错判/漏判（缓存 5 分钟，三分类百分比总和=100%）"""
    return service.load_qa_result_distribution(grain, selected_date, group_name)


# ==================== Phase 1 新增维度缓存函数 ====================

@st.cache_data(show_spinner=False, ttl=300)
def load_error_top5_cached(grain: str, selected_date: date, group_name: str | None = None, limit: int = 5) -> pd.DataFrame:
    """高频错误 Top5（缓存）"""
    return repo.get_error_top5(grain, selected_date, group_name, limit)

@st.cache_data(show_spinner=False, ttl=300)
def load_label_accuracy_cached(grain: str, selected_date: date, group_name: str | None = None, min_cnt: int = 10, limit: int = 15) -> pd.DataFrame:
    """标签准确率排行（缓存）"""
    return repo.get_label_accuracy(grain, selected_date, group_name, min_cnt, limit)

@st.cache_data(show_spinner=False, ttl=300)
def load_content_type_distribution_cached(grain: str, selected_date: date, group_name: str | None = None) -> pd.DataFrame:
    """内容类型分布（缓存）"""
    return repo.get_content_type_distribution(grain, selected_date, group_name)

@st.cache_data(show_spinner=False, ttl=300)
def load_hourly_heatmap_cached(grain: str, selected_date: date, group_name: str | None = None) -> pd.DataFrame:
    """时段质量热力图（缓存）"""
    return repo.get_hourly_heatmap(grain, selected_date, group_name)

@st.cache_data(show_spinner=False, ttl=300)
def load_appeal_analysis_cached(grain: str, selected_date: date, group_name: str | None = None) -> dict:
    """申诉分析多维指标（缓存）"""
    return repo.get_appeal_analysis(grain, selected_date, group_name)

# ==================== Phase 2 新增维度缓存函数 ====================

@st.cache_data(show_spinner=False, ttl=300)
def load_error_type_trend_cached(selected_date: date, group_name: str | None = None, days: int = 14, top_n: int = 3) -> pd.DataFrame:
    """错误类型日趋势（缓存）"""
    return repo.get_error_type_trend(selected_date, group_name, days, top_n)

@st.cache_data(show_spinner=False, ttl=300)
def load_error_affected_reviewers_cached(grain: str, selected_date: date, group_name: str | None = None, error_type: str | None = None, top_n: int = 10) -> pd.DataFrame:
    """错误类型受影响审核人（缓存）"""
    return repo.get_error_affected_reviewers(grain, selected_date, group_name, error_type, top_n)

@st.cache_data(show_spinner=False, ttl=300)
def load_data_health_cached(selected_date: date) -> dict:
    """数据健康指标（缓存）"""
    return repo.get_data_health_indicators(selected_date)

@st.cache_data(show_spinner=False, ttl=300)
def load_error_reason_wordcloud_cached(grain: str, selected_date: date, group_name: str | None = None, top_n: int = 20) -> pd.DataFrame:
    """error_reason 词频统计（缓存）"""
    return repo.get_error_reason_wordcloud(grain, selected_date, group_name, top_n)

@st.cache_data(show_spinner=False, ttl=300)
def load_inspect_type_cached(grain: str, selected_date: date, group_name: str | None = None) -> pd.DataFrame:
    """inspect_type 分布（缓存）"""
    return repo.get_inspect_type_distribution(grain, selected_date, group_name)

@st.cache_data(show_spinner=False, ttl=300)
def load_workforce_type_cached(grain: str, selected_date: date, group_name: str | None = None) -> pd.DataFrame:
    """workforce_type 分布（缓存）"""
    return repo.get_workforce_type_distribution(grain, selected_date, group_name)

@st.cache_data(show_spinner=False)
def to_csv_bytes(df: pd.DataFrame) -> bytes:
    export_df = df.copy()
    for column in export_df.columns:
        if pd.api.types.is_datetime64_any_dtype(export_df[column]):
            export_df[column] = export_df[column].astype(str)
    return export_df.to_csv(index=False).encode("utf-8-sig")


def safe_file_part(value: str | None) -> str:
    text = (value or "").strip()
    if not text:
        return ""
    for old in ["/", "\\", " ", "|", ":"]:
        text = text.replace(old, "-")
    return text


def build_export_file_name(
    prefix: str,
    grain: str,
    anchor_date: date,
    group_name: str | None = None,
    queue_name: str | None = None,
    rule_code: str | None = None,
    reviewer_name: str | None = None,
    error_type: str | None = None,
) -> str:
    parts = [prefix, grain, str(anchor_date)]
    for value in [rule_code, group_name, queue_name, reviewer_name, error_type]:
        safe_value = safe_file_part(value)
        if safe_value:
            parts.append(safe_value)
    return "_".join(parts) + ".csv"


# ==================== Hero 区 ====================
st.markdown(
    """
    <div style="margin-bottom: 1rem; padding: 1.25rem 1.5rem; background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%); border-radius: 14px; border-left: 4px solid #22c55e; box-shadow: 0 1px 3px rgba(0,0,0,0.04);">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 2px;">
            <h1 style="margin: 0; font-size: 20px; font-weight: 700; color: #14532d;">📊 质培运营看板</h1>
            <div style="font-size: 12px; color: #166534; font-weight: 500; background: #fff; padding: 3px 10px; border-radius: 6px;">实时监控 · 智能告警 · 数据驱动</div>
        </div>
        <div style="font-size: 12.5px; color: #15803d; line-height: 1.7;">
            🎯 日看异常 · 周看复发 · 月看治理 &nbsp;
            📍 下探链路：<span style="background:#fff; padding:2px 8px;border-radius:4px;color:#166534;font-weight:600;margin:0 2px;">组别</span> →
            <span style="background:#fff; padding:2px 8px;border-radius:4px;color:#166534;font-weight:600;margin:0 2px;">队列</span> →
            <span style="background:#fff; padding:2px 8px;border-radius:4px;color:#166534;font-weight:600;margin:0 2px;">审核人</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# 处理快捷模式（从上次按钮点击恢复）
_quick_mode = st.session_state.pop("quick_mode", None)

# 快捷入口按钮（紧凑横排）
quick_c1, quick_c2, quick_c3, quick_c4 = st.columns([1, 1, 1, 1])
with quick_c1:
    if st.button("🚨 今日异常", use_container_width=True, help="快速定位今日 P0/P1 告警"):
        st.session_state["quick_mode"] = "alert"
        st.rerun()
with quick_c2:
    if st.button("📉 最差队列", use_container_width=True, help="查看本周正确率最低的队列"):
        st.session_state["quick_mode"] = "worst_queue"
        st.rerun()
with quick_c3:
    if st.button("🔄 申诉异常", use_container_width=True, help="查看申诉改判率高的组别"):
        st.session_state["quick_mode"] = "appeal"
        st.rerun()
with quick_c4:
    if st.button("📊 数据总览", use_container_width=True, help="跳转到数据总览页"):
        st.switch_page("pages/02_数据总览.py")

# quick_mode 模式提示
if _quick_mode == "alert":
    st.info("🚨 **今日异常模式** — 已定位到告警区域，请查看下方告警详情")
elif _quick_mode == "worst_queue":
    st.info("📉 **最差队列模式** — 队列表已按正确率升序排列，问题队列优先展示")
elif _quick_mode == "appeal":
    st.info("🔄 **申诉异常模式** — 请在下方队列选择器中选择组别查看申诉数据")

# 获取数据日期范围，设置默认日期为数据最新日期
_data_min_date, _data_max_date = get_data_date_range()
_default_date = _data_max_date if _data_max_date <= date.today() else date.today()

# 模式切换 + 日期选择（一行搞定）
mode_col1, mode_col2, mode_col3, mode_col4 = st.columns([1, 1, 1, 2])
with mode_col1:
    grain = st.radio(
        "看板模式",
        options=["day", "week", "month"],
        format_func=lambda x: GRAIN_LABELS[x],
        horizontal=True,
        label_visibility="collapsed",
    )
with mode_col2:
    selected_date = st.date_input("业务日期", value=_default_date, label_visibility="collapsed")
with mode_col3:
    date_start = st.date_input("起始日期", value=selected_date - timedelta(days=6), label_visibility="collapsed", key="start_d")
with mode_col4:
    date_end = st.date_input("截止日期", value=selected_date, label_visibility="collapsed", key="end_d")

st.markdown("---")

# ==================== 加载数据（带容错处理） ====================
_db_error_msg = None
try:
    payload = load_group_overview(grain, selected_date)
    group_df: pd.DataFrame = payload["group_df"]
    alerts_df: pd.DataFrame = payload["alerts_df"]
    alert_summary: dict[str, int] = payload["alert_summary"]
except Exception as e:
    # 数据库连接失败或 SQL 执行错误时的友好提示
    import traceback as tb
    _db_error_msg = str(e)
    _db_tb = tb.format_exc()
    st.error(f"⚠️ 数据加载失败：{_db_error_msg[:200]}")
    st.markdown(f"""
    <details>
    <summary>🔧 技术详情（点击展开）</summary>
    <pre style="font-size:12px; overflow:auto;">{_db_tb}</pre>
    </details>
    """, unsafe_allow_html=True)
    group_df = pd.DataFrame()
    alerts_df = pd.DataFrame()
    alert_summary: dict[str, int] = {"total": 0, "P0": 0, "P1": 0, "P2": 0}

if not _db_error_msg:
    alert_status_summary = service.summarize_alert_status(alerts_df)
    alert_sla_summary = service.summarize_alert_sla(alerts_df)
else:
    alert_status_summary = {}
    alert_sla_summary = {}

# 预先获取选中的组别（用于队列数据过滤）
selected_group = st.session_state.get("selected_group")
if not selected_group and not group_df.empty:
    # 默认选中第一个组别
    selected_group = group_df.iloc[0]["group_name"]
    st.session_state["selected_group"] = selected_group

# 数据库错误时提前终止，避免后续查询继续报错
if _db_error_msg:
    st.warning("看板数据无法加载，请检查 TiDB 连接配置或联系管理员。")
    st.stop()

# 加载队列概览数据（跟随选中的组别）
# B组整体：展示所有 B组开头的队列
# A组/B组-评论/B组-账号：仅展示该组数据
queue_data = load_queue_overview_data(grain, date_start, date_end, selected_group)
queue_df = queue_data["queue_df"]
trend_df = queue_data["trend_df"]

# 加载前一天/上周/上月的数据用于环比对比
if grain == "day":
    prev_date = selected_date - timedelta(days=1)
elif grain == "week":
    prev_date = selected_date - timedelta(weeks=1)
else:
    prev_date = (selected_date.replace(day=1) - timedelta(days=1)).replace(day=1)

prev_payload = load_group_overview(grain, prev_date)
prev_group_df: pd.DataFrame = prev_payload["group_df"]

if group_df.empty:
    st.warning("当前还没有 fact 数据。请先导入质检数据。")
    st.stop()

# 计算环比变化
def _color_val(val: float, warn_thresh: float, bad_thresh: float, suffix: str = "%", inverse: bool = False) -> str:
    """将数值格式化为字符串，根据阈值自动标色（用于 st.dataframe TextColumn 模式）。
    inverse=True 时，越大越好（如申诉改判率）。
    """
    text = f"{val:.2f}{suffix}"
    if inverse:
        if val > warn_thresh:
            return f'<span style="color: #dc2626; font-weight: 700;">{text}</span>'
        return text
    if val < bad_thresh:
        return f'<span style="color: #dc2626; font-weight: 700;">{text}</span>'
    if val < warn_thresh:
        return f'<span style="color: #d97706; font-weight: 600;">{text}</span>'
    return text


def calc_change(current_rate: float, prev_rate: float | None) -> str:
    if prev_rate is None or pd.isna(prev_rate):
        return ""
    delta = current_rate - prev_rate
    if abs(delta) < 0.01:
        return "<span style='color:#64748B; font-size:0.7rem;'>→0.00%</span>"
    elif delta > 0:
        return f"<span style='color:#10B981; font-size:0.7rem;'>↑{delta:.2f}%</span>"
    else:
        return f"<span style='color:#EF4444; font-size:0.7rem;'>↓{abs(delta):.2f}%</span>"


def _metric_card(label: str, value: str, delta_html: str, fallback_text: str,
                 theme: str = "neutral"):
    """渲染单个核心指标卡片。
    theme: neutral(白底) / good(绿底) / bad(红底)
    """
    themes = {
        "neutral": {"bg": "#fff", "border": "#e2e8f0", "label_color": "#64748b",
                     "value_color": "#0f172a", "delta_color": "#94a3b8"},
        "good":    {"bg": "#f0fdf4", "border": "#bbf7d0", "label_color": "#166534",
                     "value_color": "#16a34a", "delta_color": "#15803d"},
        "bad":     {"bg": "#fef2f2", "border": "#fecaca", "label_color": "#b91c1c",
                     "value_color": "#dc2626", "delta_color": "#ef4444"},
    }
    t = themes.get(theme, themes["neutral"])
    st.markdown(f"""
        <div style="background: {t['bg']}; padding: 14px; border-radius: 12px; border: 1px solid {t['border']}; box-shadow: 0 1px 3px rgba(0,0,0,0.04);">
            <div style="font-size: 11.5px; color: {t['label_color']}; margin-bottom: 6px; font-weight:500;">{label}</div>
            <div style="font-size: 26px; font-weight: 700; color: {t['value_color']}; margin-bottom: 2px;">{value}</div>
            <div style="font-size: 11px; color: {t['delta_color']};">{delta_html if delta_html else fallback_text}</div>
        </div>
    """, unsafe_allow_html=True)


def _calc_abs_delta(current: int, prev: int | None, up_good: bool = True) -> str:
    """计算绝对值环比，返回 delta HTML。up_good=True 时上升为绿。"""
    if prev is None:
        return ""
    diff = current - prev
    if diff == 0:
        return "<span style='color:#64748B; font-size:0.7rem;'>→0</span>"
    good_color, bad_color = ("#10B981", "#EF4444") if up_good else ("#EF4444", "#10B981")
    if diff > 0:
        color = good_color if up_good else bad_color
    else:
        color = bad_color if up_good else good_color
    arrow = "↑" if diff > 0 else "↓"
    return f"<span style='color:{color}; font-size:0.7rem;'>{arrow}{abs(diff):,}</span>"

# ==================== 第一行：核心指标 ====================
st.markdown("#### 📈 核心指标概览")
total_qa = group_df["qa_cnt"].sum()
avg_raw_acc = (group_df["raw_accuracy_rate"] * group_df["qa_cnt"]).sum() / total_qa if total_qa > 0 else 0
avg_final_acc = (group_df["final_accuracy_rate"] * group_df["qa_cnt"]).sum() / total_qa if total_qa > 0 else 0
# 直接汇总错误量，避免计算精度损失
if "raw_error_cnt" in group_df.columns:
    total_raw_errors = int(group_df["raw_error_cnt"].sum())
else:
    total_raw_errors = int(total_qa * (100 - avg_raw_acc) / 100)
if "final_error_cnt" in group_df.columns:
    total_final_errors = int(group_df["final_error_cnt"].sum())
else:
    total_final_errors = int(total_qa * (100 - avg_final_acc) / 100)

# 核心指标卡片（用公共函数渲染）
metric_col1, metric_col2, metric_col3, metric_col4, metric_col5 = st.columns(5)

# 计算环比
prev_total_qa = prev_group_df["qa_cnt"].sum() if not prev_group_df.empty else 0
prev_raw = (prev_group_df["raw_accuracy_rate"] * prev_group_df["qa_cnt"]).sum() / prev_total_qa if prev_total_qa > 0 else None
prev_final = (prev_group_df["final_accuracy_rate"] * prev_group_df["qa_cnt"]).sum() / prev_total_qa if prev_total_qa > 0 else None
prev_raw_errors = int(prev_total_qa * (100 - (prev_group_df["raw_accuracy_rate"] * prev_group_df["qa_cnt"]).sum() / prev_total_qa) / 100) if prev_total_qa > 0 else None
prev_final_errors = int(prev_total_qa * (100 - (prev_group_df["final_accuracy_rate"] * prev_group_df["qa_cnt"]).sum() / prev_total_qa) / 100) if prev_total_qa > 0 else None

with metric_col1:
    _metric_card("📊 质检总量", f"{int(total_qa):,}",
                 _calc_abs_delta(int(total_qa), prev_total_qa, up_good=True),
                 "累计抽检样本数", "neutral")
with metric_col2:
    _metric_card("✓ 原始正确率", f"{avg_raw_acc:.2f}%",
                 calc_change(avg_raw_acc, prev_raw), "一审正确率", "good")
with metric_col3:
    _metric_card("✓✓ 最终正确率", f"{avg_final_acc:.2f}%",
                 calc_change(avg_final_acc, prev_final), "终审准确率", "good")
with metric_col4:
    _metric_card("✗ 原始错误量", f"{total_raw_errors:,}",
                 _calc_abs_delta(total_raw_errors, prev_raw_errors, up_good=False),
                 "一审错误样本", "bad")
with metric_col5:
    _metric_card("✗✗ 终审错误量", f"{total_final_errors:,}",
                 _calc_abs_delta(total_final_errors, prev_final_errors, up_good=False),
                 "终审错误样本", "bad")

st.markdown("---")

# ==================== 第二行：告警区域（紧凑版） ====================
st.markdown("#### 🚨 实时告警监控")
# 级别统计（横向紧凑展示）
alert_col1, alert_col2, alert_col3, alert_col4, alert_col5 = st.columns([1, 1, 1, 1, 2])
with alert_col1:
    st.markdown(f"""
        <div style='background: linear-gradient(135deg, #FEF2F2 0%, #FEE2E2 100%); padding: 0.75rem; border-radius: 0.75rem; text-align: center; border: 2px solid {COLOR_P0};'>
            <div style='color:{COLOR_P0}; font-size:1.75rem; font-weight:700;'>{alert_summary.get('P0', 0)}</div>
            <div style='font-size:0.75rem; color:#991B1B; font-weight:600;'>🔴 P0 紧急</div>
        </div>
    """, unsafe_allow_html=True)
with alert_col2:
    st.markdown(f"""
        <div style='background: linear-gradient(135deg, #FFFBEB 0%, #FEF3C7 100%); padding: 0.75rem; border-radius: 0.75rem; text-align: center; border: 2px solid {COLOR_P1};'>
            <div style='color:{COLOR_P1}; font-size:1.75rem; font-weight:700;'>{alert_summary.get('P1', 0)}</div>
            <div style='font-size:0.75rem; color:#92400E; font-weight:600;'>🟡 P1 重要</div>
        </div>
    """, unsafe_allow_html=True)
with alert_col3:
    st.markdown(f"""
        <div style='background: linear-gradient(135deg, #EFF6FF 0%, #DBEAFE 100%); padding: 0.75rem; border-radius: 0.75rem; text-align: center; border: 2px solid {COLOR_P2};'>
            <div style='color:{COLOR_P2}; font-size:1.75rem; font-weight:700;'>{alert_summary.get('P2', 0)}</div>
            <div style='font-size:0.75rem; color:#1E40AF; font-weight:600;'>🔵 P2 关注</div>
        </div>
    """, unsafe_allow_html=True)
with alert_col4:
    st.markdown(f"""
        <div style='background: linear-gradient(135deg, #F1F5F9 0%, #E2E8F0 100%); padding: 0.75rem; border-radius: 0.75rem; text-align: center; border: 2px solid #64748B;'>
            <div style='font-size:1.75rem; font-weight:700; color:#1E293B;'>{alert_summary.get('total', 0)}</div>
            <div style='font-size:0.75rem; color:#475569; font-weight:600;'>📊 总计</div>
        </div>
    """, unsafe_allow_html=True)
with alert_col5:
    # SLA 超时提示（如果有）
    if alert_sla_summary.get("total_overdue", 0) > 0:
        st.markdown(f"""
            <div style='background: linear-gradient(135deg, #FEF2F2 0%, #FEE2E2 100%); padding: 0.75rem; border-radius: 0.75rem; border-left: 4px solid #DC2626;'>
                <div style='color:#DC2626; font-weight:700; font-size: 1rem; margin-bottom: 0.25rem;'>⚠️ SLA 超时 {alert_sla_summary.get('total_overdue', 0)} 条</div>
                <div style='font-size:0.7rem; color:#991B1B;'>待处理 {alert_sla_summary.get('open_overdue', 0)} · 已认领 {alert_sla_summary.get('claimed_overdue', 0)}</div>
            </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
            <div style='background: linear-gradient(135deg, #ECFDF5 0%, #D1FAE5 100%); padding: 0.75rem; border-radius: 0.75rem; text-align: center; border-left: 4px solid #10B981;'>
                <div style='color: #10B981; font-weight: 700; font-size: 1rem;'>✅ 无 SLA 超时</div>
                <div style='font-size: 0.7rem; color: #047857;'>所有告警均在处理时限内</div>
            </div>
        """, unsafe_allow_html=True)

# 告警详情折叠面板
if not alerts_df.empty:
    with st.expander("📋 查看告警详情", expanded=False):
        # 筛选
        filter_col1, filter_col2, filter_col3 = st.columns([1, 1, 2])
        with filter_col1:
            severity_filters = st.multiselect("级别", options=["P0", "P1", "P2"], default=["P0", "P1"], label_visibility="collapsed", placeholder="级别", key="alert_severity")
        with filter_col2:
            status_filters = st.multiselect("状态", options=ALERT_STATUS_OPTIONS, default=["open"], format_func=service.get_alert_status_label, label_visibility="collapsed", placeholder="状态", key="alert_status")
        with filter_col3:
            keyword = st.text_input("关键词", placeholder="搜索...", label_visibility="collapsed", key="alert_keyword")
        
        filtered_alerts = service.filter_alerts(alerts_df, severity_filters, status_filters, ["system", "group", "queue"], keyword)
        
        if not filtered_alerts.empty:
            st.caption(f"共 {len(filtered_alerts)} 条告警")
            # 显示前 10 条
            display_alerts = filtered_alerts.head(10)
            alert_show = pd.DataFrame()
            alert_show["级别"] = display_alerts["severity"]
            alert_show["规则"] = display_alerts.apply(lambda r: r.get("rule_name") or r.get("rule_code", ""), axis=1)
            alert_show["对象"] = display_alerts["target_key"]
            alert_show["当前值"] = display_alerts.apply(lambda r: f"{r.get('metric_value', 0):.2f}%" if r.get('metric_value') else "—", axis=1)
            alert_show["阈值"] = display_alerts.apply(lambda r: f"{r.get('threshold_value', 0):.2f}%" if r.get('threshold_value') else "—", axis=1)
            alert_show["责任人"] = display_alerts["owner_name"]
            alert_show["状态"] = display_alerts["alert_status"].apply(service.get_alert_status_label)
            alert_show["时间"] = display_alerts["alert_date"]
            st.dataframe(alert_show, use_container_width=True, hide_index=True)
        else:
            st.info("当前筛选条件下无告警")

st.markdown("---")

# ==================== 第三行：组别卡片 ====================
st.markdown("#### 🏢 组别经营视图")
st.caption("💡 点击组别卡片可切换查看详细数据，卡片颜色代表达标状态")

# 计算 B 组整体
b_groups = group_df[group_df["group_name"].str.startswith("B组")]
if not b_groups.empty:
    b_total_qa = b_groups["qa_cnt"].sum()
    # 直接汇总正确量，避免用正确率反推
    b_total_raw_correct = b_groups["raw_correct_cnt"].sum() if "raw_correct_cnt" in b_groups.columns else (b_groups["raw_accuracy_rate"] * b_groups["qa_cnt"] / 100).sum()
    b_total_final_correct = b_groups["final_correct_cnt"].sum() if "final_correct_cnt" in b_groups.columns else (b_groups["final_accuracy_rate"] * b_groups["qa_cnt"] / 100).sum()
    # 计算错误量：优先用 raw_error_cnt，否则用 qa_cnt - raw_correct_cnt
    if "raw_error_cnt" in b_groups.columns:
        b_total_raw_error = b_groups["raw_error_cnt"].sum()
    else:
        b_total_raw_error = b_total_qa - b_total_raw_correct
    b_summary = pd.DataFrame([{
        "group_name": "B组",
        "raw_accuracy_rate": b_total_raw_correct / b_total_qa * 100 if b_total_qa > 0 else 0,
        "final_accuracy_rate": b_total_final_correct / b_total_qa * 100 if b_total_qa > 0 else 0,
        "qa_cnt": b_total_qa,
        "raw_error_cnt": int(b_total_raw_error),
        "misjudge_rate": (b_groups["misjudge_rate"] * b_groups["qa_cnt"] / 100).sum() / b_total_qa * 100 if b_total_qa > 0 else 0,
        "missjudge_rate": (b_groups["missjudge_rate"] * b_groups["qa_cnt"] / 100).sum() / b_total_qa * 100 if b_total_qa > 0 else 0,
    }])
    extended_df = pd.concat([group_df, b_summary], ignore_index=True)
else:
    extended_df = group_df

# 显示顺序
order_map = {"A组-评论": 1, "B组": 2, "B组-评论": 3, "B组-账号": 4}
extended_df["_order"] = extended_df["group_name"].map(order_map).fillna(99)
extended_df = extended_df.sort_values("_order").drop(columns="_order")

# 显示所有组别卡片（增加悬停效果和阴影）
display_groups = extended_df.head(4)
group_cols = st.columns(len(display_groups))

# 获取当前选中的组别（已在前面初始化）
selected_group = st.session_state.get("selected_group")

for idx, (_, row) in enumerate(display_groups.iterrows()):
    group_name = row["group_name"]
    raw_rate = row["raw_accuracy_rate"]
    final_rate = row["final_accuracy_rate"]
    qa_cnt = int(row["qa_cnt"])
    # 直接从 mart 表读取错误量，避免计算精度损失
    raw_error_cnt_val = row.get("raw_error_cnt")
    if pd.isna(raw_error_cnt_val):
        # 如果没有 raw_error_cnt，用 qa_cnt - raw_correct_cnt 计算
        raw_correct = row.get("raw_correct_cnt", qa_cnt)
        raw_error_cnt = int(qa_cnt - raw_correct)
    else:
        raw_error_cnt = int(raw_error_cnt_val)
    
    # 获取前一天的环比数据
    prev_row = prev_group_df[prev_group_df["group_name"] == group_name]
    prev_raw_rate = prev_row.iloc[0]["raw_accuracy_rate"] if not prev_row.empty else None
    prev_final_rate = prev_row.iloc[0]["final_accuracy_rate"] if not prev_row.empty else None
    
    # 计算环比变化
    raw_change = calc_change(raw_rate, prev_raw_rate)
    final_change = calc_change(final_rate, prev_final_rate)
    
    # 状态颜色和图标
    if raw_rate >= 99:
        bg_gradient = "linear-gradient(135deg, #ECFDF5 0%, #D1FAE5 100%)"
        border_color = "#10B981"
        status_icon = "✅"
        status_text = "达标"
        status_color = "#047857"
    elif raw_rate >= 98:
        bg_gradient = "linear-gradient(135deg, #FFFBEB 0%, #FEF3C7 100%)"
        border_color = "#F59E0B"
        status_icon = "⚠️"
        status_text = "观察中"
        status_color = "#92400E"
    else:
        bg_gradient = "linear-gradient(135deg, #FEF2F2 0%, #FEE2E2 100%)"
        border_color = "#EF4444"
        status_icon = "❌"
        status_text = "需关注"
        status_color = "#991B1B"
    
    # 读取错判率、漏判率
    misjudge_rate = row.get("misjudge_rate", 0) or 0
    missjudge_rate = row.get("missjudge_rate", 0) or 0
    misjudge_color = '#EF4444' if misjudge_rate > 0.5 else '#F59E0B' if misjudge_rate > 0.3 else '#10B981'
    missjudge_color = '#EF4444' if missjudge_rate > 0.35 else '#F59E0B' if missjudge_rate > 0.2 else '#10B981'
    
    is_selected = selected_group == group_name
    border = f"3px solid #3B82F6" if is_selected else f"2px solid {border_color}"
    shadow = "0 8px 16px rgba(59, 130, 246, 0.2)" if is_selected else "0 2px 8px rgba(0,0,0,0.1)"
    display_name = "B组（整体）" if group_name == "B组" else group_name
    
    with group_cols[idx]:
        st.markdown(
            f"""
            <div style="padding: 1.25rem; border-radius: 1rem; background: {bg_gradient}; border: {border}; margin-bottom: 0.5rem; box-shadow: {shadow}; transition: all 0.3s ease; min-height: 220px;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
                    <div style="font-weight: 700; font-size: 1.1rem; color: #1E293B;">{display_name}</div>
                    <div style="font-size: 0.75rem; color: {status_color}; font-weight: 600; background: white; padding: 0.25rem 0.5rem; border-radius: 0.5rem;">{status_icon} {status_text}</div>
                </div>
                <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 0.4rem; margin-bottom: 0.5rem;">
                    <div style="background: white; padding: 0.6rem; border-radius: 0.5rem; text-align: center; border: 1px solid #E5E7EB;">
                        <div style="font-size: 0.65rem; color: #64748B; margin-bottom: 0.15rem;">原始正确率</div>
                        <div style="font-size: 1.1rem; font-weight: 700; color: {'#10B981' if raw_rate >= 99 else '#EF4444'};">{raw_rate:.2f}%</div>
                        <div style="font-size: 0.6rem; margin-top: 0.15rem; height: 0.8rem;">{raw_change}</div>
                    </div>
                    <div style="background: white; padding: 0.6rem; border-radius: 0.5rem; text-align: center; border: 1px solid #E5E7EB;">
                        <div style="font-size: 0.65rem; color: #64748B; margin-bottom: 0.15rem;">最终正确率</div>
                        <div style="font-size: 1.1rem; font-weight: 700; color: {'#10B981' if final_rate >= 99 else '#EF4444'};">{final_rate:.2f}%</div>
                        <div style="font-size: 0.6rem; margin-top: 0.15rem; height: 0.8rem;">{final_change}</div>
                    </div>
                    <div style="background: white; padding: 0.6rem; border-radius: 0.5rem; text-align: center; border: 1px solid #E5E7EB;">
                        <div style="font-size: 0.65rem; color: #64748B; margin-bottom: 0.15rem;">质检量</div>
                        <div style="font-size: 1.1rem; font-weight: 700; color: #1E293B;">{qa_cnt:,}</div>
                    </div>
                    <div style="background: white; padding: 0.6rem; border-radius: 0.5rem; text-align: center; border: 1px solid #E5E7EB;">
                        <div style="font-size: 0.65rem; color: #64748B; margin-bottom: 0.15rem;">原始错误量</div>
                        <div style="font-size: 1rem; font-weight: 700; color: #EF4444;">{raw_error_cnt:,}</div>
                    </div>
                    <div style="background: white; padding: 0.6rem; border-radius: 0.5rem; text-align: center; border: 1px solid #E5E7EB;">
                        <div style="font-size: 0.65rem; color: #64748B; margin-bottom: 0.15rem;">错判率</div>
                        <div style="font-size: 1rem; font-weight: 700; color: {misjudge_color};">{misjudge_rate:.2f}%</div>
                    </div>
                    <div style="background: white; padding: 0.6rem; border-radius: 0.5rem; text-align: center; border: 1px solid #E5E7EB;">
                        <div style="font-size: 0.65rem; color: #64748B; margin-bottom: 0.15rem;">漏判率</div>
                        <div style="font-size: 1rem; font-weight: 700; color: {missjudge_color};">{missjudge_rate:.2f}%</div>
                    </div>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if st.button(f"🔍 查看详情", key=f"btn_{group_name}", use_container_width=True):
            st.session_state["selected_group"] = group_name
            st.rerun()

st.markdown("---")

# ==================== 第四行：队列概览 + 趋势 ====================
queue_col, trend_col = st.columns([1, 1.2])

with queue_col:
    st.markdown("#### 🥧 队列抽检分布")
    if not queue_df.empty:
        # 饼图：取前 8 队列
        top_n = 8
        if len(queue_df) > top_n:
            top_queues = queue_df.head(top_n).copy()
            other_total = queue_df.iloc[top_n:]["total_qa_cnt"].sum()
            other_row = pd.DataFrame([{"queue_name": "其他", "total_qa_cnt": other_total}])
            pie_df = pd.concat([top_queues, other_row], ignore_index=True)
        else:
            pie_df = queue_df
        
        fig_pie = px.pie(pie_df, values="total_qa_cnt", names="queue_name", hole=0.4)
        
        # 计算总量用于占比计算
        pie_total_qa = pie_df["total_qa_cnt"].sum()
        
        # 自定义 hover 显示：名称、占比、量级
        fig_pie.update_traces(
            textposition="inside", 
            textinfo="percent+label", 
            textfont_size=11,
            hovertemplate="<b>%{label}</b><br>占比: %{percent}<br>量级: %{value:,} 条<extra></extra>"
        )
        fig_pie.update_layout(
            height=300, 
            margin=dict(l=20, r=20, t=10, b=10), 
            showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_pie, use_container_width=True)
        
        # 添加说明
        st.caption(f"💡 共 {len(queue_df)} 个队列，展示前 {top_n} 个")
    else:
        st.info("暂无队列数据")

with trend_col:
    st.markdown("#### 📈 正确率趋势")
    if not trend_df.empty:
        trend_df["anchor_date"] = pd.to_datetime(trend_df["anchor_date"])
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=trend_df["anchor_date"], y=trend_df["final_accuracy_rate"],
            mode="lines+markers", name="最终正确率",
            line=dict(color=COLOR_SUCCESS, width=3), marker=dict(size=8),
            text=[f"{v:.2f}%" for v in trend_df["final_accuracy_rate"]],
            hovertemplate="<b>%{x|%Y-%m-%d}</b><br>最终正确率: %{text}<extra></extra>"
        ))
        fig.add_trace(go.Scatter(
            x=trend_df["anchor_date"], y=trend_df["raw_accuracy_rate"],
            mode="lines+markers", name="原始正确率",
            line=dict(color="#94A3B8", width=2, dash="dot"), marker=dict(size=6),
            text=[f"{v:.2f}%" for v in trend_df["raw_accuracy_rate"]],
            hovertemplate="<b>%{x|%Y-%m-%d}</b><br>原始正确率: %{text}<extra></extra>"
        ))
        fig.add_hline(y=99.0, line_dash="dash", line_color=COLOR_WARN, annotation_text="目标 99%", annotation_position="right")
        # 动态 y 轴范围：数据在 99% 以上时不截断
        all_rates = pd.concat([trend_df["final_accuracy_rate"], trend_df["raw_accuracy_rate"]]).dropna()
        if not all_rates.empty:
            y_min = max(0, all_rates.min() - 1)
            y_max = min(101, all_rates.max() + 1)
        else:
            y_min, y_max = 95, 100.5
        fig.update_layout(
            height=300, margin=dict(l=20, r=20, t=10, b=30),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            yaxis_range=[y_min, y_max], yaxis_title="正确率 (%)",
            xaxis=dict(tickformat="%Y-%m-%d", tickangle=-45),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        # 支持点击交互
        clicked = st.plotly_chart(fig, use_container_width=True, on_select="rerun", key="trend_chart")
        
        # 处理点击事件
        if clicked and clicked.get("selection"):
            selection = clicked["selection"]
            if selection.get("point_indices"):
                clicked_idx = selection["point_indices"][0]
                if clicked_idx < len(trend_df):
                    clicked_date = trend_df.iloc[clicked_idx]["anchor_date"]
                    st.info(f"💡 点击了 {clicked_date.strftime('%Y-%m-%d')}，可切换到对应日期查看详情")
    else:
        st.info("暂无趋势数据")

st.markdown("---")

# ==================== 第四行半：Phase 1 新增维度 ====================
st.markdown("### 🔍 深度维度分析")
st.caption("💡 高频错误、申诉分析、标签准确率 — 数据已有但之前未展示的维度")

# ── 加载 Phase 1 新数据 ──
_error_top5_df = load_error_top5_cached(grain, selected_date, selected_group)
_appeal_analysis = load_appeal_analysis_cached(grain, selected_date, selected_group)
_label_accuracy_df = load_label_accuracy_cached(grain, selected_date, selected_group)

error_col, appeal_col = st.columns([1, 1.2])

with error_col:
    st.markdown("#### 🔥 高频错误 Top5")
    if not _error_top5_df.empty:
        # 横条图
        _err_display = _error_top5_df.sort_values("error_cnt", ascending=True)
        fig_err = px.bar(
            _err_display, x="error_cnt", y="error_type",
            orientation="h",
            text=_err_display["pct"].apply(lambda x: f"{x}%"),
            color="error_cnt",
            color_continuous_scale="Reds",
        )
        fig_err.update_traces(textposition="outside", textfont_size=11)
        fig_err.update_layout(
            height=280, margin=dict(l=20, r=20, t=10, b=20),
            xaxis_title="错误量", yaxis_title="",
            showlegend=False, coloraxis_showscale=False,
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_err, use_container_width=True)
        st.caption(f"共 {_error_top5_df['error_cnt'].sum():,} 条错误记录")
    else:
        st.info("暂无错误类型数据")

with appeal_col:
    st.markdown("#### 🔄 申诉分析")
    if _appeal_analysis.get("summary"):
        _s = _appeal_analysis["summary"]
        _total_qa = _s.get("total_qa", 0) or 0
        _appeal_cnt = _s.get("appeal_cnt", 0) or 0
        _appeal_rate = _s.get("appeal_rate", 0) or 0
        _reversed_cnt = _s.get("reversed_cnt", 0) or 0
        _reverse_rate = _s.get("reverse_success_rate", 0) or 0

        # 申诉概览卡片
        ac1, ac2, ac3, ac4 = st.columns(4)
        with ac1:
            st.metric("申诉量", f"{_appeal_cnt:,}", delta=f"{_appeal_rate}% 占比")
        with ac2:
            st.metric("改判成功", f"{_reversed_cnt:,}", delta=f"{_reverse_rate}% 成功率")
        with ac3:
            st.metric("总质检量", f"{int(_total_qa):,}")
        with ac4:
            st.metric("未申诉率", f"{100 - _appeal_rate:.1f}%")

        # Top 申诉理由
        if not _appeal_analysis.get("reasons", pd.DataFrame()).empty:
            _reasons = _appeal_analysis["reasons"]
            st.markdown("**📝 Top 申诉理由：**")
            _reason_show = pd.DataFrame()
            _reason_show["申诉理由"] = _reasons["appeal_reason"].apply(lambda x: x[:30] + "..." if len(str(x)) > 30 else x)
            _reason_show["次数"] = _reasons["cnt"]
            _reason_show["占比"] = _reasons["pct"].apply(lambda x: f"{x}%")
            st.dataframe(_reason_show, use_container_width=True, hide_index=True, height=max(120, min(200, 35 * len(_reasons) + 40)))
    else:
        st.info("暂无申诉数据")

# ==================== Phase 2 新增：错误趋势 + 下探 + 词频 ====================
st.markdown("---")
st.markdown("### 📈 错误深度分析（Phase 2 新增）")
st.caption("💡 Top3错误类型趋势 · 点击错误类型下探到审核人 · 错误原因词频")

# ── 1. 错误类型趋势面积图 ──
_error_trend_df = load_error_type_trend_cached(selected_date, selected_group, days=14, top_n=3)

st.markdown("#### 📉 Top3 错误类型趋势（近14天）")
if not _error_trend_df.empty:
    fig_trend = px.area(
        _error_trend_df, x="biz_date", y="error_cnt", color="error_type",
        markers=True,
        color_discrete_sequence=px.colors.qualitative.Set2,
    )
    fig_trend.update_traces(hovertemplate="<b>%{fullData.name}</b><br>日期: %{x}<br>错误量: %{y:,}<extra></extra>")
    fig_trend.update_layout(
        height=280, margin=dict(l=20, r=20, t=10, b=30),
        xaxis_title="日期", yaxis_title="错误量",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
    )
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("暂无错误趋势数据")

# ── 2. 错误类型→受影响审核人 下探 ──
st.markdown("#### 🎯 错误类型下探 → 受影响审核人")
_error_types_for_select = _error_top5_df["error_type"].tolist() if not _error_top5_df.empty else []
if _error_types_for_select:
    probe_col1, probe_col2 = st.columns([1, 2])
    with probe_col1:
        selected_error_type = st.selectbox(
            "选择错误类型", options=_error_types_for_select,
            key="error_type_probe", index=0
        )
    with probe_col2:
        if selected_error_type:
            _affected_df = load_error_affected_reviewers_cached(grain, selected_date, selected_group, selected_error_type, top_n=10)
            if not _affected_df.empty:
                aff_show = pd.DataFrame()
                aff_show["审核人"] = _affected_df["reviewer_name"].apply(lambda x: x.split("-")[-1] if "-" in x else x)
                aff_show["队列"] = _affected_df["queue_name"]
                aff_show["错误量"] = _affected_df["error_cnt"].apply(lambda x: f"{int(x):,}")
                aff_show["错误率"] = _affected_df["error_rate"].apply(lambda x: f"{x:.1f}%")
                st.dataframe(aff_show, use_container_width=True, hide_index=True,
                             height=max(120, min(320, 35 * len(_affected_df) + 40)))
            else:
                st.info(f"「{selected_error_type}」暂无受影响审核人数据")
else:
    st.info("暂无错误类型可选")

# ── 3. error_reason 词频统计 ──
_reason_freq_df = load_error_reason_wordcloud_cached(grain, selected_date, selected_group, top_n=15)
st.markdown("#### 📝 错误原因 Top15")
if not _reason_freq_df.empty:
    reason_col1, reason_col2 = st.columns([1.2, 1])
    with reason_col1:
        _reason_display = _reason_freq_df.sort_values("cnt", ascending=True)
        fig_reason = px.bar(
            _reason_display, x="cnt", y="error_reason",
            orientation="h",
            text=_reason_display["cnt"].apply(lambda x: f"{int(x):,}"),
            color="cnt",
            color_continuous_scale="Oranges",
        )
        fig_reason.update_traces(textposition="outside", textfont_size=10)
        fig_reason.update_layout(
            height=max(250, 22 * len(_reason_display) + 40),
            margin=dict(l=20, r=20, t=10, b=20),
            xaxis_title="频次", yaxis_title="",
            showlegend=False, coloraxis_showscale=False,
            yaxis=dict(tickfont=dict(size=10)),
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_reason, use_container_width=True)
    with reason_col2:
        # 文本形式的表格
        rr_show = pd.DataFrame()
        rr_show["错误原因"] = _reason_freq_df["error_reason"].apply(lambda x: str(x)[:40] + "..." if len(str(x)) > 40 else x)
        rr_show["频次"] = _reason_freq_df["cnt"].apply(lambda x: f"{int(x):,}")
        rr_show["占比"] = (_reason_freq_df["cnt"] / _reason_freq_df["cnt"].sum() * 100).apply(lambda x: f"{x:.1f}%")
        st.dataframe(rr_show, use_container_width=True, hide_index=True,
                     height=max(250, 22 * len(_reason_freq_df) + 40))
else:
    st.info("暂无 error_reason 数据")

# ==================== 质检结果分布（正确/错判/漏判） ====================
st.markdown("### 🏷️ 质检标签分布")
_result_dist_df = load_qa_result_distribution_cached(grain, selected_date, selected_group)
if not _result_dist_df.empty:
    _total_qa_records = int(_result_dist_df["cnt"].sum())
    st.caption(f"前10个标签（按质检量降序），共 {_total_qa_records:,} 条质检记录")
    _result_display = _result_dist_df.copy()
    # 按数量降序排列（正确最多排最后，横向条形图视觉上从上到下递增）
    _result_display = _result_display.sort_values("cnt", ascending=True)

    # 颜色映射
    _result_colors = {"正确": "#1e3a5f", "错判": "#ef4444", "漏判": "#f97316"}

    fig_result = px.bar(
        _result_display, x="cnt", y="result_label",
        orientation="h",
        text=_result_display.apply(
            lambda r: f"{r['pct']}%" if r['result_label'] in ('错判', '漏判') else f"{int(r['cnt']):,}",
            axis=1,
        ),
        color="result_label",
        color_discrete_map=_result_colors,
    )
    fig_result.update_traces(textposition="outside", textfont_size=12)
    fig_result.update_layout(
        height=260, margin=dict(l=20, r=20, t=10, b=20),
        xaxis_title="质检量", yaxis_title="",
        showlegend=False,
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
    )
    st.plotly_chart(fig_result, use_container_width=True)

    # 验证：显示百分比总和（调试用，确认 = 100%）
    _pct_sum = _result_dist_df["pct"].sum()
    if abs(_pct_sum - 100.0) > 0.1:
        st.caption(f"⚠️ 百分比总和={_pct_sum}%（应接近100%，如有偏差请检查数据完整性）")
else:
    st.info("暂无质检结果分布数据")

st.markdown("### 🔍 数据下探分析")
st.caption("💡 通过选择队列和审核人，逐层下探到具体问题样本")

detail_payload = load_group_detail(grain, selected_date, selected_group, None, None, None, None)
detail_queue_df: pd.DataFrame = detail_payload["queue_df"]
detail_auditor_df: pd.DataFrame = detail_payload["auditor_df"]

# 队列和审核人选择器（优化布局）
select_col1, select_col2, select_col3 = st.columns([1.5, 1.5, 2])
with select_col1:
    queue_options = ["(全部)"] + detail_queue_df["queue_name"].tolist() if not detail_queue_df.empty else ["(全部)"]
    selected_queue = st.selectbox("🎯 选择队列", options=queue_options, key="queue_selector", label_visibility="visible")
with select_col2:
    # 如果选择了队列，过滤审核人列表
    if selected_queue != "(全部)" and not detail_auditor_df.empty:
        # 重新加载选定队列的审核人数据
        filtered_auditor_payload = load_group_detail(grain, selected_date, selected_group, selected_queue, None, None, None)
        filtered_auditor_df = filtered_auditor_payload["auditor_df"]
        auditor_options = ["(全部)"] + filtered_auditor_df["reviewer_name"].tolist() if not filtered_auditor_df.empty else ["(全部)"]
    else:
        auditor_options = ["(全部)"] + detail_auditor_df["reviewer_name"].tolist() if not detail_auditor_df.empty else ["(全部)"]
    selected_auditor = st.selectbox("👤 选择审核人", options=auditor_options, key="auditor_selector", label_visibility="visible")
with select_col3:
    # 面包屑导航（优化样式）
    breadcrumb_parts = [f"<span style='font-weight:600; color:#3B82F6; background: #EFF6FF; padding: 0.25rem 0.5rem; border-radius: 0.375rem;'>{selected_group}</span>"]
    if selected_queue != "(全部)":
        breadcrumb_parts.append(f"<span style='color:#94A3B8;'>›</span> <span style='font-weight:600; color:#10B981; background: #F0FDF4; padding: 0.25rem 0.5rem; border-radius: 0.375rem;'>{selected_queue}</span>")
    if selected_auditor != "(全部)":
        breadcrumb_parts.append(f"<span style='color:#94A3B8;'>›</span> <span style='font-weight:600; color:#F59E0B; background: #FFFBEB; padding: 0.25rem 0.5rem; border-radius: 0.375rem;'>{selected_auditor}</span>")
    breadcrumb_html = " ".join(breadcrumb_parts)
    st.markdown(f"""
        <div style='padding: 0.75rem; background: linear-gradient(135deg, #F8FAFC 0%, #F1F5F9 100%); border-radius: 0.5rem; border: 1px solid #E5E7EB;'>
            <div style='font-size: 0.75rem; color: #64748B; margin-bottom: 0.5rem;'>📍 当前下探路径</div>
            <div style='font-size: 0.9rem;'>{breadcrumb_html}</div>
        </div>
    """, unsafe_allow_html=True)

# 快捷筛选按钮（优化样式）
_quick_filter = st.session_state.get("quick_filter")
st.markdown("##### ⚡ 快捷筛选")
quick_filter_col1, quick_filter_col2, quick_filter_col3, quick_filter_col4, _ = st.columns([1.2, 1.2, 1.2, 1.2, 0.8])
with quick_filter_col1:
    if st.button("🔴 错误量TOP5", use_container_width=True, help="筛选错误量最多的5个队列"):
        st.session_state["quick_filter"] = "error_top5"
        st.rerun()
with quick_filter_col2:
    if st.button("📉 正确率<99%", use_container_width=True, help="筛选正确率低于99%的队列"):
        st.session_state["quick_filter"] = "low_rate"
        st.rerun()
with quick_filter_col3:
    if st.button("⚠️ 有错判/漏判", use_container_width=True, help="筛选有错判或漏判的队列"):
        st.session_state["quick_filter"] = "has_judge_error"
        st.rerun()
with quick_filter_col4:
    if st.button("🔄 重置筛选", use_container_width=True, help="清除所有筛选条件"):
        st.session_state["quick_filter"] = None
        st.session_state["queue_selector"] = "(全部)"
        st.session_state["auditor_selector"] = "(全部)"
        st.rerun()

# 根据选择重新加载数据
if selected_queue != "(全部)" or selected_auditor != "(全部)":
    final_payload = load_group_detail(
        grain, selected_date, selected_group,
        selected_queue if selected_queue != "(全部)" else None,
        selected_auditor if selected_auditor != "(全部)" else None,
        None, None
    )
    final_auditor_df = final_payload["auditor_df"]
else:
    final_auditor_df = detail_auditor_df

rank_col, auditor_col = st.columns([1.2, 1])

with rank_col:
    st.markdown("#### 🏆 队列正确率排名")
    if not detail_queue_df.empty:
        # 应用快捷筛选
        display_queue_df = detail_queue_df.copy()
        filter_label = "按最终正确率升序排列（问题队列优先展示）"
        if _quick_filter == "error_top5":
            display_queue_df = display_queue_df.nlargest(5, "final_error_cnt")
            filter_label = "🔴 错误量 TOP5"
        elif _quick_filter == "low_rate":
            display_queue_df = display_queue_df[display_queue_df["final_accuracy_rate"] < 99]
            filter_label = "📉 正确率 < 99% 的队列"
        elif _quick_filter == "has_judge_error":
            display_queue_df = display_queue_df[
                (display_queue_df.get("misjudge_rate", 0) > 0) | (display_queue_df.get("missjudge_rate", 0) > 0)
            ] if "misjudge_rate" in display_queue_df.columns else display_queue_df
            filter_label = "⚠️ 有错判/漏判的队列"
        
        st.caption(f"共 {len(display_queue_df)} 个队列 · {filter_label}")
        
        queue_show = pd.DataFrame()
        queue_show["队列"] = display_queue_df["queue_name"]
        # 审核人数（mart 表有 reviewer_cnt 字段）
        _reviewer_cnt_col = display_queue_df["reviewer_cnt"] if "reviewer_cnt" in display_queue_df.columns else None
        # 出错量：优先 final_error_cnt，否则用 qa_cnt - final_correct_cnt
        if "final_error_cnt" in display_queue_df.columns:
            _error_cnt_col = display_queue_df["final_error_cnt"].fillna(0)
        else:
            _error_cnt_col = (display_queue_df["qa_cnt"] - display_queue_df.get("final_correct_cnt", display_queue_df["qa_cnt"])).fillna(0)
        # 申诉改判率
        _appeal_col = display_queue_df.get("appeal_reverse_rate")

        # ── 一步到位：全部格式化为带条件的字符串（Styler / GDG 兼容）──
        def _fmt_queue_row(row):
            """将一行数据格式化为最终显示字符串，阈值自动标色。"""
            qa = row["qa_cnt"]
            rc = row["raw_accuracy_rate"]
            fc = row["final_accuracy_rate"]
            mj = row.get("misjudge_rate", 0) or 0
            msj = row.get("missjudge_rate", 0) or 0
            ar = _appeal_col.get(row.name) if _appeal_col is not None and hasattr(_appeal_col, 'get') else (_appeal_col.iloc[row.name] if _appeal_col is not None else 0)
            rcnt = _reviewer_cnt_col.iloc[row.name] if _reviewer_cnt_col is not None else None
            ec = _error_cnt_col.iloc[row.name] if hasattr(_error_cnt_col, 'iloc') else _error_cnt_col

            return pd.Series({
                "队列": str(row["queue_name"]),
                "质检量": f"{int(qa):,}",
                "审核人数": f"{int(rcnt):,}" if rcnt is not None and pd.notna(rcnt) else "—",
                "出错量": f"{int(ec):,}" if pd.notna(ec) else "—",
                "原始正确率": _color_val(float(rc), 99.5, 99),
                "最终正确率": _color_val(float(fc), 99.5, 99),
                "错判率": _color_val(float(mj), 0.5, 0.3),
                "漏判率": _color_val(float(msj), 0.35, 0.2),
                "申诉改判率": _color_val(float(ar) if pd.notna(ar) else 0, 18, 999, inverse=True) if pd.notna(ar) else "—",
                "达标": "⚠️ 不达标" if pd.notna(fc) and fc < 99 else "✅ 达标",
            })

        queue_show = display_queue_df.apply(_fmt_queue_row, axis=1)

        # 构建列配置：全部强制为 TextColumn，绕过 GDG 类型推断
        _qcc = {"队列": st.column_config.TextColumn("队列", width="medium")}
        for _cn in ["质检量", "审核人数", "出错量"]:
            if _cn in queue_show.columns:
                _qcc[_cn] = st.column_config.TextColumn(_cn, width="small")
        for _cn in ["原始正确率", "最终正确率", "错判率", "漏判率", "申诉改判率"]:
            if _cn in queue_show.columns:
                _qcc[_cn] = st.column_config.TextColumn(_cn, width="small")
        if "达标" in queue_show.columns:
            _qcc["达标"] = st.column_config.TextColumn("达标", width="small")

        st.dataframe(
            queue_show,
            use_container_width=True,
            hide_index=True,
            height=320,
            column_config=_qcc,
        )
    else:
        st.info("暂无队列数据")

with auditor_col:
    st.markdown("#### 👥 审核人视图")
    if not final_auditor_df.empty:
        # 添加提示信息
        st.caption(f"共 {len(final_auditor_df)} 位审核人，按最终正确率升序排列（需关注的审核人优先展示）")
        
        # ── 一步到位：全部格式化为带条件的字符串（Styler / GDG 兼容）──
        _auditor_appeal_col = final_auditor_df.get("appeal_reverse_rate")

        def _fmt_auditor_row(row):
            qa = row["qa_cnt"]
            mj_cnt = row.get("misjudge_cnt", 0) or 0
            msj_cnt = row.get("missjudge_cnt", 0) or 0
            rc = row["raw_accuracy_rate"]
            fc = row["final_accuracy_rate"]
            mj_rate = row.get("misjudge_rate", 0) or 0
            msj_rate = row.get("missjudge_rate", 0) or 0
            ar = _auditor_appeal_col.iloc[row.name] if _auditor_appeal_col is not None else 0

            return pd.Series({
                "审核人": str(row["reviewer_name"]),
                "质检量": f"{int(qa):,}",
                "错判量": f"{int(mj_cnt):,}" if pd.notna(mj_cnt) else "—",
                "漏判量": f"{int(msj_cnt):,}" if pd.notna(msj_cnt) else "—",
                "原始正确率": _color_val(float(rc), 99.5, 99),
                "最终正确率": _color_val(float(fc), 99.5, 99),
                "错判率": _color_val(float(mj_rate), 0.5, 0.3),
                "漏判率": _color_val(float(msj_rate), 0.35, 0.2),
                "申诉改判率": _color_val(float(ar) if pd.notna(ar) else 0, 18, 999, inverse=True) if pd.notna(ar) else "—",
                "达标": "⚠️ 不达标" if pd.notna(fc) and fc < 99 else "✅ 达标",
            })

        auditor_show = final_auditor_df.apply(_fmt_auditor_row, axis=1)

        # 构建列配置：全部强制为 TextColumn
        _acc = {"审核人": st.column_config.TextColumn("审核人", width="medium")}
        for _cn in ["质检量", "错判量", "漏判量"]:
            _acc[_cn] = st.column_config.TextColumn(_cn, width="small")
        for _cn in ["原始正确率", "最终正确率", "错判率", "漏判率", "申诉改判率"]:
            if _cn in auditor_show.columns:
                _acc[_cn] = st.column_config.TextColumn(_cn, width="small")
        if "达标" in auditor_show.columns:
            _acc["达标"] = st.column_config.TextColumn("达标", width="small")

        st.dataframe(
            auditor_show,
            use_container_width=True,
            hide_index=True,
            height=320,
            column_config=_acc,
        )
    else:
        st.info("暂无审核人数据")

# ==================== 第六行：Phase 1 升级 — 标签准确率排行 + 内容类型/时段 ====================
st.markdown("---")
st.markdown("### 📊 质量深度分析（Phase 1 新增）")
st.caption("💡 标签准确率 → 哪些标签最难判 · 内容类型差异 · 时段质量波动")

# ── 加载新维度数据 ──
_content_type_df = load_content_type_distribution_cached(grain, selected_date, selected_group)
_hourly_df = load_hourly_heatmap_cached(grain, selected_date, selected_group)

# 左：标签准确率排行
# 右：内容类型分布
label_col2, ct_col = st.columns([1.2, 1])

with label_col2:
    st.markdown("#### 🏷️ 标签准确率排行")
    if not _label_accuracy_df.empty:
        st.caption(f"按正确率升序排列（最难判的标签在前），共 {_label_accuracy_df['qa_cnt'].sum():,} 条记录")
        # 横条图，x轴=正确率，颜色=质检量级
        _lab_display = _label_accuracy_df.copy()
        fig_lab = px.bar(
            _lab_display, x="accuracy_rate", y="label_name",
            orientation="h",
            text=_lab_display.apply(lambda r: f"{r['accuracy_rate']}% ({int(r['qa_cnt']):,})", axis=1),
            color="qa_cnt",
            color_continuous_scale="Blues",
            range_x=[0, 100],
        )
        fig_lab.add_vline(x=99, line_dash="dash", line_color="#F59E0B", annotation_text="99%目标", annotation_position="top")
        fig_lab.update_traces(textposition="outside", textfont_size=10)
        fig_lab.update_layout(
            height=max(280, 30 * len(_lab_display) + 40),
            margin=dict(l=20, r=20, t=10, b=20),
            xaxis_title="正确率 (%)", yaxis_title="",
            showlegend=False, coloraxis_showscale=False,
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_lab, use_container_width=True)
    else:
        st.info("暂无标签数据")

with ct_col:
    st.markdown("#### 📂 内容类型分布")
    if not _content_type_df.empty:
        # 饼图 + 表格组合
        ct_pie = px.pie(_content_type_df, values="qa_cnt", names="content_type", hole=0.45)
        ct_pie.update_traces(
            textposition="inside", textinfo="percent+label", textfont_size=11,
            hovertemplate="<b>%{label}</b><br>占比: %{percent}<br>量级: %{value:,} <extra></extra>"
        )
        ct_pie.update_layout(height=260, margin=dict(l=10, r=10, t=10, b=10),
                            showlegend=False, paper_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(ct_pie, use_container_width=True)

        # 简要表格
        ct_show = pd.DataFrame()
        ct_show["内容类型"] = _content_type_df["content_type"]
        ct_show["量级"] = _content_type_df["qa_cnt"].apply(lambda x: f"{int(x):,}")
        ct_show["正确率"] = _content_type_df["accuracy_rate"].apply(lambda x: f"{x:.1f}%")
        st.dataframe(ct_show, use_container_width=True, hide_index=True, height=min(180, 35 * len(_content_type_df) + 40))
    else:
        st.info("暂无内容类型数据")

# ── 第七行：时段质量热力图 ──
st.markdown("#### 📅 时段质量热力图")
if not _hourly_df.empty:
    import numpy as np

    # 补全天 0-23 小时
    all_hours = set(range(24))
    exist_hours = set(_hourly_df["hour"].tolist())
    for h in all_hours - exist_hours:
        _hourly_df = pd.concat([_hourly_df, pd.DataFrame([{"hour": h, "qa_cnt": 0, "correct_cnt": 0, "accuracy_rate": None}])], ignore_index=True)
    _hourly_df = _hourly_df.sort_values("hour").reset_index(drop=True)

    # 热力图用 Plotly heatmap
    fig_heat = go.Figure()
    # 用色阶表示正确率，气泡大小表示质检量
    fig_heat.add_trace(go.Bar(
        x=[f"{h:02d}:00" for h in _hourly_df["hour"]],
        y=_hourly_df["qa_cnt"],
        marker_color=_hourly_df["accuracy_rate"],
        marker_colorscale="RdYlGn",
        marker_line_color="#E5E7EB",
        marker_line_width=1,
        name="质检量",
        hovertemplate=(
            "<b>%{x}</b><br>"
            "质检量: %{y:,}<br>"
            "正确率: %{marker.color:.1f}%<extra></extra>"
        ),
    ))
    # 目标线 99%
    fig_heat.add_hline(y=99, line_dash="dash", line_color="#F59E0B", annotation_text="99%目标线", annotation_position="right")

    # 计算均值标注低峰时段
    avg_acc = _hourly_df[_hourly_df["qa_cnt"] > 0]["accuracy_rate"].mean()
    fig_heat.update_layout(
        height=250,
        margin=dict(l=20, r=20, t=10, b=40),
        xaxis_title="时间段 (小时)", yaxistitle="质检量",
        xaxis=dict(tickangle=-45),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        annotations=[
            dict(x=0.98, y=0.95, xref="paper", yref="paper",
                 text=f"平均正确率: {avg_acc:.1f}%", showarrow=False, font=dict(size=11, color="#64748B"))
        ]
    )
    st.plotly_chart(fig_heat, use_container_width=True)
    st.caption(f"💡 颜色深浅代表正确率（绿高红低），柱高代表质检量 | 平均正确率: **{avg_acc:.1f}%**")
else:
    st.info("暂无时段数据（需要 qa_time 字段有精确时间）")

# ── 质检员工作量（保留原有功能） ──
st.markdown("---")
owner_col2 = st.container()
with owner_col2:
    st.markdown("#### 👨‍💼 质检员工作量 Top10")
    owner_df = load_qa_owner_distribution_cached(grain, selected_date, selected_group, top_n=10)
    if not owner_df.empty:
        owner_show = pd.DataFrame()
        owner_show["质检员"] = owner_df["owner_name"].apply(lambda x: x.split("-")[-1] if "-" in x else x)
        owner_show["质检量"] = owner_df["qa_cnt"].apply(lambda x: f"{int(x):,}")
        owner_show["正确率"] = owner_df["accuracy_rate"].apply(lambda x: f"{x:.2f}%")
        owner_show["出错量"] = owner_df["error_cnt"].apply(lambda x: f"{int(x):,}")
        
        st.dataframe(
            owner_show,
            use_container_width=True,
            hide_index=True,
            height=320,
            column_config={
                "质检员": st.column_config.TextColumn("质检员", width="medium"),
                "质检量": st.column_config.TextColumn("质检量", width="small"),
                "正确率": st.column_config.TextColumn("正确率", width="small"),
                "出错量": st.column_config.TextColumn("出错量", width="small"),
            }
        )
    else:
        st.info("暂无质检员数据")

# ==================== inspect_type / workforce_type 分布（Phase 3 #11） ====================
st.markdown("---")
st.markdown("### 🏷️ 质检类型 & 人员类型分布（Phase 3 新增）")
st.caption("💡 外检/内检分类 · 正式/新人/复培/外检人员类型")

_it_df = load_inspect_type_cached(grain, selected_date, selected_group)
_wt_df = load_workforce_type_cached(grain, selected_date, selected_group)

it_col1, it_col2 = st.columns(2)

with it_col1:
    st.markdown("#### 🔍 质检类型（inspect_type）")
    if not _it_df.empty:
        fig_it = px.pie(
            _it_df, names="inspect_type", values="qa_cnt",
            hole=0.45,
            color="inspect_type",
            color_discrete_map={"internal": "#4CAF50", "external": "#2196F3", "未知": "#9E9E9E"},
        )
        fig_it.update_traces(
            textposition="inside",
            textinfo="label+percent+value",
            hovertemplate="<b>%{label}</b><br>数量: %{value:,}<br>占比: %{percent}<extra></extra>",
        )
        fig_it.update_layout(height=260, margin=dict(l=10, r=10, t=10, b=10), showlegend=False)
        st.plotly_chart(fig_it, use_container_width=True)
        # 表格
        it_show = pd.DataFrame()
        it_show["类型"] = _it_df["inspect_type"]
        it_show["数量"] = _it_df["qa_cnt"].apply(lambda x: f"{int(x):,}")
        it_show["正确率"] = _it_df["accuracy_rate"].apply(lambda x: f"{x:.2f}%")
        st.dataframe(it_show, use_container_width=True, hide_index=True, height=min(120, 35 * len(_it_df) + 40))
    else:
        st.info("暂无 inspect_type 数据")

with it_col2:
    st.markdown("#### 👥 人员类型（workforce_type）")
    if not _wt_df.empty:
        fig_wt = px.bar(
            _wt_df, x="workforce_type", y="qa_cnt",
            color="accuracy_rate",
            color_continuous_scale="RdYlGn",
            text=_wt_df["qa_cnt"].apply(lambda x: f"{int(x):,}"),
        )
        fig_wt.update_traces(textposition="outside", textfont_size=10)
        fig_wt.update_layout(
            height=260, margin=dict(l=10, r=10, t=10, b=30),
            xaxis_title="", yaxis_title="质检量",
            showlegend=False, coloraxis_showscale=False,
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_wt, use_container_width=True)
        # 表格
        wt_show = pd.DataFrame()
        wt_show["类型"] = _wt_df["workforce_type"]
        wt_show["数量"] = _wt_df["qa_cnt"].apply(lambda x: f"{int(x):,}")
        wt_show["正确率"] = _wt_df["accuracy_rate"].apply(lambda x: f"{x:.2f}%")
        st.dataframe(wt_show, use_container_width=True, hide_index=True, height=min(120, 35 * len(_wt_df) + 40))
    else:
        st.info("暂无 workforce_type 数据")

# ==================== 数据健康指示器（Phase 2 #10） ====================
st.markdown("---")
st.markdown("### 🏥 数据健康指示器")
st.caption("💡 监控数据质量：关联匹配率、缺失主键率、重复率")

_health = load_data_health_cached(selected_date)

health_col1, health_col2, health_col3, health_col4 = st.columns(4)

with health_col1:
    _am = _health.get("appeal_match", {})
    _total_qa_h = _am.get("total_qa", 0) or 0
    _amr = _am.get("appeal_match_rate", 0) or 0
    _am_color = "🟢" if _amr >= 10 else ("🟡" if _amr >= 5 else "🔴")
    st.metric(f"{_am_color} 申诉关联率", f"{_amr}%",
              delta=f"总质检 {_total_qa_h:,}",
              delta_color="off")

with health_col2:
    _mk = _health.get("missing_key", {})
    _missing_rate = _mk.get("missing_rate", 0) or 0
    _missing_cnt = _mk.get("missing_cnt", 0) or 0
    _mk_color = "🟢" if _missing_rate <= 1 else ("🟡" if _missing_rate <= 5 else "🔴")
    st.metric(f"{_mk_color} 缺失主键率", f"{_missing_rate}%",
              delta=f"{_missing_cnt:,} 条缺失",
              delta_color="inverse")

with health_col3:
    _dp = _health.get("duplicate", {})
    _dup_rate = _dp.get("dup_rate", 0) or 0
    _dup_cnt = _dp.get("dup_cnt", 0) or 0
    _dp_color = "🟢" if _dup_rate <= 0.5 else ("🟡" if _dup_rate <= 2 else "🔴")
    st.metric(f"{_dp_color} 重复数据率", f"{_dup_rate}%",
              delta=f"{_dup_cnt:,} 条重复",
              delta_color="inverse")

with health_col4:
    _al = _health.get("appeal_link", {})
    _link_rate = _al.get("link_rate", 0) or 0
    _al_color = "🟢" if _link_rate >= 90 else ("🟡" if _link_rate >= 70 else "🔴")
    st.metric(f"{_al_color} 申诉→质检关联率", f"{_link_rate}%",
              delta="申诉表能关联到质检事件的比例",
              delta_color="off")

# ==================== 底部说明 ====================
st.markdown("---")
st.markdown("""
<div style='background: linear-gradient(135deg, #F8FAFC 0%, #F1F5F9 100%); padding: 1rem; border-radius: 0.75rem; border: 1px solid #E5E7EB; margin-top: 1rem;'>
    <div style='font-size: 0.85rem; color: #475569; line-height: 1.6;'>
        <strong>💡 看板设计原则：</strong>异常先暴露 → 支持下探 → 沉淀培训动作<br>
        <span style='color: #64748B; font-size: 0.8rem;'>数据每日自动更新 · 告警自动推送 · 支持 CSV 导出</span>
    </div>
</div>
""", unsafe_allow_html=True)
