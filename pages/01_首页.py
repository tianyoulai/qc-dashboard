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

# 全局CSS样式优化
st.markdown("""
<style>
    /* 整体字体优化 */
    .main > div {
        padding-top: 1rem;
    }
    
    /* 卡片悬停效果 */
    .metric-card {
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    }
    
    /* 按钮美化 */
    .stButton > button {
        border-radius: 0.5rem;
        transition: all 0.2s ease;
        font-weight: 500;
    }
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 2px 8px rgba(59, 130, 246, 0.3);
    }
    
    /* 表格美化 */
    .stDataFrame {
        border-radius: 0.5rem;
        overflow: hidden;
        border: 1px solid #E5E7EB;
    }
    
    /* 折叠面板美化 */
    .streamlit-expanderHeader {
        background: #F8FAFC;
        border-radius: 0.5rem;
        border: 1px solid #E5E7EB;
    }
    
    /* 段落间距优化 */
    .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* 标题样式优化 */
    h3 {
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    
    /* 下划线样式 */
    hr {
        margin: 1.5rem 0;
        border-color: #E5E7EB;
    }
</style>
""", unsafe_allow_html=True)

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
    <div style="margin-bottom: 1.5rem; padding: 1.5rem; background: #ffffff; border-radius: 1rem; border-left: 4px solid #2e7d32; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
            <h1 style="margin: 0; font-size: 2rem; font-weight: 700; color: #1a1a1a;">📊 质培运营看板</h1>
            <div style="font-size: 0.85rem; color: #6b7280;">实时监控 · 智能告警 · 数据驱动</div>
        </div>
        <div style="font-size: 0.9rem; color: #4b5563; line-height: 1.6; margin-top: 0.75rem;">
            🎯 日看异常 · 周看复发 · 月看治理<br>
            📍 支持下探链路：<span style="background: #f3f4f6; padding: 0.15rem 0.5rem; border-radius: 0.3rem; color: #374151;">组别</span> → 
            <span style="background: #f3f4f6; padding: 0.15rem 0.5rem; border-radius: 0.3rem; color: #374151;">队列</span> → 
            <span style="background: #f3f4f6; padding: 0.15rem 0.5rem; border-radius: 0.3rem; color: #374151;">审核人</span> → 
            <span style="background: #f3f4f6; padding: 0.15rem 0.5rem; border-radius: 0.3rem; color: #374151;">问题样本</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# 快捷入口按钮（增强版）
st.markdown("#### ⚡ 快捷入口")
quick_col1, quick_col2, quick_col3, quick_col4 = st.columns([1, 1, 1, 1])
with quick_col1:
    if st.button("🚨 今日异常", use_container_width=True, help="快速定位今日 P0/P1 告警"):
        st.session_state["quick_mode"] = "alert"
        st.rerun()
with quick_col2:
    if st.button("📉 最差队列", use_container_width=True, help="查看本周正确率最低的队列"):
        st.session_state["quick_mode"] = "worst_queue"
        st.rerun()
with quick_col3:
    if st.button("🔄 申诉异常", use_container_width=True, help="查看申诉改判率高的组别"):
        st.session_state["quick_mode"] = "appeal"
        st.rerun()
with quick_col4:
    if st.button("📊 数据总览", use_container_width=True, help="跳转到数据总览页"):
        st.switch_page("pages/02_数据总览.py")

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

# ==================== 加载数据 ====================
payload = load_group_overview(grain, selected_date)
group_df: pd.DataFrame = payload["group_df"]
alerts_df: pd.DataFrame = payload["alerts_df"]
alert_summary: dict[str, int] = payload["alert_summary"]
alert_status_summary = service.summarize_alert_status(alerts_df)
alert_sla_summary = service.summarize_alert_sla(alerts_df)

# 预先获取选中的组别（用于队列数据过滤）
selected_group = st.session_state.get("selected_group")
if not selected_group and not group_df.empty:
    # 默认选中第一个组别
    selected_group = group_df.iloc[0]["group_name"]
    st.session_state["selected_group"] = selected_group

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

# 核心指标卡片（美化版 - 增加图标和描述）
metric_col1, metric_col2, metric_col3, metric_col4, metric_col5 = st.columns(5)
with metric_col1:
    st.markdown(f"""
        <div style="background: #ffffff; 
                    padding: 1.25rem; border-radius: 0.75rem; border: 1px solid #e5e7eb; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
            <div style="font-size: 0.8rem; color: #6b7280; margin-bottom: 0.5rem; font-weight: 500;">📊 质检总量</div>
            <div style="font-size: 2rem; font-weight: 700; margin-bottom: 0.25rem; color: #1f2937;">{int(total_qa):,}</div>
            <div style="font-size: 0.7rem; opacity: 0.8;">累计抽检样本数</div>
        </div>
    """, unsafe_allow_html=True)
with metric_col2:
    # 环比变化
    prev_total_qa = prev_group_df["qa_cnt"].sum() if not prev_group_df.empty else None
    qa_change = f"↑{(total_qa - prev_total_qa):,}" if prev_total_qa and total_qa > prev_total_qa else (f"↓{(prev_total_qa - total_qa):,}" if prev_total_qa and total_qa < prev_total_qa else "")
    st.markdown(f"""
        <div style="background: #ffffff; 
                    padding: 1.25rem; border-radius: 0.75rem; border: 1px solid #e5e7eb; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
            <div style="font-size: 0.8rem; color: #6b7280; margin-bottom: 0.5rem; font-weight: 500;">✓ 原始正确率</div>
            <div style="font-size: 2rem; font-weight: 700; margin-bottom: 0.25rem; color: #2e7d32;">{avg_raw_acc:.2f}%</div>
            <div style="font-size: 0.7rem; opacity: 0.8;">{qa_change if qa_change else '一审准确率'}</div>
        </div>
    """, unsafe_allow_html=True)
with metric_col3:
    st.markdown(f"""
        <div style="background: #ffffff; 
                    padding: 1.25rem; border-radius: 0.75rem; border: 1px solid #e5e7eb; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
            <div style="font-size: 0.8rem; color: #6b7280; margin-bottom: 0.5rem; font-weight: 500;">✓✓ 最终正确率</div>
            <div style="font-size: 2rem; font-weight: 700; margin-bottom: 0.25rem; color: #2e7d32;">{avg_final_acc:.2f}%</div>
            <div style="font-size: 0.7rem; opacity: 0.8;">终审准确率</div>
        </div>
    """, unsafe_allow_html=True)
with metric_col4:
    st.markdown(f"""
        <div style="background: #ffffff; 
                    padding: 1.25rem; border-radius: 0.75rem; border: 1px solid #e5e7eb; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
            <div style="font-size: 0.8rem; color: #6b7280; margin-bottom: 0.5rem; font-weight: 500;">✗ 原始错误量</div>
            <div style="font-size: 2rem; font-weight: 700; margin-bottom: 0.25rem; color: #dc2626;">{total_raw_errors:,}</div>
            <div style="font-size: 0.7rem; opacity: 0.8;">一审错误样本</div>
        </div>
    """, unsafe_allow_html=True)
with metric_col5:
    st.markdown(f"""
        <div style="background: #ffffff; 
                    padding: 1.25rem; border-radius: 0.75rem; border: 1px solid #e5e7eb; box-shadow: 0 1px 2px rgba(0,0,0,0.05);">
            <div style="font-size: 0.8rem; color: #6b7280; margin-bottom: 0.5rem; font-weight: 500;">✗✗ 终审错误量</div>
            <div style="font-size: 2rem; font-weight: 700; margin-bottom: 0.25rem; color: #dc2626;">{total_final_errors:,}</div>
            <div style="font-size: 0.7rem; opacity: 0.8;">终审错误样本</div>
        </div>
    """, unsafe_allow_html=True)

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
    
    is_selected = selected_group == group_name
    border = f"3px solid #3B82F6" if is_selected else f"2px solid {border_color}"
    shadow = "0 8px 16px rgba(59, 130, 246, 0.2)" if is_selected else "0 2px 8px rgba(0,0,0,0.1)"
    display_name = "B组（整体）" if group_name == "B组" else group_name
    
    with group_cols[idx]:
        st.markdown(
            f"""
            <div style="padding: 1.25rem; border-radius: 1rem; background: {bg_gradient}; border: {border}; margin-bottom: 0.5rem; box-shadow: {shadow}; transition: all 0.3s ease; min-height: 180px;">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.75rem;">
                    <div style="font-weight: 700; font-size: 1.1rem; color: #1E293B;">{display_name}</div>
                    <div style="font-size: 0.75rem; color: {status_color}; font-weight: 600; background: white; padding: 0.25rem 0.5rem; border-radius: 0.5rem;">{status_icon} {status_text}</div>
                </div>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; margin-bottom: 0.5rem;">
                    <div style="background: white; padding: 0.75rem; border-radius: 0.5rem; text-align: center; border: 1px solid #E5E7EB;">
                        <div style="font-size: 0.7rem; color: #64748B; margin-bottom: 0.25rem;">原始正确率</div>
                        <div style="font-size: 1.25rem; font-weight: 700; color: {'#10B981' if raw_rate >= 99 else '#EF4444'};">{raw_rate:.2f}%</div>
                        <div style="font-size: 0.65rem; margin-top: 0.25rem; height: 0.9rem;">{raw_change}</div>
                    </div>
                    <div style="background: white; padding: 0.75rem; border-radius: 0.5rem; text-align: center; border: 1px solid #E5E7EB;">
                        <div style="font-size: 0.7rem; color: #64748B; margin-bottom: 0.25rem;">最终正确率</div>
                        <div style="font-size: 1.25rem; font-weight: 700; color: {'#10B981' if final_rate >= 99 else '#EF4444'};">{final_rate:.2f}%</div>
                        <div style="font-size: 0.65rem; margin-top: 0.25rem; height: 0.9rem;">{final_change}</div>
                    </div>
                    <div style="background: white; padding: 0.75rem; border-radius: 0.5rem; text-align: center; border: 1px solid #E5E7EB;">
                        <div style="font-size: 0.7rem; color: #64748B; margin-bottom: 0.25rem;">质检量</div>
                        <div style="font-size: 1.25rem; font-weight: 700; color: #1E293B;">{qa_cnt:,}</div>
                    </div>
                    <div style="background: white; padding: 0.75rem; border-radius: 0.5rem; text-align: center; border: 1px solid #E5E7EB;">
                        <div style="font-size: 0.7rem; color: #64748B; margin-bottom: 0.25rem;">原始错误量</div>
                        <div style="font-size: 1.1rem; font-weight: 700; color: #EF4444;">{raw_error_cnt:,}</div>
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
        total_qa = pie_df["total_qa_cnt"].sum()
        
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
        fig.update_layout(
            height=300, margin=dict(l=20, r=20, t=10, b=30),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            yaxis_range=[95, 100.5], yaxis_title="正确率 (%)",
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

# ==================== 第五行：队列排名 + 下探 ====================
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
st.markdown("##### ⚡ 快捷筛选")
quick_filter_col1, quick_filter_col2, quick_filter_col3, quick_filter_col4, _ = st.columns([1.2, 1.2, 1.2, 1.2, 0.8])
with quick_filter_col1:
    if st.button("🔴 错误量TOP5", use_container_width=True, help="筛选错误量最多的5个队列"):
        st.session_state["quick_filter"] = "error_top5"
with quick_filter_col2:
    if st.button("📉 正确率<99%", use_container_width=True, help="筛选正确率低于99%的队列"):
        st.session_state["quick_filter"] = "low_rate"
with quick_filter_col3:
    if st.button("⚠️ 有错判/漏判", use_container_width=True, help="筛选有错判或漏判的队列"):
        st.session_state["quick_filter"] = "has_judge_error"
with quick_filter_col4:
    if st.button("🔄 重置筛选", use_container_width=True, help="清除所有筛选条件"):
        st.session_state["quick_filter"] = None
        st.session_state["queue_selector"] = "(全部)"
        st.session_state["auditor_selector"] = "(全部)"

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
        # 添加提示信息
        st.caption(f"共 {len(detail_queue_df)} 个队列，按最终正确率升序排列（问题队列优先展示）")
        
        queue_show = pd.DataFrame()
        queue_show["队列"] = detail_queue_df["queue_name"]
        queue_show["质检量"] = detail_queue_df["qa_cnt"]
        queue_show["出错量"] = detail_queue_df["final_error_cnt"]
        queue_show["原始正确率"] = detail_queue_df["raw_accuracy_rate"]
        queue_show["最终正确率"] = detail_queue_df["final_accuracy_rate"]
        queue_show["错判率"] = detail_queue_df["misjudge_rate"]
        queue_show["漏判率"] = detail_queue_df["missjudge_rate"]

        st.dataframe(
            queue_show,
            use_container_width=True,
            hide_index=True,
            height=320,
            column_config={
                "队列": st.column_config.TextColumn("队列", width="medium"),
                "质检量": st.column_config.NumberColumn("质检量", width="small", format="d"),
                "出错量": st.column_config.NumberColumn("出错量", width="small", format="d"),
                "原始正确率": st.column_config.NumberColumn("原始正确率", width="small", format="%.2f%%"),
                "最终正确率": st.column_config.NumberColumn("最终正确率", width="small", format="%.2f%%"),
                "错判率": st.column_config.NumberColumn("错判率", width="small", format="%.2f%%"),
                "漏判率": st.column_config.NumberColumn("漏判率", width="small", format="%.2f%%"),
            }
        )
    else:
        st.info("暂无队列数据")

with auditor_col:
    st.markdown("#### 👥 审核人视图")
    if not final_auditor_df.empty:
        # 添加提示信息
        st.caption(f"共 {len(final_auditor_df)} 位审核人，按最终正确率升序排列（需关注的审核人优先展示）")
        
        auditor_show = pd.DataFrame()
        auditor_show["审核人"] = final_auditor_df["reviewer_name"]
        auditor_show["质检量"] = final_auditor_df["qa_cnt"]
        auditor_show["原始正确率"] = final_auditor_df["raw_accuracy_rate"]
        auditor_show["最终正确率"] = final_auditor_df["final_accuracy_rate"]
        auditor_show["错判量"] = final_auditor_df["misjudge_cnt"]
        auditor_show["漏判量"] = final_auditor_df["missjudge_cnt"]

        st.dataframe(
            auditor_show,
            use_container_width=True,
            hide_index=True,
            height=320,
            column_config={
                "审核人": st.column_config.TextColumn("审核人", width="medium"),
                "质检量": st.column_config.NumberColumn("质检量", width="small", format="d"),
                "原始正确率": st.column_config.NumberColumn("原始正确率", width="small", format="%.2f%%"),
                "最终正确率": st.column_config.NumberColumn("最终正确率", width="small", format="%.2f%%"),
                "错判量": st.column_config.NumberColumn("错判量", width="small", format="d"),
                "漏判量": st.column_config.NumberColumn("漏判量", width="small", format="d"),
            }
        )
    else:
        st.info("暂无审核人数据")

# ==================== 第六行：质检标签分布 + 质检员工作量 ====================
st.markdown("---")
st.markdown("### 📊 质检维度分析")
st.caption("💡 了解质检标签分布和质检员工作量分布，帮助识别问题高发区域")

label_col, owner_col = st.columns([1, 1])

with label_col:
    st.markdown("#### 🏷️ 质检标签分布")
    label_df = load_qa_label_distribution_cached(grain, selected_date, selected_group, top_n=10)
    if not label_df.empty:
        # 添加统计信息
        total_labels = label_df["cnt"].sum()
        st.caption(f"前10个标签（按质检量降序），共 {total_labels:,} 条质检记录")
        
        # 使用水平条形图展示
        fig_label = px.bar(
            label_df.sort_values("cnt", ascending=True),
            x="cnt", y="label_name",
            orientation="h",
            text=label_df.sort_values("cnt", ascending=True)["pct"].apply(lambda x: f"{x:.1f}%"),
            color="cnt",
            color_continuous_scale="Blues"
        )
        fig_label.update_traces(textposition="outside", textfont_size=11)
        fig_label.update_layout(
            height=320, margin=dict(l=20, r=20, t=10, b=20),
            xaxis_title="质检量", yaxis_title="",
            showlegend=False, coloraxis_showscale=False,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)'
        )
        st.plotly_chart(fig_label, use_container_width=True)
    else:
        st.info("暂无标签数据")

with owner_col:
    st.markdown("#### 👨‍💼 质检员工作量")
    owner_df = load_qa_owner_distribution_cached(grain, selected_date, selected_group, top_n=10)
    if not owner_df.empty:
        # 添加统计信息
        st.caption(f"前10名质检员（按质检量降序）")
        
        # 表格展示
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
