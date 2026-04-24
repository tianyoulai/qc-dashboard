"""数据总览页：跨周期趋势对比 + 全量组别排名。

简化版：专注于核心指标展示，移除复杂的筛选逻辑。
"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from storage.repository import DashboardRepository

st.set_page_config(page_title="质培运营看板-数据总览", page_icon="📈", layout="wide")

# ── 加载统一 CSS ──
import os as _os
_css_path = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "custom.css")
if _os.path.exists(_css_path):
    with open(_css_path, "r", encoding="utf-8") as _f:
        _CSS = _f.read()
    st.markdown(f'<style>{_CSS}</style>', unsafe_allow_html=True)


repo = DashboardRepository()

# ── 粒度配置 ──
GRAIN_CONFIG = {
    "day": {
        "label": "📅 日维度",
        "table": "mart_day_group",
        "date_col": "biz_date",
        "start_key": "day_start",
        "end_key": "day_end",
    },
    "week": {
        "label": "📆 周维度",
        "table": "mart_week_group",
        "date_col": "week_begin_date",
        "start_key": "week_start",
        "end_key": "week_end",
    },
    "month": {
        "label": "🗓️ 月维度",
        "table": "mart_month_group",
        "date_col": "month_begin_date",
        "start_key": "month_start",
        "end_key": "month_end",
    },
}


@st.cache_data(show_spinner=False, ttl=300)
def get_date_range() -> tuple[date, date]:
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
def load_group_data(grain: str) -> pd.DataFrame:
    cfg = GRAIN_CONFIG[grain]
    return repo.fetch_df(f"SELECT * FROM {cfg['table']} ORDER BY {cfg['date_col']}, group_name")


# ── 公共函数 ──

def calc_group_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """计算全量组别加权排名"""
    if df.empty:
        return pd.DataFrame()
    group_agg = df.groupby("group_name").agg(qa_cnt=("qa_cnt", "sum")).reset_index()
    for grp in group_agg["group_name"]:
        grp_data = df[df["group_name"] == grp]
        total_qa = grp_data["qa_cnt"].sum()
        group_agg.loc[group_agg["group_name"] == grp, "raw_accuracy_rate"] = (
            (grp_data["raw_accuracy_rate"] * grp_data["qa_cnt"]).sum() / total_qa
        )
        group_agg.loc[group_agg["group_name"] == grp, "final_accuracy_rate"] = (
            (grp_data["final_accuracy_rate"] * grp_data["qa_cnt"]).sum() / total_qa
        )
        # 加权错判率、漏判率、申诉改判率
        for rate_col in ("misjudge_rate", "missjudge_rate", "appeal_reverse_rate"):
            if rate_col in grp_data.columns:
                group_agg.loc[group_agg["group_name"] == grp, rate_col] = (
                    (grp_data[rate_col].fillna(0) * grp_data["qa_cnt"]).sum() / total_qa
                )
    return group_agg.sort_values("final_accuracy_rate", ascending=False)


def render_ranking_table(group_agg: pd.DataFrame):
    """渲染组别排名表（含条件高亮，为监控数据问题服务）"""
    group_show = pd.DataFrame()
    group_show["组别"] = group_agg["group_name"]
    group_show["总质检量"] = group_agg["qa_cnt"].fillna(0).astype(int)
    group_show["原始正确率"] = group_agg["raw_accuracy_rate"]
    group_show["最终正确率"] = group_agg["final_accuracy_rate"]
    # 错判率、漏判率
    if "misjudge_rate" in group_agg.columns:
        group_show["错判率"] = group_agg["misjudge_rate"].fillna(0)
    if "missjudge_rate" in group_agg.columns:
        group_show["漏判率"] = group_agg["missjudge_rate"].fillna(0)
    # 申诉改判率
    if "appeal_reverse_rate" in group_agg.columns:
        group_show["申诉改判率"] = group_agg["appeal_reverse_rate"].fillna(0)
    # 达标标记
    group_show["达标"] = group_agg["final_accuracy_rate"].apply(
        lambda x: "⚠️ 不达标" if pd.notna(x) and x < 99 else "✅ 达标"
    )

    # 条件高亮
    def _highlight_group(val, col_name):
        if col_name in ("原始正确率", "最终正确率"):
            if pd.notna(val) and val < 99:
                return "color: #dc2626; font-weight: 700; background-color: #fef2f2"
            if pd.notna(val) and val < 99.5:
                return "color: #d97706; font-weight: 600; background-color: #fffbeb"
        if col_name == "漏判率":
            if pd.notna(val) and val > 0.35:
                return "color: #dc2626; font-weight: 700; background-color: #fef2f2"
        if col_name == "错判率":
            if pd.notna(val) and val > 0.5:
                return "color: #d97706; font-weight: 600; background-color: #fffbeb"
        if col_name == "申诉改判率":
            if pd.notna(val) and val > 18:
                return "color: #dc2626; font-weight: 700; background-color: #fef2f2"
        return ""

    styled = group_show.style
    for col in ["原始正确率", "最终正确率", "漏判率", "错判率", "申诉改判率"]:
        if col in group_show.columns:
            styled = styled.map(
                lambda val, c=col: _highlight_group(val, c), subset=[col]
            )
    # 达标列颜色
    if "达标" in group_show.columns:
        styled = styled.map(
            lambda val: "color: #dc2626; font-weight: 700" if "不达标" in str(val) else "color: #16a34a",
            subset=["达标"]
        )

    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        column_config={
            "组别": st.column_config.TextColumn("组别", width="medium"),
            "总质检量": st.column_config.NumberColumn("总质检量", width="small", format="%,d"),
            "原始正确率": st.column_config.NumberColumn("原始正确率", width="small", format="%.2f%%"),
            "最终正确率": st.column_config.NumberColumn("最终正确率", width="small", format="%.2f%%"),
            "错判率": st.column_config.NumberColumn("错判率", width="small", format="%.2f%%"),
            "漏判率": st.column_config.NumberColumn("漏判率", width="small", format="%.2f%%"),
            "申诉改判率": st.column_config.NumberColumn("申诉改判率", width="small", format="%.2f%%"),
            "达标": st.column_config.TextColumn("达标", width="small"),
        }
    )


def render_trend_chart(df: pd.DataFrame, grain: str, sel_group: str):
    """渲染组别趋势图"""
    cfg = GRAIN_CONFIG[grain]
    trend = df[df["group_name"] == sel_group].sort_values(cfg["date_col"])
    if trend.empty:
        st.info("该组别暂无趋势数据")
        return

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=trend[cfg["date_col"]], y=trend["final_accuracy_rate"],
        mode="lines+markers", name="最终正确率",
        line=dict(color="#3b82f6", width=3), marker=dict(size=8),
        text=[f"{v:.2f}%" for v in trend["final_accuracy_rate"]],
        hovertemplate="<b>%{x}</b><br>最终正确率: %{text}<extra></extra>"
    ))
    fig.add_trace(go.Scatter(
        x=trend[cfg["date_col"]], y=trend["raw_accuracy_rate"],
        mode="lines+markers", name="原始正确率",
        line=dict(color="#94A3B8", width=2, dash="dot"), marker=dict(size=6),
        text=[f"{v:.2f}%" for v in trend["raw_accuracy_rate"]],
        hovertemplate="<b>%{x}</b><br>原始正确率: %{text}<extra></extra>"
    ))
    fig.add_hline(y=99.0, line_dash="dash", line_color="#F59E0B", annotation_text="目标 99%", annotation_position="right")

    # 动态 y 轴范围
    all_rates = pd.concat([trend["final_accuracy_rate"], trend["raw_accuracy_rate"]]).dropna()
    if not all_rates.empty:
        y_min = max(0, all_rates.min() - 1)
        y_max = min(101, all_rates.max() + 1)
    else:
        y_min, y_max = 95, 100.5

    fig.update_layout(
        height=400, margin=dict(l=20, r=20, t=30, b=30),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        yaxis_range=[y_min, y_max], yaxis_title="正确率 (%)",
        xaxis=dict(tickformat="%Y-%m-%d", tickangle=-45),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)'
    )
    st.plotly_chart(fig, use_container_width=True)

    # 最新数据快照
    row_latest = trend.iloc[-1]
    st.markdown(f"""
    <div class="result-summary" style="margin-top:0;">
        <div class="result-item">
            <div class="num">{row_latest['raw_accuracy_rate']:.2f}%</div>
            <div class="label">原始正确率</div>
        </div>
        <div class="result-item">
            <div class="num">{row_latest['final_accuracy_rate']:.2f}%</div>
            <div class="label">最终正确率</div>
        </div>
        <div class="result-item">
            <div class="num">{int(row_latest['qa_cnt']):,}</div>
            <div class="label">质检量</div>
        </div>
        <div class="result-item">
            <div class="num">{'—' if pd.isna(row_latest.get('appeal_reverse_rate')) else f"{row_latest['appeal_reverse_rate']:.2f}%"}</div>
            <div class="label">申诉改判率</div>
        </div>
    </div>
    """, unsafe_allow_html=True)


def render_grain_section(grain: str, df: pd.DataFrame, min_d: date, max_d: date):
    """渲染单个粒度的完整区域（排名 + 趋势图）"""
    cfg = GRAIN_CONFIG[grain]

    if df.empty:
        st.markdown(f'<div class="alert-box warning">⚠️ 暂无{cfg["label"]}数据。请运行 `python jobs/refresh_warehouse.py` 刷新数仓。</div>', unsafe_allow_html=True)
        return

    # 日期筛选（仅日维度提供）
    if grain == "day":
        st.markdown('<div class="card"><div class="section-title"><span class="emoji">📅</span> 日期筛选</div>', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        with col1:
            date_start = st.date_input("起始日期", value=min_d, key=cfg["start_key"])
        with col2:
            date_end = st.date_input("截止日期", value=max_d, key=cfg["end_key"])

        df[cfg["date_col"]] = pd.to_datetime(df[cfg["date_col"]]).dt.date
        filtered = df[
            df[cfg["date_col"]].notna() &
            (df[cfg["date_col"]] >= date_start) &
            (df[cfg["date_col"]] <= date_end)
        ]

        if filtered.empty:
            st.markdown('<div class="alert-box warning">⚠️ 所选日期范围内没有数据。</div>', unsafe_allow_html=True)
            st.markdown("</div>", unsafe_allow_html=True)
            return
    else:
        filtered = df

    # 全量组别排名
    st.markdown("### 🏆 全量组别排名")
    group_agg = calc_group_ranking(filtered)
    if not group_agg.empty:
        render_ranking_table(group_agg)

    if grain == "day":
        st.markdown("</div>", unsafe_allow_html=True)

    # 组别趋势
    emoji_map = {"day": "📊", "week": "📈", "month": "📉"}
    st.markdown(f'<div class="card"><div class="section-title"><span class="emoji">{emoji_map[grain]}</span> 组别趋势图</div>', unsafe_allow_html=True)
    groups = sorted(filtered["group_name"].unique().tolist())
    sel_group = st.selectbox("选择组别", options=groups, key=f"{grain}_group")

    if sel_group:
        render_trend_chart(filtered, grain, sel_group)
    st.markdown("</div>", unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════
#  主程序
# ════════════════════════════════════════════════════════════════

min_d, max_d = get_date_range()

# ---- 标题区 ----
st.markdown(f"""
<div style="padding: 20px 0 10px 0;">
    <div class="page-title">📈 数据总览</div>
    <div class="page-subtitle">跨周期趋势对比 · 全量组别排名 · 数据全景洞察</div>
</div>
""", unsafe_allow_html=True)

# 检查数据
if min_d is None or max_d is None:
    st.markdown('<div class="alert-box warning">⚠️ 数据库中没有质检数据，请先导入数据。</div>', unsafe_allow_html=True)
    st.stop()

# 数据范围
st.markdown(f"""
<div class="card" style="padding:12px 20px;">
    <div class="result-summary" style="margin-top:0;">
        <div class="result-item">
            <div class="num">{min_d}</div>
            <div class="label">最早日期</div>
        </div>
        <div class="result-item">
            <div class="num">{max_d}</div>
            <div class="label">最新日期</div>
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

tab_grain = st.tabs([cfg["label"] for cfg in GRAIN_CONFIG.values()])

# 三个粒度共享同一套渲染逻辑
for i, grain in enumerate(GRAIN_CONFIG):
    with tab_grain[i]:
        df = load_group_data(grain)
        render_grain_section(grain, df, min_d, max_d)
