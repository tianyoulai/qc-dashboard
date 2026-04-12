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

# 全局CSS样式优化
st.markdown("""
<style>
    .main > div { padding-top: 1rem; }
    .stDataFrame { 
        border-radius: 0.75rem; 
        overflow: hidden; 
        border: 1px solid #E5E7EB;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    h1 { margin-bottom: 0.5rem; }
    h3 { margin-top: 1.5rem; margin-bottom: 1rem; }
    hr { margin: 1.5rem 0; border-color: #E5E7EB; }
</style>
""", unsafe_allow_html=True)

repo = DashboardRepository()


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
def load_all_group_daily() -> pd.DataFrame:
    return repo.fetch_df("SELECT * FROM mart_day_group ORDER BY biz_date, group_name")


@st.cache_data(show_spinner=False, ttl=300)
def load_all_group_weekly() -> pd.DataFrame:
    return repo.fetch_df("SELECT * FROM mart_week_group ORDER BY week_begin_date, group_name")


@st.cache_data(show_spinner=False, ttl=300)
def load_all_group_monthly() -> pd.DataFrame:
    return repo.fetch_df("SELECT * FROM mart_month_group ORDER BY month_begin_date, group_name")


min_d, max_d = get_date_range()

# Hero 区域
st.markdown("""
<div style="margin-bottom: 1.5rem; padding: 1.5rem; background: #ffffff; border-radius: 1rem; border-left: 4px solid #2e7d32; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
        <h1 style="margin: 0; font-size: 2rem; font-weight: 700; color: #1a1a1a;">📈 数据总览</h1>
    </div>
    <div style="font-size: 0.9rem; color: #4b5563; line-height: 1.6;">
        跨周期趋势对比 · 全量组别排名 · 数据全景洞察
    </div>
</div>
""", unsafe_allow_html=True)

# 检查数据范围
if min_d is None or max_d is None:
    st.warning("数据库中没有质检数据，请先导入数据。")
    st.stop()

# 数据范围提示（优化样式）
st.markdown(f"""
<div style='background: #f9fafb; padding: 0.75rem 1rem; border-radius: 0.5rem; border-left: 4px solid #6b7280; margin-bottom: 1rem;'>
    <span style='font-weight: 600; color: #374151;'>📅 数据日期范围：</span>
    <span style='color: #374151;'>{min_d} ~ {max_d}</span>
</div>
""", unsafe_allow_html=True)

tab_grain = st.tabs(["日维度", "周维度", "月维度"])

# ==================== 日维度 ====================
with tab_grain[0]:
    df_daily = load_all_group_daily()
    
    if df_daily.empty:
        st.warning("暂无日维度数据。请运行 `python jobs/refresh_warehouse.py` 刷新数仓。")
    else:
        # 日期筛选（优化样式）
        st.markdown("#### 📅 日期筛选")
        col1, col2 = st.columns(2)
        with col1:
            date_start = st.date_input("起始日期", value=min_d, key="day_start")
        with col2:
            date_end = st.date_input("截止日期", value=max_d, key="day_end")
        
        # 转换日期格式并过滤空值
        df_daily["biz_date"] = pd.to_datetime(df_daily["biz_date"]).dt.date
        filtered = df_daily[
            df_daily["biz_date"].notna() & 
            (df_daily["biz_date"] >= date_start) & 
            (df_daily["biz_date"] <= date_end)
        ]
        
        if filtered.empty:
            st.warning("所选日期范围内没有数据。")
        else:
            # 全量组别排名（使用加权平均）
            st.markdown("### 🏆 全量组别排名")
            group_agg = filtered.groupby("group_name").agg(
                qa_cnt=("qa_cnt", "sum"),
            ).reset_index()
            # 计算加权平均正确率
            for grp in group_agg["group_name"]:
                grp_data = filtered[filtered["group_name"] == grp]
                group_agg.loc[group_agg["group_name"] == grp, "raw_accuracy_rate"] = (
                    (grp_data["raw_accuracy_rate"] * grp_data["qa_cnt"]).sum() / grp_data["qa_cnt"].sum()
                )
                group_agg.loc[group_agg["group_name"] == grp, "final_accuracy_rate"] = (
                    (grp_data["final_accuracy_rate"] * grp_data["qa_cnt"]).sum() / grp_data["qa_cnt"].sum()
                )
            group_agg = group_agg.sort_values("final_accuracy_rate", ascending=False)
            
            group_show = pd.DataFrame()
            group_show["组别"] = group_agg["group_name"]
            group_show["总质检量"] = group_agg["qa_cnt"].apply(lambda x: f"{int(x):,}")
            group_show["原始正确率"] = group_agg["raw_accuracy_rate"].apply(lambda x: f"{x:.2f}%")
            group_show["最终正确率"] = group_agg["final_accuracy_rate"].apply(lambda x: f"{x:.2f}%")
            
            st.dataframe(
                group_show, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "组别": st.column_config.TextColumn("组别", width="medium"),
                    "总质检量": st.column_config.TextColumn("总质检量", width="medium"),
                    "原始正确率": st.column_config.TextColumn("原始正确率", width="medium"),
                    "最终正确率": st.column_config.TextColumn("最终正确率", width="medium"),
                }
            )
            
            # 组别趋势
            st.markdown("### 📊 组别趋势图")
            groups = sorted(filtered["group_name"].unique().tolist())
            sel_group = st.selectbox("选择组别", options=groups, key="day_group")
            
            if sel_group:
                trend = filtered[filtered["group_name"] == sel_group].sort_values("biz_date")
                if not trend.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=trend["biz_date"], y=trend["final_accuracy_rate"],
                        mode="lines+markers", name="最终正确率",
                        line=dict(color="#10B981", width=3), marker=dict(size=8),
                        text=[f"{v:.2f}%" for v in trend["final_accuracy_rate"]],
                        hovertemplate="<b>%{x}</b><br>最终正确率: %{text}<extra></extra>"
                    ))
                    fig.add_trace(go.Scatter(
                        x=trend["biz_date"], y=trend["raw_accuracy_rate"],
                        mode="lines+markers", name="原始正确率",
                        line=dict(color="#94A3B8", width=2, dash="dot"), marker=dict(size=6),
                        text=[f"{v:.2f}%" for v in trend["raw_accuracy_rate"]],
                        hovertemplate="<b>%{x}</b><br>原始正确率: %{text}<extra></extra>"
                    ))
                    fig.add_hline(y=99.0, line_dash="dash", line_color="#F59E0B", annotation_text="目标 99%", annotation_position="right")
                    fig.update_layout(
                        height=400, margin=dict(l=20, r=20, t=30, b=30),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                        yaxis_range=[95, 100.5], yaxis_title="正确率 (%)",
                        xaxis=dict(tickformat="%Y-%m-%d", tickangle=-45),
                        paper_bgcolor='rgba(0,0,0,0)',
                        plot_bgcolor='rgba(0,0,0,0)'
                    )
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # 最新数据指标（优化样式）
                    row_latest = trend.iloc[-1]
                    st.markdown("#### 📈 最新数据快照")
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("最新原始正确率", f"{row_latest['raw_accuracy_rate']:.2f}%")
                    m2.metric("最新最终正确率", f"{row_latest['final_accuracy_rate']:.2f}%")
                    m3.metric("质检量", f"{int(row_latest['qa_cnt']):,}")
                    # 安全访问申诉改判率字段
                    appeal_rate = row_latest.get('appeal_reverse_rate')
                    if pd.notna(appeal_rate):
                        m4.metric("申诉改判率", f"{appeal_rate:.2f}%", help="基于联表匹配数据计算，当前联表命中率极低，仅供参考")
                    else:
                        m4.metric("申诉改判率", "—", help="暂无联表匹配数据")


# ==================== 周维度 ====================
with tab_grain[1]:
    df_weekly = load_all_group_weekly()
    
    if df_weekly.empty:
        st.warning("暂无周维度数据。")
    else:
        # 全量组别周度排名（使用加权平均）
        st.markdown("### 🏆 全量组别周度排名")
        group_agg_w = df_weekly.groupby("group_name").agg(
            qa_cnt=("qa_cnt", "sum"),
        ).reset_index()
        # 计算加权平均正确率
        for grp in group_agg_w["group_name"]:
            grp_data = df_weekly[df_weekly["group_name"] == grp]
            group_agg_w.loc[group_agg_w["group_name"] == grp, "raw_accuracy_rate"] = (
                (grp_data["raw_accuracy_rate"] * grp_data["qa_cnt"]).sum() / grp_data["qa_cnt"].sum()
            )
            group_agg_w.loc[group_agg_w["group_name"] == grp, "final_accuracy_rate"] = (
                (grp_data["final_accuracy_rate"] * grp_data["qa_cnt"]).sum() / grp_data["qa_cnt"].sum()
            )
        group_agg_w = group_agg_w.sort_values("final_accuracy_rate", ascending=False)
        
        group_show_w = pd.DataFrame()
        group_show_w["组别"] = group_agg_w["group_name"]
        group_show_w["总质检量"] = group_agg_w["qa_cnt"].apply(lambda x: f"{int(x):,}")
        group_show_w["平均原始正确率"] = group_agg_w["raw_accuracy_rate"].apply(lambda x: f"{x:.2f}%")
        group_show_w["平均最终正确率"] = group_agg_w["final_accuracy_rate"].apply(lambda x: f"{x:.2f}%")
        
        st.dataframe(
            group_show_w, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "组别": st.column_config.TextColumn("组别", width="medium"),
                "总质检量": st.column_config.TextColumn("总质检量", width="medium"),
            }
        )
        
        # 组别趋势
        st.markdown("### 📊 组别周趋势")
        groups_w = sorted(df_weekly["group_name"].unique().tolist())
        sel_group_w = st.selectbox("选择组别", options=groups_w, key="week_group")
        
        if sel_group_w:
            trend_w = df_weekly[df_weekly["group_name"] == sel_group_w].sort_values("week_begin_date")
            if not trend_w.empty:
                fig_w = go.Figure()
                fig_w.add_trace(go.Scatter(
                    x=trend_w["week_begin_date"], y=trend_w["final_accuracy_rate"],
                    mode="lines+markers", name="最终正确率",
                    line=dict(color="#10B981", width=3), marker=dict(size=8),
                    text=[f"{v:.2f}%" for v in trend_w["final_accuracy_rate"]],
                    hovertemplate="<b>%{x}</b><br>最终正确率: %{text}<extra></extra>"
                ))
                fig_w.add_trace(go.Scatter(
                    x=trend_w["week_begin_date"], y=trend_w["raw_accuracy_rate"],
                    mode="lines+markers", name="原始正确率",
                    line=dict(color="#94A3B8", width=2, dash="dot"), marker=dict(size=6),
                    text=[f"{v:.2f}%" for v in trend_w["raw_accuracy_rate"]],
                    hovertemplate="<b>%{x}</b><br>原始正确率: %{text}<extra></extra>"
                ))
                fig_w.add_hline(y=99.0, line_dash="dash", line_color="#F59E0B", annotation_text="目标 99%", annotation_position="right")
                fig_w.update_layout(
                    height=400, margin=dict(l=20, r=20, t=30, b=30),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    yaxis_range=[95, 100.5], yaxis_title="正确率 (%)",
                    xaxis=dict(tickformat="%Y-%m-%d", tickangle=-45),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig_w, use_container_width=True)


# ==================== 月维度 ====================
with tab_grain[2]:
    df_monthly = load_all_group_monthly()
    
    if df_monthly.empty:
        st.warning("暂无月维度数据。")
    else:
        # 全量组别月度排名（使用加权平均）
        st.markdown("### 🏆 全量组别月度排名")
        group_agg_m = df_monthly.groupby("group_name").agg(
            qa_cnt=("qa_cnt", "sum"),
        ).reset_index()
        # 计算加权平均正确率
        for grp in group_agg_m["group_name"]:
            grp_data = df_monthly[df_monthly["group_name"] == grp]
            group_agg_m.loc[group_agg_m["group_name"] == grp, "raw_accuracy_rate"] = (
                (grp_data["raw_accuracy_rate"] * grp_data["qa_cnt"]).sum() / grp_data["qa_cnt"].sum()
            )
            group_agg_m.loc[group_agg_m["group_name"] == grp, "final_accuracy_rate"] = (
                (grp_data["final_accuracy_rate"] * grp_data["qa_cnt"]).sum() / grp_data["qa_cnt"].sum()
            )
        group_agg_m = group_agg_m.sort_values("final_accuracy_rate", ascending=False)
        
        group_show_m = pd.DataFrame()
        group_show_m["组别"] = group_agg_m["group_name"]
        group_show_m["总质检量"] = group_agg_m["qa_cnt"].apply(lambda x: f"{int(x):,}")
        group_show_m["平均原始正确率"] = group_agg_m["raw_accuracy_rate"].apply(lambda x: f"{x:.2f}%")
        group_show_m["平均最终正确率"] = group_agg_m["final_accuracy_rate"].apply(lambda x: f"{x:.2f}%")
        
        st.dataframe(
            group_show_m, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "组别": st.column_config.TextColumn("组别", width="medium"),
                "总质检量": st.column_config.TextColumn("总质检量", width="medium"),
            }
        )
        
        # 组别趋势
        st.markdown("### 📊 组别月趋势")
        groups_m = sorted(df_monthly["group_name"].unique().tolist())
        sel_group_m = st.selectbox("选择组别", options=groups_m, key="month_group")
        
        if sel_group_m:
            trend_m = df_monthly[df_monthly["group_name"] == sel_group_m].sort_values("month_begin_date")
            if not trend_m.empty:
                fig_m = go.Figure()
                fig_m.add_trace(go.Scatter(
                    x=trend_m["month_begin_date"], y=trend_m["final_accuracy_rate"],
                    mode="lines+markers", name="最终正确率",
                    line=dict(color="#10B981", width=3), marker=dict(size=10),
                    text=[f"{v:.2f}%" for v in trend_m["final_accuracy_rate"]],
                    hovertemplate="<b>%{x}</b><br>最终正确率: %{text}<extra></extra>"
                ))
                fig_m.add_trace(go.Scatter(
                    x=trend_m["month_begin_date"], y=trend_m["raw_accuracy_rate"],
                    mode="lines+markers", name="原始正确率",
                    line=dict(color="#94A3B8", width=2, dash="dot"), marker=dict(size=8),
                    text=[f"{v:.2f}%" for v in trend_m["raw_accuracy_rate"]],
                    hovertemplate="<b>%{x}</b><br>原始正确率: %{text}<extra></extra>"
                ))
                fig_m.add_hline(y=99.0, line_dash="dash", line_color="#F59E0B", annotation_text="目标 99%", annotation_position="right")
                fig_m.update_layout(
                    height=400, margin=dict(l=20, r=20, t=30, b=30),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    yaxis_range=[95, 100.5], yaxis_title="正确率 (%)",
                    xaxis=dict(tickformat="%Y-%m-%d", tickangle=-45),
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig_m, use_container_width=True)
