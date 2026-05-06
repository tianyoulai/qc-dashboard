"""
iLabel 质检明细：长沙云雀一审质检数据看板
数据源：data/ilabel_detail.db
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE, "data", "ilabel_detail.db")

# ── 配色 ──
C_OK = "#22c55e"
C_ERR = "#ef4444"
C_WARN = "#f59e0b"

st.set_page_config(
    page_title="iLabel 质检明细",
    page_icon="📋",
    layout="wide",
)

# ── 数据加载 ──
@st.cache_data(ttl=300)
def load_daily_summary():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM daily_summary ORDER BY data_date", conn)
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_queue_summary():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM queue_daily_summary ORDER BY data_date, total_qa DESC", conn)
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_auditor_summary():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM auditor_daily_summary ORDER BY data_date, total_qa DESC", conn)
    conn.close()
    return df

@st.cache_data(ttl=300)
def load_detail(dates=None, queue=None, judges=None, auditor=None, limit=500):
    conn = sqlite3.connect(DB_PATH)
    query = "SELECT * FROM ilabel_detail WHERE 1=1"
    params = []
    if dates:
        placeholders = ",".join("?" * len(dates))
        query += f" AND data_date IN ({placeholders})"
        params.extend(dates)
    if queue:
        query += " AND queue = ?"
        params.append(queue)
    if judges:
        placeholders = ",".join("?" * len(judges))
        query += f" AND qc_judge IN ({placeholders})"
        params.extend(judges)
    if auditor:
        query += " AND auditor = ?"
        params.append(auditor)
    query += f" ORDER BY id DESC LIMIT {limit}"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

@st.cache_data(ttl=300)
def get_available_dates():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT DISTINCT data_date FROM daily_summary ORDER BY data_date", conn)
    conn.close()
    return df["data_date"].tolist()

@st.cache_data(ttl=300)
def get_available_queues():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT DISTINCT queue FROM queue_daily_summary ORDER BY queue", conn)
    conn.close()
    return df["queue"].tolist()

# ── 页面 ──
st.title("📋 长沙云雀 iLabel 质检明细")
st.caption("数据来源：iLabel 质检平台每日导出 | 长沙云雀联营")

if not os.path.exists(DB_PATH):
    st.error("数据文件不存在，请先运行数据导入。")
    st.stop()

daily = load_daily_summary()
queue_df = load_queue_summary()
auditor_df = load_auditor_summary()
dates = get_available_dates()
queues = get_available_queues()

# ── 筛选 ──
st.sidebar.header("筛选条件")
sel_dates = st.sidebar.multiselect("日期", dates, default=dates)
sel_queues = st.sidebar.multiselect("队列", queues, default=queues)
sel_judge = st.sidebar.multiselect("质检判断", ["正确", "错判", "漏判"], default=["正确", "错判", "漏判"])

# ── KPI ──
mask = daily["data_date"].isin(sel_dates)
filt_daily = daily[mask]

total_qa = int(filt_daily["total_qa"].sum()) if len(filt_daily) else 0
total_correct = int(filt_daily["correct_cnt"].sum()) if len(filt_daily) else 0
total_wrong = int(filt_daily["wrong_cnt"].sum()) if len(filt_daily) else 0
total_miss = int(filt_daily["miss_cnt"].sum()) if len(filt_daily) else 0
acc = (total_correct / total_qa * 100) if total_qa > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("质检总量", f"{total_qa:,}")
col2.metric("一审正确率", f"{acc:.2f}%", delta=f"目标 99.00%", delta_color="off")
col3.metric("错判", f"{total_wrong:,}", delta_color="off")
col4.metric("漏判", f"{total_miss:,}", delta_color="off")

st.markdown("---")

# ── 日报趋势 ──
st.subheader("📈 每日质检趋势")
q_mask = queue_df["data_date"].isin(sel_dates) & queue_df["queue"].isin(sel_queues)
filt_queue = queue_df[q_mask].copy()
filt_queue["正确率"] = (filt_queue["correct_cnt"] / filt_queue["total_qa"] * 100).round(2)

# 按日期汇总
daily_agg = filt_queue.groupby("data_date").agg(
    质检量=("total_qa", "sum"),
    正确数=("correct_cnt", "sum"),
    错判数=("wrong_cnt", "sum"),
    漏判数=("miss_cnt", "sum"),
).reset_index()
daily_agg["正确率"] = (daily_agg["正确数"] / daily_agg["质检量"] * 100).round(2)

fig_trend = go.Figure()
fig_trend.add_trace(go.Bar(
    x=daily_agg["data_date"], y=daily_agg["质检量"],
    name="质检量", marker_color="#3b82f6", yaxis="y",
))
fig_trend.add_trace(go.Scatter(
    x=daily_agg["data_date"], y=daily_agg["正确率"],
    name="正确率(%)", mode="lines+markers+text",
    line=dict(color=C_OK, width=2),
    text=daily_agg["正确率"].astype(str) + "%",
    textposition="top center", textfont=dict(size=11, color=C_OK),
    yaxis="y2",
))
fig_trend.update_layout(
    xaxis_title="", yaxis=dict(title="质检量", side="left"),
    yaxis2=dict(title="正确率(%)", side="right", overlaying="y", range=[97, 100.5]),
    height=380, legend=dict(orientation="h", yanchor="bottom", y=1.02),
    margin=dict(b=20),
)
st.plotly_chart(fig_trend, use_container_width=True)

st.markdown("---")

# ── 队列分析 ──
st.subheader("📊 各队列质检分析")

queue_agg = filt_queue.groupby("queue").agg(
    质检量=("total_qa", "sum"),
    正确数=("correct_cnt", "sum"),
    错判数=("wrong_cnt", "sum"),
    漏判数=("miss_cnt", "sum"),
).reset_index()
queue_agg["正确率"] = (queue_agg["正确数"] / queue_agg["质检量"] * 100).round(2)
queue_agg["错误数"] = queue_agg["错判数"] + queue_agg["漏判数"]
queue_agg = queue_agg.sort_values("质检量", ascending=False)

col_q1, col_q2 = st.columns([1, 1], gap="large")

with col_q1:
    fig_bar = px.bar(queue_agg, x="queue", y=["错判数", "漏判数"],
                     barmode="group", color_discrete_map={"错判数": C_ERR, "漏判数": C_WARN})
    fig_bar.update_layout(title="各队列错判/漏判数量", xaxis_title="",
                          xaxis=dict(tickangle=-30, tickfont=dict(size=9)),
                          height=420, legend=dict(orientation="h", yanchor="bottom", y=1.02),
                          margin=dict(b=120))
    st.plotly_chart(fig_bar, use_container_width=True)

with col_q2:
    st.dataframe(
        queue_agg[["queue", "质检量", "正确率", "错判数", "漏判数", "错误数"]].style.format({"正确率": "{:.2f}%"}),
        use_container_width=True, height=420,
    )

st.markdown("---")

# ── 一审员排行 ──
st.subheader("👥 一审员质检排行 Top 30")

a_mask = auditor_df["data_date"].isin(sel_dates)
filt_auditor = auditor_df[a_mask].copy()

auditor_agg = filt_auditor.groupby("auditor").agg(
    质检量=("total_qa", "sum"),
    正确数=("correct_cnt", "sum"),
    错判数=("wrong_cnt", "sum"),
    漏判数=("miss_cnt", "sum"),
).reset_index()
auditor_agg["正确率"] = (auditor_agg["正确数"] / auditor_agg["质检量"] * 100).round(2)
auditor_agg = auditor_agg.sort_values("质检量", ascending=False).head(30)

col_a1, col_a2 = st.columns([1, 1], gap="large")

with col_a1:
    # Horizontal bar for top 30
    names = auditor_agg["auditor"].str.replace("云雀联营-", "", regex=False).tolist()
    fig_hbar = go.Figure(go.Bar(
        y=names, x=auditor_agg["质检量"].tolist(),
        orientation="h", marker_color="#6366f1",
    ))
    fig_hbar.update_layout(
        title="Top 30 一审员质检量", height=550,
        yaxis=dict(autorange="reversed"),
        margin=dict(l=10),
    )
    st.plotly_chart(fig_hbar, use_container_width=True)

with col_a2:
    st.dataframe(
        auditor_agg[["auditor", "质检量", "正确率", "错判数", "漏判数"]].style.format({"正确率": "{:.2f}%"}),
        use_container_width=True, height=550,
    )

st.markdown("---")

# ── 错误类型分布 ──
st.subheader("⚠️ 错误记录分析")

err_detail = load_detail(
    dates=sel_dates if sel_dates else None,
    judges=["错判", "漏判"],
    limit=100000,
)

col_e1, col_e2 = st.columns(2, gap="large")

with col_e1:
    if not err_detail.empty:
        err_label = err_detail["qc_result"].value_counts().reset_index()
        err_label.columns = ["质检结果", "数量"]
        fig_pie = px.pie(err_label.head(10), values="数量", names="质检结果", hole=0.5)
        fig_pie.update_layout(title="错误记录标签分布", height=380)
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("选定范围内无错误记录")

with col_e2:
    if not err_detail.empty:
        err_judge = err_detail["qc_judge"].value_counts().reset_index()
        err_judge.columns = ["质检判断", "数量"]
        fig_j = px.bar(err_judge, x="质检判断", y="数量",
                       color="质检判断", color_discrete_map={"错判": C_ERR, "漏判": C_WARN})
        fig_j.update_layout(title="错判 vs 漏判", height=380,
                            coloraxis_showscale=False)
        st.plotly_chart(fig_j, use_container_width=True)
    else:
        st.info("选定范围内无错误记录")

st.markdown("---")

# ── 错误明细浏览 ──
st.subheader("🔍 错误明细浏览")

if not err_detail.empty:
    sel_err_date = st.selectbox("选择日期", sorted(err_detail["data_date"].unique(), reverse=True))
    err_queues_for_date = sorted(err_detail[err_detail["data_date"] == sel_err_date]["queue"].unique())
    sel_err_queue = st.selectbox("选择队列", ["全部"] + err_queues_for_date, key="err_queue")

    show_err = err_detail[err_detail["data_date"] == sel_err_date]
    if sel_err_queue != "全部":
        show_err = show_err[show_err["queue"] == sel_err_queue]

    disp_cols = ["qc_judge", "queue", "auditor", "audit_result", "qc_result", "comment_text"]
    rename = {"qc_judge": "质检判断", "queue": "队列", "auditor": "一审员",
              "audit_result": "一审结果", "qc_result": "质检结果", "comment_text": "评论文本"}
    show_disp = show_err[disp_cols].rename(columns=rename).head(200)

    st.dataframe(show_disp, use_container_width=True, height=480)

    csv = show_disp.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    st.download_button("📥 导出错误明细 CSV", data=csv,
                       file_name=f"ilabel错误明细_{sel_err_date}.csv", mime="text/csv")
else:
    st.info("选定范围内无错误记录")
