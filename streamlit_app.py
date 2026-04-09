"""
============================================================
QC Dashboard — Streamlit 独立看板
============================================================
用法:  cd qc-dashboard && streamlit run streamlit_app.py
依赖: streamlit, plotly, pandas (pip install -r requirements.txt)
数据源: data/metrics.db (SQLite)
"""

import os
import json
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import streamlit as st

# ── 路径 ──
BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "data", "metrics.db")

# ── 队列配置（与 _dashboard_logic.js 保持一致）─────────
QUEUES = [
    {
        "id": "q1_toufang",
        "name": "投放误漏",
        "full_name": "【供应商】投放误漏case",
        "icon": "📢",
        "color": "#3b82f6",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q2_erjiansimple",
        "name": "简单二审",
        "full_name": "【供应商】简单二审误漏case",
        "icon": "📋",
        "color": "#22c55e",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q3_erjian_4qi_gt",
        "name": "四期-二审GT",
        "full_name": "【四期供应商】二审周推质检分歧单（二审GT）",
        "icon": "🔄",
        "color": "#f97316",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q3b_erjian_4qi_qiepian",
        "name": "四期-切片GT",
        "full_name": "【四期供应商】二审周推质检分歧单（二审切片GT）",
        "icon": "🔪",
        "color": "#f59e0b",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q4_jubao_4qi",
        "name": "四期-举报",
        "full_name": "【四期供应商】举报周推质检分歧单",
        "icon": "🚨",
        "color": "#a855f7",
        "has_pre_post": True,
        "metric_keys": [
            "pre_violation_rate", "pre_miss_rate", "pre_accuracy",
            "post_violation_rate", "post_miss_rate", "post_accuracy",
        ],
        "metric_labels": {
            "pre_violation_rate": "申诉前-违规准确率", "pre_miss_rate": "申诉前-漏率", "pre_accuracy": "申诉前-准确率",
            "post_violation_rate": "申诉后-违规准确率", "post_miss_rate": "申诉后-漏率", "post_accuracy": "申诉后-准确率",
        },
    },
    {
        "id": "q5_lahei",
        "name": "拉黑误漏",
        "full_name": "【供应商】拉黑误漏case",
        "icon": "🚫",
        "color": "#ef4444",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q6_shangqiang",
        "name": "上墙文本",
        "full_name": "上墙文本申诉-云雀",
        "icon": "📝",
        "color": "#06b6d4",
        "metric_keys": ["audit_accuracy"],
        "metric_labels": {"audit_accuracy": "审核准确率"},
    },
]

QUEUE_MAP = {q["id"]: q for q in QUEUES}


# ── 数据加载 ──
@st.cache_data(ttl=60)
def load_all_queue_data():
    """从 SQLite 加载全部队列数据，返回 {qid: DataFrame}"""
    conn = sqlite3.connect(DB_PATH)
    all_data = {}

    for q in QUEUES:
        qid = q["id"]
        df = pd.read_sql_query(
            "SELECT date, metric_key, metric_value FROM daily_metrics WHERE queue_id=? ORDER BY date",
            conn,
            params=(qid,),
        )
        if df.empty:
            all_data[qid] = pd.DataFrame(columns=["date"] + q["metric_keys"])
            continue

        # pivot: date × metric_key → wide DataFrame
        df_wide = df.pivot(index="date", columns="metric_key", values="metric_value").reset_index()
        # 确保所有 metric_key 列都存在
        for mk in q["metric_keys"]:
            if mk not in df_wide.columns:
                df_wide[mk] = None
        df_wide["date"] = pd.to_datetime(df_wide["date"]).dt.strftime("%Y-%m-%d")
        all_data[qid] = df_wide

    conn.close()
    return all_data


def get_date_range(all_data):
    """获取所有队列的日期范围"""
    all_dates = []
    for df in all_data.values():
        if not df.empty and "date" in df.columns:
            all_dates.extend(df["date"].tolist())
    if not all_dates:
        return None, None
    all_dates = sorted(set(all_dates))
    return all_dates[0], all_dates[-1]


def filter_by_date(df, date_from, date_to):
    """按日期范围过滤"""
    if df.empty:
        return df
    mask = pd.Series([True] * len(df), index=df.index)
    if date_from:
        mask &= df["date"].astype(str) >= str(date_from)
    if date_to:
        mask &= df["date"].astype(str) <= str(date_to)
    return df.loc[mask].reset_index(drop=True)


def find_latest_nonzero(df, keys):
    """倒序查找最新非零行（避免周末零值问题）"""
    if df.empty:
        return None
    for idx in range(len(df) - 1, -1, -1):
        row = df.iloc[idx]
        for k in keys:
            v = row.get(k) if k in row.index else None
            if v is not None and isinstance(v, (int, float)) and v != 0:
                return row
    return df.iloc[-1]


def fmt_pct(val):
    """格式化为百分比字符串"""
    if val is None or (isinstance(val, float) and (val != val)):
        return "--"
    try:
        return f"{float(val) * 100:.2f}%"
    except (ValueError, TypeError):
        return str(val)


def fmt_pct1(val):
    """百分比，1位小数（用于统计卡片大数字）"""
    if val is None or (isinstance(val, float) and (val != val)):
        return "--"
    try:
        return f"{float(val) * 100:.1f}%"
    except (ValueError, TypeError):
        return str(val)


# ── 页面配置 ──
st.set_page_config(
    page_title="QC 质检数据看板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    /* 队列 tab 样式 */
    .queue-tab-btn button {
        font-size: 13px !important;
        font-weight: 600 !important;
        padding: 10px 20px !important;
        border-radius: 10px !important;
    }
    /* 统计卡片 */
    [data-testid="stMetric"] {
        background-color: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 8px !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }
    /* 主标题 */
    h1 { font-size: 26px !important; font-weight: 700 !important; margin-bottom: 4px !important; }
    .subtitle { color: #64748b; font-size: 13px; }
    /* 表格样式 */
    [data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ── 加载数据 ──
with st.spinner("加载中..."):
    all_data = load_all_queue_data()

min_date, max_date = get_date_range(all_data)

# ── Header ──
st.markdown("### 📊 QC 质检数据统一看板")
st.caption(f"多队列 · 按日期聚合指标 · 数据来源：企业微信智能表格 · 共 **{sum(len(d) for d in all_data.values())}** 条记录 · 更新于 `{datetime.now().strftime('%Y-%m-%d %H:%M')}`")

# ── 侧边栏：日期筛选 + 快捷按钮 ──
with st.sidebar:
    st.subheader("📅 日期筛选")
    col_a, col_b = st.columns(2)
    with col_a:
        d_from = st.date_input("起始日期", value=None, min_value=None, max_value=None, key="df", label_visibility="collapsed")
    with col_b:
        d_to = st.date_input("截止日期", value=None, min_value=None, max_value=None, key="dt", label_visibility="collapsed")

    date_from_str = d_from.strftime("%Y-%m-%d") if d_from else None
    date_to_str = d_to.strftime("%Y-%m-%d") if d_to else None

    st.markdown("---")
    st.subheader("⏱️ 快捷范围")
    quick_cols = st.columns(3)
    if quick_cols[0].button("📅 近7天", use_container_width=True):
        if max_date:
            st.session_state["_quick"] = ("week", max_date)
            st.rerun()
    if quick_cols[1].button("📅 近30天", use_container_width=True):
        if max_date:
            st.session_state["_quick"] = ("month", max_date)
            st.rerun()
    if quick_cols[2].button("📅 全部", use_container_width=True):
        st.session_state["_quick"] = ("all", None)
        st.rerun()

    # 处理快捷选择
    if "_quick" in st.session_state:
        mode, ref = st.session_state["_quick"]
        if mode == "week" and ref:
            dt = datetime.strptime(ref, "%Y-%m-%d")
            date_from_str = (dt - timedelta(days=6)).strftime("%Y-%m-%d")
            date_to_str = ref
        elif mode == "month" and ref:
            dt = datetime.strptime(ref, "%Y-%m-%d")
            date_from_str = (dt - timedelta(days=29)).strftime("%Y-%m-%d")
            date_to_str = ref
        else:
            date_from_str, date_to_str = None, None
        del st.session_state["_quick"]

    st.markdown("---")
    st.info(f"💡 数据范围：`{min_date}` ~ `{max_date}`" if min_date else "暂无数据")


# ── Overview 卡片行（所有队列概览）──
st.markdown("#### 📋 全局概览")
ov_cols = st.columns(len(QUEUES))
for i, q in enumerate(QUEUES):
    df_raw = all_data.get(q["id"], pd.DataFrame())
    df_f = filter_by_date(df_raw, date_from_str, date_to_str)
    lr = find_latest_nonzero(df_f, q["metric_keys"])

    first_mk = q["metric_keys"][0]
    display_val = fmt_pct1(lr[first_mk]) if lr is not None and first_mk in lr.index else "--"
    latest_date = lr["date"] if lr is not None else "--"

    with ov_cols[i]:
        st.metric(
            label=f"{q['icon']} {q['name']}",
            value=display_val,
            delta=f"{len(df_f)} 天 | {latest_date}",
            delta_color="off",
        )

st.divider()


# ── 主内容区：Queue Tabs ──
tab_labels = [f"{q['icon']} {q['name']}" for q in QUEUES]
tab_ids = [q["id"] for q in QUEUES]
tabs = st.tabs(tab_labels)

for tab_idx, tab in enumerate(tabs):
    q = QUEUES[tab_idx]
    qid = q["id"]
    df_raw = all_data.get(qid, pd.DataFrame())

    with tab:
        df = filter_by_date(df_raw, date_from_str, date_to_str)

        if df.empty:
            st.info(f"😴 **{q['name']}** 在选定日期范围内暂无数据")
            continue

        # ── 统计卡片 ──
        st.markdown("##### 📌 核心指标")
        n_metrics = len(q["metric_keys"]) + 1  # +1 for data count card
        stat_cols = st.columns(min(n_metrics, 5))

        # 第一个卡片：数据天数
        with stat_cols[0]:
            date_range_str = (
                f"`{df['date'].iloc[0]} ~ {df['date'].iloc[-1]}"
                if len(df) > 0 else ""
            )
            st.metric(label="📅 数据天数", value=f"{len(df)} 天", delta=date_range_str)

        # 各指标卡片：最新非零值、均值、趋势
        for ki, mk in enumerate(q["metric_keys"]):
            if ki + 1 < len(stat_cols):
                sc = stat_cols[ki + 1]
            else:
                sc = st.column_config

            # 收集所有有效值
            valid_vals = []
            for _, r in df.iterrows():
                v = r.get(mk) if mk in r.index else None
                if v is not None and isinstance(v, (int, float)) and not (v != v):
                    valid_vals.append(v)

            if not valid_vals:
                continue

            # 最新非零值
            lr_nz = find_latest_nonzero(df, [mk])
            last_val = lr_nz[mk] if (lr_nz is not None and mk in lr_nz.index) else valid_vals[-1]

            # 均值
            avg_val = sum(valid_vals) / len(valid_vals)

            # 趋势（对比上一个非零值）
            trend_str = ""
            non_zero_indices = [i for i, v in enumerate(valid_vals) if v != 0]
            if len(non_zero_indices) >= 2:
                prev_i = non_zero_indices[-2]
                curr_i = non_zero_indices[-1]
                prev_v = valid_vals[prev_i]
                curr_v = valid_vals[curr_i]
                if prev_v != 0:
                    chg = ((curr_v - prev_v) / abs(prev_v)) * 100
                    arrow = "↑" if chg > 0 else ("↓" if chg < 0 else "→")
                    trend_str = f"{arrow} {abs(chg):.1f}%"

            lbl = q["metric_labels"].get(mk, mk)
            with (stat_cols[ki + 1] if ki + 1 < len(stat_cols) else st.columns(1)[0]):
                st.metric(
                    label=lbl,
                    value=fmt_pct(last_val),
                    delta=f"均{fmt_pct1(avg_val)} {trend_str}" if trend_str else f"均值 {fmt_pct1(avg_val)}",
                )

        # ── 图表区 ──
        has_pp = q.get("has_pre_post", False)

        c1, c2 = st.columns([2, 1])

        with c1:
            st.markdown("##### 📈 指标走势")
            fig_trend = go.Figure()

            if has_pp:
                pre_keys = [k for k in q["metric_keys"] if k.startswith("pre_")]
                post_keys = [k for k in q["metric_keys"] if k.startswith("post_")]

                for pk in pre_keys:
                    lbl = q["metric_labels"].get(pk, pk)
                    vals = df[pk].tolist() if pk in df.columns else []
                    fig_trend.add_trace(go.Scatter(
                        x=df["date"].tolist(), y=vals,
                        name=f"申诉前·{lbl.replace('申诉前-', '')}",
                        line=dict(color="#ef4444", dash="dot"),
                        mode="lines+markers",
                        marker=dict(size=4),
                    ))

                for pk in post_keys:
                    lbl = q["metric_labels"].get(pk, pk)
                    vals = df[pk].tolist() if pk in df.columns else []
                    fig_trend.add_trace(go.Scatter(
                        x=df["date"].tolist(), y=vals,
                        name=f"申诉后·{lbl.replace('申诉后-', '')}",
                        line=dict(color="#22c55e"),
                        mode="lines+markers",
                        marker=dict(size=4),
                    ))
            else:
                chart_colors = ["#3b82f6", "#22c55e", "#ef4444", "#f97316", "#eab308", "#a855f7", "#06b6d4"]
                for ki, mk in enumerate(q["metric_keys"]):
                    if mk not in df.columns:
                        continue
                    lbl = q["metric_labels"].get(mk, mk)
                    vals = df[mk].tolist()
                    color = chart_colors[ki % len(chart_colors)]
                    fig_trend.add_trace(go.Scatter(
                        x=df["date"].tolist(), y=vals,
                        name=lbl,
                        line=dict(color=color),
                        fill="tonexty" if ki <= 1 else None,
                        mode="lines+markers",
                        marker=dict(size=3 if len(vals) > 15 else 5),
                    ))

            fig_trend.update_layout(
                height=380,
                margin=dict(l=40, r=30, t=20, b=50),
                legend=dict(font_size=11, orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
                yaxis=dict(ticksuffix="", tickformat=".0%", range=[0, 1.05]),
                hovermode="x unified",
                template="plotly_white",
            )
            fig_trend.update_yaxes(tickformat=".0%")
            st.plotly_chart(fig_trend, use_container_width=True)

        # ── 辅助图表 ──
        with c2:
            lr_nz = find_latest_nonzero(df, q["metric_keys"])

            if has_pp and lr_nz is not None:
                st.markdown("##### ⚖️ 申诉效果对比")
                pre_cats = ["违规准", "漏率", "准确率"]
                pre_vals = [
                    float(lr_nz.get("pre_violation_rate") or 0),
                    float(lr_nz.get("pre_miss_rate") or 0),
                    float(lr_nz.get("pre_accuracy") or 0),
                ]
                post_vals = [
                    float(lr_nz.get("post_violation_rate") or 0),
                    float(lr_nz.get("post_miss_rate") or 0),
                    float(lr_nz.get("post_accuracy") or 0),
                ]
                fig_bar = go.Figure()
                fig_bar.add_trace(go.Bar(
                    name="申诉前", x=pre_cats, y=pre_vals,
                    marker_color="rgba(239,68,68,0.6)", text=[fmt_pct(v) for v in pre_vals],
                    textposition="outside", textfont=dict(size=10),
                ))
                fig_bar.add_trace(go.Bar(
                    name="申诉后", x=post_cats, y=post_vals,
                    marker_color="rgba(34,197,94,0.6)", text=[fmt_pct(v) for v in post_vals],
                    textposition="outside", textfont=dict(size=10),
                ))
                fig_bar.update_layout(
                    height=300, barmode="group", legend_orientation="h",
                    yaxis=dict(tickformat=".0%", range=[0, 1.05]),
                    margin=dict(l=40, r=30, t=20, b=40), template="plotly_white",
                )
                st.plotly_chart(fig_bar, use_container_width=True)

            elif lr_nz is not None:
                st.markdown("##### 🍩 最新指标构成")
                dk = [mk for mk in q["metric_keys"] if mk in lr_nz.index and pd.notna(lr_nz.get(mk))]
                dl = [q["metric_labels"].get(k, k) for k in dk]
                dv = [float(lr_nz[k]) for k in dk]
                if dv:
                    fig_donut = go.Figure(go.Pie(
                        labels=dl, values=dv,
                        hole=0.55,
                        textinfo="label+percent",
                        textfont=dict(size=11),
                        marker=dict(colors=[q["color"]] + px.colors.qualitative.Set2[:len(dv)-1]),
                    ))
                    fig_donut.update_layout(height=300, margin=dict(t=20, b=10, l=10, r=10), showlegend=True)
                    st.plotly_chart(fig_donut, use_container_width=True)

        # ── 雷达图（仅当 metrics ≥ 2）──
        if len(q["metric_keys"]) >= 2 and not has_pp:
            st.markdown("##### 🎯 指标雷达（均值）")
            avgs = []
            for mk in q["metric_keys"]:
                if mk not in df.columns:
                    avgs.append(0)
                    continue
                vals = df[mk].dropna().tolist()
                avgs.append(sum(vals) / len(vals) if vals else 0)

            radar_labels = [q["metric_labels"].get(k, k) for k in q["metric_keys"]]
            fig_radar = go.Figure(go.Scatterpolar(
                r=avgs, theta=radar_labels, fill="toself", name="均值",
                line=dict(color="#3b82f6"), marker=dict(color="#3b82f6", size=8),
            ))
            fig_radar.update_layout(
                height=280, polar=dict(radialaxis=dict(visible=True, tickformat=".0%", range=[0, 1])),
                template="plotly_white", margin=dict(t=20, b=10),
            )
            st.plotly_chart(fig_radar, use_container_width=True)

        # ── 数据明细表 ──
        st.divider()
        st.markdown(f"##### 📋 **{q['name']}** 数据明细 ({len(df)} 条)")

        # 准备展示用的 DataFrame
        disp_cols = ["date"] + q["metric_keys"]
        disp_df = df[disp_cols].copy().sort_values("date", ascending=False).reset_index(drop=True)

        # 格式化百分比
        for mk in q["metric_keys"]:
            if mk in disp_df.columns:
                disp_df[mk] = disp_df[mk].apply(fmt_pct)

        # 重命名列
        rename_map = {"date": "📅 日期"}
        for mk in q["metric_keys"]:
            rename_map[mk] = q["metric_labels"].get(mk, mk)
        disp_df.rename(columns=rename_map, inplace=True)

        # 高亮颜色函数
        def highlight_cells(s):
            """<90% 红色，≥98% 绿色"""
            result = [""] * len(s)
            for i, v in enumerate(s):
                if isinstance(v, str) and v.endswith("%"):
                    try:
                        num = float(v.rstrip("%")) / 100
                        if num < 0.9:
                            result[i] = "background-color: #fef2f2; color: #dc2626; font-weight:600;"
                        elif num >= 0.98:
                            result[i] = "background-color: #f0fdf4; color: #16a34a; font-weight:600;"
                    except (ValueError, TypeError):
                        pass
            return result

        styled_df = disp_df.style.apply(highlight_cells, axis=0)
        st.dataframe(styled_df, use_container_width=True, hide_index=True, height=min(max(40 * len(disp_df), 200), 500))

        # ── 导出 CSV ──
        csv_data = disp_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button(
            label="📥 导出 CSV（Excel 兼容）",
            data=csv_data,
            file_name=f"{q['name']}_质检数据_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            key=f"dl_{qid}",
        )


# ── Footer ──
st.divider()
st.markdown(
    '<div style="text-align:center;color:#94a3b8;font-size:11px;padding:10px 0">'
    '📊 QC Dashboard · 7 队列 · Powered by Streamlit + Plotly · '
    f'<span style="opacity:0.7">{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</span></div>',
    unsafe_allow_html=True,
)
