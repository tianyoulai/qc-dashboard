"""
============================================================
QC Dashboard — Streamlit 看板  v2.0（视觉重构版）
============================================================
用法:  cd qc-dashboard && streamlit run streamlit_app.py
依赖: streamlit, plotly, pandas (pip install -r requirements.txt)
数据源: data/metrics.db (SQLite)
"""

import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import streamlit as st

# ── 路径 ──
BASE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE, "src"))
from db_helper import get_db
DB_PATH = os.path.join(BASE, "data", "metrics.db")

# ── 队列配置 ──
QUEUES = [
    {
        "id": "q1_toufang", "name": "投放误漏",
        "full_name": "【供应商】投放误漏case",
        "icon": "📢", "color": "#3b82f6",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q2_erjiansimple", "name": "简单二审",
        "full_name": "【供应商】简单二审误漏case",
        "icon": "📋", "color": "#22c55e",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q3_erjian_4qi_gt", "name": "四期-二审GT",
        "full_name": "【四期供应商】二审周推质检分歧单（二审GT）",
        "icon": "🔄", "color": "#f97316",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q3b_erjian_4qi_qiepian", "name": "四期-切片GT",
        "full_name": "【四期供应商】二审周推质检分歧单（二审切片GT）",
        "icon": "🔪", "color": "#f59e0b",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q4_jubao_4qi", "name": "四期-举报",
        "full_name": "【四期供应商】举报周推质检分歧单",
        "icon": "🚨", "color": "#a855f7",
        "has_pre_post": True,
        "metric_keys": ["pre_violation_rate", "pre_miss_rate", "pre_accuracy",
                        "post_violation_rate", "post_miss_rate", "post_accuracy"],
        "metric_labels": {
            "pre_violation_rate": "申诉前-违规准确率", "pre_miss_rate": "申诉前-漏率", "pre_accuracy": "申诉前-准确率",
            "post_violation_rate": "申诉后-违规准确率", "post_miss_rate": "申诉后-漏率", "post_accuracy": "申诉后-准确率",
        },
    },
    {
        "id": "q5_lahei", "name": "拉黑误漏",
        "full_name": "【供应商】拉黑误漏case",
        "icon": "🚫", "color": "#ef4444",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q6_shangqiang", "name": "上墙文本",
        "full_name": "上墙文本申诉-云雀",
        "icon": "📝", "color": "#06b6d4",
        "metric_keys": ["audit_accuracy"],
        "metric_labels": {"audit_accuracy": "审核准确率"},
    },
]

QUEUE_MAP = {q["id"]: q for q in QUEUES}


# ════════════════════ 数据层 ════════════════════

@st.cache_data(ttl=120)
def load_all_queue_data():
    """从 SQLite 加载全部队列数据"""
    conn = get_db(DB_PATH)
    all_data = {}
    for q in QUEUES:
        qid = q["id"]
        df = pd.read_sql_query(
            "SELECT date, metric_key, metric_value FROM daily_metrics WHERE queue_id=? ORDER BY date",
            conn, params=(qid,),
        )
        if df.empty:
            all_data[qid] = pd.DataFrame(columns=["date"] + q["metric_keys"])
            continue
        df_wide = df.pivot(index="date", columns="metric_key", values="metric_value").reset_index()
        for mk in q["metric_keys"]:
            if mk not in df_wide.columns:
                df_wide[mk] = None
        df_wide["date"] = pd.to_datetime(df_wide["date"]).dt.strftime("%Y-%m-%d")
        all_data[qid] = df_wide
    conn.close()
    return all_data


def get_date_range(all_data):
    all_dates = []
    for df in all_data.values():
        if not df.empty and "date" in df.columns:
            all_dates.extend(df["date"].tolist())
    if not all_dates:
        return None, None
    all_dates = sorted(set(all_dates))
    return all_dates[0], all_dates[-1]


def filter_by_date(df, d_from, d_to):
    if df.empty:
        return df
    mask = pd.Series([True] * len(df), index=df.index)
    if d_from:
        mask &= df["date"].astype(str) >= str(d_from)
    if d_to:
        mask &= df["date"].astype(str) <= str(d_to)
    return df.loc[mask].reset_index(drop=True)


def find_latest_nonzero(df, keys):
    """倒序查找最新非零行"""
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
    if val is None or (isinstance(val, float) and val != val):
        return "—"
    try:
        return f"{float(val) * 100:.2f}%"
    except (ValueError, TypeError):
        return str(val)


def fmt_pct1(val):
    if val is None or (isinstance(val, float) and val != val):
        return "—"
    try:
        return f"{float(val) * 100:.1f}%"
    except (ValueError, TypeError):
        return str(val)


# ════════════════════ 页面配置 ════════════════════

st.set_page_config(
    page_title="QC 质量看板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# 注入样式
st.markdown(open(os.path.join(BASE, "custom.css"), encoding="utf-8").read(), unsafe_allow_html=True)


# ── 加载数据 ──
with st.spinner("加载数据中..."):
    all_data = load_all_queue_data()

min_date, max_date = get_date_range(all_data)
total_records = sum(len(d) for d in all_data.values())


# ════════════════════ 顶部工具栏 ════════════════════

st.markdown("""
<div class="dash-header">
  <div class="dash-header-left">
    <span class="dash-logo">📊</span>
    <div>
      <div class="dash-title">A组 QC 质量看板</div>
      <div class="dash-subtitle">7 个质检队列 · 实时指标监控</div>
    </div>
  </div>
  <div class="dash-header-right">
    <span class="dash-badge">{records} 条记录</span>
    <span class="dash-badge dash-badge-updated">更新于 {update}</span>
  </div>
</div>
""".format(records=f"<b>{total_records}</b>", update=datetime.now().strftime('%H:%M')), unsafe_allow_html=True)

# 日期筛选栏（移到主区域，不再依赖侧边栏）
st.markdown('<div class="filter-bar">', unsafe_allow_html=True)

fcol1, fcol2, fcol3, fcol4, fcol5, fcol6 = st.columns([1, 2, 0.6, 1, 2, 1.4])
with fcol1:
    st.markdown('<span class="filter-label">📅 起</span>', unsafe_allow_html=True)
with fcol2:
    d_from = st.date_input("起始日期", value=None, label_visibility="collapsed", key="df_main")
with fcol3:
    st.markdown('<div style="text-align:center;margin-top:12px;color:#94a3b8">→</div>', unsafe_allow_html=True)
with fcol4:
    st.markdown('<span class="filter-label">📅 止</span>', unsafe_allow_html=True)
with fcol5:
    d_to = st.date_input("截止日期", value=None, label_visibility="collapsed", key="dt_main")
with fcol6:
    quick = st.segmented_control("快捷范围", options=["近7天", "近30天", "全部"], default=None, label_visibility="collapsed", key="quick_rng")

date_from_str = d_from.strftime("%Y-%m-%d") if d_from else None
date_to_str = d_to.strftime("%Y-%m-%d") if d_to else None

if quick == "近7天" and max_date:
    dt = datetime.strptime(max_date, "%Y-%m-%d") if isinstance(max_date, str) else max_date
    date_from_str = (dt - timedelta(days=6)).strftime("%Y-%m-%d")
    date_to_str = max_date
elif quick == "近30天" and max_date:
    dt = datetime.strptime(max_date, "%Y-%m-%d") if isinstance(max_date, str) else max_date
    date_from_str = (dt - timedelta(days=29)).strftime("%Y-%m-%d")
    date_to_str = max_date
elif quick == "全部":
    date_from_str, date_to_str = None, None

st.markdown('</div>', unsafe_allow_html=True)
st.markdown('<div style="height:8px"></div>', unsafe_allow_html=True)


# ════════════════════ Overview KPI 卡片 ════════════════════

# 响应式网格：每行最多4张卡片
N_COL = 4
n_rows = (len(QUEUES) + N_COL - 1) // N_COL

for row_idx in range(n_rows):
    start_i = row_idx * N_COL
    end_i = min(start_i + N_COL, len(QUEUES))
    row_qs = QUEUES[start_i:end_i]
    cols = st.columns(len(row_qs))

    for ci, q in enumerate(row_qs):
        df_raw = all_data.get(q["id"], pd.DataFrame())
        df_f = filter_by_date(df_raw, date_from_str, date_to_str)
        lr = find_latest_nonzero(df_f, q["metric_keys"])

        first_mk = q["metric_keys"][0]
        display_val = fmt_pct1(lr[first_mk]) if lr is not None and first_mk in lr.index else "—"
        latest_date = lr["date"] if lr is not None else "—"

        # 计算趋势箭头
        trend_html = ""
        valid_vals = []
        if not df_f.empty and first_mk in df_f.columns:
            for _, r in df_f.iterrows():
                v = r.get(first_mk)
                if v is not None and isinstance(v, (int, float)) and not (v != v):
                    valid_vals.append(float(v))
        if len(valid_vals) >= 2:
            chg = valid_vals[-1] - valid_vals[-2]
            if abs(chg) > 0.001:
                direction = "↑" if chg > 0 else "↓"
                color = "#ef4444" if chg > 0 else "#22c55e"
                # 对于准确率类指标：上升是好；对于漏率类指标：下降是好
                if "漏" in q["metric_labels"].get(first_mk, "") or "miss" in first_mk:
                    color = "#22c55e" if chg < 0 else "#ef4444"
                else:
                    color = "#22c55e" if chg > 0 else "#ef4444"
                trend_html = f'<span class="trend-{direction}" style="color:{color}">{direction} {abs(chg)*100:.1f}pp</span>'

        with cols[ci]:
            st.markdown(f"""
<div class="kpi-card" style="border-left: 4px solid {q['color']};">
  <div class="kpi-icon-row">
    <span class="kpi-icon">{q['icon']}</span>
    <span class="kpi-name">{q['name']}</span>
  </div>
  <div class="kpi-value-row">
    <span class="kpi-value">{display_val}</span>
    {trend_html}
  </div>
  <div class="kpi-meta">
    <span>{len(df_f)} 天数据</span>
    <span>·</span>
    <span>{latest_date}</span>
  </div>
</div>""", unsafe_allow_html=True)

st.markdown("---")


# ════════════════════ 队列详情 Tabs ════════════════════

tab_labels = [f"{q['icon']} {q['name']}" for q in QUEUES]
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

        # ── 核心指标卡片区（紧凑横排）──
        metric_keys = q["metric_keys"]
        n_mk = len(metric_keys)
        stat_cols = st.columns(min(n_mk + 1, 6))

        with stat_cols[0]:
            dr_str = f"`{df['date'].iloc[0]} ~ {df['date'].iloc[-1]}`" if len(df) > 0 else ""
            st.metric("📅 数据量", f"{len(df)} 天", delta=dr_str, delta_color="off")

        for ki, mk in enumerate(metric_keys):
            if ki + 1 >= len(stat_cols):
                break
            # 收集有效值
            valid_vals = []
            for _, r in df.iterrows():
                v = r.get(mk) if mk in r.index else None
                if v is not None and isinstance(v, (int, float)) and not (v != v):
                    valid_vals.append(float(v))
            if not valid_vals:
                continue

            lr_nz = find_latest_nonzero(df, [mk])
            last_val = lr_nz[mk] if (lr_nz is not None and mk in lr_nz.index) else valid_vals[-1]
            avg_val = sum(valid_vals) / len(valid_vals)

            lbl = q["metric_labels"].get(mk, mk)
            with stat_cols[ki + 1]:
                st.metric(lbl, fmt_pct(last_val), delta=f"均值 {fmt_pct1(avg_val)}", delta_color="off")

        # ── 图表区 ──
        has_pp = q.get("has_pre_post", False)
        c_chart, c_aux = st.columns([2.2, 1])

        with c_chart:
            st.markdown("##### 📈 指标走势")
            fig_trend = go.Figure()

            if has_pp:
                pre_keys = [k for k in metric_keys if k.startswith("pre_")]
                post_keys = [k for k in metric_keys if k.startswith("post_")]
                for pk in pre_keys:
                    lbl = q["metric_labels"].get(pk, pk).replace("申诉前-", "")
                    vals = df[pk].tolist() if pk in df.columns else []
                    fig_trend.add_trace(go.Scatter(
                        x=df["date"].tolist(), y=vals,
                        name=f"申诉前·{lbl}",
                        line=dict(color="#ef4444", width=2, dash="dot"),
                        mode="lines+markers", marker=dict(size=4),
                    ))
                for pk in post_keys:
                    lbl = q["metric_labels"].get(pk, pk).replace("申诉后-", "")
                    vals = df[pk].tolist() if pk in df.columns else []
                    fig_trend.add_trace(go.Scatter(
                        x=df["date"].tolist(), y=vals,
                        name=f"申诉后·{lbl}",
                        line=dict(color="#22c55e", width=2),
                        mode="lines+markers", marker=dict(size=4),
                    ))
            else:
                palette = ["#3b82f6", "#22c55e", "#f97316", "#a855f7", "#06b6d4", "#ec4899"]
                for ki, mk in enumerate(metric_keys):
                    if mk not in df.columns:
                        continue
                    lbl = q["metric_labels"].get(mk, mk)
                    vals = df[mk].tolist()
                    fig_trend.add_trace(go.Scatter(
                        x=df["date"].tolist(), y=vals, name=lbl,
                        line=dict(color=palette[ki % len(palette)], width=2),
                        fill="tonexty" if ki <= 1 else None,
                        mode="lines+markers", marker=dict(size=3 if len(vals) > 15 else 5),
                    ))

            fig_trend.update_layout(
                height=360,
                margin=dict(l=45, r=25, t=15, b=45),
                legend=dict(font_size=11, orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                xaxis=dict(tickangle=-40, tickfont=dict(size=10), showgrid=False, linecolor="#e2e8f0"),
                yaxis=dict(tickformat=".0%", range=[0, 1.05], showgrid=True, gridcolor="#f1f5f9"),
                hovermode="x unified",
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(248,250,252,0.5)",
            )
            fig_trend.update_yaxes(tickformat=".0%")
            fig_trend.update_xaxes(tickfont=dict(size=10))
            st.plotly_chart(fig_trend, use_container_width=True)

        # ── 辅助图 ──
        with c_aux:
            lr_nz = find_latest_nonzero(df, metric_keys)

            if has_pp and lr_nz is not None:
                try:
                    st.markdown("##### ⚖️ 申诉对比")
                    cats = ["违规准", "漏率", "准确率"]
                    pre_v = [float(lr_nz.get("pre_violation_rate") or 0),
                             float(lr_nz.get("pre_miss_rate") or 0),
                             float(lr_nz.get("pre_accuracy") or 0)]
                    post_v = [float(lr_nz.get("post_violation_rate") or 0),
                              float(lr_nz.get("post_miss_rate") or 0),
                              float(lr_nz.get("post_accuracy") or 0)]
                    fig_bar = go.Figure()
                    fig_bar.add_trace(go.Bar(
                        name="申诉前", x=cats, y=pre_v,
                        marker_color="#fecaca", marker_line_color="#ef4444", marker_line_width=1,
                        text=[fmt_pct(v) for v in pre_v], textposition="outside", textfont=dict(size=9, color="#991b1b"),
                    ))
                    fig_bar.add_trace(go.Bar(
                        name="申诉后", x=cats, y=post_v,
                        marker_color="#bbf7d0", marker_line_color="#22c55e", marker_line_width=1,
                        text=[fmt_pct(v) for v in post_v], textposition="outside", textfont=dict(size=9, color="#166534"),
                    ))
                    fig_bar.update_layout(
                        height=280, barmode="group", legend_orientation="h", legend_font_size=10,
                        yaxis=dict(tickformat=".0%", range=[0, 1.05]),
                        margin=dict(l=35, r=20, t=15, b=35),
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(248,250,252,0.5)",
                        bargap=0.2,
                    )
                    st.plotly_chart(fig_bar, use_container_width=True)
                except Exception as e:
                    pass

            elif lr_nz is not None:
                dk = [mk for mk in metric_keys if mk in lr_nz.index and pd.notna(lr_nz.get(mk))]
                dl = [q["metric_labels"].get(k, k) for k in dk]
                dv = [float(lr_nz[k]) for k in dk]
                if dv:
                    st.markdown("##### 🍩 最新构成")
                    donut_colors = [q["color"]] + list(px.colors.qualitative.Set2[:len(dv)-1])[:len(dv)-1]
                    fig_donut = go.Figure(go.Pie(
                        labels=dl, values=dv, hole=0.58,
                        textinfo="label+percent", textfont=dict(size=11),
                        marker=dict(colors=donut_colors),
                        hovertemplate="%{label}: %{value:.1%}<extra></extra>",
                    ))
                    fig_donut.update_layout(height=280, margin=dict(t=15, b=10, l=10, r=10),
                                            legend=dict(font_size=10, orientation="h", yanchor="bottom", y=-0.08),
                                            paper_bgcolor="rgba(0,0,0,0)")
                    st.plotly_chart(fig_donut, use_container_width=True)

        # ── 雷达图 ──
        if len(metric_keys) >= 2 and not has_pp:
            avgs = []
            for mk in metric_keys:
                if mk not in df.columns:
                    avgs.append(0); continue
                vals = df[mk].dropna().tolist()
                avgs.append(sum(vals) / len(vals) if vals else 0)

            radar_labels = [q["metric_labels"].get(k, k) for k in metric_keys]
            fig_radar = go.Figure(go.Scatterpolar(
                r=avgs, theta=radar_labels, fill="toself", name="均值",
                line=dict(color=q["color"], width=2),
                fillcolor=f"rgba{tuple(int(q['color'].lstrip('#')[i:i+2], 16) for i in (0,2,4)) + (0.12,)}",
                marker=dict(color=q["color"], size=7),
            ))
            fig_radar.update_layout(
                height=260, polar=dict(radialaxis=dict(visible=True, tickformat=".0%", range=[0, 1])),
                paper_bgcolor="rgba(0,0,0,0)", margin=dict(t=15, b=10),
            )
            st.plotly_chart(fig_radar, use_container_width=True)

        # ── 数据明细表 ──
        st.divider()
        st.markdown(f"""<div class="table-header-row">
  <span class="table-header-title">📋 {q['full_name']} · 数据明细</span>
  <span class="table-header-count">{len(df)} 条记录</span>
</div>""", unsafe_allow_html=True)

        disp_cols = ["date"] + metric_keys
        disp_df = df[disp_cols].copy().sort_values("date", ascending=False).reset_index(drop=True)
        for mk in metric_keys:
            if mk in disp_df.columns:
                disp_df[mk] = disp_df[mk].apply(fmt_pct)
        rename_map = {"date": "📅 日期"}
        for mk in metric_keys:
            rename_map[mk] = q["metric_labels"].get(mk, mk)
        disp_df.rename(columns=rename_map, inplace=True)

        def highlight_cells(s):
            result = [""] * len(s)
            for i, v in enumerate(s):
                if isinstance(v, str) and v.endswith("%"):
                    try:
                        num = float(v.rstrip("%")) / 100
                        if num < 0.90:
                            result[i] = "background-color: #fef2f2; color: #dc2626; font-weight:600; border-radius: 4px;"
                        elif num >= 0.98:
                            result[i] = "background-color: #f0fdf4; color: #16a34a; font-weight:600; border-radius: 4px;"
                    except (ValueError, TypeError):
                        pass
            return result

        styled_df = disp_df.style.apply(highlight_cells, axis=0)
        st.dataframe(styled_df, use_container_width=True, hide_index=True,
                     height=min(max(38 * len(disp_df), 180), 480))

        csv_data = disp_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
        st.download_button(
            label="📥 导出 CSV",
            data=csv_data,
            file_name=f"{q['name']}_质检数据_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv", key=f"dl_{qid}",
        )


# ════════════════════ Footer ════════════════════

st.divider()
st.markdown(f"""
<div class="dash-footer">
  QC Dashboard v2.0 · A组质量看板 · Powered by Streamlit + Plotly
  <span class="footer-sep">|</span>
  <span id="footer-ts">{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</span>
</div>""", unsafe_allow_html=True)
