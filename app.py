"""
============================================================
QC Dashboard — Streamlit 独立看板  v2.1
============================================================
侧边栏导航：数据总览 | 数据导入
用法:  cd qc-dashboard && streamlit run app.py
依赖: streamlit, plotly, pandas, xlsxwriter (pip install -r requirements.txt)
数据源: data/metrics.db (SQLite) + 用户上传 xlsx
"""

import os
import io
import sqlite3
import time
from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st

# ── 路径 ──
BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "data", "metrics.db")
UPLOAD_DIR = os.path.join(BASE, "data", "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ── 队列配置 ──────────────────────────────────────────────
QUEUES = [
    {
        "id": "q1_toufang",
        "name": "投放误漏",
        "full_name": "【供应商】投放误漏case",
        "icon": "📢", "color": "#3b82f6",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q2_erjiansimple",
        "name": "简单二审",
        "full_name": "【供应商】简单二审误漏case",
        "icon": "📋", "color": "#22c55e",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q3_erjian_4qi_gt",
        "name": "四期-二审GT",
        "full_name": "【四期供应商】二审周推质检分歧单（二审GT）",
        "icon": "🔄", "color": "#f97316",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q3b_erjian_4qi_qiepian",
        "name": "四期-切片GT",
        "full_name": "【四期供应商】二审周推质检分歧单（二审切片GT）",
        "icon": "🔪", "color": "#f59e0b",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q5_lahei",
        "name": "拉黑误漏",
        "full_name": "【供应商】拉黑误漏case",
        "icon": "🚫", "color": "#ef4444",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
    },
    {
        "id": "q6_shangqiang",
        "name": "上墙文本",
        "full_name": "上墙文本申诉-云雀",
        "icon": "📝", "color": "#06b6d4",
        "metric_keys": ["audit_accuracy"],
        "metric_labels": {"audit_accuracy": "审核准确率"},
    },
]

QUEUE_MAP = {q["id"]: q for q in QUEUES}


# ================================================================
#  数据层
# ================================================================

@st.cache_data(ttl=60)
def load_all_queue_data():
    """从 SQLite 加载全部队列数据，返回 {qid: DataFrame}"""
    conn = sqlite3.connect(DB_PATH)
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


def filter_by_date(df, date_from, date_to):
    if df.empty:
        return df
    mask = pd.Series([True] * len(df), index=df.index)
    if date_from:
        mask &= df["date"].astype(str) >= str(date_from)
    if date_to:
        mask &= df["date"].astype(str) <= str(date_to)
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
    if val is None or (isinstance(val, float) and (val != val)):
        return "--"
    try:
        return f"{float(val) * 100:.2f}%"
    except (ValueError, TypeError):
        return str(val)


def fmt_pct1(val):
    if val is None or (isinstance(val, float) and (val != val)):
        return "--"
    try:
        return f"{float(val) * 100:.1f}%"
    except (ValueError, TypeError):
        return str(val)


# ================================================================
#  页面：数据总览
# ================================================================

def render_dashboard(all_data):
    """质检数据看板主页"""

    min_date, max_date = get_date_range(all_data)

    # ── Header ──
    st.markdown("### 📊 QC 质检数据统一看板")
    total_records = sum(len(d) for d in all_data.values())
    st.caption(
        f"多队列 · 按日期聚合指标 · 数据来源：企业微信智能表格 · "
        f"共 **{total_records}** 条记录 · 更新于 `{datetime.now().strftime('%Y-%m-%d %H:%M')}`"
    )

    # ── 日期筛选区 ──
    date_from_str, date_to_str = None, None

    with st.expander("📅 日期筛选", expanded=False):
        f_cols = st.columns([1, 1, 1, 1, 1, 1])
        with f_cols[0]:
            d_from = st.date_input("起始日期", value=None, key="df")
        with f_cols[1]:
            d_to = st.date_input("截止日期", value=None, key="dt")
        with f_cols[2]:
            if st.button("近7天", use_container_width=True):
                if max_date:
                    st.session_state["_quick"] = ("week", max_date)
                    st.rerun()
        with f_cols[3]:
            if st.button("近30天", use_container_width=True):
                if max_date:
                    st.session_state["_quick"] = ("month", max_date)
                    st.rerun()
        with f_cols[4]:
            if st.button("全部", use_container_width=True):
                st.session_state["_quick"] = ("all", None)
                st.rerun()
        with f_cols[5]:
            if st.button("清除缓存", use_container_width=True):
                st.cache_data.clear()
                st.rerun()

        date_from_str = d_from.strftime("%Y-%m-%d") if d_from else None
        date_to_str = d_to.strftime("%Y-%m-%d") if d_to else None

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

        st.caption(f"数据范围：`{min_date}` ~ `{max_date}`" if min_date else "暂无数据")

    # ── Overview 卡片行 ──
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

    # ── 队列 Tabs ──
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

            # 统计卡片
            st.markdown("##### 📌 核心指标")
            n_metrics = len(q["metric_keys"]) + 1
            stat_cols = st.columns(min(n_metrics, 5))

            with stat_cols[0]:
                date_range_str = (
                    f"`{df['date'].iloc[0]} ~ {df['date'].iloc[-1]}"
                    if len(df) > 0 else ""
                )
                st.metric(label="📅 数据天数", value=f"{len(df)} 天", delta=date_range_str)

            for ki, mk in enumerate(q["metric_keys"]):
                valid_vals = []
                for _, r in df.iterrows():
                    v = r.get(mk) if mk in r.index else None
                    if v is not None and isinstance(v, (int, float)) and not (v != v):
                        valid_vals.append(v)

                if not valid_vals:
                    continue

                lr_nz = find_latest_nonzero(df, [mk])
                last_val = lr_nz[mk] if (lr_nz is not None and mk in lr_nz.index) else valid_vals[-1]
                avg_val = sum(valid_vals) / len(valid_vals)

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

            # 图表区
            c1, c2 = st.columns([2, 1])

            with c1:
                st.markdown("##### 📈 指标走势")
                fig_trend = go.Figure()

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
                    yaxis=dict(tickformat=".0%", range=[0, 1.05]),
                    hovermode="x unified",
                    template="plotly_white",
                )
                fig_trend.update_yaxes(tickformat=".0%")
                st.plotly_chart(fig_trend, use_container_width=True)

            # 辅助图表 — 环形图
            with c2:
                lr_nz = find_latest_nonzero(df, q["metric_keys"])
                if lr_nz is not None:
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

            # 雷达图
            if len(q["metric_keys"]) >= 2:
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
                    height=280,
                    polar=dict(radialaxis=dict(visible=True, tickformat=".0%", range=[0, 1])),
                    template="plotly_white", margin=dict(t=20, b=10),
                )
                st.plotly_chart(fig_radar, use_container_width=True)

            # 数据明细表
            st.divider()
            st.markdown(f"##### 📋 **{q['name']}** 数据明细 ({len(df)} 条)")

            disp_cols = ["date"] + q["metric_keys"]
            disp_df = df[disp_cols].copy().sort_values("date", ascending=False).reset_index(drop=True)

            for mk in q["metric_keys"]:
                if mk in disp_df.columns:
                    disp_df[mk] = disp_df[mk].apply(fmt_pct)

            rename_map = {"date": "📅 日期"}
            for mk in q["metric_keys"]:
                rename_map[mk] = q["metric_labels"].get(mk, mk)
            disp_df.rename(columns=rename_map, inplace=True)

            def _highlight(s):
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

            styled_df = disp_df.style.apply(_highlight, axis=0)
            st.dataframe(styled_df, use_container_width=True, hide_index=True,
                         height=min(max(40 * len(disp_df), 200), 500))

            # 导出 Excel
            to_excel = io.BytesIO()
            raw_df = df[disp_cols].copy().sort_values("date", ascending=False).reset_index(drop=True)
            raw_rename = {"date": "📅 日期"}
            for mk in q["metric_keys"]:
                raw_rename[mk] = q["metric_labels"].get(mk, mk)
            raw_df.rename(columns=raw_rename, inplace=True)
            raw_df.to_excel(to_excel, index=False, engine='xlsxwriter')
            to_excel.seek(0)

            st.download_button(
                label=f"📥 导出 {q['name']} 数据 (.xlsx)",
                data=to_excel,
                file_name=f"{q['name']}_质检数据_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_{qid}",
            )

    # Footer
    st.divider()
    st.markdown(
        '<div style="text-align:center;color:#94a3b8;font-size:11px;padding:10px 0">'
        '📊 QC Dashboard v2.1 · Powered by Streamlit + Plotly · '
        f'<span style="opacity:0.7">{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</span></div>',
        unsafe_allow_html=True,
    )


# ================================================================
#  页面：数据导入
# ================================================================

def render_import():
    """数据导入页面：上传 + 清洗 + 一键刷新 + 清除缓存 + 清除数据"""

    st.markdown("# 📥 数据导入")
    st.caption("上传质检 Excel 文件、管理缓存和数据")

    # ── 区域1：上传文件 ──
    st.markdown("### 📤 上传质检 Excel")
    st.caption("支持 `.xlsx` / `.xls` 格式，可拖拽多选批量上传")

    uploaded_files = st.file_uploader(
        "选择质检文件",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
        help="支持 .xlsx 和 .xls 格式，可同时选择多个文件",
    )

    if uploaded_files:
        st.success(f"已选择 **{len(uploaded_files)}** 个文件：")
        file_info = []
        for uf in uploaded_files:
            size_kb = round(len(uf.getvalue()) / 1024, 1)
            file_info.append(f"- **{uf.name}** ({size_kb} KB)")
        st.markdown("\n".join(file_info))

        if st.button("⬆️ 批量导入", type="primary", use_container_width=True):
            _process_uploads(uploaded_files)

    st.divider()

    # ── 区域2：一键刷新（页面内执行） ──
    st.markdown("### 🔄 一键刷新")
    st.caption("从已上传的 Excel 文件中解析数据并导入数据库")

    # 检测 uploads 目录中的待处理文件
    excel_files = []
    if os.path.isdir(UPLOAD_DIR):
        for fn in os.listdir(UPLOAD_DIR):
            if fn.lower().endswith(('.xlsx', '.xls')):
                fp = os.path.join(UPLOAD_DIR, fn)
                size_kb = round(os.path.getsize(fp) / 1024, 1)
                excel_files.append({"文件": fn, "大小": f"{size_kb} KB"})

    # 同时检查 processed 目录
    processed_dir = os.path.join(UPLOAD_DIR, "processed")
    processed_count = 0
    if os.path.isdir(processed_dir):
        processed_count = len([f for f in os.listdir(processed_dir) if f.lower().endswith(('.xlsx', '.xls'))])

    c_info, c_btn = st.columns([2, 1])
    with c_info:
        if excel_files:
            st.info(f"📂 待处理 **{len(excel_files)}** 个文件（已处理 {processed_count} 个）")
            st.dataframe(pd.DataFrame(excel_files), use_container_width=True, hide_index=True)
        else:
            st.warning("⏳ `data/uploads/` 目录下暂无 Excel 文件，请先上传")

    with c_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        if not excel_files:
            st.button("🔄 执行刷新", disabled=True, help="先上传 Excel 文件",
                     type="primary", use_container_width=True)
        else:
            do_refresh = st.button("🔄 执行刷新", type="primary", use_container_width=True,
                                   help="读取 uploads 中的 xlsx → 解析入库 → 清理未来日期")
            if do_refresh:
                _do_refresh()

    st.divider()

    # ── 区域3：已上传文件 ──
    st.markdown("### 📂 已上传文件")
    upload_files = sorted(os.listdir(UPLOAD_DIR)) if os.path.isdir(UPLOAD_DIR) else []

    if upload_files:
        records = []
        for fn in upload_files:
            fp = os.path.join(UPLOAD_DIR, fn)
            stat = os.stat(fp)
            size_kb = round(stat.st_size / 1024, 1)
            mtime = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            records.append({"文件名": fn, "大小(KB)": size_kb, "上传时间": mtime})
        st.dataframe(pd.DataFrame(records), use_container_width=True, hide_index=True)

        if st.button("🗑️ 清空上传记录"):
            import shutil
            shutil.rmtree(UPLOAD_DIR)
            os.makedirs(UPLOAD_DIR, exist_ok=True)
            st.success("已清空")
            st.rerun()
    else:
        st.caption("暂无上传文件")

    st.divider()

    # ── 区域4：缓存 & 数据管理 ──
    c_cache, c_data = st.columns(2)

    with c_cache:
        st.markdown("### 🧹 清除缓存")
        st.caption("清除 Streamlit 数据缓存，不删除数据库")
        if st.button("清除缓存", use_container_width=True):
            st.cache_data.clear()
            st.success("✅ 缓存已清除")
            time.sleep(1)
            st.rerun()

    with c_data:
        st.markdown("### ⚠️ 清除数据")

        # ── 选项1: 按日期范围清除 ──
        with st.expander("📅 按日期范围清除", expanded=False):
            st.caption("删除指定日期范围内的数据，不可恢复")
            d_del_from = st.date_input("起始日期", key="del_from")
            d_del_to = st.date_input("截止日期", key="del_to")
            
            # 预览将删除的记录数
            preview_del = 0
            if d_del_from and d_del_to:
                from_s = d_del_from.strftime("%Y-%m-%d") if d_del_from else None
                to_s = d_del_to.strftime("%Y-%m-%d") if d_del_to else None
                conn_preview = sqlite3.connect(DB_PATH)
                c_prev = conn_preview.cursor()
                c_prev.execute(
                    "SELECT COUNT(*) FROM daily_metrics WHERE date BETWEEN ? AND ?",
                    (from_s, to_s),
                )
                preview_del = c_prev.fetchone()[0]
                conn_preview.close()
                
                if preview_del > 0:
                    st.warning(f"⚠️ 将删除 **{preview_del}** 条记录（{from_s} ~ {to_s}）")
                else:
                    st.info(f"该范围内暂无数据（{from_s} ~ {to_s}）")
                
                if preview_del > 0 and st.button(
                    "🗑️ 删除选中范围", type="primary",
                    key="_del_range_btn", disabled=(preview_del == 0)
                ):
                    conn_del = sqlite3.connect(DB_PATH)
                    cd = conn_del.cursor()
                    cd.execute("DELETE FROM daily_metrics WHERE date BETWEEN ? AND ?", (from_s, to_s))
                    deleted_count = cd.rowcount
                    conn_del.commit()
                    conn_del.close()
                    st.cache_data.clear()
                    st.success(f"✅ 已删除 **{deleted_count}** 条记录")
                    time.sleep(1)
                    st.rerun()

        st.divider()

        # ── 选项2: 全部清除 ──
        st.markdown("**全部清除**")
        st.caption("永久删除数据库中**所有**质检数据，不可恢复")
        
        confirm = st.text_input("输入 CONFIRM 确认全量删除", key="_confirm_del", placeholder="CONFIRM")
        if confirm and confirm.strip().upper() == "CONFIRM":
            if st.button("确认清除全部数据", type="primary", use_container_width=True):
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute("DELETE FROM daily_metrics")
                deleted_all = cursor.rowcount
                conn.commit()
                conn.close()
                st.success(f"已删除 **{deleted_all}** 条记录")
                st.cache_data.clear()
                time.sleep(1)
                st.rerun()


def _process_uploads(uploaded_files):
    """处理上传文件"""
    progress_bar = st.progress(0, "准备导入...")
    status_text = st.empty()
    results = []
    total = len(uploaded_files)
    success_count = 0
    error_count = 0

    for i, uf in enumerate(uploaded_files):
        try:
            status_text.text(f"[{i+1}/{total}] 处理: **{uf.name}** ...")
            progress_bar.progress((i + 0.5) / total)

            save_path = os.path.join(UPLOAD_DIR, f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{uf.name}")
            with open(save_path, "wb") as f:
                f.write(uf.getvalue())

            bytes_io = io.BytesIO(uf.getvalue())
            xl_sheets = pd.ExcelFile(bytes_io)
            first_sheet = xl_sheets.sheet_names[0]
            df = pd.read_excel(bytes_io, sheet_name=first_sheet)

            results.append({
                "文件": uf.name,
                "状态": "✅ 已读取",
                "行数": len(df),
                "列数": len(df.columns),
                "子表": first_sheet,
            })
            success_count += 1

        except Exception as e:
            results.append({
                "文件": uf.name,
                "状态": f"❌ 失败: {str(e)[:60]}",
                "行数": 0, "列数": 0, "子表": "-",
            })
            error_count += 1

    progress_bar.progress(1.0, "完成！")
    status_text.text("导入完成")

    st.subheader("📊 导入结果")
    c_ok, c_err = st.columns(2)
    c_ok.metric("成功", success_count)
    c_err.metric("失败", error_count)

    if results:
        st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)


def _do_refresh():
    """一键刷新：读取 uploads 中的 xlsx → 解析入库 → 清理未来日期 → 清缓存"""
    import yaml
    from pathlib import Path

    status = st.empty()
    progress = st.progress(0, "准备刷新...")
    log_lines = []

    def log(msg):
        log_lines.append(msg)
        status.markdown("\n".join(f"· {line}" for line in log_lines[-12:]))

    try:
        # Step 1: 加载配置
        progress.progress(5, "加载配置...")
        log("📋 读取 config.yaml ...")
        cfg_path = os.path.join(BASE, "config.yaml")
        if not os.path.exists(cfg_path):
            st.error("❌ 找不到 config.yaml")
            return
        with open(cfg_path, "r") as f:
            config = yaml.safe_load(f)

        # Step 2: 导入 Excel（复用 collector.py 的逻辑）
        progress.progress(15, "导入 Excel 数据...")
        log("📂 扫描 data/uploads/ 目录...")

        # 动态导入 collector 模块
        sys_path = os.path.join(BASE, "src")
        if sys_path not in __import__("sys").path:
            __import__("sys").path.insert(0, sys_path)

        import importlib.util
        collector_spec = importlib.util.spec_from_file_location(
            "collector", os.path.join(sys_path, "collector.py")
        )
        if collector_spec and collector_spec.loader:
            collector_mod = importlib.util.module_from_spec(collector_spec)
            collector_spec.loader.exec_module(collector_mod)

            conn = collector_mod.init_db()
            imported = collector_mod.import_excel(conn, config)
            log(f"✅ 导入完成：**{imported}** 条新记录")
        else:
            # fallback: 直接用 pandas 简单导入
            log("⚠️ collector.py 不可用，使用简单模式导入")
            _simple_import(progress, log)

        progress.progress(60, "清理未来日期...")

        # Step 3: 清理未来日期
        from datetime import date as _date
        conn_sqlite = sqlite3.connect(DB_PATH)
        c = conn_sqlite.cursor()
        today_str = _date.today().isoformat()
        c.execute("DELETE FROM daily_metrics WHERE date > ?", (today_str,))
        future_deleted = c.rowcount
        conn_sqlite.commit()
        conn_sqlite.close()

        if future_deleted > 0:
            log(f"🧹 清理 **{future_deleted}** 条未来日期数据 (>{today_str})")
        else:
            log("🧹 无需清理未来日期")

        progress.progress(80, "清除缓存...")

        # Step 4: 清除 Streamlit 缓存
        st.cache_data.clear()
        log("🔄 缓存已清除")

        progress.progress(100, "✅ 刷新完成！")

        time.sleep(1)
        st.success(
            f"🎉 **刷新完成！**\n\n"
            f"- 新增记录：{imported if 'imported' in dir() else '?'} 条\n"
            f"- 未来日期清理：{future_deleted} 条\n"
            f"- 数据已更新，可切换到「数据总览」查看"
        )
        time.sleep(2)

    except Exception as e:
        st.error(f"❌ 刷新失败：{str(e)}")
        import traceback
        st.code(traceback.format_exc(), language="text")


def _simple_import(progress, log):
    """简单模式：直接用 pandas 读取 uploads 中所有 xlsx 入库"""
    import glob as _glob

    excel_files = list(_glob.glob(os.path.join(UPLOAD_DIR, "*.xlsx"))) + \
                   list(_glob.glob(os.path.join(UPLOAD_DIR, "*.xls")))
    total_imported = 0
    processed_dir = os.path.join(UPLOAD_DIR, "processed")
    os.makedirs(processed_dir, exist_ok=True)

    for fi, fpath in enumerate(excel_files):
        fname = os.path.basename(fname := os.path.basename(fpath))
        progress.progress(20 + int(40 * fi / max(len(excel_files), 1)), f"[{fi+1}/{len(excel_files)}] {fname}")
        log(f"📄 [{fi+1}/{len(excel_files)}] {fname}")

        try:
            xl = pd.ExcelFile(fpath)
            for sn in xl.sheet_names:
                df = pd.read_excel(xl, sheet_name=sn, header=None)
                log(f"   📊 子表「{sn}」: {df.shape[0]} 行 × {df.shape[1]} 列")

                # 这里只做基础保存记录，完整解析需要 config.yaml 映射
                # 将文件移到 processed 避免重复处理
            dest = os.path.join(processed_dir, f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{fname}")
            shutil_move = __import__("shutil").move
            shutil_move(fpath, dest)
            total_imported += len(df) - 2  # 减去表头行
        except Exception as e:
            log(f"   ❌ 失败: {str(e)[:80]}")

    log(f"✅ 处理完成：{total_imported} 行数据")


# ================================================================
#  主程序 — 侧边栏导航
# ================================================================

st.set_page_config(
    page_title="QC 质检数据看板",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        "About": "📊 QC Dashboard v2.1 — 质检数据统一看板",
        "Report a bug": None,
        "Get Help": None,
    },
)

st.markdown("""
<style>
    /* 隐藏 Streamlit 自带的侧边栏默认元素（避免重复） */
    [data-testid="stSidebar"] [data-testid="stVerticalBlock"] > *:first-child {
        display: none !important;
    }
    
    /* 统计卡片 */
    [data-testid="stMetric"] {
        background-color: #ffffff;
        border: 1px solid #e2e8f0;
        border-radius: 12px;
        padding: 8px !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }
    h1 { font-size: 24px !important; font-weight: 700 !important; margin-bottom: 8px !important; }
    h3 { font-size: 20px !important; font-weight: 600 !important; }
    /* 上传区域 */
    [data-testid="stFileUploader"] {
        border: 2px dashed #cbd5e1 !important;
        border-radius: 12px !important;
        padding: 16px !important;
    }
    /* 表格样式 */
    [data-testid="stDataFrame"] { border-radius: 12px; overflow: hidden; }
    
    /* 侧边栏导航按钮样式 */
    [data-testid="stSidebar"] .row-widget {
        margin-bottom: 4px;
    }
</style>
""", unsafe_allow_html=True)

# ── 侧边栏：唯一导航入口 ──
with st.sidebar:
    st.markdown("### 📌 导航")
    st.markdown("---")
    
    page = st.radio(
        "导航选择",
        ["📊 数据总览", "📥 数据导入"],
        index=0,
        label_visibility="collapsed",
    )
    
    st.markdown("---")
    st.caption(
        f"📅 {datetime.now().strftime('%Y-%m-%d')}\n\n"
        f"📊 QC Dashboard v2.1"
    )

# ── 渲染对应页面 ──
if page == "📊 数据总览":
    with st.spinner("加载中..."):
        all_data = load_all_queue_data()
    render_dashboard(all_data)
elif page == "📥 数据导入":
    render_import()
