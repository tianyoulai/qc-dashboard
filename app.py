"""
============================================================
QC Dashboard — Streamlit 看板  v4.0（AI 增强版）
============================================================
对标 HTML 模板版 UI 风格（白底卡片/胶囊Tab/轻量走势图）
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
import streamlit as st
import requests
import yaml

# ── 路径 ──
BASE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE, "data", "metrics.db")
UPLOAD_DIR = os.path.join(BASE, "data", "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

# ════════════════════════════════════════════════════════════════
#  队列配置
# ════════════════════════════════════════════════════════════════
QUEUES = [
    {
        "id": "q1_toufang", "name": "投放误漏",
        "full_name": "【供应商】投放误漏case",
        "icon": "📢", "color": "#3b82f6",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
        "thresholds": {"violation_rate": {"min": 0.98}, "miss_rate": {"max": 0.02}},
        "primary_metric": "violation_rate",
    },
    {
        "id": "q2_erjiansimple", "name": "简单二审",
        "full_name": "【供应商】简单二审误漏case",
        "icon": "📋", "color": "#22c55e",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
        "thresholds": {"violation_rate": {"min": 0.98}, "miss_rate": {"max": 0.02}},
        "primary_metric": "violation_rate",
    },
    {
        "id": "q3_erjian_4qi_gt", "name": "四期-二审GT",
        "full_name": "【四期供应商】二审周推质检分歧单（二审GT）",
        "icon": "🔄", "color": "#f97316",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
        "thresholds": {"violation_rate": {"min": 0.99}, "miss_rate": {"max": 0.01}},
        "primary_metric": "violation_rate",
    },
    {
        "id": "q3b_erjian_4qi_qiepian", "name": "四期-切片GT",
        "full_name": "【四期供应商】二审周推质检分歧单（二审切片GT）",
        "icon": "🔪", "color": "#f59e0b",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
        "thresholds": {"violation_rate": {"min": 0.99}, "miss_rate": {"max": 0.01}},
        "primary_metric": "violation_rate",
    },
    {
        "id": "q4_jubao_4qi", "name": "四期-举报",
        "full_name": "【四期供应商】举报周推质检分歧单",
        "icon": "🚨", "color": "#a855f7",
        "metric_keys": ["pre_violation_rate", "pre_miss_rate", "post_violation_rate", "post_miss_rate"],
        "metric_labels": {
            "pre_violation_rate": "申诉前-违规率", "pre_miss_rate": "申诉前-漏率",
            "post_violation_rate": "申诉后-违规率", "post_miss_rate": "申诉后-漏率",
        },
        "thresholds": {
            "pre_violation_rate": {"min": 0.99}, "pre_miss_rate": {"max": 0.01},
            "post_violation_rate": {"min": 0.99}, "post_miss_rate": {"max": 0.01},
        },
        "primary_metric": "post_violation_rate",
    },
    {
        "id": "q5_lahei", "name": "拉黑误漏",
        "full_name": "【供应商】拉黑误漏case",
        "icon": "🚫", "color": "#ef4444",
        "metric_keys": ["violation_rate", "miss_rate"],
        "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
        "thresholds": {"violation_rate": {"min": 0.98}, "miss_rate": {"max": 0.02}},
        "primary_metric": "violation_rate",
    },
    {
        "id": "q6_shangqiang", "name": "上墙文本",
        "full_name": "上墙文本申诉-云雀",
        "icon": "📝", "color": "#06b6d4",
        "metric_keys": ["audit_accuracy"],
        "metric_labels": {"audit_accuracy": "审核准确率"},
        "thresholds": {"audit_accuracy": {"min": 0.98}},
        "primary_metric": "audit_accuracy",
    },
]

QUEUE_MAP = {q["id"]: q for q in QUEUES}


# ════════════════════════════════════════════════════════════════
#  数据层
# ════════════════════════════════════════════════════════════════

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
    """倒序查找最新非零行（任一 key 非零即返回）"""
    if df.empty:
        return None
    for idx in range(len(df) - 1, -1, -1):
        row = df.iloc[idx]
        for k in keys:
            v = row.get(k) if k in row.index else None
            if v is not None and isinstance(v, (int, float)) and v != 0:
                return row
    return df.iloc[-1]


def find_latest_nonzero_per_key(df, mk):
    """对单个 metric_key 倒序查找最新非零值"""
    if df.empty or mk not in df.columns:
        return None
    for idx in range(len(df) - 1, -1, -1):
        v = df.iloc[idx][mk]
        if v is not None and isinstance(v, (int, float)) and not (v != v) and v != 0:
            return v
    v = df.iloc[-1][mk]
    return v if pd.notna(v) else None


def get_valid_values(df, mk):
    """获取某指标的非空非零值列表"""
    if df.empty or mk not in df.columns:
        return []
    vals = []
    for v in df[mk]:
        if v is not None and isinstance(v, (int, float)) and not (v != v) and v != 0:
            vals.append(v)
    return vals


def fmt_pct(val):
    """格式化百分比（2位小数）"""
    if val is None or (isinstance(val, float) and (val != val)):
        return "--"
    try:
        return f"{float(val) * 100:.2f}%"
    except (ValueError, TypeError):
        return str(val)


def fmt_pct1(val):
    """格式化百分比（1位小数）"""
    if val is None or (isinstance(val, float) and (val != val)):
        return "--"
    try:
        return f"{float(val) * 100:.1f}%"
    except (ValueError, TypeError):
        return str(val)


# ════════════════════════════════════════════════════════════════
#  底线阈值检查
# ════════════════════════════════════════════════════════════════

def check_threshold(q, metric_key, value):
    """检查某指标是否达标。返回: (is_ok, status_str, css_color)"""
    thresholds = q.get("thresholds", {})
    rule = thresholds.get(metric_key)
    if not rule or value is None:
        return True, "", ""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return True, "", ""
    if "min" in rule and v < rule["min"]:
        return False, f"⚠️ 低于底线 {rule['min']*100:.0f}%", "#dc2626"
    if "max" in rule and v > rule["max"]:
        return False, f"⚠️ 超出上限 {rule['max']*100:.0f}%", "#dc2626"
    return True, "✅ 达标", "#16a34a"


def get_threshold_label(q, metric_key):
    """返回某指标的底线说明文本"""
    rule = q.get("thresholds", {}).get(metric_key)
    if not rule:
        return ""
    if "min" in rule:
        return f"≥{rule['min']*100:.0f}%"
    if "max" in rule:
        return f"≤{rule['max']*100:.0f}%"
    return ""


# ════════════════════════════════════════════════════════════════
#  AI 智能分析模块
# ════════════════════════════════════════════════════════════════

def _load_deepseek_config():
    """从 config.yaml 读取 DeepSeek 配置，返回 (api_key, base_url)"""
    cfg_path = os.path.join(BASE, "config.yaml")
    if not os.path.exists(cfg_path):
        return None, None
    try:
        with open(cfg_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        ds_cfg = config.get("global", {}).get("deepseek", {})
        key = os.environ.get("DEEPSEEK_API_KEY", "") or ds_cfg.get("api_key", "")
        url = os.environ.get("DEEPSEEK_BASE_URL", "") or ds_cfg.get("base_url", "https://api.deepseek.com")
        return key or None, url
    except Exception:
        return None, None


def _call_deepseek(prompt, system_msg=None):
    """调用 DeepSeek API，返回文本或 None"""
    api_key, base_url = _load_deepseek_config()
    if not api_key:
        return None
    try:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_msg or "你是质检数据分析师，擅长从数据中提取关键信息并给出专业建议。"},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.3,
            "max_tokens": 800,
        }
        resp = requests.post(
            f"{base_url}/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        st.error(f"❌ AI 调用失败: {str(e)}")
        return None


@st.cache_data(ttl=300)  # 缓存 5 分钟避免重复调用
def _cached_ai_summary(cache_key, prompt, system_msg=None):
    """带缓存的 AI 调用包装（cache_key 用于使缓存失效）"""
    return _call_deepseek(prompt, system_msg)


def build_global_ai_data(all_data):
    """组装全局 AI 摘要所需的各队列最新指标数据"""
    items = []
    for q in QUEUES:
        df_raw = all_data.get(q["id"], pd.DataFrame())
        if df_raw.empty:
            continue
        lr = find_latest_nonzero(df_raw, q["metric_keys"])
        if lr is None:
            continue
        metrics_str = {}
        alerts = []
        for mk in q["metric_keys"]:
            if mk in df_raw.columns:
                v = find_latest_nonzero_per_key(df_raw, mk)
                if v is not None:
                    label = q["metric_labels"].get(mk, mk)
                    metrics_str[mk] = f"{label}: {fmt_pct(v)}"
                    is_ok, alert_txt = check_threshold(q, mk, v)
                    if not is_ok:
                        alerts.append(f"{label}{alert_txt}")
        items.append({
            "icon": q["icon"],
            "name": q["name"],
            "date": str(lr["date"]),
            "metrics": metrics_str,
            "metric_labels": q["metric_labels"],
            "alerts": alerts,
        })
    return items


def render_ai_summary_section(all_data):
    """渲染全局 AI 日报摘要区域"""
    # 检查是否有 DeepSeek 配置
    api_key, _ = _load_deepseek_config()

    st.markdown("""<div class="qc-section-title">🤖 AI 日报摘要</div>""", unsafe_allow_html=True)

    ai_col1, ai_col2 = st.columns([1, 5])
    with ai_col1:
        generate = st.button("🔄 生成分析", type="primary", use_container_width=True,
                             help="调用 AI 生成今日质检数据分析")

    with ai_col2:
        if not api_key:
            st.info('💡 **未配置 DeepSeek API Key** — 在 `config.yaml` 填入 `deepseek.api_key` 或设置环境变量 `DEEPSEEK_API_KEY` 即可启用 AI 分析')
            return

    if not generate and "_ai_generated" not in st.session_state:
        st.caption("点击「生成分析」按钮获取 AI 洞察 → 对标企微推送完整版（含整体情况 + 不达标详情 + 改善建议）")
        return

    if generate or "_ai_summary_html" not in st.session_state:
        with st.spinner("🤖 AI 正在分析数据..."):
            items = build_global_ai_data(all_data)
            if not items:
                st.warning("暂无数据可供分析")
                return

            # 组装 prompt（对标 daily_push.py 的提示词结构）
            data_text = ""
            alert_queues = []
            for item in items:
                data_text += f"\n{item['icon']} **{item['name']}**（{item['date']}）:\n"
                for mk, val in item["metrics"].items():
                    data_text += f"  - {val}\n"
                if item["alerts"]:
                    alert_queues.append(item["name"])
                    data_text += f"  ⚠️ 不达标: {', '.join(item['alerts'])}\n"

            alert_note = ""
            if alert_queues:
                alert_note = f"\n特别关注：以下队列未达标 - {', '.join(alert_queues)}"

            prompt = f"""你是质检数据分析师。根据以下各队列最新指标数据，生成一份简洁的日报摘要。

要求：
1. 用 3-5 句话概括整体情况
2. 重点标注不达标的队列和指标
3. 给出简短的改善建议（如有不达标项）
4. 语气专业简洁，不要啰嗦
5. 使用 Markdown 格式

今日数据：{data_text}
{alert_note}"""

            summary = _cached_ai_summary(
                f"global_{datetime.now().strftime('%Y%m%d%H')}",
                prompt,
            )

            if summary:
                st.session_state["_ai_summary_html"] = summary
                st.session_state["_ai_generated"] = True

    if "_ai_summary_html" in st.session_state:
        # 渲染为美观的卡片
        st.markdown(f'''<div class="qc-ai-result">
            <div class="qc-ai-header">
                <span class="qc-ai-icon">🤖</span>
                <span class="qc-ai-title">AI 分析结果</span>
                <span class="qc-ai-time">DeepSeek · {datetime.now().strftime('%H:%M')}</span>
            </div>
            <div class="qc-ai-body">
            {st.session_state["_ai_summary_html"].replace('\n', '<br>')}
            </div></div>''', unsafe_allow_html=True)


def render_queue_ai_insight(q, df):
    """渲染单队列 AI 洞察按钮及结果"""
    api_key, _ = _load_deepseek_config()
    if not api_key:
        return

    # 用 expander 包裹，默认折叠
    with st.expander("🔍 AI 队列洞察", expanded=False):
        c_btn, c_hint = st.columns([1, 3])
        with c_btn:
            do_analyze = st.button("分析此队列", key=f"_ai_q_{q['id']}", type="primary", use_container_width=True)
        with c_hint:
            st.caption("基于当前筛选范围的数据生成针对性分析")

        if do_analyze or f"_ai_q_{q['id']}_html" in st.session_state:
            if do_analyze:
                with st.spinner(f"🤖 正在分析 {q['name']} ..."):
                    # 组装该队列的数据摘要
                    latest = find_latest_nonzero(df, q["metric_keys"])

                    data_lines = [f"**队列**: {q['full_name']}"]
                    if latest is not None:
                        data_lines.append(f"**最新日期**: {latest['date']}")
                        data_lines.append(f"**数据天数**: {len(df)}")

                    metric_lines = []
                    alert_lines = []
                    for mk in q["metric_keys"]:
                        if mk not in df.columns:
                            continue
                        vv = get_valid_values(df, mk)
                        if not vv:
                            continue
                        label = q["metric_labels"].get(mk, mk)
                        lv = vv[-1]
                        av = sum(vv) / len(vv)
                        metric_lines.append(f"- **{label}**: 最新={fmt_pct(lv)}, 均值={fmt_pct1(av)}, 数据点={len(vv)}")

                        # 达标率
                        ok_cnt = sum(1 for v in vv if check_threshold(q, mk, v)[0])
                        pc = ok_cnt / len(vv) * 100
                        metric_lines.append(f"  - 达标率: {pc:.0f}% ({ok_cnt}/{len(vv)})")

                        is_ok, alert_txt = check_threshold(q, mk, lv)
                        if not is_ok:
                            alert_lines.append(f"  - ⚠️ {label}: {fmt_pct(lv)} {alert_txt}")

                    # 趋势信息
                    trend_lines = []
                    for mk in q["metric_keys"]:
                        if mk not in df.columns:
                            continue
                        vv = get_valid_values(df, mk)
                        if len(vv) >= 7:
                            recent = vv[-7:]
                            older = vv[-14:-7] if len(vv) >= 14 else vv[:-7]
                            if older:
                                r_avg = sum(recent) / len(recent)
                                o_avg = sum(older) / len(older)
                                label = q["metric_labels"].get(mk, mk)
                                chg = ((r_avg - o_avg) / abs(o_avg)) * 100 if o_avg != 0 else 0
                                direction = "上升 ↗️" if chg > 0.5 else ("下降 ↘️" if chg < -0.5 else "稳定 →")
                                trend_lines.append(f"- 近7天 vs 前7天 **{label}**: {direction} ({chg:+.1f}%)")

                    all_lines = data_lines + ["\n**指标详情**:"] + metric_lines
                    if alert_lines:
                        all_lines.append("\n**⚠️ 不达标项**:")
                        all_lines.extend(alert_lines)
                    if trend_lines:
                        all_lines.append("\n**趋势变化**:")
                        all_lines.extend(trend_lines)

                    prompt = f"""你是一位资深质检分析师。请对以下质检数据进行深入分析：

{''.join(all_lines)}

请用中文给出：
1. **数据概览**：1-2句话总结当前状态
2. **问题诊断**：如有不达标项，分析可能原因
3. **趋势判断**：根据近期走势预判后续风险
4. **行动建议**：2-3条具体可执行的建议
语气专业、简洁、有洞察力。使用 Markdown 格式。"""

                    result = _cached_ai_summary(
                        f"q_{q['id']}_{datetime.now().strftime('%Y%m%d%H')}",
                        prompt,
                        "你是一位资深质检分析师，擅长从质检数据中发现隐藏问题和趋势，并给出可执行的改善建议。",
                    )

                    if result:
                        st.session_state[f"_ai_q_{q['id']}_html"] = result

            if f"_ai_q_{q['id']}_html" in st.session_state:
                st.markdown(f'''<div style="background:#faf5ff;border:1px solid #e9d5ff;
                    border-radius:10px;padding:14px 16px;margin-top:8px;">
                    <div style="font-size:12px;color:#7c3aed;margin-bottom:6px;">🔮 AI 洞察 · {q['icon']} {q['name']}</div>
                    <div style="font-size:13px;line-height:1.7;color:#334155;">
                    {st.session_state[f'_ai_q_{q["id"]}_html'].replace(chr(10), '<br>')}
                    </div></div>''', unsafe_allow_html=True)

def render_dashboard(all_data):
    """质检数据看板主页 — 对标参考设计（纯原生组件，兼容所有 Streamlit 版本）"""

    min_date, max_date = get_date_range(all_data)
    total_records = sum(len(d) for d in all_data.values())

    # ── Header ──
    st.markdown(f"""
    <div style="display:flex;align-items:center;justify-content:space-between;
        flex-wrap:wrap;gap:12px;margin-bottom:2px;padding-bottom:12px;border-bottom:1px solid #e2e8f0;">
        <div>
            <div style="font-size:22px;font-weight:700;color:#1e293b;">📊 质检数据统一看板</div>
            <div style="font-size:12px;color:#64748b;margin-top:2px;">多队列 · 按日期聚合指标 · 数据来源：企业微信智能表格 · 共 <b>{total_records}</b> 条记录</div>
        </div>
        <div style="font-size:11px;color:#94a3b8;">{datetime.now().strftime('%Y-%m-%d %H:%M')}</div>
    </div>""", unsafe_allow_html=True)

    # ── 日期筛选行（对标HTML模板版：紧凑一行）──
    c_d1, c_d2, c_btns = st.columns([2, 2, 4])
    with c_d1:
        d_from = st.date_input("起始", value=None, key="df", label_visibility="collapsed")
    with c_d2:
        d_to = st.date_input("截止", value=None, key="dt", label_visibility="collapsed")
    with c_btns:
        bc1, bc2, bc3, bc4 = st.columns(4)
        if bc1.button("周", use_container_width=True, key="_bw"):
            if max_date: st.session_state["_quick"] = ("week", max_date); st.rerun()
        if bc2.button("月", use_container_width=True, key="_bm"):
            if max_date: st.session_state["_quick"] = ("month", max_date); st.rerun()
        if bc3.button("全部", use_container_width=True, key="_ba"):
            st.session_state["_quick"] = ("all", None); st.rerun()
        if bc4.button("清缓存", use_container_width=True, key="_bc"):
            st.cache_data.clear(); st.rerun()

    date_from_str = d_from.strftime("%Y-%m-%d") if d_from else None
    date_to_str = d_to.strftime("%Y-%m-%d") if d_to else None

    if "_quick" in st.session_state:
        mode, ref = st.session_state["_quick"]
        if mode == "week" and ref:
            dt = datetime.strptime(ref, "%Y-%m-%d")
            date_from_str, date_to_str = (dt - timedelta(days=6)).strftime("%Y-%m-%d"), ref
        elif mode == "month" and ref:
            dt = datetime.strptime(ref, "%Y-%m-%d")
            date_from_str, date_to_str = (dt - timedelta(days=29)).strftime("%Y-%m-%d"), ref
        else:
            date_from_str, date_to_str = None, None
        del st.session_state["_quick"]

    # ════════════════════════════════════════════════════════════
    #  Overview 卡片行（HTML 卡片 — 对标模板版的 overview-card 样式）
    # ════════════════════════════════════════════════════════════
    ov_cards_html = []
    for ci, q in enumerate(QUEUES):
        df_f = filter_by_date(all_data.get(q["id"], pd.DataFrame()), date_from_str, date_to_str)
        lr = find_latest_nonzero(df_f, q["metric_keys"])
        latest_date = lr["date"] if lr is not None else "--"
        pm = q.get("primary_metric", q["metric_keys"][0])
        main_val = find_latest_nonzero_per_key(df_f, pm)

        val_str = fmt_pct(main_val) if main_val is not None else "--"
        card_color = q.get("color", "#3b82f6")

        ov_cards_html.append(f'''
        <div class="qc-ov-card">
            <div class="qc-ov-icon">{q['icon']}</div>
            <div class="qc-ov-name">{q['name']}</div>
            <div class="qc-ov-val" style="color:{card_color};">{val_str}</div>
            <div class="qc-ov-date">{latest_date}</div>
        </div>''')

    st.markdown(f'''<div class="qc-ov-grid">
        {''.join(ov_cards_html)}</div>''', unsafe_allow_html=True)

    # ════════════════════════════════════════════════════════════
    #  🤖 AI 日报摘要（对标企微推送完整版）
    # ════════════════════════════════════════════════════════════
    render_ai_summary_section(all_data)

    # ════════════════════════════════════════════════════════════
    #  队列选择（胶囊 Tab 按钮 — 全部用 st.button + 统一 CSS 美化）
    # ════════════════════════════════════════════════════════════
    if "active_qidx" not in st.session_state:
        st.session_state.active_qidx = 0

    current_idx = st.session_state.active_qidx

    # 用一个容器包裹所有 Tab 按钮 + 自定义 class 标识
    st.markdown('<div class="qc-tab-row">', unsafe_allow_html=True)
    tab_col_row = st.columns(len(QUEUES))
    for i, q in enumerate(QUEUES):
        df_f = filter_by_date(all_data.get(q["id"], pd.DataFrame()), date_from_str, date_to_str)
        n_days = len(df_f)
        active = (i == current_idx)
        bgt = f"{n_days}天" if n_days > 0 else "待接入"
        q_color = q.get("color", "#3b82f6")

        with tab_col_row[i]:
            # 每个按钮带 data-qidx 属性供 CSS 区分激活态
            btn_label = f"{q['icon']} {q['name']}\n<span style='font-size:10px;opacity:0.7;'>{bgt}</span>"
            if st.button(f"{q['icon']} {q['name']} {bgt}",
                         key=f"_tab_{i}", use_container_width=True,
                         help=f"切换到 {q['name']}" + (f" (当前选中)" if active else "")):
                st.session_state.active_qidx = i
                st.rerun()

    st.markdown('</div>', unsafe_allow_html=True)

    # 胶囊 Tab CSS 已移至 custom.css 统一加载（见页面底部）

    # ════════════════════════════════════════════════════════════
    #  当前队列详情
    # ════════════════════════════════════════════════════════════
    q = QUEUES[st.session_state.active_qidx]
    qid = q["id"]
    df_raw = all_data.get(qid, pd.DataFrame())
    df = filter_by_date(df_raw, date_from_str, date_to_str)

    if df.empty:
        st.info(f"😴 **{q['name']}** 在选定日期范围内暂无数据")
        return

    # ── 统计区：左侧天数 + 右侧各指标 ──
    sc_left, sc_right = st.columns([1, 3])
    with sc_left:
        dr_s = f"{df['date'].iloc[0]} ~ {df['date'].iloc[-1]}" if len(df) > 0 else ""
        st.markdown(f"""
        <div class="qc-stats-card">
            <div class="qc-stats-label">数据天数</div>
            <div class="qc-stats-num">{len(df)}<span class="qc-stats-unit">天</span></div>
            <div class="qc-stats-daterange">{dr_s}</div>
        </div>""", unsafe_allow_html=True)

    with sc_right:
        mcols = st.columns(len(q["metric_keys"]))
        for ki, mk in enumerate(q["metric_keys"]):
            vv = get_valid_values(df, mk)
            if not vv:
                with mcols[ki]:
                    st.caption(f"{q['metric_labels'].get(mk, mk)}: 无数据")
                continue

            last_v = vv[-1]
            avg_v = sum(vv) / len(vv)
            is_ok, _, _ = check_threshold(q, mk, last_v)
            tl = get_threshold_label(q, mk)

            trend_html = ""
            if len(vv) >= 2:
                pv, cv = vv[-2], vv[-1]
                if pv != 0:
                    chg = ((cv - pv) / abs(pv)) * 100
                    arr = "↑" if chg > 0 else ("↓" if chg < 0 else "→")
                    tc = "#ef4444" if abs(chg) > 0.5 else ("#22c55e" if chg != 0 else "#94a3b8")
                    trend_html = f'<span style="color:{tc};font-weight:500;">{arr} {abs(chg):.1f}%</span>'

            lbl = q["metric_labels"].get(mk, mk)
            bg_c = "#fef2f2" if not is_ok else "#f0fdf4"
            bl_c = "#dc2626" if not is_ok else "#16a34a"
            vc = "#dc2626" if not is_ok else "#1e293b"

            with mcols[ki]:
                st.markdown(f'''<div class="qc-metric-card" style="border-left-color:{bl_c};background:{bg_c};">
                    <div class="qc-metric-card-label">
                    {lbl} <span class="qc-sep">|</span> {tl}</div>
                    <div class="qc-metric-card-val" style="color:{vc};">{fmt_pct(last_v)}</div>
                    <div class="qc-metric-card-sub">
                    均值 <b>{fmt_pct1(avg_v)}</b>&nbsp;&nbsp;{trend_html}</div></div>''', unsafe_allow_html=True)

    # ── 走势图标题 ──
    st.markdown(f"""<div class="qc-section-title">
        <span class="qc-dot" style="background:{q['color']};"></span>
        {q['icon']} {q['name']} — 指标走势</div>""", unsafe_allow_html=True)

    fig = go.Figure()
    ccolors = ["#3b82f6", "#22c55e", "#ef4444", "#f97316", "#eab308", "#a855f7", "#06b6d4"]

    for ki, mk in enumerate(q["metric_keys"]):
        if mk not in df.columns:
            continue
        lbl = q["metric_labels"].get(mk, mk)
        pdf = df[["date", mk]].copy()
        pdf.loc[pdf[mk] == 0, mk] = None
        clr = ccolors[ki % len(ccolors)]
        fig.add_trace(go.Scatter(
            x=pdf["date"].tolist(), y=pdf[mk].tolist(),
            name=lbl, line=dict(color=clr, width=2),
            mode="lines", connectgaps=True,
        ))

    # 底线参考线
    shapes, annots = [], []
    for ki, mk in enumerate(q["metric_keys"]):
        rule = q.get("thresholds", {}).get(mk, {})
        clr = ccolors[ki % len(ccolors)]
        if "min" in rule:
            shapes.append(dict(type="line", yref="y", y0=rule["min"], y1=rule["min"],
                xref="paper", x0=0, x1=1, line=dict(color=clr, width=1, dash="dot")))
            annots.append(dict(x=1, y=rule["min"], xref="paper", yref="y",
                text=f"底线≥{rule['min']*100:.0f}%", showarrow=False,
                font=dict(size=9, color=clr), xanchor="right", yanchor="bottom"))
        elif "max" in rule:
            shapes.append(dict(type="line", yref="y", y0=rule["max"], y1=rule["max"],
                xref="paper", x0=0, x1=1, line=dict(color=clr, width=1, dash="dot")))
            annots.append(dict(x=1, y=rule["max"], xref="paper", yref="y",
                text=f"上限≤{rule['max']*100:.0f}%", showarrow=False,
                font=dict(size=9, color=clr), xanchor="right", yanchor="top"))

    # Y轴自动缩放 ±15%
    all_nv = [v for mk in q["metric_keys"] for v in get_valid_values(df, mk)]
    if all_nv:
        y_lo, y_hi = min(all_nv), max(all_nv)
        sp = max(y_hi - y_lo, 0.05)
        y_min, y_max = max(0, y_lo - sp * 0.15), min(1.05, y_hi + sp * 0.15)
    else:
        y_min, y_max = 0, 1.05

    fig.update_layout(
        height=320, margin=dict(l=40, r=35, t=10, b=45),
        legend=dict(font_size=11, orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
        yaxis=dict(tickformat=".0%", range=[y_min, y_max]),
        hovermode="x unified", template="plotly_white",
        shapes=shapes, annotations=annots,
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── 🔍 单队列 AI 洞察（可折叠）──
    render_queue_ai_insight(q, df)

    # ── 最新指标构成 ──
    st.markdown("""<div class="qc-section-title-simple">✅ 最新指标构成</div>""", unsafe_allow_html=True)
    icols = st.columns(len(q["metric_keys"]))
    for ki, mk in enumerate(q["metric_keys"]):
        vv = get_valid_values(df, mk)
        if not vv:
            with icols[ki]:
                st.caption(f"{q['metric_labels'].get(mk, mk)}: 无数据")
            continue
        lv = vv[-1]
        ok, _, _ = check_threshold(q, mk, lv)
        lbl = q["metric_labels"].get(mk, mk)
        pc = sum(1 for v in vv if check_threshold(q, mk, v)[0]) / len(vv) * 100
        bc = "#16a34a" if ok else "#dc2626"
        em = "✅" if ok else "❌"

        with icols[ki]:
            st.markdown(f'''<div class="qc-indicator-card">
                <div class="qc-indicator-icon">{em}</div>
                <div class="qc-indicator-label">{lbl}</div>
                <div class="qc-indicator-val" style="color:{bc};">{fmt_pct(lv)}</div>
                <div class="qc-bar-track">
                <div class="qc-bar-fill" style="background:{bc};width:{pc:.0f}%;"></div></div>
                <div class="qc-indicator-rate">
                达标率 {pc:.0f}% ({sum(1 for v in vv if check_threshold(q,mk,v)[0])}/{len(vv)})</div></div>''', unsafe_allow_html=True)

    # ── 数据明细表 ──
    st.markdown(f"""<div class="qc-section-title-simple">
        📋 {q['name']} 数据明细 ({len(df)} 条)</div>""", unsafe_allow_html=True)

    disp_cols = ["date"] + q["metric_keys"]
    disp_df = df[disp_cols].copy().sort_values("date", ascending=False).reset_index(drop=True)
    for mk in q["metric_keys"]:
        if mk in disp_df.columns:
            disp_df[mk] = disp_df[mk].apply(fmt_pct)
    rmap = {"date": "📅 日期"}
    for mk in q["metric_keys"]:
        rmap[mk] = q["metric_labels"].get(mk, mk)
    disp_df.rename(columns=rmap, inplace=True)

    def _hl(s):
        r = [""] * len(s)
        rm = {v: k for k, v in rmap.items()}
        mk = rm.get(s.name, "")
        if not mk: return r
        for i, v in enumerate(s):
            if isinstance(v, str) and v.endswith("%"):
                try:
                    n = float(v.rstrip("%")) / 100
                    if n == 0: r[i] = "color:#94a3b8;"
                    elif not check_threshold(q, mk, n)[0]:
                        r[i] = "color:#dc2626;font-weight:700;background-color:#fef2f2;"
                except (ValueError, TypeError): pass
        return r

    st.dataframe(disp_df.style.apply(_hl, axis=0), use_container_width=True, hide_index=True,
                 height=min(max(40 * len(disp_df), 200), 500))

    # 导出 Excel
    buf = io.BytesIO()
    rdf = df[disp_cols].copy().sort_values("date", ascending=False).reset_index(drop=True)
    rr = {"date": "📅 日期"}
    for mk in q["metric_keys"]: rr[mk] = q["metric_labels"].get(mk, mk)
    rdf.rename(columns=rr, inplace=True)
    rdf.to_excel(buf, index=False, engine='xlsxwriter')
    buf.seek(0)
    st.download_button(label=f"📥 导出 {q['name']} 数据 (.xlsx)", data=buf,
                       file_name=f"{q['name']}_质检数据_{datetime.now().strftime('%Y%m%d')}.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       key=f"dl_{qid}")

    # Footer
    st.markdown(f'<div class="qc-footer">'
            f'📊 QC Dashboard v4.0 (AI) · {datetime.now().strftime("%Y-%m-%d %H:%M")}</div>', unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════
#  页面：数据导入（保持不变）
# ════════════════════════════════════════════════════════════════

def render_import():
    """数据导入页面"""

    st.markdown("# 📥 数据导入")
    st.caption("上传质检 Excel 文件、管理缓存和数据")

    # 上传
    st.markdown("### 📤 上传质检 Excel")
    uploaded_files = st.file_uploader("选择质检文件", type=["xlsx", "xls"],
                                      accept_multiple_files=True)
    if uploaded_files:
        st.success(f"已选择 **{len(uploaded_files)}** 个文件")
        if st.button("⬆️ 批量导入", type="primary", use_container_width=True):
            _process_uploads(uploaded_files)

    st.divider()

    # 一键刷新
    st.markdown("### 🔄 一键刷新")
    excel_files = [(f, os.path.join(UPLOAD_DIR, f)) for f in sorted(os.listdir(UPLOAD_DIR))
                   if os.path.isdir(UPLOAD_DIR) and f.lower().endswith(('.xlsx', '.xls'))]
    processed_count = len([f for f in os.listdir(os.path.join(UPLOAD_DIR, "processed"))
                           if f.lower().endswith(('.xlsx', '.xls'))]) if os.path.isdir(os.path.join(UPLOAD_DIR, "processed")) else 0

    ci, cb = st.columns([2, 1])
    with ci:
        if excel_files:
            st.info(f"📂 待处理 **{len(excel_files)}** 个文件（已处理 {processed_count} 个）")
            st.dataframe(pd.DataFrame([{"文件": f, "大小(KB)": round(os.path.getsize(p)/1024, 1)}
                                       for f, p in excel_files]), use_container_width=True, hide_index=True)
        else:
            st.warning("⏳ 暂无文件，请先上传")

    with cb:
        st.markdown("<br>", unsafe_allow_html=True)
        if not excel_files:
            st.button("🔄 执行刷新", disabled=True, type="primary", use_container_width=True)
        elif st.button("🔄 执行刷新", type="primary", use_container_width=True):
            _do_refresh()

    st.divider()

    # 已上传文件
    st.markdown("### 📂 已上传文件")
    ufiles = sorted(os.listdir(UPLOAD_DIR)) if os.path.isdir(UPLOAD_DIR) else []
    if ufiles:
        st.dataframe(pd.DataFrame([{"文件名": f, "大小(KB)": round(os.stat(os.path.join(UPLOAD_DIR,f)).st_size/1024, 1),
                                     "时间": datetime.fromtimestamp(os.stat(os.path.join(UPLOAD_DIR,f)).st_mtime).strftime("%Y-%m-%d %H:%M")} for f in ufiles]),
                      use_container_width=True, hide_index=True)
        if st.button("🗑️ 清空上传记录"):
            import shutil
            shutil.rmtree(UPLOAD_DIR); os.makedirs(UPLOAD_DIR, exist_ok=True)
            st.success("已清空"); st.rerun()

    st.divider()

    cc, cd = st.columns(2)
    with cc:
        st.markdown("### 🧹 清除缓存")
        if st.button("清除缓存", use_container_width=True):
            st.cache_data.clear(); st.success("✅"); time.sleep(1); st.rerun()

    with cd:
        st.markdown("### ⚠️ 清除数据")
        with st.expander("📅 按日期范围清除"):
            dd1 = st.date_input("起始", key="del_from")
            dd2 = st.date_input("截止", key="del_to")
            if dd1 and dd2:
                fs, ts = dd1.strftime("%Y-%m-%d"), dd2.strftime("%Y-%m-%d")
                cnt = sqlite3.connect(DB_PATH).execute(
                    "SELECT COUNT(*) FROM daily_metrics WHERE date BETWEEN ? AND ?", (fs, ts)).fetchone()[0]
                if cnt > 0:
                    st.warning(f"将删除 **{cnt}** 条（{fs} ~ {ts}）")
                    if st.button("🗑️ 删除选中范围", type="primary"):
                        conn = sqlite3.connect(DB_PATH)
                        conn.execute("DELETE FROM daily_metrics WHERE date BETWEEN ? AND ?", (fs, ts))
                        conn.commit(); conn.close(); st.cache_data.clear()
                        st.success(f"✅ 删除 {cnt} 条"); time.sleep(1); st.rerun()
                else:
                    st.info(f"该范围无数据（{fs} ~ {ts}）")

        st.markdown("**全部清除**")
        conf = st.text_input("输入 CONFIRM 确认全量删除", key="_conf_del")
        if conf and conf.strip().upper() == "CONFIRM":
            if st.button("确认清除全部数据", type="primary"):
                conn = sqlite3.connect(DB_PATH)
                c = conn.execute("DELETE FROM daily_metrics"); conn.commit()
                st.success(f"已删除 {c.rowcount} 条"); conn.close(); st.cache_data.clear(); time.sleep(1); st.rerun()


def _process_uploads(uploaded_files):
    pb = st.progress(0); status = st.empty(); res = []; ok = err = 0
    for i, uf in enumerate(uploaded_files):
        try:
            status.text(f"[{i+1}/{len(uploaded_files)}] 处理: **{uf.name}** ...")
            pb.progress((i+0.5)/len(uploaded_files))
            sp = os.path.join(UPLOAD_DIR, f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{uf.name}")
            with open(sp, "wb") as f: f.write(uf.getvalue())
            xl = pd.ExcelFile(io.BytesIO(uf.getvalue()))
            sn = xl.sheet_names[0]
            df = pd.read_excel(io.BytesIO(uf.getvalue()), sheet_name=sn)
            res.append({"文件": uf.name, "状态": "✅", "行数": len(df), "子表": sn}); ok += 1
        except Exception as e:
            res.append({"文件": uf.name, "状态": f"❌ {str(e)[:40]}", "行数": 0, "子表": "-"}); err += 1
    pb.progress(1.0)
    st.subheader("结果")
    st.columns(2)[0].metric("成功", ok); st.columns(2)[1].metric("失败", err)
    if res: st.dataframe(pd.DataFrame(res), use_container_width=True)


def _do_refresh():
    import yaml
    from pathlib import Path
    status = st.empty(); progress = st.progress(0); logs = []
    def log(msg): logs.append(msg); status.markdown("\n".join(f"· {l}" for l in logs[-12:]))
    try:
        progress.progress(5); log("📋 加载配置...")
        cfg_path = os.path.join(BASE, "config.yaml")
        if not os.path.exists(cfg_path): st.error("❌ config.yaml 不存在"); return
        with open(cfg_path) as f: config = yaml.safe_load(f)

        progress.progress(15); log("📂 扫描 uploads...")
        sys_path = os.path.join(BASE, "src")
        if sys_path not in __import__("sys").path: __import__("sys").path.insert(0, sys_path)
        import importlib.util
        spec = importlib.util.spec_from_file_location("collector", os.path.join(sys_path, "collector.py"))
        if spec and spec.loader:
            mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
            conn = mod.init_db(); imported = mod.import_excel(conn, config)
            log(f"✅ 导入 **{imported}** 条")
        else:
            log("⚠️ 简单模式"); _simple_import(progress, log); imported = "?"

        progress.progress(60); log("🧹 清理未来日期...")
        from datetime import date as _date
        today_s = _date.today().isoformat()
        cn = sqlite3.connect(DB_PATH); c = cn.cursor()
        c.execute("DELETE FROM daily_metrics WHERE date > ?", (today_s,))
        fd = c.rowcount; cn.commit(); cn.close()
        if fd: log(f"🧹 清理 {fd} 条未来日期")
        else: log("🧹 无需清理")

        progress.progress(80); st.cache_data.clear(); log("🔄 缓存已清除")
        progress.progress(100)
        st.success(f"🎉 完成！新增 {imported} 条 · 未来清理 {fd} 条")
        time.sleep(2)
    except Exception as e:
        st.error(f"❌ 失败: {str(e)}")


def _simple_import(progress, log):
    import glob
    files = list(glob.glob(os.path.join(UPLOAD_DIR, "*.xlsx"))) + list(glob.glob(os.path.join(UPLOAD_DIR, "*.xls")))
    proc_dir = os.path.join(UPLOAD_DIR, "processed"); os.makedirs(proc_dir, exist_ok=True)
    total = 0
    for fi, fp in enumerate(files):
        fn = os.path.basename(fp)
        progress.progress(0.2 + 0.4*fi/max(len(files),1))
        log(f"📄 [{fi+1}/{len(files)}] {fn}")
        try:
            xl = pd.ExcelFile(fp)
            for sn in xl.sheet_names:
                df = pd.read_excel(xl, sheet_name=sn, header=None)
                log(f"   📊 「{sn}」: {df.shape}")
            import shutil
            shutil.move(fp, os.path.join(proc_dir, f"{datetime.now().strftime('%Y%m%d%H%M%S')}_{fn}"))
            total += len(df) - 2
        except Exception as e:
            log(f"   ❌ {str(e)[:60]}")
    log(f"✅ 处理完成 {total} 行")


# ════════════════════════════════════════════════════════════════
#  主程序入口
# ════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="QC 质检数据看板", page_icon="📊", layout="wide",
    initial_sidebar_state="collapsed",
    menu_items={"About": "📊 QC Dashboard v4.0 (AI)", "Report a bug": None, "Get Help": None},
)

# 全局 CSS — 通过 static 目录加载（避免 <style> 标签和 data URI 在旧版 Streamlit 上暴露为文本）
st.markdown('<link rel="stylesheet" href="static/custom.css">', unsafe_allow_html=True)

# ── 顶部导航条（替代侧边栏）──
nav_col1, nav_col2, nav_spacer = st.columns([1, 1, 5])
with nav_col1:
    if st.button("📊 数据总览", use_container_width=True, type="primary",
                 disabled=(st.session_state.get("_page", "dash") == "dash")):
        st.session_state._page = "dash"; st.rerun()
with nav_col2:
    if st.button("📥 数据导入", use_container_width=True,
                 disabled=(st.session_state.get("_page", "dash") == "import")):
        st.session_state._page = "import"; st.rerun()

if st.session_state.get("_page", "dash") == "import":
    render_import()
else:
    with st.spinner("加载中..."):
        all_data = load_all_queue_data()
    render_dashboard(all_data)
