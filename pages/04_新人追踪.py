"""新人追踪页：批次上传 + 批次管理 + 成员列表 + 个人追踪（近80条错误明细）。

数据来源：
- 新人名单：vw_qa_base WHERE workforce_type = '新人'（或 queue_name 含 10816）
- 个人指标：从 vw_qa_base 聚合计算
- 错误明细：is_final_correct = 0 或 is_raw_correct = 0 的原始记录
- 上传名单：本地 data/newcomers.json 持久化（支持 xlsx 上传匹配）
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from services.dashboard_service import DashboardService
from storage.repository import DashboardRepository

st.set_page_config(page_title="新人追踪-质培运营看板", page_icon="👤", layout="wide")

# CSS
import os as _os
_css_path = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "custom.css")
if _os.path.exists(_css_path):
    with open(_css_path, "r", encoding="utf-8") as _f:
        st.markdown(f'<style>{_f.read()}</style>', unsafe_allow_html=True)

service = DashboardService()
repo = DashboardRepository()

# ==================== 新人名单本地存储路径 ====================
_NEWCOMER_FILE = Path(__file__).resolve().parents[1] / "data" / "newcomers.json"


def load_stored_newcomers() -> list[dict]:
    """加载已存储的新人名单"""
    if _NEWCOMER_FILE.exists():
        try:
            return json.loads(_NEWCOMER_FILE.read_text(encoding="utf-8"))
        except Exception:
            return []
    return []


def save_newcomers(records: list[dict]) -> None:
    """保存新人名单到本地 JSON"""
    _NEWCOMER_FILE.parent.mkdir(parents=True, exist_ok=True)
    _NEWCOMER_FILE.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8")


# ==================== 新人批次上传区域 ====================
def render_batch_upload():
    """渲染新人批次上传区域"""
    st.markdown("#### 📤 新人批次上传")
    st.caption("上传包含「人员名称」+「批次」列的 Excel 文件，系统自动识别并纳入新人追踪")

    uploaded = st.file_uploader(
        "选择新人名单文件（.xlsx / .xls）",
        type=["xlsx", "xls"],
        key="newcomer_uploader",
        help="需包含「人员名称」和「批次」两列，支持多个批次混合",
    )

    if not uploaded:
        # 显示已有数据概况
        existing = load_stored_newcomers()
        if existing:
            batches = {}
            for r in existing:
                b = r.get("batch_raw", "未知")
                batches[b] = batches.get(b, 0) + 1
            b_summary = ", ".join([f"{k}({v}人)" for k, v in sorted(batches.items())])
            st.info(f"📋 已有 **{len(existing)}** 人名单 — {b_summary}")
        return

    try:
        df_raw = pd.read_excel(uploaded)
    except Exception as e:
        st.error(f"❌ 文件读取失败：{e}")
        return

    # 列名自动识别（兼容多种命名）
    col_map = {}
    for c in df_raw.columns:
        cs = str(c).strip()
        if "姓名" in cs or "人员" in cs or "名称" in cs or cs == "reviewer_name":
            col_map["name"] = c
        elif "批次" in cs or "batch" in cs.lower() or cs == "batch":
            col_map["batch"] = c

    if "name" not in col_map or "batch" not in col_map:
        st.markdown(f"""
        <div class="alert-box warning">
        ⚠️ 未识别到标准列名。检测到的列：<code>{', '.join(df_raw.columns.tolist())}</code><br>
        请确保文件包含「人员名称」（或 姓名/ reviewer_name）和「批次」（或 batch）两列。
        </div>
        """, unsafe_allow_html=True)
        with st.expander("📄 原始数据预览", expanded=False):
            st.dataframe(df_raw.head(10), use_container_width=True)
        return

    # 构建标准化记录
    records = []
    for _, row in df_raw.iterrows():
        name = str(row[col_map["name"]]).strip()
        batch_raw = str(row[col_map["batch"])].strip()
        if not name or name.lower() == "nan":
            continue
        records.append({
            "reviewer_name": name,
            "batch_raw": batch_raw,
            "workforce_type": "新人",
            "is_practice_sample": 0,
            "uploaded_at": date.today().isoformat(),
        })

    if not records:
        st.error("❌ 文件中没有有效的数据行")
        return

    # 预览
    prev_df = pd.DataFrame(records)
    batch_counts = prev_df["batch_raw"].value_counts()

    col_prev, col_btn = st.columns([3, 1])
    with col_prev:
        st.markdown(f"""<div class="alert-box info">📋 识别到 **{len(records)}** 人，
            **{len(batch_counts)}** 个批次</div>""", unsafe_allow_html=True)
        st.dataframe(
            prev_df[["reviewer_name", "batch_raw", "workforce_type"]],
            use_container_width=True, hide_index=True,
            height=min(200, 28 * len(prev_df) + 40),
        )

    with col_btn:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("✅ 确认导入", key="btn_import_newcomers", type="primary", use_container_width=True):
            # 合并去重（基于 reviewer_name + batch）
            existing = load_stored_newcomers()
            existing_keys = {(r["reviewer_name"], r.get("batch_raw", "")) for r in existing}
            new_records = [r for r in records if (r["reviewer_name"], r["batch_raw"]) not in existing_keys]
            dup_count = len(records) - len(new_records)

            all_records = existing + new_records
            save_newcomers(all_records)

            if new_records:
                st.success(f"✅ 导入成功！新增 **{len(new_records)}** 人{f'，跳过重复 {dup_count} 人' if dup_count else ''}")
                st.rerun()
            else:
                st.warning(f"⚠️ 全部 {len(records)} 人均为重复，未新增。当前共 {len(existing)} 人。")

        # 清空操作
        if st.button("🗑️ 清空名单", key="btn_clear_newcomers", use_container_width=True):
            if _NEWCOMER_FILE.exists():
                _NEWCOMER_FILE.unlink()
            st.success("🗑️ 已清空新人名单")
            st.rerun()

# ==================== 共享工具函数（不依赖首页） ====================

@st.cache_data(show_spinner=False, ttl=300)
def _get_data_date_range() -> tuple[date, date]:
    """获取数据库中的日期范围（独立实现，不依赖首页模块）"""
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


def _normalize_anchor_date(grain: str, sel_date: date) -> date:
    """将选定日期规范化为对应粒度的锚点日期（简化版，不依赖 service 层）"""
    # 对于 day 粒度直接返回选中日期；week/month 可根据需要扩展
    return sel_date


# ==================== 缓存数据加载函数 ====================

@st.cache_data(show_spinner=False, ttl=300)
def get_newcomer_batches(grain: str, anchor_date: date) -> pd.DataFrame:
    """获取新人批次列表（按队列分组，筛选 workforce_type='新人'）"""
    grain_column = repo._grain_column(grain)
    sql = f"""
    SELECT DISTINCT
        CASE
            WHEN queue_name LIKE '%10816%' THEN '0408批·白龙湖'
            WHEN queue_name LIKE '%18365%' THEN '复培'
            ELSE SUBSTRING(queue_name FROM '[0-9]{4}') || '批'
        END AS batch_name,
        queue_name,
        reviewer_name,
        COUNT(*) AS qa_cnt,
        SUM(CASE WHEN COALESCE(is_final_correct, 0) = 1 THEN 1 ELSE 0 END) AS final_correct_cnt,
        ROUND(SUM(CASE WHEN COALESCE(is_final_correct, 0) = 1 THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0), 2) AS final_accuracy_rate,
        MIN(biz_date) AS first_date,
        MAX(biz_date) AS last_date
    FROM vw_qa_base
    WHERE {grain_column} = %s
      AND (queue_name LIKE '%%10816%%' OR workforce_type = '新人')
      AND reviewer_name IS NOT NULL AND reviewer_name != ''
    GROUP BY queue_name, reviewer_name
    ORDER BY first_date ASC, qa_cnt DESC
    """
    return repo.fetch_df(sql, [anchor_date])


@st.cache_data(show_spinner=False, ttl=300)
def get_newcomer_person_detail(
    grain: str, anchor_date: date, reviewer_name: str, batch_queue: str | None = None,
) -> dict:
    """获取单个新人的详细指标 + 近80条错误明细

    汇总指标：受 grain + anchor_date 限制（看特定时间窗口表现）
    错误明细：不受 grain 日期限制，查该审核人全部历史错误（新人数据量少、分布散）
    """
    grain_column = repo._grain_column(grain)
    summary_conditions = [f"{grain_column} = %s", "reviewer_name = %s"]
    summary_params: list = [anchor_date, reviewer_name]

    if batch_queue:
        summary_conditions.append("queue_name = %s")
        summary_params.append(batch_queue)

    summary_where_sql = " AND ".join(summary_conditions)

    # ---- 汇总指标（受 grain 日期限制）----
    summary_sql = f"""
    SELECT
        reviewer_name,
        queue_name,
        COUNT(*) AS total_qa,
        SUM(CASE WHEN COALESCE(is_raw_correct, 0) = 1 THEN 1 ELSE 0 END) AS raw_correct_cnt,
        SUM(CASE WHEN COALESCE(is_final_correct, 0) = 1 THEN 1 ELSE 0 END) AS final_correct_cnt,
        SUM(CASE WHEN COALESCE(is_misjudge, 0) = 1 THEN 1 ELSE 0 END) AS misjudge_cnt,
        SUM(CASE WHEN COALESCE(is_missjudge, 0) = 1 THEN 1 ELSE 0 END) AS missjudge_cnt,
        ROUND(SUM(CASE WHEN COALESCE(is_raw_correct, 0) = 1 THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0), 2) AS raw_accuracy_rate,
        ROUND(SUM(CASE WHEN COALESCE(is_final_correct, 0) = 1 THEN 1 ELSE 0 END) * 100.0
            / NULLIF(COUNT(*), 0), 2) AS final_accuracy_rate,
        MIN(biz_date) AS first_qa_date,
        MAX(biz_date) AS last_qa_date
    FROM vw_qa_base
    WHERE {summary_where_sql}
    GROUP BY reviewer_name, queue_name
    """
    summary_df = repo.fetch_df(summary_sql, summary_params)

    # ---- 近80条错误明细（不受 grain 日期限制，查全部历史）----
    error_conditions = ["reviewer_name = %s"]
    error_params: list = [reviewer_name]
    if batch_queue:
        error_conditions.append("queue_name = %s")
        error_params.append(batch_queue)
    # 新人限定条件
    error_conditions.append("(queue_name LIKE '%%10816%%' OR workforce_type = '新人')")
    error_where_sql = " AND ".join(error_conditions)

    error_sql = f"""
    SELECT
        biz_date,
        queue_name,
        raw_judgement,
        final_review_result,
        COALESCE(error_type, '') AS error_type,
        COALESCE(final_label, '') AS final_label,
        CASE WHEN COALESCE(is_raw_correct, 0) = 0 THEN '原始错误' END AS error_category,
        CASE WHEN COALESCE(is_misjudge, 0) = 1 THEN '错判'
             WHEN COALESCE(is_missjudge, 0) = 1 THEN '漏判'
             WHEN COALESCE(is_raw_correct, 0) = 0 AND COALESCE(is_misjudge, 0) = 0 AND COALESCE(is_missjudge, 0) = 0 THEN '原始错误'
        END AS judge_error_type,
        comment_text
    FROM vw_qa_base
    WHERE {error_where_sql}
      AND (COALESCE(is_final_correct, 0) = 0 OR COALESCE(is_misjudge, 0) = 1 OR COALESCE(is_missjudge, 0) = 1)
    ORDER BY biz_date DESC
    LIMIT 80
    """
    error_df = repo.fetch_df(error_sql, error_params)

    return {"summary": summary_df, "error_detail": error_df}


# ==================== 页面主体 ====================
st.markdown("# 👤 新人追踪")
st.caption("新人质检表现监控 · 个人下探到错误样本")

# ── 新人批次上传区域（置顶）──
render_batch_upload()

# 日期选择
_grain_options = {"日监控": "day", "周复盘": "week", "月管理": "month"}
_grain_labels = list(_grain_options.keys())
_selected_grain_label = st.radio("看板模式", options=_grain_labels, horizontal=True, label_visibility="collapsed")
_grain = _grain_options[_selected_grain_label]

_data_min, _data_max = _get_data_date_range()
_default = _data_max if _data_max <= date.today() else date.today()
selected_date = st.date_input("业务日期", value=_default, label_visibility="collapsed")

# ==================== 第一区域：批次列表 + 成员列表 ====================
st.markdown("---")
st.markdown("#### 📋 新人批次概览")

batches_df = get_newcomer_batches(_grain, _normalize_anchor_date(_grain, selected_date))

if batches_df.empty:
    st.warning("暂无新人数据。请确认：\n1. fact_qa_event 中有包含 10816 队列或 workforce_type='新人' 的记录\n2. 已运行 ETL 刷新 mart 表")
    st.info("💡 **提示**：如需导入新人数据，请在「数据导入」页面上传包含 10816 队列的质检文件。")
    st.stop()

# 按批次聚合显示
batch_names = sorted(batches_df["batch_name"].unique().tolist())
batch_col, member_col = st.columns([1, 2])

with batch_col:
    st.markdown("**批次列表**")
    for bn in batch_names:
        batch_members = batches_df[batches_df["batch_name"] == bn]
        batch_total_qa = int(batch_members["qa_cnt"].sum())
        batch_people = len(batch_members)
        with st.expander(f"📦 **{bn}** ({batch_people}人 · {batch_total_qa:,}条)", expanded=(bn == batch_names[0])):
            for _, row in batch_members.iterrows():
                acc = row.get("final_accuracy_rate", 0) or 0
                acc_tag = ":green:" if acc >= 99 else (":orange:" if acc >= 97 else ":red:")
                st.markdown(f"- {row['reviewer_name']} `{int(row['qa_cnt']):,}`条 正确率{acc:.1f}% {acc_tag}")

with member_col:
    st.markdown("**成员快捷选择**")
    # 合并选项文本：批次名 + 姓名
    member_options = [
        f"{row['batch_name']} · {row['reviewer_name']}"
        for _, row in batches_df.iterrows()
    ]
    selected_member_key = st.selectbox(
        "选择新人",
        options=member_options,
        format_func=lambda x: x.split(" · ", 1)[-1] if " · " in x else x,
        label_visibility="collapsed",
    )

# 解析选择
if " · " in selected_member_key:
    sel_batch, sel_name = selected_member_key.split(" · ", 1)
else:
    sel_batch, sel_name = "", selected_member_key

# 获取该新人所在队列
_sel_row = batches_df[
    (batches_df["reviewer_name"] == sel_name) & (batches_df["batch_name"] == sel_batch)
]
sel_queue = _sel_row["queue_name"].iloc[0] if not _sel_row.empty else None

# ==================== 第二区域：个人追踪 ====================
st.markdown("---")
st.markdown("### 👤 个人追踪")
st.caption("个人追踪页只展示单人样本正确率和错误明细，不展示聚合层的人均正确率。")

# 加载个人详情
person_data = get_newcomer_person_detail(_grain, _normalize_anchor_date(_grain, selected_date), sel_name, sel_queue)

person_summary = person_data["summary"]
person_errors = person_data["error_detail"]

if not person_summary.empty:
    row = person_summary.iloc[0]
    total_qa = int(row.get("total_qa", 0) or 0)

    # 即使 total_qa=0 也展示信息卡片（帮助排查为什么没数据）
    first_dt = row.get("first_qa_date")
    days_on_board = (selected_date - first_dt).days if pd.notna(first_dt) and hasattr(first_dt, 'day') else 15

    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #F8FAFC 0%, #F1F5F9 100%); padding: 1.25rem; border-radius: 12px; border: 1px solid #E2E8F0; margin-bottom: 1rem;">
        <div style="font-size: 1.4rem; font-weight: 700; color: #1E293B; margin-bottom: 0.5rem;">{sel_name}</div>
        <div style="font-size: 0.9rem; color: #64748B; margin-bottom: 0.25rem;">
            {sel_queue or sel_batch or '未知队列'} · 入职约 {max(days_on_board, 0)} 天
        </div>
        <div style="font-size: 0.85rem; color: #94A3B8;">
            联营管理：待填充 · 交付PM：未填写 · 质培owner：天有 · 导师/质检：宋效愚
        </div>
    </div>
    """, unsafe_allow_html=True)

    raw_acc = row.get("raw_accuracy_rate", 0) or 0
    final_acc = row.get("final_accuracy_rate", 0) or 0
    formal_acc = final_acc

    kpi_c1, kpi_c2, kpi_c3, kpi_c4 = st.columns(4)
    with kpi_c1:
        st.metric("📥 内部正确率", f"{raw_acc:.1f}%")
    with kpi_c2:
        st.metric("🔍 外部正确率", f"{final_acc:.1f}%")
    with kpi_c3:
        st.metric(":green: 正式正确率", f"{formal_acc:.1f}%")
    with kpi_c4:
        st.metric(":package: 累计质检量", f"{total_qa:,}")

    # 如果 total_qa=0 给出明确诊断
    if total_qa == 0:
        st.warning(f"""
        ⚠️ **该审核人在当前筛选条件下无质检记录**，可能原因：
        1. **日期不匹配**：选定日期 `{selected_date}` 可能没有此人的质检数据
        2. **粒度不匹配**：当前模式「{_grain}」可能不覆盖此人的数据周期
        3. **数据未导入**：此人的质检文件可能尚未导入系统

        💡 建议：
        - 切换到「周复盘」或「月管理」模式试试
        - 选择更早的日期范围
        - 在「数据导入」页确认已上传包含此人的质检文件
        """)

    # 最近80条错误明细
    st.markdown("---")
    st.markdown("#### :clipboard: 最近 80 条错误明细")

    if not person_errors.empty:
        # 格式化显示
        disp_err = person_errors.copy()
        # 截断长文本
        if "comment_text" in disp_err.columns:
            disp_err["comment_text"] = disp_err["comment_text"].apply(
                lambda x: str(x)[:60] + "..." if pd.notna(x) and len(str(x)) > 60 else (str(x) if pd.notna(x) else "")
            )
        if "error_type" in disp_err.columns:
            disp_err["error_type"] = disp_err["error_type"].apply(lambda x: str(x)[:20] if x else "")
        if "final_label" in disp_err.columns:
            disp_err["final_label"] = disp_err["final_label"].apply(lambda x: str(x)[:15] if x else "")

        # 列名映射
        col_rename = {
            "biz_date": "日期",
            "queue_name": "队列",
            "raw_judgement": "原始判定",
            "final_review_result": "终审结果",
            "error_type": "错误类型",
            "final_label": "终审标签",
            "judge_error_type": "错误分类",
            "comment_text": "评论内容",
        }
        disp_err = disp_err.rename(columns=col_rename)

        st.dataframe(
            disp_err,
            use_container_width=True,
            hide_index=True,
            height=min(max(200, 28 * len(disp_err) + 40), 500),
        )
        st.caption(f"共 **{len(person_errors)}** 条错误记录（最多显示80条）")
    else:
        st.info(":tada: 该新人暂无错误记录。")
else:
    st.warning(f"未找到「{sel_name}」的质检数据，可能该审核人在选定时间范围内没有记录。")

# 底部
st.markdown("---")
st.caption(f':footnote: 新人追踪页 · 数据截至 {selected_date} · Grain={_grain}')
