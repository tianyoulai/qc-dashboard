"""新人追踪页：批次管理 + 成员列表 + 个人追踪（近80条错误明细）。

数据来源：
- 新人名单：vw_qa_base WHERE workforce_type = '新人'（或 queue_name 含 10816）
- 个人指标：从 vw_qa_base 聚合计算
- 错误明细：is_final_correct = 0 或 is_raw_correct = 0 的原始记录
"""
from __future__ import annotations

from datetime import date, timedelta

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
    """获取单个新人的详细指标 + 近80条错误明细"""
    grain_column = repo._grain_column(grain)
    conditions = [f"{grain_column} = %s", "reviewer_name = %s"]
    params: list = [anchor_date, reviewer_name]

    if batch_queue:
        conditions.append("queue_name = %s")
        params.append(batch_queue)

    where_sql = " AND ".join(conditions)

    # ---- 汇总指标 ----
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
    WHERE {where_sql}
    GROUP BY reviewer_name, queue_name
    """
    summary_df = repo.fetch_df(summary_sql, params)

    # ---- 近80条错误明细 ----
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
    WHERE {where_sql}
      AND (COALESCE(is_final_correct, 0) = 0 OR COALESCE(is_misjudge, 0) = 1 OR COALESCE(is_missjudge, 0) = 1)
    ORDER BY biz_date DESC
    LIMIT 80
    """
    error_df = repo.fetch_df(error_sql, params)

    return {"summary": summary_df, "error_detail": error_df}


# ==================== 页面主体 ====================
st.markdown("# 👤 新人追踪")
st.caption("新人质检表现监控 · 个人下探到错误样本")

# 日期选择
_grain_options = {"日监控": "day", "周复盘": "week", "月管理": "month"}
_grain_labels = list(_grain_options.keys())
_selected_grain_label = st.radio("看板模式", options=_grain_labels, horizontal=True, label_visibility="collapsed")
_grain = _grain_options[_selected_grain_label]

_data_min, _data_max = get_data_date_range() if 'get_data_date_range' in dir() else (date.today(), date.today())
_default = _data_max if _data_max <= date.today() else date.today()
selected_date = st.date_input("业务日期", value=_default, label_visibility="collapsed")

# ==================== 第一区域：批次列表 + 成员列表 ====================
st.markdown("---")
st.markdown("#### 📋 新人批次概览")

batches_df = get_newcomer_batches(_grain, service.normalize_anchor_date(_grain, selected_date))

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
person_data = get_newcomer_person_detail(_grain, service.normalize_anchor_date(_grain, selected_date), sel_name, sel_queue)

person_summary = person_data["summary"]
person_errors = person_data["error_detail"]

if not person_summary.empty:
    row = person_summary.iloc[0]
    # 计算入职天数
    first_dt = row.get("first_qa_date")
    days_on_board = (selected_date - first_dt).days if pd.notna(first_dt) and hasattr(first_dt, 'day') else 15

    # 信息卡片
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #F8FAFC 0%, #F1F5F9 100%); padding: 1.25rem; border-radius: 12px; border: 1px solid #E2E8F0; margin-bottom: 1rem;">
        <div style="font-size: 1.4rem; font-weight: 700; color: #1E293B; margin-bottom: 0.5rem;">{sel_name}</div>
        <div style="font-size: 0.9rem; color: #64748B; margin-bottom: 0.25rem;">
            {sel_batch or '未知批次'} · 入职 {days_on_board} 天
        </div>
        <div style="font-size: 0.85rem; color: #94A3B8;">
            联营管理：待填充 · 交付PM：未填写 · 质培owner：天有 · 导师/质检：宋效愚
        </div>
    </div>
    """, unsafe_allow_html=True)

    # 四个核心指标
    raw_acc = row.get("raw_accuracy_rate", 0) or 0
    final_acc = row.get("final_accuracy_rate", 0) or 0
    formal_acc = final_acc  # 暂时等同最终正确率
    total_qa = int(row.get("total_qa", 0) or 0)

    kpi_c1, kpi_c2, kpi_c3, kpi_c4 = st.columns(4)
    with kpi_c1:
        st.metric("📥 内部正确率", f"{raw_acc:.1f}%")
    with kpi_c2:
        st.metric("🔍 外部正确率", f"{final_acc:.1f}%")
    with kpi_c3:
        st.metric(":green: 正式正确率", f"{formal_acc:.1f}%")
    with kpi_c4:
        st.metric(":package: 累计质检量", f"{total_qa:,}")

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
