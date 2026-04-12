"""明细查询页：灵活筛选 + 下钻 + CSV 导出。"""
from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import streamlit as st

from services.dashboard_service import DashboardService
from storage.repository import DashboardRepository

st.set_page_config(page_title="质培运营看板-明细查询", page_icon="🔍", layout="wide")

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
    .stDownloadButton > button {
        background: #2e7d32;
        color: white;
        border-radius: 0.5rem;
        padding: 0.5rem 1rem;
        font-weight: 600;
        transition: all 0.2s ease;
    }
    .stDownloadButton > button:hover {
        background: #1b5e20;
        transform: translateY(-1px);
        box-shadow: 0 2px 8px rgba(46, 125, 50, 0.3);
    }
    .block-container { padding-top: 2rem; padding-bottom: 2rem; }
    h1 { margin-bottom: 0.5rem; }
</style>
""", unsafe_allow_html=True)

repo = DashboardRepository()
service = DashboardService()

# Hero 区域
st.markdown("""
<div style="margin-bottom: 1.5rem; padding: 1.5rem; background: #ffffff; border-radius: 1rem; border-left: 4px solid #2e7d32; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
        <h1 style="margin: 0; font-size: 2rem; font-weight: 700; color: #1a1a1a;">🔍 明细查询</h1>
    </div>
    <div style="font-size: 0.9rem; color: #4b5563; line-height: 1.6;">
        多维度筛选 · 问题下钻 · 数据导出
    </div>
</div>
""", unsafe_allow_html=True)


@st.cache_data(show_spinner=False, ttl=600)
def get_filter_options() -> dict:
    """获取筛选选项（缓存 10 分钟）。
    
    优化：从 fact_qa_event 直表查询 DISTINCT，避免扫描 vw_qa_base 视图（122 万行）。
    """
    groups = repo.fetch_df("SELECT DISTINCT sub_biz AS group_name FROM fact_qa_event WHERE sub_biz IS NOT NULL ORDER BY 1")
    queues = repo.fetch_df("SELECT DISTINCT sub_biz AS group_name, queue_name FROM fact_qa_event WHERE queue_name IS NOT NULL ORDER BY 1, 2")
    reviewers = repo.fetch_df("SELECT DISTINCT reviewer_name FROM fact_qa_event WHERE reviewer_name IS NOT NULL ORDER BY 1")
    error_types = repo.fetch_df("SELECT DISTINCT error_type FROM fact_qa_event WHERE error_type IS NOT NULL AND error_type <> '' ORDER BY 1")
    dates = repo.fetch_df("SELECT DISTINCT biz_date FROM fact_qa_event ORDER BY 1")
    return {
        "groups": groups["group_name"].tolist() if not groups.empty else [],
        "queues": queues if not queues.empty else pd.DataFrame(columns=["group_name", "queue_name"]),
        "reviewers": reviewers["reviewer_name"].tolist() if not reviewers.empty else [],
        "error_types": error_types["error_type"].tolist() if not error_types.empty else [],
        "min_date": dates["biz_date"].min() if not dates.empty else date.today(),
        "max_date": dates["biz_date"].max() if not dates.empty else date.today(),
    }


@st.cache_data(show_spinner=False)
def query_detail(
    date_start: date,
    date_end: date,
    group_name: str | None,
    queue_name: str | None,
    reviewer_name: str | None,
    error_type: str | None,
    only_issues: bool,
    issue_filter: str,
    limit: int = 2000,
) -> pd.DataFrame:
    conditions = ["biz_date >= %s", "biz_date <= %s"]
    params: list = [date_start, date_end]

    if group_name:
        conditions.append("group_name = %s")
        params.append(group_name)
    if queue_name:
        conditions.append("queue_name = %s")
        params.append(queue_name)
    if reviewer_name:
        conditions.append("reviewer_name = %s")
        params.append(reviewer_name)
    if error_type:
        conditions.append("error_type = %s")
        params.append(error_type)

    if only_issues:
        if issue_filter == "原始错误":
            conditions.append("NOT is_raw_correct")
        elif issue_filter == "最终错误":
            conditions.append("NOT is_final_correct")
        elif issue_filter == "错判":
            conditions.append("is_misjudge")
        elif issue_filter == "漏判":
            conditions.append("is_missjudge")
        elif issue_filter == "申诉改判":
            conditions.append("is_appeal_reversed")

    where = " AND ".join(conditions)
    sql = f"""
    SELECT
        biz_date AS 业务日期,
        COALESCE(sub_biz, '—') AS 组别,
        COALESCE(queue_name, '—') AS 队列,
        COALESCE(reviewer_name, '—') AS 审核人,
        COALESCE(content_type, '—') AS 内容类型,
        raw_judgement AS 一审结果,
        final_review_result AS 最终结果,
        appeal_status AS 申诉状态,
        appeal_result AS 申诉结果,
        CASE WHEN is_raw_correct = 1 THEN '正确' ELSE '错误' END AS 原始判断,
        CASE WHEN is_final_correct = 1 THEN '正确' ELSE '错误' END AS 最终判断,
        CASE WHEN is_misjudge = 1 THEN '是' ELSE '否' END AS 错判,
        CASE WHEN is_missjudge = 1 THEN '是' ELSE '否' END AS 漏判,
        CASE WHEN is_appeal_reversed = 1 THEN '是' ELSE '否' END AS 申诉改判,
        COALESCE(error_type, '—') AS 错误类型,
        COALESCE(error_reason, '—') AS 错误归因,
        comment_text AS 评论文本,
        COALESCE(qa_note, '—') AS 备注,
        join_key AS 关联主键,
        qa_time AS 质检时间
    FROM fact_qa_event
    WHERE {where}
    ORDER BY qa_time IS NULL, qa_time DESC, biz_date DESC
    LIMIT {int(limit)}
    """
    return repo.fetch_df(sql, params)


@st.cache_data(show_spinner=False)
def to_csv_bytes(df: pd.DataFrame) -> bytes:
    export_df = df.copy()
    for column in export_df.columns:
        if pd.api.types.is_datetime64_any_dtype(export_df[column]):
            export_df[column] = export_df[column].astype(str)
    return export_df.to_csv(index=False).encode("utf-8-sig")


opts = get_filter_options()

# Hero 区域
with st.container(border=True):
    st.markdown("### 🎯 筛选条件")
    st.caption("💡 提示：选择组别后会自动过滤队列列表，缩小查询范围可提高查询速度")
    
    c1, c2, c3 = st.columns(3)
    with c1:
        # 默认查最近7天，减少首次加载时间
        default_start = max(opts["min_date"], opts["max_date"] - timedelta(days=6))
        date_start = st.date_input("起始日期", value=default_start, key="detail_start")
    with c2:
        date_end = st.date_input("截止日期", value=opts["max_date"], key="detail_end")
    with c3:
        group_sel = st.selectbox("组别", options=["(全部)"] + opts["groups"], key="detail_group")

    c4, c5, c6 = st.columns(3)
    queue_options = []
    if group_sel != "(全部)" and not opts["queues"].empty:
        queue_options = opts["queues"][opts["queues"]["group_name"] == group_sel]["queue_name"].tolist()
    with c4:
        queue_sel = st.selectbox("队列", options=["(全部)"] + queue_options, key="detail_queue")
    with c5:
        reviewer_sel = st.selectbox("审核人", options=["(全部)"] + opts["reviewers"], key="detail_reviewer")
    with c6:
        error_sel = st.selectbox("错误类型", options=["(全部)"] + opts["error_types"], key="detail_error")

    c7, c8, c9 = st.columns([1, 2, 1])
    with c7:
        only_issues = st.checkbox("只看问题样本", value=False, key="detail_only_issues", help="勾选后仅显示有问题的样本")
    with c8:
        issue_filter = st.selectbox(
            "问题类型",
            options=["全部问题", "原始错误", "最终错误", "错判", "漏判", "申诉改判"],
            disabled=not only_issues,
            key="detail_issue_filter",
        )
    with c9:
        limit_options = {
            "2000条（默认）": 2000,
            "5000条": 5000,
            "10000条": 10000,
            "50000条": 50000,
        }
        limit_sel = st.selectbox(
            "显示条数",
            options=list(limit_options.keys()),
            index=0,
            key="detail_limit",
            help="选择显示的数据条数限制，过多数据可能影响性能",
        )
        query_limit = limit_options[limit_sel]

group_val = group_sel if group_sel != "(全部)" else None
queue_val = queue_sel if queue_sel != "(全部)" else None
reviewer_val = reviewer_sel if reviewer_sel != "(全部)" else None
error_val = error_sel if error_sel != "(全部)" else None
issue_mode = issue_filter if only_issues and issue_filter != "全部问题" else None

df = query_detail(date_start, date_end, group_val, queue_val, reviewer_val, error_val, only_issues, issue_filter if issue_filter != "全部问题" else "", query_limit)

if df.empty:
    st.info("当前筛选条件下没有数据。试试放宽筛选条件。")
else:
    # 结果统计（优化样式）
    st.markdown("---")
    st.markdown("### 📊 查询结果")
    
    # 统计概览卡片
    s1, s2, s3, s4, s5 = st.columns(5)
    total = len(df)
    raw_err = (df["原始判断"] == "错误").sum()
    final_err = (df["最终判断"] == "错误").sum()
    mis = (df["错判"] == "是").sum()
    miss = (df["漏判"] == "是").sum()
    
    with s1:
        st.markdown(f"""
            <div style='background: linear-gradient(135deg, #EFF6FF 0%, #DBEAFE 100%); padding: 0.75rem; border-radius: 0.75rem; text-align: center; border: 2px solid #3B82F6;'>
                <div style='font-size: 0.75rem; color: #1E40AF; margin-bottom: 0.25rem;'>总记录</div>
                <div style='font-size: 1.5rem; font-weight: 700; color: #1E40AF;'>{total:,}</div>
            </div>
        """, unsafe_allow_html=True)
    with s2:
        st.markdown(f"""
            <div style='background: linear-gradient(135deg, #FEF2F2 0%, #FEE2E2 100%); padding: 0.75rem; border-radius: 0.75rem; text-align: center; border: 2px solid #EF4444;'>
                <div style='font-size: 0.75rem; color: #991B1B; margin-bottom: 0.25rem;'>原始错误</div>
                <div style='font-size: 1.5rem; font-weight: 700; color: #EF4444;'>{raw_err:,}</div>
                <div style='font-size: 0.7rem; color: #991B1B;'>{raw_err/total*100:.1f}%</div>
            </div>
        """, unsafe_allow_html=True)
    with s3:
        st.markdown(f"""
            <div style='background: linear-gradient(135deg, #FEF2F2 0%, #FEE2E2 100%); padding: 0.75rem; border-radius: 0.75rem; text-align: center; border: 2px solid #DC2626;'>
                <div style='font-size: 0.75rem; color: #7F1D1D; margin-bottom: 0.25rem;'>最终错误</div>
                <div style='font-size: 1.5rem; font-weight: 700; color: #DC2626;'>{final_err:,}</div>
                <div style='font-size: 0.7rem; color: #7F1D1D;'>{final_err/total*100:.1f}%</div>
            </div>
        """, unsafe_allow_html=True)
    with s4:
        st.markdown(f"""
            <div style='background: linear-gradient(135deg, #FFFBEB 0%, #FEF3C7 100%); padding: 0.75rem; border-radius: 0.75rem; text-align: center; border: 2px solid #F59E0B;'>
                <div style='font-size: 0.75rem; color: #92400E; margin-bottom: 0.25rem;'>错判</div>
                <div style='font-size: 1.5rem; font-weight: 700; color: #F59E0B;'>{mis:,}</div>
            </div>
        """, unsafe_allow_html=True)
    with s5:
        st.markdown(f"""
            <div style='background: linear-gradient(135deg, #F0FDF4 0%, #DCFCE7 100%); padding: 0.75rem; border-radius: 0.75rem; text-align: center; border: 2px solid #10B981;'>
                <div style='font-size: 0.75rem; color: #047857; margin-bottom: 0.25rem;'>漏判</div>
                <div style='font-size: 1.5rem; font-weight: 700; color: #10B981;'>{miss:,}</div>
            </div>
        """, unsafe_allow_html=True)
    
    # 导出按钮（优化样式）
    csv_data = to_csv_bytes(df)
    st.markdown("")  # 添加间距
    st.download_button(
        "📥 导出当前筛选结果 CSV",
        data=csv_data,
        file_name=f"qa_detail_{date_start}_{date_end}.csv",
        mime="text/csv",
        use_container_width=True,
    )

    # 错误类型分布（如果有问题样本）
    if only_issues or raw_err > 0:
        with st.expander("📊 错误类型分布", expanded=False):
            err_dist = df[df["错误类型"] != "—"]["错误类型"].value_counts().reset_index()
            err_dist.columns = ["错误类型", "数量"]
            st.dataframe(err_dist, use_container_width=True, hide_index=True)

    # 明细表格（优化样式）
    st.markdown("#### 📋 明细数据")
    st.caption(f"显示前 {min(len(df), query_limit)} 条记录，共 {len(df)} 条")
    st.dataframe(df, use_container_width=True, hide_index=True, height=500)
