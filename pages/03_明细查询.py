"""
QC 评论看板 — 明细查询页
功能：多维度筛选、关键词搜索、评论详情展示、导出
数据源：vw_qa_base（TiDB 视图，统一 fact_qa_event + 申诉数据）
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
from storage.repository import DashboardRepository

PAGE_ICON = "🔍"
PAGE_TITLE = "明细查询"

repo = DashboardRepository()


# ============================================================
#  缓存的数据加载
# ============================================================
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


@st.cache_data(show_spinner=False, ttl=60)
def get_queue_list() -> list[str]:
    """获取所有队列名"""
    df = repo.fetch_df("SELECT DISTINCT queue_name FROM vw_qa_base WHERE queue_name IS NOT NULL AND queue_name != '' ORDER BY 1")
    return df["queue_name"].tolist() if not df.empty else []


@st.cache_data(show_spinner=False, ttl=60)
def get_group_list() -> list[str]:
    """获取所有组别名"""
    df = repo.fetch_df("SELECT DISTINCT group_name FROM vw_qa_base WHERE group_name IS NOT NULL AND group_name != '' ORDER BY 1")
    return df["group_name"].tolist() if not df.empty else []


@st.cache_data(show_spinner=False, ttl=60)
def get_qa_owner_list() -> list[str]:
    """获取所有质检员"""
    df = repo.fetch_df("SELECT DISTINCT qa_owner_name FROM vw_qa_base WHERE qa_owner_name IS NOT NULL AND qa_owner_name != '' ORDER BY 1")
    return df["qa_owner_name"].tolist() if not df.empty else []


@st.cache_data(show_spinner=False, ttl=60)
def get_error_type_list() -> list[str]:
    """获取所有错误类型（从 vw_qa_base 动态加载）"""
    df = repo.fetch_df("SELECT DISTINCT error_type FROM vw_qa_base WHERE error_type IS NOT NULL AND error_type != '' ORDER BY 1")
    return df["error_type"].tolist() if not df.empty else []


@st.cache_data(show_spinner=False, ttl=60)
def get_queue_list_by_group(group_name: str | None) -> list[str]:
    """获取指定组别下的队列名（联动筛选）"""
    if not group_name:
        return get_queue_list()
    df = repo.fetch_df(
        "SELECT DISTINCT queue_name FROM vw_qa_base WHERE group_name = %s AND queue_name IS NOT NULL AND queue_name != '' ORDER BY 1",
        [group_name]
    )
    return df["queue_name"].tolist() if not df.empty else []


def render():
    st.set_page_config(page_title=f"{PAGE_TITLE} | QC 评论看板", page_icon=PAGE_ICON, layout="wide")

    # ── 加载统一 CSS ──
    import os as _os
    _css_path = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "custom.css")
    if _os.path.exists(_css_path):
        with open(_css_path, "r", encoding="utf-8") as _f:
            _CSS = _f.read()
        st.markdown(f'<style>{_CSS}</style>', unsafe_allow_html=True)

    # ---- 标题区 ----
    st.markdown(f"""
    <div style="padding: 20px 0 10px 0;">
        <div class="page-title">{PAGE_ICON} {PAGE_TITLE}</div>
        <div class="page-subtitle">多维度筛选与搜索，定位目标质检记录</div>
    </div>
    """, unsafe_allow_html=True)

    # ================================================================
    #  筛选条件
    # ================================================================
    filters = _render_filters()

    # ================================================================
    #  查询 + 结果
    # ================================================================
    if filters is not None:
        query_result = _execute_query(filters)
        if query_result is not None:
            df_result, total_count, filter_summary = query_result
            _render_result_stats(total_count, filter_summary, df_result)
            _render_result_table(df_result, total_count)

            # 导出（独立于查询逻辑）
            if filters.get("do_export") and not df_result.empty:
                _render_export(filters)


# ============================================================
#  筛选区域
# ============================================================
def _render_filters() -> dict | None:
    """渲染筛选条件卡片，返回筛选参数 dict；用户未点查询时返回 None"""

    st.markdown('<div class="card"><div class="section-title"><span class="emoji">🎛️</span> 筛选条件</div></div>', unsafe_allow_html=True)

    f_col1, f_col2, f_col3 = st.columns(3)

    with f_col1:
        date_range = st.selectbox(
            "时间范围",
            ["近 7 天", "近 30 天", "近 90 天", "全部时间", "自定义"],
            key="filter_date_range",
            label_visibility="collapsed"
        )
        start_date = None
        end_date = None
        if date_range == "自定义":
            start_date = st.date_input("起始日期", value=date.today() - timedelta(days=30), key="filter_start")
            end_date = st.date_input("截止日期", value=date.today(), key="filter_end")

    with f_col2:
        group_options = ["(全部)"] + get_group_list()
        selected_group = st.selectbox("组别", options=group_options, key="filter_group", label_visibility="collapsed")
        # 队列联动组别：选了组别后只显示该组别下的队列
        _filtered_queues = get_queue_list_by_group(selected_group if selected_group != "(全部)" else None)
        queue_options = ["(全部)"] + _filtered_queues
        # 如果当前选中的队列不在新选项里，自动回退到"(全部)"
        _prev_queue = st.session_state.get("filter_queue", "(全部)")
        if _prev_queue not in queue_options:
            st.session_state["filter_queue"] = "(全部)"
        selected_queue = st.selectbox("队列", options=queue_options, key="filter_queue", label_visibility="collapsed")

    with f_col3:
        keywords = st.text_input("🔤 关键词搜索", placeholder="输入评论内容 / 审核员 / 记录ID ...",
                                 key="filter_keywords")
        qa_owner_options = ["(全部)"] + get_qa_owner_list()
        selected_qa_owner = st.selectbox("质检员", options=qa_owner_options, key="filter_qa_owner", label_visibility="collapsed")

    # 更多筛选项
    with st.expander("⚙️ 更多筛选", expanded=False):
        mf_col1, mf_col2, mf_col3 = st.columns(3)
        with mf_col1:
            qc_results = ["全部", "违规", "通过", "待审核"]
            selected_qc = st.multiselect("质检结果", qc_results, default=["全部"], key="filter_qc_result")
        with mf_col2:
            # 动态加载错误类型，同时保留错判/漏判分类
            _db_error_types = get_error_type_list()
            error_types = ["全部", "错判", "漏判"] + [t for t in _db_error_types if t not in ("错判", "漏判")]
            selected_error = st.multiselect("错误类型", error_types, default=["全部"], key="filter_error_type")
        with mf_col3:
            sort_by = st.selectbox("排序方式",
                                    ["时间降序", "时间升序"],
                                    key="filter_sort_by")
            only_appeal = st.checkbox("仅看有申诉记录", key="filter_only_appeal")

    # 查询按钮行
    btn_col1, btn_col2, btn_col3, _ = st.columns([2, 1, 1, 2])
    with btn_col1:
        do_search = st.button("🔍 查询", type="primary", use_container_width=True)
    with btn_col2:
        do_reset = st.button("🔄 重置", use_container_width=True)
    with btn_col3:
        do_export = st.button("📥 导出结果", use_container_width=True)

    if do_reset:
        _reset_filters()

    # 首次进入不自动查询
    if not do_search and "_search_triggered" not in st.session_state:
        return None

    if do_search:
        st.session_state["_search_triggered"] = True
        st.session_state["query_page"] = 1  # 新查询重置分页

    return {
        "date_range": date_range,
        "start_date": start_date,
        "end_date": end_date,
        "group": selected_group,
        "queue": selected_queue,
        "keywords": keywords,
        "qa_owner": selected_qa_owner,
        "qc_results": selected_qc,
        "error_types": selected_error,
        "sort_by": sort_by,
        "only_appeal": only_appeal,
        "do_export": do_export,
    }


def _reset_filters():
    """重置所有筛选项到默认值"""
    keys_to_clear = [k for k in st.session_state.keys() if k.startswith("filter_") or k == "_search_triggered"]
    for k in keys_to_clear:
        del st.session_state[k]
    st.rerun()


# ============================================================
#  执行查询
# ============================================================
def _execute_query(filters: dict):
    """根据筛选条件执行查询，返回 (df, total_count, filter_summary) 或 None"""

    try:
        conditions = []
        params: list = []

        # 时间范围
        date_range = filters["date_range"]
        if date_range == "近 7 天":
            conditions.append("biz_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)")
        elif date_range == "近 30 天":
            conditions.append("biz_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)")
        elif date_range == "近 90 天":
            conditions.append("biz_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)")
        elif date_range == "自定义":
            sd = filters["start_date"]
            ed = filters["end_date"]
            if sd:
                conditions.append("biz_date >= %s")
                params.append(str(sd))
            if ed:
                conditions.append("biz_date <= %s")
                params.append(str(ed))

        # 组别
        if filters["group"] != "(全部)":
            conditions.append("group_name = %s")
            params.append(filters["group"])

        # 队列
        if filters["queue"] != "(全部)":
            conditions.append("queue_name = %s")
            params.append(filters["queue"])

        # 关键词（安全处理：只用确定存在的列）
        keywords = filters["keywords"]
        if keywords:
            kw = f"%{keywords}%"
            conditions.append("(comment_text LIKE %s OR reviewer_name LIKE %s)")
            params.extend([kw, kw])

        # 质检员
        if filters["qa_owner"] != "(全部)":
            conditions.append("qa_owner_name = %s")
            params.append(filters["qa_owner"])

        # 质检结果
        qc_res = filters["qc_results"]
        if "全部" not in qc_res and qc_res:
            res_clauses = []
            for r in qc_res:
                if r == "违规":
                    res_clauses.append("is_raw_correct = 0")
                elif r == "通过":
                    res_clauses.append("is_raw_correct = 1")
                elif r == "待审核":
                    res_clauses.append("qa_result IS NULL")
            if res_clauses:
                conditions.append(f"({' OR '.join(res_clauses)})")

        # 错误类型（支持动态 error_type 值 + 错判/漏判分类）
        err_types = filters["error_types"]
        if "全部" not in err_types and err_types:
            err_clauses = []
            # 错判/漏判是判定分类，对应 is_misjudge/is_missjudge
            _judge_types = []
            _error_type_values = []
            for t in err_types:
                if t == "错判":
                    _judge_types.append("is_misjudge = 1")
                elif t == "漏判":
                    _judge_types.append("is_missjudge = 1")
                else:
                    # 动态 error_type 值
                    _error_type_values.append(t)
            if _judge_types:
                err_clauses.extend(_judge_types)
            if _error_type_values:
                placeholders = ", ".join(["%s"] * len(_error_type_values))
                err_clauses.append(f"error_type IN ({placeholders})")
                params.extend(_error_type_values)
            if err_clauses:
                conditions.append(f"({' OR '.join(err_clauses)})")

        # 仅看申诉
        if filters["only_appeal"]:
            conditions.append("is_appealed = 1")

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # 排序
        sort = filters["sort_by"]
        order_by = "biz_date DESC, qa_time DESC"
        if sort == "时间升序":
            order_by = "biz_date ASC, qa_time ASC"

        # 先查总数
        count_sql = f"SELECT COUNT(*) FROM vw_qa_base WHERE {where_clause}"
        total = repo.fetch_one(count_sql, params)
        total_count = list(total.values())[0] if total else 0

        # 分页
        page_size = 50
        page = st.session_state.get("query_page", 1)
        offset = (page - 1) * page_size

        data_sql = f"""
            SELECT
                biz_date, qa_time, group_name, queue_name, reviewer_name,
                qa_owner_name, content_type, scene_name,
                raw_label, final_label,
                raw_judgement, final_review_result,
                qa_result, error_type, error_level, error_reason, risk_level,
                is_raw_correct, is_final_correct,
                is_misjudge, is_missjudge,
                is_appealed, appeal_status, appeal_result,
                comment_text, qa_note, join_key
            FROM vw_qa_base
            WHERE {where_clause}
            ORDER BY {order_by}
            LIMIT %s OFFSET %s
        """
        params.extend([page_size, offset])
        df = repo.fetch_df(data_sql, params)

        # 构建筛选摘要
        filter_parts = []
        if date_range != "全部时间":
            filter_parts.append(f"📅 {date_range}")
        if filters["group"] != "(全部)":
            filter_parts.append(f"🏢 {filters['group']}")
        if filters["queue"] != "(全部)":
            filter_parts.append(f"📋 {filters['queue']}")
        if keywords:
            display_kw = keywords[:20] + ("..." if len(keywords) > 20 else "")
            filter_parts.append(f"🔤 \"{display_kw}\"")
        if filters["qa_owner"] != "(全部)":
            filter_parts.append(f"👤 {filters['qa_owner']}")

        filter_summary = " · ".join(filter_parts) if filter_parts else "全部数据"

        return df, total_count, filter_summary

    except Exception as e:
        st.markdown(f'<div class="alert-box warning">⚠️ 查询出错：{e}</div>', unsafe_allow_html=True)
        return None


# ============================================================
#  结果渲染
# ============================================================
def _render_result_stats(total_count: int, filter_summary: str, df: pd.DataFrame):
    """统计摘要条"""

    # 计算违规率/通过率
    violation_cnt = 0
    pass_cnt = 0
    if not df.empty and "is_raw_correct" in df.columns:
        violation_cnt = int((df["is_raw_correct"] == 0).sum())
        pass_cnt = int((df["is_raw_correct"] == 1).sum())

    violation_rate = f"{violation_cnt / len(df) * 100:.2f}%" if len(df) > 0 else "—"
    pass_rate = f"{pass_cnt / len(df) * 100:.2f}%" if len(df) > 0 else "—"

    col_stat1, col_stat2, col_stat3, col_stat4 = st.columns([2, 1, 1, 1])

    with col_stat1:
        st.markdown(f'<div class="filter-row" style="font-size:13px;color:#475569;">{filter_summary}</div>', unsafe_allow_html=True)

    with col_stat2:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-number">{total_count:,}</div>
            <div class="stat-label">匹配总数</div>
        </div>
        """, unsafe_allow_html=True)

    with col_stat3:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-number" style="color:#EF4444;">{violation_rate}</div>
            <div class="stat-label">违规率</div>
        </div>
        """, unsafe_allow_html=True)

    with col_stat4:
        st.markdown(f"""
        <div class="stat-card">
            <div class="stat-number" style="color:#10B981;">{pass_rate}</div>
            <div class="stat-label">通过率</div>
        </div>
        """, unsafe_allow_html=True)


def _render_result_table(df: pd.DataFrame, total_count: int):
    """结果表格 + 详情展开 + 分页"""

    if df.empty:
        st.markdown("""
        <div class="empty-state">
            <div class="icon">🔎</div>
            <div>没有找到匹配的记录</div>
            <div style="font-size:0.85rem;margin-top:6px;">尝试调整筛选条件或清空部分过滤项</div>
        </div>
        """, unsafe_allow_html=True)
        return

    # 格式化展示
    show_df = pd.DataFrame()
    show_df["业务日期"] = df["biz_date"].astype(str) if "biz_date" in df.columns else "—"
    if "qa_time" in df.columns:
        show_df["质检时间"] = pd.to_datetime(df["qa_time"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
    show_df["组别"] = df["group_name"].fillna("—") if "group_name" in df.columns else "—"
    show_df["队列"] = df["queue_name"].fillna("—") if "queue_name" in df.columns else "—"
    show_df["审核员"] = df["reviewer_name"].fillna("—") if "reviewer_name" in df.columns else "—"
    show_df["质检员"] = df["qa_owner_name"].fillna("—") if "qa_owner_name" in df.columns else "—"
    show_df["内容类型"] = df["content_type"].fillna("—") if "content_type" in df.columns else "—"
    show_df["场景"] = df["scene_name"].fillna("—") if "scene_name" in df.columns else "—"
    show_df["一审结果"] = df["raw_judgement"].fillna("—") if "raw_judgement" in df.columns else "—"
    show_df["终审结果"] = df["final_review_result"].fillna("—") if "final_review_result" in df.columns else "—"
    show_df["错误类型"] = df["error_type"].fillna("—") if "error_type" in df.columns else "—"
    show_df["错误等级"] = df["error_level"].fillna("—") if "error_level" in df.columns else "—"
    show_df["风险等级"] = df["risk_level"].fillna("—") if "risk_level" in df.columns else "—"
    show_df["错误原因"] = df["error_reason"].fillna("—") if "error_reason" in df.columns else "—"
    show_df["申诉状态"] = df["appeal_status"].fillna("—") if "appeal_status" in df.columns else "—"
    # 状态标记列
    def _mark_correct(row):
        if "is_raw_correct" in df.columns and pd.notna(row.get("is_raw_correct")):
            return "✅ 通过" if int(row["is_raw_correct"]) == 1 else "❌ 违规"
        return "—"

    show_df["正确/违规"] = df.apply(_mark_correct, axis=1)

    # 评论内容截断
    if "comment_text" in df.columns:
        show_df["评论内容"] = df["comment_text"].fillna("").astype(str).apply(
            lambda x: x[:80] + "..." if len(x) > 80 else x
        )

    # 条件高亮：违规行、高错误等级、高风险等级标红
    def _highlight_detail(val, col_name):
        """明细表条件格式化"""
        if col_name == "正确/违规":
            if "违规" in str(val):
                return "color: #dc2626; font-weight: 700; background-color: #fef2f2"
        if col_name == "错误等级":
            v = str(val).strip().upper()
            if v in ("P0", "高", "HIGH", "严重"):
                return "color: #dc2626; font-weight: 700; background-color: #fef2f2"
            if v in ("P1", "中", "MEDIUM"):
                return "color: #d97706; font-weight: 600; background-color: #fffbeb"
        if col_name == "风险等级":
            v = str(val).strip().upper()
            if v in ("高", "HIGH", "严重"):
                return "color: #dc2626; font-weight: 700; background-color: #fef2f2"
            if v in ("中", "MEDIUM"):
                return "color: #d97706; font-weight: 600; background-color: #fffbeb"
        if col_name == "错误类型":
            if str(val).strip() in ("错判", "漏判"):
                return "color: #dc2626; font-weight: 600"
        return ""

    styled = show_df.style
    for col in ["正确/违规", "错误等级", "风险等级", "错误类型"]:
        if col in show_df.columns:
            styled = styled.map(
                lambda val, c=col: _highlight_detail(val, c), subset=[col]
            )

    # 列配置
    col_config = {
        "业务日期": st.column_config.TextColumn("业务日期", width="small"),
        "质检时间": st.column_config.TextColumn("质检时间", width="small"),
        "组别": st.column_config.TextColumn("组别", width="small"),
        "队列": st.column_config.TextColumn("队列", width="small"),
        "审核员": st.column_config.TextColumn("审核员", width="small"),
        "质检员": st.column_config.TextColumn("质检员", width="small"),
        "内容类型": st.column_config.TextColumn("内容类型", width="small"),
        "场景": st.column_config.TextColumn("场景", width="small"),
        "一审结果": st.column_config.TextColumn("一审结果", width="small"),
        "终审结果": st.column_config.TextColumn("终审结果", width="small"),
        "错误类型": st.column_config.TextColumn("错误类型", width="small"),
        "错误等级": st.column_config.TextColumn("错误等级", width="small"),
        "风险等级": st.column_config.TextColumn("风险等级", width="small"),
        "错误原因": st.column_config.TextColumn("错误原因", width="small"),
        "申诉状态": st.column_config.TextColumn("申诉状态", width="small"),
        "正确/违规": st.column_config.TextColumn("正确/违规", width="small"),
        "评论内容": st.column_config.TextColumn("评论内容", width="large"),
    }

    st.dataframe(styled, use_container_width=True, hide_index=True, height=450, column_config=col_config)

    # 分页控制
    page_size = 50
    page = st.session_state.get("query_page", 1)
    total_pages = max(1, (total_count + page_size - 1) // page_size)

    pg_col1, pg_col2, pg_col3, pg_col4, pg_col5 = st.columns([1, 1, 2, 1, 1])
    with pg_col1:
        if st.button("⬅️ 上一页", disabled=(page <= 1), use_container_width=True, key="btn_prev_page"):
            st.session_state["query_page"] = max(1, page - 1)
            st.rerun()
    with pg_col2:
        st.markdown(f'<div style="padding-top:8px;text-align:center;font-size:13px;color:#475569;">第 {page}/{total_pages} 页</div>', unsafe_allow_html=True)
    with pg_col3:
        st.markdown(f'<div style="padding-top:8px;text-align:center;font-size:12px;color:#94A3B8;">每页 {page_size} 条 · 共 {total_count:,} 条</div>', unsafe_allow_html=True)
    with pg_col5:
        if st.button("➡️ 下一页", disabled=(page >= total_pages), use_container_width=True, key="btn_next_page"):
            st.session_state["query_page"] = page + 1
            st.rerun()

    # ---- 详情展开 ----
    with st.expander("📝 查看选中行详情", expanded=False):
        sel_idx = st.number_input("选择行号（从 1 开始）", min_value=1, max_value=len(df), value=1, step=1)
        if 1 <= sel_idx <= len(df):
            row = df.iloc[sel_idx - 1]
            detail_html = '<div class="detail-box">'
            for col_name in df.columns:
                val = row[col_name]
                val_str = (
                    pd.to_datetime(val).strftime("%Y-%m-%d %H:%M:%S")
                    if isinstance(val, (pd.Timestamp, datetime))
                    else (str(val) if not pd.isna(val) else "—")
                )
                detail_html += f'''
                <div class="detail-field">
                    <span class="field-key">{col_name}</span>
                    <span class="field-val">{val_str[:300]}{'...' if len(str(val_str))>300 else ''}</span>
                </div>
                '''
            detail_html += "</div>"
            st.markdown(detail_html, unsafe_allow_html=True)


def _render_export(filters: dict):
    """独立导出功能：查询全量匹配数据并生成下载按钮"""
    try:
        conditions = []
        params: list = []

        # 时间范围
        date_range = filters["date_range"]
        if date_range == "近 7 天":
            conditions.append("biz_date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)")
        elif date_range == "近 30 天":
            conditions.append("biz_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)")
        elif date_range == "近 90 天":
            conditions.append("biz_date >= DATE_SUB(CURDATE(), INTERVAL 90 DAY)")
        elif date_range == "自定义":
            if filters["start_date"]:
                conditions.append("biz_date >= %s")
                params.append(str(filters["start_date"]))
            if filters["end_date"]:
                conditions.append("biz_date <= %s")
                params.append(str(filters["end_date"]))

        if filters["group"] != "(全部)":
            conditions.append("group_name = %s")
            params.append(filters["group"])
        if filters["queue"] != "(全部)":
            conditions.append("queue_name = %s")
            params.append(filters["queue"])
        keywords = filters["keywords"]
        if keywords:
            kw = f"%{keywords}%"
            conditions.append("(comment_text LIKE %s OR reviewer_name LIKE %s)")
            params.extend([kw, kw])
        if filters["qa_owner"] != "(全部)":
            conditions.append("qa_owner_name = %s")
            params.append(filters["qa_owner"])

        qc_res = filters["qc_results"]
        if "全部" not in qc_res and qc_res:
            res_clauses = []
            for r in qc_res:
                if r == "违规": res_clauses.append("is_raw_correct = 0")
                elif r == "通过": res_clauses.append("is_raw_correct = 1")
                elif r == "待审核": res_clauses.append("qa_result IS NULL")
            if res_clauses:
                conditions.append(f"({' OR '.join(res_clauses)})")

        err_types = filters["error_types"]
        if "全部" not in err_types and err_types:
            err_clauses = []
            if "错判" in err_types: err_clauses.append("is_misjudge = 1")
            if "漏判" in err_types: err_clauses.append("is_missjudge = 1")
            if err_clauses:
                conditions.append(f"({' OR '.join(err_clauses)})")

        if filters["only_appeal"]:
            conditions.append("is_appealed = 1")

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        sort = filters["sort_by"]
        order_by = "biz_date DESC, qa_time DESC"
        if sort == "时间升序":
            order_by = "biz_date ASC, qa_time ASC"

        export_sql = f"""
            SELECT
                biz_date, qa_time, group_name, queue_name, reviewer_name,
                qa_owner_name, content_type, scene_name,
                raw_label, final_label,
                raw_judgement, final_review_result,
                qa_result, error_type, error_level, error_reason, risk_level,
                is_raw_correct, is_final_correct,
                is_misjudge, is_missjudge,
                is_appealed, appeal_status, appeal_result,
                comment_text, qa_note, join_key
            FROM vw_qa_base
            WHERE {where_clause}
            ORDER BY {order_by}
        """
        export_df = repo.fetch_df(export_sql, params)
        csv_bytes = export_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            "📥 下载 CSV",
            data=csv_bytes,
            file_name=f"明细查询_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
            use_container_width=True,
        )
    except Exception as e:
        st.markdown(f'<div class="alert-box warning">⚠️ 导出出错：{e}</div>', unsafe_allow_html=True)


# 入口
if __name__ == "__main__":
    render()
