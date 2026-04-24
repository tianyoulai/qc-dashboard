"""
QC 评论看板 — 数据导入页
功能：Excel 文件上传、数据校验、入库（fact_comment / fact_qa_event）
"""

import streamlit as st
import pandas as pd
import os
import time
import hashlib
from datetime import datetime, date

from utils.database import get_engine, fact_comment_exists, insert_fact_comments, \
    fact_qa_event_exists, insert_fact_qa_events, get_latest_comment_date, \
    get_latest_qa_event_date, record_upload_log, upload_exists, \
    get_db_type, is_sqlite
from utils.config import DATA_DIR, MAX_UPLOAD_SIZE_MB, SUPPORTED_EXCEL_EXTENSIONS
from utils.logger import get_logger

logger = get_logger(__name__)

PAGE_ICON = "📥"
PAGE_TITLE = "数据导入"


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
        <div class="page-subtitle">上传 Excel 文件，自动校验并导入评论数据与质检事件</div>
    </div>
    """, unsafe_allow_html=True)

    engine = get_engine()
    db_type = get_db_type(engine)

    # ================================================================
    #  第一行：文件上传 + 快速状态概览
    # ================================================================
    col_up, col_status = st.columns([3, 2])

    with col_up:
        st.markdown(f'<div class="card"><div class="section-title"><span class="emoji">📁</span> 上传文件</div></div>', unsafe_allow_html=True)
        uploaded_file = st.file_uploader(
            "选择 Excel 文件（支持 .xlsx / .xls）",
            type=SUPPORTED_EXCEL_EXTENSIONS,
            key="comment_file_uploader",
            help=f"最大 {MAX_UPLOAD_SIZE_MB} MB，支持多 Sheet 自动识别"
        )

    with col_status:
        st.markdown(f'<div class="card"><div class="section-title"><span class="emoji">📊</span> 数据库状态</div></div>', unsafe_allow_html=True)
        try:
            latest_comment = get_latest_comment_date(engine) or "—"
            latest_qa = get_latest_qa_event_date(engine) or "—"
            st.markdown(f"""
            <div class="result-summary" style="margin-top:0;">
                <div class="result-item">
                    <div class="num">{latest_comment}</div>
                    <div class="label">最新评论日期</div>
                </div>
                <div class="result-item">
                    <div class="num">{latest_qa}</div>
                    <div class="label">最新质检日期</div>
                </div>
                <div class="result-item">
                    <div class="num">{"SQLite" if is_sqlite(engine) else db_type.upper()}</div>
                    <div class="label">数据库类型</div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        except Exception as e:
            st.markdown(f'<div class="alert-box error">⚠️ 无法连接数据库：{e}</div>', unsafe_allow_html=True)

    # ================================================================
    #  如果有文件，展示预览和操作
    # ================================================================
    if uploaded_file:
        _handle_uploaded_file(uploaded_file, engine)

    # ================================================================
    #  底部：最近上传记录
    # ================================================================
    _render_upload_history(engine)


# ============================================================
#  文件处理核心逻辑
# ============================================================
def _handle_uploaded_file(uploaded_file, engine):
    """读取、校验、预览，提供导入按钮"""

    # 文件大小检查
    file_size_mb = len(uploaded_file.getbuffer()) / (1024 * 1024)
    if file_size_mb > MAX_UPLOAD_SIZE_MB:
        st.markdown(
            f'<div class="alert-box error">❌ 文件过大（{file_size_mb:.1f} MB），上限 {MAX_UPLOAD_SIZE_MB} MB</div>',
            unsafe_allow_html=True
        )
        return

    # 读取 Excel
    try:
        xl = pd.ExcelFile(uploaded_file)
        sheet_names = xl.sheet_names
    except Exception as e:
        st.markdown(f'<div class="alert-box error">❌ 文件读取失败：{e}</div>', unsafe_allow_html=True)
        return

    st.markdown("---")

    # --- Sheet 选择 ---
    tab_preview, tab_import = st.tabs(["📋 数据预览与配置", "🚀 执行导入"])

    with tab_preview:
        selected_sheet = st.selectbox("选择工作表", sheet_names, key="sheet_selector")
        if selected_sheet:
            try:
                df_raw = pd.read_excel(xl, sheet_name=selected_sheet)

                # 基本信息
                cols_info, cols_req = _get_column_requirements()
                actual_cols = list(df_raw.columns)
                missing = [c for c in cols_req if c not in actual_cols]

                st.markdown(f"""
                <div class="card">
                    <div class="result-summary" style="margin-top:0;">
                        <div class="result-item">
                            <div class="num">{len(df_raw):,}</div>
                            <div class="label">总行数</div>
                        </div>
                        <div class="result-item">
                            <div class="num">{len(actual_cols)}</div>
                            <div class="label">列数</div>
                        </div>
                        <div class="result-item">
                            <div class="num" style="color:{'#166534' if not missing else '#991b1b'};">{'✅' if not missing else '⚠️'} {len(missing)}</div>
                            <div class="label">缺失必填列</div>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                if missing:
                    st.markdown(
                        f'<div class="alert-box warning">⚠️ 缺少必填列：<code>{", ".join(missing)}</code>，可能导致部分数据无法导入</div>',
                        unsafe_allow_html=True
                    )

                # 列映射提示
                st.markdown('<div class="section-title"><span class="emoji">🔗</span> 列名映射</div>', unsafe_allow_html=True)
                mapping_col1, mapping_col2 = st.columns(2)
                with mapping_col1:
                    st.markdown("**识别到的列：**")
                    for c in actual_cols:
                        status = "✅" if c in cols_req else "🔹"
                        st.text(f"  {status} {c}")
                with mapping_col2:
                    st.markdown("**必填列清单：**")
                    for c in cols_req:
                        status = "✅" if c in actual_cols else "❌"
                        st.text(f"  {status} {c}")

                # 数据预览
                st.markdown('<div class="section-title"><span class="emoji">👁</span> 数据预览（前 10 行）</div>', unsafe_allow_html=True)
                preview_df = df_raw.head(10).fillna("")
                st.dataframe(preview_df, use_container_width=True, height=360)

                # 存入 session 供导入用
                st.session_state["upload_df"] = df_raw
                st.session_state["upload_sheet"] = selected_sheet
                st.session_state["upload_filename"] = uploaded_file.name

            except Exception as e:
                st.markdown(f'<div class="alert-box error">❌ 读取工作表失败：{e}</div>', unsafe_allow_html=True)

    with tab_import:
        if "upload_df" not in st.session_state:
            st.info("请先在「数据预览」中选择工作表确认数据。")
            return

        df = st.session_state["upload_df"]
        total_rows = len(df)

        st.markdown(f"""
        <div class="card">
            <div class="alert-box info">📌 即将导入 <strong>{total_rows:,}</strong> 行数据 · 文件：<strong>{uploaded_file.name}</strong> · 工作表：<strong>{st.session_state.get('upload_sheet', '')}</strong></div>
        """, unsafe_allow_html=True)

        # 导入选项
        opt_col1, opt_col2, opt_col3 = st.columns(3)
        with opt_col1:
            skip_duplicates = st.checkbox("跳过重复数据（基于 MD5 去重）", value=True,
                                          help="对每行数据生成 MD5，已存在的记录会跳过")
        with opt_col2:
            batch_size = st.number_input("批次写入大小", min_value=100, max_value=10000,
                                         value=1000, step=500,
                                         help="每批写入的行数，大数据量建议调大以提升性能")
        with opt_col3:
            dry_run = st.checkbox("试运行（不实际写入）", value=False,
                                  help="只做校验和统计，不写入数据库")

        # 执行按钮
        do_import = st.button("🚀 开始导入", key="btn_do_import", type="primary",
                              use_container_width=True)

        if do_import:
            _execute_import(df, engine, uploaded_file.name, skip_duplicates, batch_size, dry_run)

        st.markdown("</div>", unsafe_allow_html=True)


def _execute_import(df, engine, filename, skip_duplicates, batch_size, dry_run):
    """执行实际的导入流程"""
    start_time = time.time()

    # 进度容器
    progress_placeholder = empty = st.empty()
    status_placeholder = st.empty()
    result_placeholder = st.empty()

    try:
        # Step 1: 校验 + 清洗
        status_placeholder.markdown('<div class="alert-box info">🔄 正在校验数据...</div>', unsafe_allow_html=True)
        df_clean, validation_result = _validate_and_cleanse(df)

        if validation_result["errors"] > 0:
            status_placeholder.markdown(
                f'<div class="alert-box warning">⚠️ 校验完成：{validation_result["errors"]} 行有问题被跳过'
                f'，{validation_result["valid"]} 行有效</div>',
                unsafe_allow_html=True
            )
        else:
            status_placeholder.markdown(
                f'<div class="alert-box success">✅ 校验通过：{validation_result["valid"]} 行数据有效</div>',
                unsafe_allow_html=True
            )

        if df_clean.empty:
            status_placeholder.markdown('<div class="alert-box error">❌ 没有有效数据可导入</div>', unsafe_allow_html=True)
            return

        # Step 2: 去重检查
        if skip_duplicates and not dry_run:
            status_placeholder.markdown('<div class="alert-box info">🔄 正在检查重复数据...</div>', unsafe_allow_html=True)
            df_clean = _deduplicate_data(df_clean, engine, status_placeholder)

        total_to_insert = len(df_clean)
        inserted = 0
        skipped = 0
        errors = []

        # Step 3: 分批写入
        if not dry_run:
            total_batches = (total_to_insert // batch_size) + (1 if total_to_insert % batch_size else 0)

            for i in range(0, total_to_insert, batch_size):
                batch = df_clean.iloc[i:i + batch_size]
                batch_num = i // batch_size + 1

                # 更新进度
                pct = min(100, int((i / total_to_insert) * 100))
                progress_placeholder.markdown(f"""
                <div class="progress-container"><div class="progress-bar" style="width:{pct}%"></div></div>
                <div style="text-align:center;font-size:0.85rem;color:#64748b;">
                    写入中... 第 {batch_num}/{total_batches} 批 ({pct}%)
                </div>
                """, unsafe_allow_html=True)

                try:
                    n = _insert_batch(batch, engine)
                    inserted += n
                    skipped += len(batch) - n
                except Exception as err:
                    errors.append(f"批次{batch_num}: {err}")

            # 进度到 100%
            progress_placeholder.markdown("""
            <div class="progress-container"><div class="progress-bar" style="width:100%"></div></div>
            """, unsafe_allow_html=True)

        elapsed = time.time() - start_time

        # 记录日志
        if not dry_run:
            try:
                file_hash = _compute_file_hash(filename, str(date.today()))
                record_upload_log(
                    engine, filename=filename, sheet_name=st.session_state.get("upload_sheet", ""),
                    rows_total=len(df), rows_valid=validation_result["valid"],
                    rows_error=validation_result["errors"], rows_inserted=inserted,
                    rows_duplicate=skipped, file_hash=file_hash, status="success" if not errors else "partial",
                    error_message="; ".join(errors) if errors else None
                )
            except Exception:
                pass  # 日志失败不影响主流程

        # 展示结果
        _render_import_result(result_placeholder, {
            "total": len(df),
            "valid": validation_result["valid"],
            "error_rows": validation_result["errors"],
            "inserted": inserted,
            "skipped": skipped,
            "dry_run": dry_run,
            "elapsed": elapsed,
            "errors": errors
        })

    except Exception as e:
        status_placeholder.markdown(f'<div class="alert-box error">❌ 导入异常中断：{e}</div>', unsafe_allow_html=True)
        logger.error(f"导入失败: {e}", exc_info=True)


def _render_import_result(container, result):
    """渲染导入结果摘要"""
    status_tag = "试运行" if result["dry_run"] else ("成功" if not result["errors"] else "部分成功")
    status_class = "status-info" if result["dry_run"] else ("status-success" if not result["errors"] else "status-warning")

    container.markdown(f"""
    <div class="card">
        <div class="section-title"><span class="emoji">📋</span> 导入结果 · <span class="status-badge {status_class}">{status_tag}</span></div>

        <div class="result-summary">
            <div class="result-item">
                <div class="num">{result['total']:,}</div>
                <div class="label">原始行数</div>
            </div>
            <div class="result-item">
                <div class="num">{result['valid']:,}</div>
                <div class="label">有效行数</div>
            </div>
            <div class="result-item">
                <div class="num" style="color:#991b1b;">{result['error_rows']:,}</div>
                <div class="label">校验跳过</div>
            </div>
            <div class="result-item">
                <div class="num" style="color:#166534;">{'—' if result['dry_run'] else f'{result["inserted"]:,}'}</div>
                <div class="label">新插入</div>
            </div>
            <div class="result-item">
                <div class="num" style="color:#854d0e;">{'—' if result['dry_run'] else f'{result["skipped"]:,}'}</div>
                <div class="label">重复跳过</div>
            </div>
            <div class="result-item">
                <div class="num">{result['elapsed']:.1f}s</div>
                <div class="label">耗时</div>
            </div>
        </div>
    """, unsafe_allow_html=True)

    if result["errors"]:
        container.markdown("""
        <div style="margin-top:14px;">
        <details><summary style="cursor:pointer;color:#ef4444;font-weight:500;">⚠️ 错误详情（{} 条）</summary>
        """.format(len(result["errors"])), unsafe_allow_html=True)
        for err in result["errors"]:
            container.text(f"  • {err}")
        container.markdown("</details></div>", unsafe_allow_html=True)

    container.markdown("</div>", unsafe_allow_html=True)


# ============================================================
#  辅助函数
# ============================================================
def _get_column_requirements():
    """返回列信息：(全部字段列表, 必填字段列表)"""
    all_cols = [
        "comment_id",       # 评论 ID
        "content",          # 评论内容
        "user_id",          # 用户 ID
        "nick_name",        # 昵称
        "create_time",      # 创建时间
        "qa_owner_name",    # 审核员
        "qc_result",        # 质检结果
        "qc_category",      # 违规分类
        "is_violation",     # 是否违规
    ]
    required = ["comment_id", "content", "create_time", "qa_owner_name"]
    return all_cols, required


def _validate_and_cleanse(df):
    """数据校验与清洗，返回 (清洗后df, 统计字典)"""
    all_cols, req_cols = _get_column_requirements()
    original_count = len(df)
    df = df.copy()

    # 标准化列名（去除首尾空格）
    df.columns = [str(c).strip() for c in df.columns]

    # 必填项非空检查
    for col in req_cols:
        if col in df.columns:
            df = df[df[col].notna() & (df[col].astype(str).str.strip() != "")]

    # 时间字段标准化
    time_fields = ["create_time", "update_time", "qa_event_time"]
    for tf in time_fields:
        if tf in df.columns:
            try:
                df[tf] = pd.to_datetime(df[tf], errors="coerce")
                df = df[df[tf].notna()]
            except Exception:
                pass

    valid_count = len(df)
    error_count = original_count - valid_count

    return df, {"valid": valid_count, "errors": error_count}


def _deduplicate_data(df, engine, status_container=None):
    """MD5 去重：跳过数据库中已存在的记录"""
    if status_container:
        status_container.markdown('<div class="alert-box info">🔄 正在计算 MD5 并比对去重...</div>', unsafe_allow_html=True)

    def row_md5(row):
        row_str = "|".join([str(row.get(c, "")) for c in sorted(df.columns)])
        return hashlib.md5(row_str.encode("utf-8")).hexdigest()

    df["_md5"] = df.apply(row_md5, axis=1)

    # 检查已存在
    existing_md5s = set()
    try:
        if fact_comment_exists(engine):
            import sqlalchemy as sa
            with engine.connect() as conn:
                # 分批查避免内存爆炸
                    batch_md5s = df["_md5"].iloc[i:i+5000].tolist()
                    result = conn.execute(
                        sa.text("SELECT DISTINCT file_hash FROM fact_file_dedup WHERE file_hash IN :md5s"),
                        {"md5s": tuple(batch_md5s)}
                    )
                    existing_md5s.update(r[0] for r in result.fetchall())
    except Exception as e:
        logger.warning(f"去重查询失败，跳过去重: {e}")

    if existing_md5s:
        before_dedup = len(df)
        df = df[~df["_md5"].isin(existing_md5s)].copy()
        if status_container:
            status_container.markdown(
                f'<div class="alert-box warning">🔍 去重完成：跳过 {before_dedup - len(df)} 条已存在记录</div>',
                unsafe_allow_html=True
            )

    df.drop(columns=["_md5"], inplace=True, errors="ignore")
    return df


def _insert_batch(batch_df, engine):
    """单批写入，返回成功行数"""
    try:
        # 判断是评论数据还是质检事件数据
        if "qc_result" in batch_df.columns or "qc_category" in batch_df.columns:
            return insert_fact_qa_events(engine, batch_df.to_dict("records"))
        else:
            return insert_fact_comments(engine, batch_df.to_dict("records"))
    except Exception as e:
        logger.error(f"批量写入失败: {e}")
        raise


def _compute_file_hash(filename: str, date_str: str) -> str:
    raw = f"{filename}_{date_str}"
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def _render_upload_history(engine):
    """底部：最近上传记录"""
    st.markdown("---")
    st.markdown('<div class="card"><div class="section-title"><span class="emoji">📜</span> 最近上传记录</div></div>', unsafe_allow_html=True)

    try:
        import sqlalchemy as sa
        with engine.connect() as conn:
            result = conn.execute(
                sa.text("""
                    SELECT filename, sheet_name, rows_total, rows_valid, rows_inserted,
                           rows_duplicate, status, created_at
                    FROM fact_upload_log
                    ORDER BY created_at DESC
                    LIMIT 15
                """)
            )
            records = result.fetchall()

            if records:
                history_data = []
                for r in records:
                    history_data.append({
                        "文件名": r[0] or "",
                        "工作表": r[1] or "",
                        "总行数": r[2] or 0,
                        "有效行": r[3] or 0,
                        "插入": r[4] or 0,
                        "重复跳过": r[5] or 0,
                        "状态": r[6] or "",
                        "时间": str(r[7])[:19] if r[7] else "",
                    })

                hist_df = pd.DataFrame(history_data)
                st.dataframe(hist_df, use_container_width=True, height=280)
            else:
                st.markdown('<p style="color:#94a3b8;text-align:center;padding:30px;">暂无上传记录</p>', unsafe_allow_html=True)

    except Exception as e:
        st.caption(f"无法加载历史记录：{e}")


# 入口
if __name__ == "__main__":
    render()
