"""数据导入页：上传质检 Excel / 申诉 CSV，一键刷新数仓和告警。"""
from __future__ import annotations

import hashlib
import tempfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from storage.repository import DashboardRepository

st.set_page_config(page_title="质培运营看板-数据导入", page_icon="📤", layout="wide")

repo = DashboardRepository()

# Hero 区域
st.markdown("""
<div style="margin-bottom: 1.5rem; padding: 1.5rem; background: #ffffff; border-radius: 1rem; border-left: 4px solid #2e7d32; box-shadow: 0 1px 3px rgba(0,0,0,0.1);">
    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 0.5rem;">
        <h1 style="margin: 0; font-size: 2rem; font-weight: 700; color: #1a1a1a;">📤 数据导入</h1>
    </div>
    <div style="font-size: 0.9rem; color: #4b5563; line-height: 1.6;">
        上传质检 Excel 或申诉 CSV · 自动入库并刷新数仓/告警 · 支持多文件批量上传
    </div>
</div>
""", unsafe_allow_html=True)

tab_import = st.tabs(["质检数据", "申诉数据", "Google Sheet", "一键刷新", "上传记录", "清除缓存", "清除数据"])


def preview_file_rows(file_obj, file_type: str) -> dict:
    """预览文件数据量。
    
    Returns:
        dict: {rows: int, columns: int, preview_df: DataFrame, error: str|None}
    """
    try:
        if file_type == "qa":
            # 质检文件：xlsx/xls
            df = pd.read_excel(file_obj, dtype=str, nrows=5)
            file_obj.seek(0)
            full_df = pd.read_excel(file_obj, dtype=str)
            file_obj.seek(0)
        else:
            # 申诉文件：csv
            try:
                df = pd.read_csv(file_obj, encoding="utf-8-sig", dtype=str, nrows=5)
            except UnicodeDecodeError:
                file_obj.seek(0)
                df = pd.read_csv(file_obj, encoding="gb18030", dtype=str, nrows=5)
            file_obj.seek(0)
            try:
                full_df = pd.read_csv(file_obj, encoding="utf-8-sig", dtype=str)
            except UnicodeDecodeError:
                file_obj.seek(0)
                full_df = pd.read_csv(file_obj, encoding="gb18030", dtype=str)
            file_obj.seek(0)
        
        return {
            "rows": len(full_df),
            "columns": len(full_df.columns),
            "preview_df": df,
            "error": None
        }
    except Exception as e:
        return {"rows": 0, "columns": 0, "preview_df": None, "error": str(e)}


def get_upload_history(limit: int = 20) -> list[dict]:
    """获取最近的上传记录。"""
    try:
        result = repo.fetch_df(
            """
            SELECT
                upload_id,
                upload_time,
                file_name,
                file_type,
                file_size_bytes,
                source_rows,
                inserted_rows,
                dedup_rows,
                business_line,
                upload_status,
                error_message
            FROM fact_upload_log
            ORDER BY upload_time DESC
            LIMIT %s
            """,
            [limit]
        )
        return result.to_dict(orient="records") if result is not None and not result.empty else []
    except Exception:
        return []


def check_file_exists(file_hash: str) -> dict | None:
    """检查文件是否已存在（用于前端提示）。"""
    try:
        result = repo.fetch_one(
            """
            SELECT file_name, first_upload_time, upload_count
            FROM fact_file_dedup
            WHERE file_hash = %s
            """,
            [file_hash]
        )
        return result if result else None
    except Exception:
        return None


def compute_file_hash_from_bytes(data: bytes) -> str:
    """计算文件内容哈希（适用于小文件）。"""
    return hashlib.sha256(data).hexdigest()


def compute_file_hash_chunked(file_obj, chunk_size: int = 8192) -> str:
    """分块计算文件哈希，避免大文件内存溢出。
    
    Args:
        file_obj: 文件对象（需支持 read() 和 seek()）
        chunk_size: 每次读取的块大小（字节），默认 8KB
    
    Returns:
        SHA256 哈希值（十六进制字符串）
    """
    hasher = hashlib.sha256()
    while True:
        chunk = file_obj.read(chunk_size)
        if not chunk:
            break
        hasher.update(chunk)
    file_obj.seek(0)  # 重置文件指针，允许后续读取
    return hasher.hexdigest()


# ==================== 质检数据 ====================
with tab_import[0]:
    st.markdown("### 上传质检 Excel")
    st.caption("支持 `.xlsx` / `.xls` 格式，可拖拽多个文件批量上传。")

    qa_files = st.file_uploader(
        "选择质检文件（支持拖拽多选）",
        type=["xlsx", "xls"],
        accept_multiple_files=True,
        key="qa_upload",
    )

    if qa_files:
        st.info(f"📋 已选择 {len(qa_files)} 个文件：")
        
        # 预览每个文件
        for f in qa_files:
            with st.expander(f"📄 {f.name} ({f.size / 1024:.1f} KB)", expanded=False):
                preview = preview_file_rows(f, "qa")
                if preview["error"]:
                    st.error(f"读取失败：{preview['error']}")
                else:
                    col1, col2, col3 = st.columns(3)
                    col1.metric("文件行数", f"{preview['rows']:,}")
                    col2.metric("列数", preview["columns"])
                    col3.metric("文件大小", f"{f.size / 1024:.1f} KB")
                    
                    # 检查是否已上传过
                    file_hash = compute_file_hash_from_bytes(f.getvalue())
                    existing = check_file_exists(file_hash)
                    if existing:
                        st.warning(
                            f"⚠️ 该文件已于 {existing['first_upload_time']} 上传过 "
                            f"（已上传 {existing['upload_count']} 次），导入时将自动跳过。"
                        )
                    
                    if preview["preview_df"] is not None and not preview["preview_df"].empty:
                        st.markdown("**数据预览（前5行）：**")
                        st.dataframe(preview["preview_df"].head(), use_container_width=True)

    if st.button("批量导入质检数据", key="import_qa", disabled=(not qa_files)):
        success_count = 0
        fail_count = 0
        skip_count = 0
        logs = []

        progress_bar = st.progress(0)
        status_text = st.empty()

        for idx, qa_file in enumerate(qa_files):
            status_text.text(f"正在导入 {qa_file.name}...")
            progress_bar.progress((idx + 1) / len(qa_files))

            with tempfile.NamedTemporaryFile(suffix=Path(qa_file.name).suffix, delete=False) as tmp:
                tmp.write(qa_file.getvalue())
                tmp_path = tmp.name

            try:
                import subprocess
                result = subprocess.run(
                    [".venv/bin/python", "jobs/import_fact_data.py", "--qa-file", tmp_path, "--source-name", qa_file.name],
                    capture_output=True,
                    text=True,
                    cwd=Path(__file__).parent.parent,
                )
                if result.returncode == 0:
                    # 解析结果判断是否跳过
                    import json
                    try:
                        output = json.loads(result.stdout)
                        inserted = output.get("qa_files", [{}])[0].get("inserted_rows", 0)
                        if inserted == 0:
                            skip_count += 1
                            logs.append(f"⏭️ {qa_file.name} 已跳过（文件内容已存在）")
                        else:
                            success_count += 1
                            logs.append(f"✅ {qa_file.name} 导入成功（{inserted} 行）")
                    except Exception:
                        success_count += 1
                        logs.append(f"✅ {qa_file.name} 导入成功")
                else:
                    fail_count += 1
                    logs.append(f"❌ {qa_file.name} 导入失败：{result.stderr[:200]}")
            finally:
                Path(tmp_path).unlink(missing_ok=True)

        progress_bar.empty()
        status_text.empty()

        if success_count > 0:
            st.success(f"成功导入 {success_count} 个文件" + (f"，{skip_count} 个跳过" if skip_count > 0 else "") + (f"，{fail_count} 个失败" if fail_count > 0 else ""))
        elif skip_count > 0:
            st.warning(f"所有文件均已上传过，跳过 {skip_count} 个文件")
        else:
            st.error("所有文件导入失败")

        with st.expander("查看导入日志", expanded=(fail_count > 0)):
            st.code("\n".join(logs), language="text")

    st.markdown("---")
    st.caption("💡 提示：导入后需要点击「一键刷新」来更新数仓和告警。")

# ==================== 申诉数据 ====================
with tab_import[1]:
    st.markdown("### 上传申诉 CSV")
    st.caption("支持 `.csv` 格式（UTF-8 / GBK 编码），可拖拽多个文件批量上传。")

    appeal_files = st.file_uploader(
        "选择申诉文件（支持拖拽多选）",
        type=["csv"],
        accept_multiple_files=True,
        key="appeal_upload",
    )

    if appeal_files:
        st.info(f"📋 已选择 {len(appeal_files)} 个文件：")
        
        for f in appeal_files:
            with st.expander(f"📄 {f.name} ({f.size / 1024:.1f} KB)", expanded=False):
                preview = preview_file_rows(f, "appeal")
                if preview["error"]:
                    st.error(f"读取失败：{preview['error']}")
                else:
                    col1, col2, col3 = st.columns(3)
                    col1.metric("文件行数", f"{preview['rows']:,}")
                    col2.metric("列数", preview["columns"])
                    col3.metric("文件大小", f"{f.size / 1024:.1f} KB")
                    
                    # 检查是否已上传过（使用分块哈希避免大文件内存溢出）
                    file_hash = compute_file_hash_chunked(f)
                    existing = check_file_exists(file_hash)
                    if existing:
                        st.warning(
                            f"⚠️ 该文件已于 {existing['first_upload_time']} 上传过 "
                            f"（已上传 {existing['upload_count']} 次），导入时将自动跳过。"
                        )
                    
                    if preview["preview_df"] is not None and not preview["preview_df"].empty:
                        st.markdown("**数据预览（前5行）：**")
                        st.dataframe(preview["preview_df"].head(), use_container_width=True)

    if st.button("批量导入申诉数据", key="import_appeal", disabled=(not appeal_files)):
        success_count = 0
        fail_count = 0
        skip_count = 0
        logs = []

        progress_bar = st.progress(0)
        status_text = st.empty()

        for idx, appeal_file in enumerate(appeal_files):
            status_text.text(f"正在导入 {appeal_file.name}...")
            progress_bar.progress((idx + 1) / len(appeal_files))

            with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
                tmp.write(appeal_file.getvalue())
                tmp_path = tmp.name

            try:
                import subprocess
                result = subprocess.run(
                    [".venv/bin/python", "jobs/import_fact_data.py", "--appeal-file", tmp_path, "--source-name", appeal_file.name],
                    capture_output=True,
                    text=True,
                    cwd=Path(__file__).parent.parent,
                )
                if result.returncode == 0:
                    import json
                    try:
                        output = json.loads(result.stdout)
                        inserted = output.get("appeal_files", [{}])[0].get("inserted_rows", 0)
                        if inserted == 0:
                            skip_count += 1
                            logs.append(f"⏭️ {appeal_file.name} 已跳过（文件内容已存在）")
                        else:
                            success_count += 1
                            logs.append(f"✅ {appeal_file.name} 导入成功（{inserted} 行）")
                    except Exception:
                        success_count += 1
                        logs.append(f"✅ {appeal_file.name} 导入成功")
                else:
                    fail_count += 1
                    logs.append(f"❌ {appeal_file.name} 导入失败：{result.stderr[:200]}")
            finally:
                Path(tmp_path).unlink(missing_ok=True)

        progress_bar.empty()
        status_text.empty()

        if success_count > 0:
            st.success(f"成功导入 {success_count} 个文件" + (f"，{skip_count} 个跳过" if skip_count > 0 else "") + (f"，{fail_count} 个失败" if fail_count > 0 else ""))
        elif skip_count > 0:
            st.warning(f"所有文件均已上传过，跳过 {skip_count} 个文件")
        else:
            st.error("所有文件导入失败")

        with st.expander("查看导入日志", expanded=(fail_count > 0)):
            st.code("\n".join(logs), language="text")

    st.markdown("---")
    st.caption("💡 提示：申诉数据也可以通过「Google Sheet」标签页直接拉取，无需手动上传。")

# ==================== Google Sheet ====================
with tab_import[2]:
    st.markdown("### 从 Google Sheet 拉取申诉数据")
    st.caption("直接从配置好的 Google Sheet 拉取最新申诉数据，无需手动下载。")

    if st.button("拉取 Google Sheet", key="pull_gsheet"):
        with st.spinner("正在拉取 Google Sheet..."):
            import subprocess
            result = subprocess.run(
                [".venv/bin/python", "jobs/pull_google_sheet.py"],
                capture_output=True,
                text=True,
                cwd=Path(__file__).parent.parent,
            )
            if result.returncode == 0:
                st.success("Google Sheet 拉取成功！")
                with st.expander("查看拉取日志", expanded=False):
                    st.code(result.stdout, language="text")
            else:
                st.error(f"拉取失败：\n{result.stderr}")

    st.markdown("---")
    st.caption("💡 提示：拉取后的数据会自动导入到 `fact_appeal_event` 表。")

# ==================== 一键刷新 ====================
with tab_import[3]:
    st.markdown("### 一键刷新数仓和告警")
    st.caption("执行 `jobs/daily_refresh.py`，依次：拉取 Google Sheet → 刷新数仓 → 刷新告警 → 联表校验。")

    col_skip, col_run = st.columns([1, 2])
    with col_skip:
        skip_gsheet = st.checkbox("跳过 Google Sheet 拉取", value=False)

    with col_run:
        if st.button("执行一键刷新", key="run_refresh"):
            with st.spinner("正在刷新全链路..."):
                # 使用直接函数调用（兼容 Streamlit Cloud 和本地环境）
                try:
                    from jobs.daily_refresh import main as _refresh_main
                    import sys as _sys
                    from io import StringIO

                    # 捕获 daily_refresh 的输出
                    old_stdout = _sys.stdout
                    old_stderr = _sys.stderr
                    captured_out = StringIO()
                    captured_err = StringIO()
                    try:
                        _sys.stdout = captured_out
                        _sys.stderr = captured_err
                        # 传入 --skip-gsheet 参数（通过 sys.argv）
                        old_argv = _sys.argv
                        _sys.argv = ["daily_refresh.py"]
                        if skip_gsheet:
                            _sys.argv.append("--skip-gsheet")
                        _refresh_main()
                        _sys.argv = old_argv
                        exit_code = 0
                    except SystemExit as e:
                        exit_code = e.code if e.code is not None else 0
                    finally:
                        _sys.stdout = old_stdout
                        _sys.stderr = old_stderr

                    stdout_val = captured_out.getvalue()
                    stderr_val = captured_err.getvalue()

                    if exit_code == 0:
                        st.success("一键刷新完成！")
                        with st.expander("查看刷新日志", expanded=False):
                            st.code(stdout_val, language="text")
                            if stderr_val.strip():
                                st.warning(stderr_val)
                    else:
                        st.error(f"刷新失败 (exit code {exit_code})：\n{stderr_val or stdout_val}")
                except Exception as e:
                    # fallback：如果 import 失败，尝试 subprocess（用于本地开发）
                    import subprocess, shutil, sys
                    python_exe = (
                        Path(sys.executable)  # 当前 Python 解释器
                    )
                    args = [str(python_exe), "jobs/daily_refresh.py"]
                    if skip_gsheet:
                        args.append("--skip-gsheet")

                    result = subprocess.run(
                        args,
                        capture_output=True,
                        text=True,
                        cwd=Path(__file__).parent.parent,
                    )
                    if result.returncode == 0:
                        st.success("一键刷新完成！")
                        with st.expander("查看刷新日志", expanded=False):
                            st.code(result.stdout, language="text")
                    else:
                        st.error(f"刷新失败：\n{result.stderr}")

    st.markdown("---")
    st.caption("💡 提示：定时任务每天 13:00 会自动同步数据，15:00 推送日报，通常不需要手动触发。")

# ==================== 上传记录 ====================
with tab_import[4]:
    st.markdown("### 📜 上传记录")
    st.caption("查看最近 50 条文件上传记录。")

    history = get_upload_history(limit=50)
    
    if not history:
        st.info("暂无上传记录")
    else:
        # 转换为 DataFrame 展示
        history_df = pd.DataFrame(history)
        history_df["upload_time"] = pd.to_datetime(history_df["upload_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        history_df["file_size"] = history_df["file_size_bytes"].apply(lambda x: f"{x / 1024:.1f} KB" if x else "-")
        
        # 状态颜色标记
        def status_icon(status):
            if status == "success":
                return "✅ 成功"
            elif status == "skipped":
                return "⏭️ 跳过"
            else:
                return "❌ 失败"
        
        history_df["状态"] = history_df["upload_status"].apply(status_icon)
        
        # 显示表格
        display_cols = ["upload_time", "file_name", "file_type", "file_size", "source_rows", "inserted_rows", "business_line", "状态"]
        st.dataframe(
            history_df[display_cols].rename(columns={
                "upload_time": "上传时间",
                "file_name": "文件名",
                "file_type": "类型",
                "file_size": "大小",
                "source_rows": "源行数",
                "inserted_rows": "入库行数",
                "business_line": "业务线",
            }),
            use_container_width=True,
            hide_index=True
        )
        
        # 导出按钮
        csv = history_df.to_csv(index=False).encode("utf-8-sig")
        st.download_button(
            label="📥 导出上传记录 (CSV)",
            data=csv,
            file_name=f"upload_history_{date.today():%Y%m%d}.csv",
            mime="text/csv"
        )

# ==================== 清除缓存 ====================
with tab_import[5]:
    st.markdown("### 清除 Streamlit 缓存")
    st.caption("如果看板数据展示异常（如组别名称未更新），可以清除缓存后刷新页面。")

    if st.button("清除缓存", key="clear_cache"):
        st.cache_data.clear()
        st.success("缓存已清除！请刷新浏览器页面（F5 或 Cmd+R）查看最新数据。")

    st.markdown("---")
    st.warning("⚠️ 清除缓存不会影响数据库数据，只是清除 Streamlit 的展示缓存。")

# ==================== 清除数据 ====================
with tab_import[6]:
    st.markdown("### 清除质检数据")
    st.caption("根据日期范围删除质检数据，或全部清空。⚠️ 此操作不可逆，请谨慎操作。")

    # 显示当前数据量
    total_cnt = repo.fetch_one("SELECT COUNT(*) AS cnt FROM fact_qa_event")["cnt"]
    st.info(f"📊 当前质检数据总量：**{total_cnt:,}** 条")

    # 清除方式选择
    clear_mode = st.radio("清除方式", ["按日期范围清除", "全部清除"], horizontal=True, key="clear_mode")

    if clear_mode == "按日期范围清除":
        col1, col2 = st.columns(2)
        with col1:
            date_start = st.date_input("开始日期", value=date.today(), key="clear_start")
        with col2:
            date_end = st.date_input("结束日期", value=date.today(), key="clear_end")

        if date_start > date_end:
            st.error("开始日期不能晚于结束日期")
        else:
            # 预览要删除的数据
            preview_cnt = repo.fetch_one(
                "SELECT COUNT(*) AS cnt FROM fact_qa_event WHERE biz_date >= %s AND biz_date <= %s",
                [date_start, date_end]
            )["cnt"]

            st.warning(f"将删除 **{date_start}** 至 **{date_end}** 的质检数据，共 **{preview_cnt:,}** 条")

            confirm = st.checkbox(f"我确认要删除这 {preview_cnt:,} 条数据", key="confirm_range")

            if st.button("删除选中日期的数据", key="delete_range", disabled=(not confirm or preview_cnt == 0)):
                with st.spinner("正在删除数据..."):
                    # 删除 fact 表数据
                    repo.execute(
                        "DELETE FROM fact_qa_event WHERE biz_date >= %s AND biz_date <= %s",
                        [date_start, date_end]
                    )
                    
                    # 清除日维度 mart 表
                    repo.execute("DELETE FROM mart_day_group WHERE biz_date >= %s AND biz_date <= %s", [date_start, date_end])
                    repo.execute("DELETE FROM mart_day_queue WHERE biz_date >= %s AND biz_date <= %s", [date_start, date_end])
                    repo.execute("DELETE FROM mart_day_error_topic WHERE biz_date >= %s AND biz_date <= %s", [date_start, date_end])
                    
                    # 清除周维度 mart 表（week_begin_date 对应周一，需扩展范围）
                    week_start = date_start - timedelta(days=date_start.weekday())
                    week_end = date_end - timedelta(days=date_end.weekday()) + timedelta(days=6)
                    repo.execute("DELETE FROM mart_week_group WHERE week_begin_date >= %s AND week_begin_date <= %s", [week_start, week_end])
                    repo.execute("DELETE FROM mart_week_queue WHERE week_begin_date >= %s AND week_begin_date <= %s", [week_start, week_end])
                    repo.execute("DELETE FROM mart_week_error_topic WHERE week_begin_date >= %s AND week_begin_date <= %s", [week_start, week_end])
                    
                    # 清除月维度 mart 表（month_begin_date 对应每月 1 号）
                    month_start = date_start.replace(day=1)
                    month_end = (date_end.replace(day=1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
                    month_end_begin = month_end.replace(day=1)
                    repo.execute("DELETE FROM mart_month_group WHERE month_begin_date >= %s AND month_begin_date <= %s", [month_start, month_end_begin])
                    repo.execute("DELETE FROM mart_month_queue WHERE month_begin_date >= %s AND month_begin_date <= %s", [month_start, month_end_begin])
                    repo.execute("DELETE FROM mart_month_error_topic WHERE month_begin_date >= %s AND month_begin_date <= %s", [month_start, month_end_begin])
                    
                    # 清空告警，等待刷新
                    repo.execute("DELETE FROM fact_alert_event WHERE 1=1")
                    
                    # 清空文件去重记录和上传日志，允许重新上传
                    repo.execute("DELETE FROM fact_file_dedup")
                    repo.execute("DELETE FROM fact_upload_log")

                    st.success(f"已删除 {preview_cnt:,} 条数据！请点击「一键刷新」重新生成数仓和告警。")
                    st.cache_data.clear()

    else:  # 全部清除
        st.error(f"⚠️ 警告：将删除全部 **{total_cnt:,}** 条质检数据，此操作不可逆！")

        confirm_all = st.checkbox(f"我确认要删除全部 {total_cnt:,} 条数据", key="confirm_all")

        if st.button("全部清空", key="delete_all", disabled=(not confirm_all or total_cnt == 0)):
            with st.spinner("正在清空数据..."):
                # 清空所有相关表（使用事务确保原子性）
                tables = [
                    "fact_qa_event",
                    "fact_appeal_event",
                    "mart_day_group",
                    "mart_week_group",
                    "mart_month_group",
                    "mart_day_queue",
                    "mart_week_queue",
                    "mart_month_queue",
                    "mart_day_error_topic",
                    "mart_week_error_topic",
                    "mart_month_error_topic",
                    "fact_alert_event",
                    "fact_alert_status",
                    "fact_alert_status_history",
                    "fact_file_dedup",
                    "fact_upload_log",
                ]
                sql_list = [(f"DELETE FROM {tbl}", None) for tbl in tables]
                
                try:
                    repo.execute_in_transaction(sql_list)
                    st.success("数据已全部清空！请重新上传数据并刷新。")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"清空数据失败：{str(e)}")

    st.markdown("---")
    st.caption("💡 提示：清除数据后，需要重新执行「一键刷新」来重建数仓和告警。")
