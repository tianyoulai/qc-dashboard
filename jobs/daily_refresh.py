#!/usr/bin/env python3
"""一键刷新：扫描企微缓存文件 → 拉取 Google Sheet 申诉数据 → 导入 → 刷新数仓 → 刷新告警。"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
# 自动检测 Python 解释器：优先 .venv，否则用 sys.executable
_VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
VENV_PYTHON = str(_VENV_PYTHON) if _VENV_PYTHON.exists() else sys.executable
DEFAULT_DB = ""

# 企微缓存目录路径（macOS）
WEWORK_CACHE_BASE = Path.home() / "Library" / "Containers" / "com.tencent.WeWorkMac" / "Data" / "Documents" / "Profiles"
# 质检文件名关键词（用于识别质检文件）
QA_FILE_KEYWORDS = ["质检", "ilabel", "label"]
# 排除关键词（申诉、周报、新人培训、图片质检等非目标文件）
EXCLUDE_KEYWORDS = ["申诉", "周报", "月报", "汇总", "新人培训", "图片质检", "10816"]


def run_job(script: str, extra_args: list[str] | None = None) -> subprocess.CompletedProcess:
    cmd = [str(VENV_PYTHON), str(PROJECT_ROOT / "jobs" / script)]
    if extra_args:
        cmd.extend(extra_args)
    print(f"\n{'='*60}")
    print(f"  执行: {' '.join(cmd)}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(PROJECT_ROOT))
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(f"[STDERR] {result.stderr}", file=sys.stderr)
    if result.returncode != 0:
        print(f"❌ {script} 执行失败 (exit code {result.returncode})", file=sys.stderr)
    else:
        print(f"✅ {script} 执行成功")
    return result


def find_all_wework_cache_dirs() -> list[Path]:
    """查找所有企微缓存目录（可能有多个 Profile）。"""
    if not WEWORK_CACHE_BASE.exists():
        return []
    
    cache_dirs = []
    for profile_dir in WEWORK_CACHE_BASE.iterdir():
        caches_path = profile_dir / "Caches" / "Files"
        if caches_path.exists():
            cache_dirs.append(caches_path)
    
    return cache_dirs


def scan_wework_qa_files(
    scan_hours: int = 24,
    target_date: date | None = None,
) -> list[Path]:
    """扫描企微缓存目录中今天下载的质检文件。
    
    Args:
        scan_hours: 扫描最近多少小时内修改的文件
        target_date: 目标业务日期（用于匹配文件名中的日期），默认今天
    
    Returns:
        符合条件的质检文件路径列表
    """
    cache_dirs = find_all_wework_cache_dirs()
    if not cache_dirs:
        print("⚠️ 未找到企微缓存目录")
        return []
    
    target_date = target_date or date.today()
    cutoff_time = datetime.now() - timedelta(hours=scan_hours)
    
    qa_files = []
    
    for cache_dir in cache_dirs:
        # 遍历月份目录（如 2026-03）
        for month_dir in cache_dir.iterdir():
            if not month_dir.is_dir():
                continue
            
            # 遍历文件
            for file_path in month_dir.rglob("*.xlsx"):
                # 检查文件修改时间
                mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                if mtime < cutoff_time:
                    continue
                
                file_name = file_path.name
                
                # 排除申诉、周报等
                if any(kw in file_name for kw in EXCLUDE_KEYWORDS):
                    continue
                
                # 匹配质检文件关键词
                if not any(kw.lower() in file_name.lower() for kw in QA_FILE_KEYWORDS):
                    continue
                
                # 匹配文件名中的日期（支持多种格式）
                # 格式1: 2026.3.29 或 2026.03.29
                # 格式2: 0329 或 3.29
                date_match = extract_date_from_filename(file_name, target_date.year)
                if date_match == target_date:
                    qa_files.append(file_path)
    
    return sorted(set(qa_files))


def extract_date_from_filename(filename: str, year: int) -> date | None:
    """从文件名提取业务日期。"""
    # 匹配 YYYY.M.D 或 YYYY.MM.DD
    match = re.search(r'(\d{4})\.(\d{1,2})\.(\d{1,2})', filename)
    if match:
        try:
            return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
        except ValueError:
            pass
    
    # 匹配 MMDD（如 0329）
    match = re.search(r'(?:^|[^\d])(\d{2})(\d{2})(?:[^\d]|$)', filename)
    if match:
        try:
            return date(year, int(match.group(1)), int(match.group(2)))
        except ValueError:
            pass
    
    # 匹配 M.D（如 3.29）
    match = re.search(r'(?:^|[^\d])(\d{1,2})\.(\d{1,2})(?:[^\d\.]|$)', filename)
    if match:
        try:
            return date(year, int(match.group(1)), int(match.group(2)))
        except ValueError:
            pass
    
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="一键刷新：扫描企微缓存 + 拉取申诉 + 导入 + 刷新数仓 + 刷新告警")
    parser.add_argument("--qa-files", nargs="*", default=[], help="额外质检文件路径")
    parser.add_argument("--appeal-files", nargs="*", default=[], help="额外申诉文件路径（默认拉取 Google Sheet）")
    parser.add_argument("--skip-gsheet", action="store_true", help="跳过 Google Sheet 拉取")
    parser.add_argument("--skip-alerts", action="store_true", help="跳过告警刷新")
    parser.add_argument("--skip-validate", action="store_true", help="跳过联表质量校验")
    parser.add_argument("--skip-wework-scan", action="store_true", help="跳过企微缓存目录扫描")
    parser.add_argument("--wework-scan-hours", type=int, default=24, help="扫描企微缓存最近多少小时的文件（默认24）")
    parser.add_argument("--target-date", type=str, default=None, help="目标业务日期（YYYY-MM-DD），默认今天")
    parser.add_argument("--db-path", default="")
    args = parser.parse_args()

    # 解析目标日期
    target_date = date.today()
    if args.target_date:
        try:
            target_date = datetime.strptime(args.target_date, "%Y-%m-%d").date()
        except ValueError:
            print(f"⚠️ 无效的目标日期格式: {args.target_date}，使用今天")
    
    results: dict[str, dict] = {}
    all_ok = True

    # 0. 扫描企微缓存目录
    scanned_qa_files = []
    if not args.skip_wework_scan:
        print(f"\n📂 扫描企微缓存目录（目标日期: {target_date}）...")
        scanned_qa_files = scan_wework_qa_files(
            scan_hours=args.wework_scan_hours,
            target_date=target_date,
        )
        if scanned_qa_files:
            print(f"  发现 {len(scanned_qa_files)} 个质检文件:")
            for f in scanned_qa_files:
                print(f"    - {f.name}")
        else:
            print("  未发现新的质检文件")
    else:
        print("\n⏭️ 跳过企微缓存目录扫描")

    # 1. 拉取 Google Sheet
    if not args.skip_gsheet:
        gsheet_result = run_job("pull_google_sheet.py", ["--import-to-db"])
        results["pull_google_sheet"] = {"ok": gsheet_result.returncode == 0}
        if gsheet_result.returncode != 0:
            all_ok = False
    else:
        print("\n⏭️ 跳过 Google Sheet 拉取")

    # 2. 导入文件（扫描到的 + 手动指定的）
    all_qa_files = scanned_qa_files + [Path(f) for f in args.qa_files]
    all_import_args = []
    for f in all_qa_files:
        all_import_args.extend(["--qa-file", str(f)])
    for f in args.appeal_files:
        all_import_args.extend(["--appeal-file", f])
    if all_import_args:
        import_result = run_job("import_fact_data.py", all_import_args)
        results["import_files"] = {"ok": import_result.returncode == 0}
        if import_result.returncode != 0:
            all_ok = False

    # 3. 刷新数仓
    warehouse_result = run_job("refresh_warehouse.py")
    results["refresh_warehouse"] = {"ok": warehouse_result.returncode == 0}
    if warehouse_result.returncode != 0:
        all_ok = False

    # 4. 刷新告警
    if not args.skip_alerts:
        alert_result = run_job("refresh_alerts.py")
        results["refresh_alerts"] = {"ok": alert_result.returncode == 0}
        if alert_result.returncode != 0:
            all_ok = False
    else:
        print("\n⏭️ 跳过告警刷新")

    # 5. 联表质量校验
    if not args.skip_validate:
        validate_result = run_job("validate_join_quality.py")
        results["validate_join"] = {"ok": validate_result.returncode == 0}
        if validate_result.returncode != 0:
            all_ok = False
    else:
        print("\n⏭️ 跳过联表质量校验")

    # 6. 数据质量监控（新增）
    quality_result = run_job("data_quality_check.py", ["--alert"])
    results["data_quality_check"] = {"ok": quality_result.returncode == 0}
    if quality_result.returncode != 0:
        print("⚠️ 数据质量检查发现异常，请查看告警")
        # 不影响整体流程，仅告警

    # 汇总
    print(f"\n{'='*60}")
    print(f"  刷新完成: {'✅ 全部成功' if all_ok else '⚠️ 部分失败'}")
    for step, info in results.items():
        print(f"    {step}: {'✅' if info['ok'] else '❌'}")
    print(f"{'='*60}")

    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
