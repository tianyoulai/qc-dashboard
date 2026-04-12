#!/usr/bin/env python3
"""
一次性导入 3/1-3/27 的历史质检数据。

用法：
    .venv/bin/python jobs/import_march_data.py --dry-run  # 仅扫描，不导入
    .venv/bin/python jobs/import_march_data.py            # 执行导入
"""
from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage.tidb_manager import TiDBManager

# 导入日期提取函数
from jobs.import_fact_data import extract_date_from_filename


# ==========================================================
# 配置
# ==========================================================

WEWORK_CACHE_ROOT = Path.home() / "Library/Containers/com.tencent.WeWorkMac/Data/Documents/Profiles"

# 目标关键词
TARGET_KEYWORDS = ["长沙云雀", "迁移人力ilabel", "迁移人力账号"]

# 排除关键词
EXCLUDE_KEYWORDS = ["图片", "模板", "~$", "新人培训", "新人"]

# 排除带数字编号的文件（如"10816"这类，5位以上纯数字）
def has_number_code(filename):
    """检查文件名是否包含5位以上纯数字编号（排除日期）"""
    import re
    # 匹配5位以上连续数字（日期格式已排除）
    matches = re.findall(r'\d{5,}', filename)
    return len(matches) > 0


# ==========================================================
# 工具函数
# ==========================================================


def load_config() -> dict[str, Any]:
    """加载项目配置。"""
    config_path = PROJECT_ROOT / "config/settings.json"
    if config_path.exists():
        with open(config_path, encoding="utf-8") as f:
            return json.load(f)
    return {}


def compute_file_hash(file_path: Path) -> str:
    """计算文件内容的 SHA256 哈希（与 import_fact_data.py 保持一致）。"""
    hasher = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def scan_target_files() -> list[dict[str, Any]]:
    """扫描企微缓存目录，返回 3/1-3/27 的目标文件。"""
    matched_files = []

    if not WEWORK_CACHE_ROOT.exists():
        print(f"⚠️ 企微缓存目录不存在：{WEWORK_CACHE_ROOT}")
        return matched_files

    # 遍历所有 ProfileID 目录
    for profile_dir in WEWORK_CACHE_ROOT.iterdir():
        if not profile_dir.is_dir():
            continue

        cache_dir = profile_dir / "Caches" / "Files"
        if not cache_dir.exists():
            continue

        # 遍历月份目录
        for month_dir in cache_dir.iterdir():
            if not month_dir.is_dir():
                continue

            # 遍历文件
            for file_dir in month_dir.iterdir():
                if not file_dir.is_dir():
                    continue

                for file_path in file_dir.glob("*.xlsx"):
                    file_name = file_path.name

                    # 检查是否匹配目标关键词
                    matched_keyword = None
                    for keyword in TARGET_KEYWORDS:
                        if keyword in file_name:
                            matched_keyword = keyword
                            break

                    if not matched_keyword:
                        continue

                    # 排除不需要的文件
                    if any(exclude in file_name for exclude in EXCLUDE_KEYWORDS):
                        continue

                    # 排除带数字编号的文件（如"10816"）
                    if has_number_code(file_name):
                        continue

                    # 提取业务日期
                    biz_date = extract_date_from_filename(file_name)

                    # 只保留 3/1-3/27 的文件
                    if biz_date and date(2026, 3, 1) <= biz_date <= date(2026, 3, 27):
                        matched_files.append({
                            "file_path": file_path,
                            "file_name": file_name,
                            "file_hash": compute_file_hash(file_path),
                            "matched_keyword": matched_keyword,
                            "biz_date": biz_date,
                        })

    # 按日期和文件名排序
    matched_files.sort(key=lambda x: (x["biz_date"], x["file_name"]))
    return matched_files


def run_import(file_paths: list[Path]) -> tuple[int, int, list[str]]:
    """执行导入命令。"""
    success_count = 0
    error_count = 0
    errors = []

    for file_path in file_paths:
        try:
            cmd = [
                str(PROJECT_ROOT / ".venv/bin/python"),
                str(PROJECT_ROOT / "jobs/import_fact_data.py"),
                "--qa-file", str(file_path),
            ]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(PROJECT_ROOT),
                timeout=300,
            )

            if result.returncode == 0:
                success_count += 1
                print(f"✅ 导入成功：{file_path.name}")
            else:
                error_count += 1
                error_msg = f"{file_path.name}: {result.stderr[:200]}"
                errors.append(error_msg)
                print(f"❌ 导入失败：{error_msg}")
        except Exception as e:
            error_count += 1
            error_msg = f"{file_path.name}: {str(e)}"
            errors.append(error_msg)
            print(f"❌ 导入异常：{error_msg}")

    return success_count, error_count, errors


def run_refresh() -> bool:
    """执行数仓刷新。"""
    try:
        cmd = [
            str(PROJECT_ROOT / ".venv/bin/python"),
            str(PROJECT_ROOT / "jobs/refresh_warehouse.py"),
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd=str(PROJECT_ROOT),
            timeout=600,
        )

        if result.returncode == 0:
            print("✅ 数仓刷新成功")
            return True
        else:
            print(f"❌ 数仓刷新失败：{result.stderr[:200]}")
            return False
    except Exception as e:
        print(f"❌ 数仓刷新异常：{e}")
        return False


# ==========================================================
# 主函数
# ==========================================================


def main():
    parser = argparse.ArgumentParser(description="导入 3/1-3/27 的历史质检数据")
    parser.add_argument("--dry-run", action="store_true", help="仅扫描，不导入")
    args = parser.parse_args()

    print("=" * 60)
    print("📂 导入 3/1-3/27 历史质检数据")
    print("=" * 60)

    # 1. 扫描文件
    print("\n🔍 扫描企微缓存目录...")
    matched_files = scan_target_files()

    if not matched_files:
        print("✅ 没有找到匹配的文件")
        return

    # 按日期分组展示
    by_date = defaultdict(list)
    for f in matched_files:
        by_date[f["biz_date"]].append(f["file_name"])

    print(f"\n📋 找到 {len(matched_files)} 个匹配文件：")
    for d in sorted(by_date.keys()):
        files = by_date[d]
        print(f"\n  {d}:")
        for f in files:
            print(f"    - {f}")

    if args.dry_run:
        print("\n🔍 Dry-run 模式，不执行导入")
        return

    # 2. 执行导入
    print("\n🚀 开始导入...")
    file_paths = [f["file_path"] for f in matched_files]
    success_count, error_count, errors = run_import(file_paths)

    # 3. 刷新数仓
    print("\n🔄 刷新数仓...")
    refresh_success = run_refresh()

    # 4. 汇总统计
    print("\n" + "=" * 60)
    print("📊 导入汇总：")
    print(f"  - 成功：{success_count} 个")
    print(f"  - 失败：{error_count} 个")
    print(f"  - 数仓刷新：{'成功' if refresh_success else '失败'}")
    print("=" * 60)

    # 5. 查询数据库统计
    config = load_config()
    conn = TiDBManager()

    print("\n📈 数据库统计：")
    result = conn.fetch_df("""
        SELECT 
            biz_date,
            mother_biz,
            COUNT(*) as cnt
        FROM fact_qa_event
        WHERE biz_date BETWEEN '2026-03-01' AND '2026-03-27'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """)

    # 按日期展示
    by_date_db = defaultdict(dict)
    for _, row in result.iterrows():
        by_date_db[row["biz_date"]][row["mother_biz"]] = row["cnt"]

    print(f"\n  日期       | A组    | B组")
    print("  " + "-" * 35)
    for d in sorted(by_date_db.keys()):
        data = by_date_db[d]
        a_cnt = data.get("A组", 0)
        b_cnt = data.get("B组", 0)
        print(f"  {d} | {a_cnt:>6,} | {b_cnt:>6,}")

    total = result["cnt"].sum()
    print("  " + "-" * 35)
    print(f"  总计: {total:,} 条")

    conn.close()


if __name__ == "__main__":
    main()
