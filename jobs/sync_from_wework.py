#!/usr/bin/env python3
"""
从企业微信缓存目录同步质检文件到看板数据库。

功能：
1. 扫描企微缓存目录，匹配文件名包含"长沙云雀"、"迁移人力ilabel"、"迁移人力账号"的 Excel 文件
2. 只同步每个业务线的最新日期文件（默认行为）
3. 按文件内容哈希去重，避免重复导入
4. 调用 import_fact_data.py 导入新文件
5. 执行 refresh_warehouse.py 刷新数仓
6. 推送同步结果到企微群

用法：
    # 同步每个业务线的最新文件（默认，推荐）
    .venv/bin/python jobs/sync_from_wework.py
    
    # 仅扫描并报告文件，不执行导入
    .venv/bin/python jobs/sync_from_wework.py --dry-run
    
    # 同步所有历史文件（谨慎使用）
    .venv/bin/python jobs/sync_from_wework.py --all
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

# 项目根目录
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from storage.tidb_manager import TiDBManager

# 导入日期提取函数
from jobs.import_fact_data import extract_date_from_filename


# ==========================================================
# 配置
# ==========================================================

# 企微缓存根路径（支持多个 ProfileID）
WEWORK_CACHE_ROOT = Path.home() / "Library/Containers/com.tencent.WeWorkMac/Data/Documents/Profiles"

# 目标文件关键词
TARGET_KEYWORDS = ["长沙云雀", "迁移人力ilabel", "迁移人力账号"]

# 排除的文件名模式
EXCLUDE_PATTERNS = [
    "模板", "~$",  # 排除模板文件和 Excel 临时文件
    "新人培训", "图片质检",  # 排除非目标业务类型
    "10816",  # 排除特定队列编号(新人培训队列)
]


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


def is_file_already_imported(conn: TiDBManager, file_hash: str) -> bool:
    """检查文件是否已导入（通过 fact_file_dedup 表）。"""
    result = conn.fetch_one(
        "SELECT 1 AS c FROM fact_file_dedup WHERE file_hash = %s LIMIT 1",
        [file_hash]
    )
    return result is not None


def scan_wework_cache(days: int | None = None, latest_only: bool = False) -> list[dict[str, Any]]:
    """
    扫描企微缓存目录，返回匹配的文件列表。
    
    Args:
        days: 只扫描最近 N 天的文件（按目录名中的日期判断），None 表示扫描全部
        latest_only: 只返回每个业务线的最新日期文件
        
    Returns:
        文件信息列表，每个元素包含：
        - file_path: 文件路径
        - file_name: 文件名
        - file_hash: 文件内容哈希
        - mtime: 文件修改时间
        - matched_keyword: 匹配的关键词
        - biz_date: 业务日期（从文件名提取）
    """
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
        
        # 遍历月份目录（如 2026-03）
        for month_dir in cache_dir.iterdir():
            if not month_dir.is_dir():
                continue
            
            # 如果指定了天数，检查目录名是否在范围内
            if days is not None:
                try:
                    # 目录名格式：YYYY-MM
                    dir_date = datetime.strptime(month_dir.name, "%Y-%m").date()
                    cutoff_date = date.today() - timedelta(days=days)
                    if dir_date < cutoff_date.replace(day=1):
                        continue
                except ValueError:
                    continue
            
            # 遍历文件
            for file_dir in month_dir.iterdir():
                if not file_dir.is_dir():
                    continue
                
                # 查找目录中的 Excel 文件
                for file_path in file_dir.glob("*.xlsx"):
                    file_name = file_path.name
                    
                    # 排除临时文件和模板文件
                    if any(exclude in file_name for exclude in EXCLUDE_PATTERNS):
                        continue
                    
                    # 检查是否匹配目标关键词
                    matched_keyword = None
                    for keyword in TARGET_KEYWORDS:
                        if keyword in file_name:
                            matched_keyword = keyword
                            break
                    
                    if not matched_keyword:
                        continue
                    
                    # 提取业务日期
                    biz_date = extract_date_from_filename(file_name)
                    
                    matched_files.append({
                        "file_path": file_path,
                        "file_name": file_name,
                        "file_hash": compute_file_hash(file_path),
                        "mtime": datetime.fromtimestamp(file_path.stat().st_mtime),
                        "matched_keyword": matched_keyword,
                        "biz_date": biz_date,
                    })
    
    # 如果只要最新文件，按业务线分组后只保留最新日期
    if latest_only and matched_files:
        from collections import defaultdict
        latest_files = defaultdict(list)
        
        # 按业务线分组
        for file_info in matched_files:
            biz_line = file_info["matched_keyword"]
            latest_files[biz_line].append(file_info)
        
        # 对每个业务线只保留最新日期的文件
        result = []
        for biz_line, files in latest_files.items():
            # 过滤掉没有日期的文件
            files_with_date = [f for f in files if f["biz_date"] is not None]
            if not files_with_date:
                continue
            
            # 找到最新日期
            max_date = max(f["biz_date"] for f in files_with_date)
            
            # 只保留最新日期的文件
            latest_for_line = [f for f in files_with_date if f["biz_date"] == max_date]
            result.extend(latest_for_line)
        
        matched_files = result
    
    # 按文件名排序
    matched_files.sort(key=lambda x: x["file_name"])
    return matched_files


def send_wecom_message(webhook_key: str, message: str) -> bool:
    """发送消息到企微群。"""
    import requests
    
    webhook_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={webhook_key}"
    payload = {
        "msgtype": "markdown",
        "markdown": {"content": message}
    }
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        result = response.json()
        return result.get("errcode") == 0
    except Exception as e:
        print(f"⚠️ 企微消息发送失败：{e}")
        return False


def run_import(file_paths: list[Path]) -> tuple[int, int, list[str]]:
    """
    执行导入命令。
    
    Returns:
        (成功数量, 失败数量, 错误信息列表)
    """
    success_count = 0
    error_count = 0
    errors = []
    
    for file_path in file_paths:
        try:
            cmd = [
                str(PROJECT_ROOT / ".venv/bin/python"),
                str(PROJECT_ROOT / "jobs/import_fact_data.py"),
                "--qa-file", str(file_path),
                "--skip-refresh",  # 由 run_refresh 统一刷新 mart
            ]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=str(PROJECT_ROOT),
                timeout=300,  # 5 分钟超时
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
            timeout=300,
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
    parser = argparse.ArgumentParser(description="从企业微信缓存目录同步质检文件（默认只同步最新文件）")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="仅扫描并报告文件，不执行导入",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="扫描并同步所有历史文件（默认只同步最新文件）",
    )
    args = parser.parse_args()
    
    print("=" * 60)
    print("📂 企业微信质检文件同步")
    print("=" * 60)
    
    # 1. 扫描文件（默认只取最新文件）
    print(f"\n🔍 扫描企微缓存目录...")
    latest_only = not args.all  # 默认只取最新
    matched_files = scan_wework_cache(days=None if args.all else None, latest_only=latest_only)
    
    if not matched_files:
        print("✅ 没有找到匹配的文件")
        return
    
    print(f"\n📋 找到 {len(matched_files)} 个匹配文件：")
    for i, file_info in enumerate(matched_files, 1):
        print(f"  {i:2d}. [{file_info['matched_keyword']}] {file_info['file_name']}")
    
    if args.dry_run:
        print("\n🔍 Dry-run 模式，不执行导入")
        return
    
    # 2. 检查哪些文件已导入
    conn = TiDBManager()
    
    new_files = []
    skipped_files = []
    
    for file_info in matched_files:
        if is_file_already_imported(conn, file_info["file_hash"]):
            skipped_files.append(file_info)
        else:
            new_files.append(file_info)
    
    # conn 由连接池管理，无需手动关闭
    
    print(f"\n📊 文件状态：")
    print(f"  - 新文件：{len(new_files)} 个")
    print(f"  - 已导入：{len(skipped_files)} 个")
    
    if not new_files:
        print("\n✅ 所有文件均已导入，无需同步")
        return
    
    print(f"\n📥 准备导入 {len(new_files)} 个新文件：")
    for file_info in new_files:
        print(f"  - {file_info['file_name']}")
    
    # 3. 执行导入
    print("\n🚀 开始导入...")
    file_paths = [f["file_path"] for f in new_files]
    success_count, error_count, errors = run_import(file_paths)
    
    # 4. 刷新数仓
    print("\n🔄 刷新数仓...")
    refresh_success = run_refresh()
    
    # 5. 推送企微消息（只在有新文件导入时推送）
    config = load_config()
    webhook_key = config.get("wecom_webhook_key")
    if webhook_key and success_count > 0:
        today = date.today().strftime("%Y-%m-%d")
        
        if error_count == 0 and refresh_success:
            message = f"""✅ **质检数据同步成功** | {today}

📁 同步文件：{success_count} 个
✅ 导入成功：{success_count} 个
❌ 导入失败：{error_count} 个
🔄 数仓刷新：{'成功' if refresh_success else '失败'}

<font color="comment">详情请查看看板日志</font>"""
        else:
            error_list = "\n".join([f"- {e}" for e in errors[:5]])
            message = f"""⚠️ **质检数据同步异常** | {today}

📁 同步文件：{success_count} 个
✅ 导入成功：{success_count} 个
❌ 导入失败：{error_count} 个
🔄 数仓刷新：{'成功' if refresh_success else '失败'}

**错误详情：**
{error_list}

<font color="comment">请检查日志并手动处理</font>"""
        
        send_wecom_message(webhook_key, message)
    
    print("\n" + "=" * 60)
    print(f"✅ 同步完成：成功 {success_count} 个，失败 {error_count} 个")
    print("=" * 60)


if __name__ == "__main__":
    main()
