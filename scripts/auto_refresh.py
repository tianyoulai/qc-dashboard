#!/usr/bin/env python3
"""
QC Dashboard — 自动定时刷新脚本
=====================================
由 launchd 调用，每天 18:15 自动执行：
  1. 通过 Playwright 自动下载企微表格 xlsx
  2. 解析入库并清理未来日期
  3. 检测数据缺口并记录
  4. 自动推送到企微群（如已配置 webhook）

用法:
  python scripts/auto_refresh.py          # 正式执行
  python scripts/auto_refresh.py --dry-run # 只检查不执行
"""

import os
import sys
import json
import logging
from datetime import datetime, date, timedelta
from pathlib import Path

# ── 项目路径 ──
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# ── 日志配置 ──
LOG_DIR = PROJECT_ROOT / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"refresh_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("qc-auto-refresh")

DB_FILE = PROJECT_ROOT / "data" / "metrics.db"
CONFIG_FILE = PROJECT_ROOT / "config.yaml"


def load_config():
    import yaml
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_db_latest_date(conn):
    """获取数据库中所有队列的最新日期"""
    c = conn.cursor()
    c.execute("SELECT MAX(date) FROM daily_metrics")
    row = c.fetchone()
    return row[0] if row and row[0] else None


def find_date_gaps(conn):
    """检测数据库最新日期到今天之间的缺口日期
    
    返回: {
        'latest': str,           # 数据库最新日期
        'today': str,            # 今天
        'gap_days': int,         # 缺口天数
        'gap_dates': list[str],  # 具体缺哪些天
        'weekend_count': int,    # 其中周末几天
    }
    """
    latest = get_db_latest_date(conn)
    today_str = date.today().isoformat()

    if not latest:
        return {"latest": None, "today": today_str, "gap_days": 0, "gap_dates": [], "weekend_count": 0}

    try:
        d_latest = datetime.strptime(latest, "%Y-%m-%d").date()
    except ValueError:
        return {"latest": latest, "today": today_str, "gap_days": 0, "gap_dates": [], "weekend_count": 0}

    gap_dates = []
    current = d_latest + timedelta(days=1)
    while current < date.today():
        gap_dates.append(current.isoformat())
        current += timedelta(days=1)

    weekend_count = sum(1 for d in gap_dates if datetime.strptime(d, "%Y-%m-%d").weekday() >= 5)

    return {
        "latest": latest,
        "today": today_str,
        "gap_days": len(gap_dates),
        "gap_dates": gap_dates,
        "weekend_count": weekend_count,
    }


def clean_future_dates(conn):
    """清理 > 今天的数据"""
    c = conn.cursor()
    today_str = date.today().isoformat()
    c.execute("DELETE FROM daily_metrics WHERE date > ?", (today_str,))
    deleted = c.rowcount
    conn.commit()
    return deleted


def record_run_result(success, imported, gap_info=None):
    """记录本次运行结果到状态文件"""
    status_file = PROJECT_ROOT / "data" / "logs" / "last_run.json"
    result = {
        "timestamp": datetime.now().isoformat(),
        "success": success,
        "imported": imported,
        "latest_db_date": gap_info.get("latest") if gap_info else None,
        "gap_days": gap_info.get("gap_days", 0) if gap_info else 0,
    }
    with open(status_file, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False, indent=2)


def main(dry_run=False):
    log.info("=" * 60)
    log.info("🔄 QC Dashboard 自动刷新启动")
    log.info(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"   模式: {'DRY-RUN（仅检查）' if dry_run else '正式执行'}")
    log.info("=" * 60)

    try:
        # ── Step 0: 环境检查 ──
        if not CONFIG_FILE.exists():
            log.error(f"❌ 配置文件不存在: {CONFIG_FILE}")
            record_run_result(False, 0)
            return 1

        config = load_config()

        # 初始化/连接数据库
        from collector import init_db
        conn = init_db()

        # ── Step 1: 数据缺口检测 ──
        log.info("📊 [1/4] 检测数据缺口...")
        gap_info = find_date_gaps(conn)
        
        if gap_info["gap_days"] > 0:
            log.warning(
                f"   ⚠️ 发现 {gap_info['gap_days']} 天数据缺口: "
                f"{gap_info['gap_dates'][0]} ~ {gap_info['gap_dates'][-1]}"
                f"（含周末 {gap_info['weekend_count']} 天）"
            )
            log.info("   💡 本次拉取将尝试自动补全缺失日期的数据")
        else:
            log.info(f"   ✅ 数据连续，最新日期: {gap_info['latest']}")

        if dry_run:
            log.info("🏁 DRY-RUN 模式，不执行实际操作")
            record_run_result(True, 0, gap_info)
            return 0

        # ── Step 2: 拉取数据 ──
        mode = config.get("global", {}).get("collector_mode", "playwright")
        total_imported = 0
        log.info(f"📡 [2/4] 拉取数据 (模式: {mode})...")
        
        if mode == "playwright":
            # Playwright 自动下载 xlsx → 解析入库
            import asyncio
            from auto_download import run_full_pipeline
            
            log.info("   启动 Playwright 自动下载...")
            try:
                downloaded = asyncio.run(run_full_pipeline(download_only=True, headless=True))
                log.info(f"   下载了 {len(downloaded)} 个文件: {downloaded}")
            except Exception as e:
                log.error(f"   ❌ Playwright 下载失败: {e}")
                downloaded = []
            
            if downloaded:
                # 下载成功后，逐个导入数据库
                # 注意：去重文件（如 q3/q3b 共享同一 xlsx）会出现在列表中多次，
                # import_excel 导入后会删除原文件，需要对重复文件先做备份
                from collector import import_excel
                import shutil
                # 统计每个文件出现的次数，>1 说明有队列共享
                from collections import Counter
                file_counts = Counter(str(f) for f in downloaded)
                file_used = Counter()  # 记录每个文件已处理次数

                for fpath in downloaded:
                    try:
                        fpath_str = str(fpath)
                        file_used[fpath_str] += 1
                        # 如果文件会被多次使用且这是第一次，先为后续复制备份
                        if file_counts[fpath_str] > 1 and file_used[fpath_str] == 1:
                            # 第1次使用：复制副本供后续队列，当前用原文件
                            total_shared = file_counts[fpath_str]
                            copy_path = fpath_str.replace('.xlsx', f'_shared{total_shared}.xlsx')
                            shutil.copy2(fpath_str, copy_path)
                            log.info(f"   📋 共享文件备份({total_shared}个队列共用): {Path(copy_path).name}")
                            # 将后续出现的同路径替换为副本（从当前位置之后查找）
                            current_idx = -1
                            for i, f in enumerate(downloaded):
                                if str(f) == fpath_str:
                                    current_idx = i
                                    break
                            replaced = 0
                            for i in range(current_idx + 1, len(downloaded)):
                                if str(downloaded[i]) == fpath_str:
                                    downloaded[i] = copy_path
                                    replaced += 1
                                    if replaced >= total_shared - 1:
                                        break

                        if not Path(fpath).exists():
                            log.warning(f"   ⚠️ 文件不存在，跳过: {fpath}")
                            continue

                        count = import_excel(conn, config, file_path=fpath)
                        total_imported += count
                        log.info(f"   ✅ 导入 {fpath}: {count} 条")
                    except Exception as e:
                        log.warning(f"   ⚠️ 导入失败 {fpath}: {e}")
                # 清理临时文件（包括 _shared 副本）
                from auto_download import cleanup_temp_files
                cleanup_temp_files()
                # 额外清理 uploads 目录中的 _shared 副本
                upload_dir = PROJECT_ROOT / "data" / "uploads"
                for f in upload_dir.glob("*_shared*.xlsx"):
                    try:
                        f.unlink()
                        log.info(f"   🧹 清理共享副本: {f.name}")
                    except OSError:
                        pass
            else:
                log.warning("   ⚠️ 未下载到任何文件，尝试用 uploads 目录已有文件兜底")
                from collector import import_excel
                total_imported = import_excel(conn, config)
        
        elif mode == "wcom-api":
            from collector import fetch_wecom_api, check_wecom_init
            if not check_wecom_init():
                log.error("❌ wecom-cli 未初始化！请先运行: python src/collector.py setup")
                record_run_result(False, 0, gap_info)
                return 1
            total_imported = fetch_wecom_api(conn, config)
        elif mode == "excel":
            from collector import import_excel
            total_imported = import_excel(conn, config)
        else:
            log.error(f"❌ 不支持的采集模式: {mode}")
            record_run_result(False, 0, gap_info)
            return 1

        log.info(f"   ✅ 新增/更新 {total_imported} 条记录")

        # ── Step 3: 清理未来日期 ──
        log.info("🧹 [3/4] 清理未来日期...")
        future_deleted = clean_future_dates(conn)
        if future_deleted > 0:
            log.info(f"   ✅ 删除 {future_deleted} 条未来日期记录")
        else:
            log.info("   ✅ 无需清理")

        # ── Step 4: 最终状态 ──
        final_gap = find_date_gaps(conn)
        log.info("📋 [4/4] 刷新完成汇总:")
        log.info(f"   📥 新增记录: {total_imported}")
        log.info(f"   🧹 未来日期清理: {future_deleted}")
        log.info(f"   📅 数据库最新: {final_gap['latest']}")
        if final_gap["gap_days"] > 0:
            log.warning(f"   ⚠️ 仍有 {final_gap['gap_days']} 天缺口（可能源数据尚未更新）")
        else:
            log.info(f"   ✅ 数据已同步到今天 ({final_gap['today']})")

        record_run_result(True, total_imported, final_gap)
        conn.close()

        log.info("=" * 60)
        log.info("🎉 自动刷新完成！")
        log.info("=" * 60)

        # ── Step 5: 自动推送（可选）──
        push_cfg = config.get("global", {}).get("push", {})
        if push_cfg.get("enabled", False):
            log.info("📤 触发每日数据推送...")
            try:
                import subprocess
                push_script = PROJECT_ROOT / "scripts" / "daily_push.py"
                if push_script.exists():
                    result = subprocess.run(
                        [sys.executable, str(push_script)],
                        capture_output=True, text=True, timeout=60,
                    )
                    if result.returncode == 0:
                        log.info("   ✅ 推送完成")
                    else:
                        log.warning(f"   ⚠️ 推送失败: {result.stderr[:200] if result.stderr else '未知'}")
                else:
                    log.warning(f"   ⚠️ 推送脚本不存在: {push_script}")
            except Exception as e:
                log.warning(f"   ⚠️ 推送异常: {e}")
        else:
            log.info("📤 每日推送未启用 (push.enabled=false)")

        return 0

    except Exception as e:
        log.error(f"❌ 刷新失败: {e}", exc_info=True)
        record_run_result(False, 0)
        return 1


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv or "-n" in sys.argv
    sys.exit(main(dry_run=dry))
