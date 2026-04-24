#!/usr/bin/env python3
"""
QC Dashboard — 自动定时刷新脚本
=====================================
由 launchd 调用，每天 18:25 自动执行：
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


def get_queue_latest_dates(conn):
    """获取每个队列的最新日期"""
    c = conn.cursor()
    c.execute("SELECT queue_id, MAX(date) FROM daily_metrics GROUP BY queue_id")
    return {qid: latest for qid, latest in c.fetchall()}


def analyze_queue_freshness(conn, config):
    """分析各队列数据新鲜度，避免被全局 MAX(date) 掩盖局部停更问题"""
    latest_map = get_queue_latest_dates(conn)
    today = date.today()
    stale_queues = []

    for qcfg in config.get("queues", []):
        qid = qcfg.get("id")
        qname = qcfg.get("name", qid)
        latest = latest_map.get(qid)

        if not latest:
            stale_queues.append({
                "queue_id": qid,
                "queue_name": qname,
                "latest": None,
                "lag_days": None,
                "reason": "missing",
            })
            continue

        try:
            latest_date = datetime.strptime(latest, "%Y-%m-%d").date()
            lag_days = (today - latest_date).days
        except ValueError:
            stale_queues.append({
                "queue_id": qid,
                "queue_name": qname,
                "latest": latest,
                "lag_days": None,
                "reason": "invalid-date",
            })
            continue

        if lag_days > 0:
            stale_queues.append({
                "queue_id": qid,
                "queue_name": qname,
                "latest": latest,
                "lag_days": lag_days,
                "reason": "stale",
            })

    return {
        "queue_latest_dates": latest_map,
        "stale_queues": stale_queues,
        "queue_count": len(config.get("queues", [])),
        "fresh_queue_count": len(config.get("queues", [])) - len(stale_queues),
        "fully_synced": len(stale_queues) == 0,
    }


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


def record_run_result(success, imported, gap_info=None, freshness_info=None):
    """记录本次运行结果到状态文件"""
    status_file = PROJECT_ROOT / "data" / "logs" / "last_run.json"
    result = {
        "timestamp": datetime.now().isoformat(),
        "success": success,
        "imported": imported,
        "latest_db_date": gap_info.get("latest") if gap_info else None,
        "gap_days": gap_info.get("gap_days", 0) if gap_info else 0,
        "fully_synced": freshness_info.get("fully_synced") if freshness_info else None,
        "queue_count": freshness_info.get("queue_count") if freshness_info else None,
        "fresh_queue_count": freshness_info.get("fresh_queue_count") if freshness_info else None,
        "stale_queues": freshness_info.get("stale_queues") if freshness_info else [],
        "queue_latest_dates": freshness_info.get("queue_latest_dates") if freshness_info else {},
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
        freshness_info = analyze_queue_freshness(conn, config)

        if gap_info["gap_days"] > 0:
            log.warning(
                f"   ⚠️ 发现 {gap_info['gap_days']} 天全库日期缺口: "
                f"{gap_info['gap_dates'][0]} ~ {gap_info['gap_dates'][-1]}"
                f"（含周末 {gap_info['weekend_count']} 天）"
            )
            log.info("   💡 本次拉取将尝试自动补全缺失日期的数据")
        else:
            log.info(f"   ✅ 全库最新日期: {gap_info['latest']}")

        if freshness_info["stale_queues"]:
            log.warning(
                f"   ⚠️ 当前仅 {freshness_info['fresh_queue_count']}/{freshness_info['queue_count']} 个队列同步到今天，"
                f"其余队列仍有滞后"
            )
            for item in freshness_info["stale_queues"][:5]:
                latest = item["latest"] or "无数据"
                lag = f"落后 {item['lag_days']} 天" if item["lag_days"] is not None else item["reason"]
                log.warning(f"      - [{item['queue_id']}] {item['queue_name']}: 最新 {latest}（{lag}）")
        else:
            log.info("   ✅ 所有队列都已同步到今天")

        if dry_run:
            log.info("🏁 DRY-RUN 模式，不执行实际操作")
            record_run_result(True, 0, gap_info, freshness_info)
            return 0

        # ── Step 2: 拉取数据 ──
        mode = config.get("global", {}).get("collector_mode", "playwright")
        total_imported = 0
        live_synced = 0
        log.info(f"📡 [2/4] 拉取数据 (模式: {mode})...")
        
        if mode == "playwright":
            # Playwright 自动下载 xlsx → 解析入库
            import asyncio
            from auto_download import run_full_pipeline
            
            log.info("   启动 Playwright 自动下载...")
            try:
                downloaded = asyncio.run(run_full_pipeline(download_only=True, headless=True))
                log.info(f"   下载了 {len(downloaded)} 个文件(队列映射)")
            except Exception as e:
                log.error(f"   ❌ Playwright 下载失败: {e}")
                downloaded = []
            
            if downloaded:
                # downloaded 现在是 [(file_path, queue_id), ...] 元组列表
                # 共享文件（如 q3/q3b）会出现多次，指向同一个文件但 queue_id 不同
                # import_excel 不会删除文件，所以可以安全地多次导入同一文件
                from collector import import_excel

                for fpath, qid in downloaded:
                    try:
                        if not Path(fpath).exists():
                            log.warning(f"   ⚠️ 文件不存在，跳过: {fpath} (队列 {qid})")
                            continue

                        count = import_excel(conn, config, queue_id=qid, file_path=fpath)
                        total_imported += count
                        log.info(f"   ✅ 导入 [{qid}] {Path(fpath).name}: {count} 条")
                    except Exception as e:
                        log.warning(f"   ⚠️ 导入失败 [{qid}] {fpath}: {e}")

                # 全部导入完成后统一清理
                from auto_download import cleanup_temp_files
                cleanup_temp_files()
                # 清理 uploads 目录中的 xlsx（import_excel 不再自行删除）
                upload_dir = PROJECT_ROOT / "data" / "uploads"
                for f in upload_dir.glob("*.xlsx"):
                    try:
                        f.unlink()
                        log.info(f"   🗑️ 清理: {f.name}")
                    except OSError:
                        pass
            else:
                log.warning("   ⚠️ 未下载到任何文件，尝试用 uploads 目录已有文件兜底")
                from collector import import_excel
                total_imported = import_excel(conn, config)

            # 对 q3/q3b 这类公式汇总队列，再走一遍网页直读结果兜底，覆盖 xlsx 丢公式缓存的问题
            from collector import sync_playwright_live
            live_synced = sync_playwright_live(conn, config, headless=True)
            if live_synced > 0:
                total_imported += live_synced
                log.info(f"   ✅ live 同步补写 {live_synced} 条日期记录（已覆盖公式队列）")
        
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
        final_freshness = analyze_queue_freshness(conn, config)
        log.info("📋 [4/4] 刷新完成汇总:")
        log.info(f"   📥 新增记录: {total_imported}")
        log.info(f"   🧹 未来日期清理: {future_deleted}")
        log.info(f"   📅 全库最新: {final_gap['latest']}")
        if final_gap["gap_days"] > 0:
            log.warning(f"   ⚠️ 仍有 {final_gap['gap_days']} 天全库日期缺口（可能源数据尚未更新）")
        else:
            log.info(f"   ✅ 全库日期已覆盖到今天 ({final_gap['today']})")

        if final_freshness["stale_queues"]:
            log.warning(
                f"   ⚠️ 队列级新鲜度未全绿：{final_freshness['fresh_queue_count']}/{final_freshness['queue_count']} 个队列同步到今天"
            )
            for item in final_freshness["stale_queues"][:5]:
                latest = item["latest"] or "无数据"
                lag = f"落后 {item['lag_days']} 天" if item["lag_days"] is not None else item["reason"]
                log.warning(f"      - [{item['queue_id']}] {item['queue_name']}: 最新 {latest}（{lag}）")
        else:
            log.info(f"   ✅ 所有队列都已同步到今天 ({final_gap['today']})")

        record_run_result(True, total_imported, final_gap, final_freshness)
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

        # ── Step 6: Git push metrics.db → Streamlit Cloud 自动同步 ──
        log.info("📦 [6/6] Git 推送数据库...")
        try:
            import subprocess
            git_cmds = [
                ["git", "-C", str(PROJECT_ROOT), "add", "data/metrics.db"],
                ["git", "-C", str(PROJECT_ROOT), "commit", "-m", f"data: {datetime.now().strftime('%Y-%m-%d')} 自动更新"],
                ["git", "-C", str(PROJECT_ROOT), "push"],
            ]
            for cmd in git_cmds:
                r = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                if r.returncode != 0 and "nothing to commit" not in (r.stdout + r.stderr):
                    log.warning(f"   ⚠️ Git: {' '.join(cmd[-2:])} → {r.stderr[:100]}")
                    break
            else:
                log.info("   ✅ Git push 完成 → Streamlit Cloud 将自动同步")
        except Exception as e:
            log.warning(f"   ⚠️ Git push 异常: {e}")

        return 0

    except Exception as e:
        log.error(f"❌ 刷新失败: {e}", exc_info=True)
        record_run_result(False, 0)
        return 1


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv or "-n" in sys.argv
    sys.exit(main(dry_run=dry))
