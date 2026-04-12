#!/usr/bin/env python3
"""
数据质量监控脚本。

功能：
1. 检查数据量是否异常（与历史均值对比）
2. 检查业务线数据完整性（是否有缺失）
3. 检查数据时效性（最新日期是否为 T-1）
4. 异常时推送企微告警，并输出诊断报告

用法：
    .venv/bin/python jobs/data_quality_check.py
    .venv/bin/python jobs/data_quality_check.py --alert  # 异常时推送企微
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from storage.tidb_manager import TiDBManager


# ==========================================================
# 配置
# ==========================================================

# 告警阈值
THRESHOLDS = {
    # 数据量波动阈值（相对于历史均值）
    "volume_change_pct": 0.5,  # 50% 波动告警
    # 最小数据量阈值
    "min_records": 100,  # 单日少于 100 条告警
    # 业务线完整性阈值
    "expected_biz_lines": ["A组-评论", "B组-评论", "B组-账号"],
    # 数据时效性阈值
    "max_delay_days": 1,  # 最大延迟 1 天（T-1）
}


# ==========================================================
# 检查函数
# ==========================================================


def check_volume_anomaly(conn: TiDBManager, target_date: date) -> dict[str, Any]:
    """检查数据量是否异常（与过去7天均值对比）。"""
    # 获取目标日期数据量
    result = conn.fetch_one(
        "SELECT COUNT(*) AS cnt FROM fact_qa_event WHERE biz_date = %s",
        [target_date]
    )
    target_cnt = int(list(result.values())[0]) if result else 0

    # 获取过去7天均值（排除目标日期）
    avg_result = conn.fetch_one("""
        SELECT AVG(cnt) AS avg_cnt FROM (
            SELECT COUNT(*) as cnt
            FROM fact_qa_event
            WHERE biz_date >= %s AND biz_date < %s
            GROUP BY biz_date
        ) t
    """, [target_date - timedelta(days=8), target_date])
    avg_cnt = float(list(avg_result.values())[0]) if avg_result and avg_result.get("avg_cnt") else 0

    # 计算变化率
    change_pct = 0.0
    if avg_cnt > 0:
        change_pct = (target_cnt - avg_cnt) / avg_cnt

    # 判断是否异常
    is_anomaly = False
    alerts = []

    if target_cnt < THRESHOLDS["min_records"]:
        is_anomaly = True
        alerts.append(f"数据量过少：{target_cnt} < {THRESHOLDS['min_records']}")

    if abs(change_pct) > THRESHOLDS["volume_change_pct"]:
        is_anomaly = True
        direction = "激增" if change_pct > 0 else "骤降"
        alerts.append(f"数据量{direction}：{abs(change_pct)*100:.1f}%（{target_cnt} vs 均值 {avg_cnt:.0f}）")

    return {
        "check": "volume_anomaly",
        "is_anomaly": is_anomaly,
        "target_cnt": target_cnt,
        "avg_cnt": avg_cnt,
        "change_pct": change_pct,
        "alerts": alerts,
    }


def check_biz_line_completeness(conn: TiDBManager, target_date: date) -> dict[str, Any]:
    """检查业务线数据完整性。"""
    df = conn.fetch_df("""
        SELECT sub_biz, COUNT(*) as cnt
        FROM fact_qa_event
        WHERE biz_date = %s
        GROUP BY sub_biz
    """, [target_date])

    actual_biz_lines = set(df["sub_biz"].tolist())
    expected_biz_lines = set(THRESHOLDS["expected_biz_lines"])

    # 判断是否异常
    missing_biz_lines = expected_biz_lines - actual_biz_lines
    is_anomaly = len(missing_biz_lines) > 0

    alerts = []
    if is_anomaly:
        alerts.append(f"缺失业务线：{', '.join(missing_biz_lines)}")

    return {
        "check": "biz_line_completeness",
        "is_anomaly": is_anomaly,
        "actual_biz_lines": list(actual_biz_lines),
        "expected_biz_lines": list(expected_biz_lines),
        "missing_biz_lines": list(missing_biz_lines),
        "alerts": alerts,
    }


def check_data_freshness(conn: TiDBManager) -> dict[str, Any]:
    """检查数据时效性。"""
    result = conn.fetch_one("SELECT MAX(biz_date) AS max_date FROM fact_qa_event")
    max_date_val = list(result.values())[0] if result else None
    max_date = max_date_val if max_date_val else None

    if max_date is None:
        return {
            "check": "data_freshness",
            "is_anomaly": True,
            "max_date": None,
            "expected_date": date.today() - timedelta(days=1),
            "delay_days": 999,
            "alerts": ["数据库无数据"],
        }

    expected_date = date.today() - timedelta(days=THRESHOLDS["max_delay_days"])
    delay_days = (expected_date - max_date).days

    is_anomaly = delay_days > 0

    alerts = []
    if is_anomaly:
        alerts.append(f"数据延迟 {delay_days} 天（最新：{max_date}，期望：{expected_date}）")

    return {
        "check": "data_freshness",
        "is_anomaly": is_anomaly,
        "max_date": str(max_date),
        "expected_date": str(expected_date),
        "delay_days": delay_days,
        "alerts": alerts,
    }


def check_file_import_status(conn: TiDBManager, target_date: date) -> dict[str, Any]:
    """检查文件导入状态。"""
    # 获取目标日期已导入文件（支持多种日期格式）
    date_patterns = [
        f"%{target_date.strftime('%m%d')}",  # 0331
        f"{target_date.year}.{target_date.month}.{target_date.day}",  # 2026.3.31
        f"{target_date.year}.{target_date.month:02d}.{target_date.day:02d}",  # 2026.03.31
    ]

    df = conn.fetch_df("""
        SELECT file_name, first_upload_time
        FROM fact_file_dedup
        WHERE file_name LIKE %s OR file_name LIKE %s OR file_name LIKE %s
        ORDER BY first_upload_time DESC
    """, [f"%{date_patterns[0]}%", f"%{date_patterns[1]}%", f"%{date_patterns[2]}%"])

    # 判断是否异常（期望至少 3 个文件：A组-评论、B组-评论、B组-账号）
    is_anomaly = len(df) < 3

    alerts = []
    if is_anomaly:
        alerts.append(f"导入文件数不足：{len(df)} < 3")
        if len(df) > 0:
            alerts.append(f"已导入文件：{', '.join(df['file_name'].tolist())}")

    return {
        "check": "file_import_status",
        "is_anomaly": is_anomaly,
        "imported_files": len(df),
        "file_list": df["file_name"].tolist() if len(df) > 0 else [],
        "alerts": alerts,
    }


# ==========================================================
# 诊断报告
# ==========================================================


def generate_diagnosis_report(results: list[dict[str, Any]]) -> str:
    """生成诊断报告。"""
    report_lines = ["## 数据质量诊断报告", ""]

    for result in results:
        status = "❌ 异常" if result["is_anomaly"] else "✅ 正常"
        report_lines.append(f"### {result['check']}: {status}")

        if result["is_anomaly"]:
            for alert in result["alerts"]:
                report_lines.append(f"- {alert}")

        report_lines.append("")

    return "\n".join(report_lines)


# ==========================================================
# 告警推送
# ==========================================================


def send_wecom_alert(webhook_key: str, message: str) -> bool:
    """发送企微告警。"""
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


# ==========================================================
# 主函数
# ==========================================================


def main():
    parser = argparse.ArgumentParser(description="数据质量监控")
    parser.add_argument("--alert", action="store_true", help="异常时推送企微告警")
    parser.add_argument("--target-date", type=str, default=None, help="目标日期（YYYY-MM-DD），默认昨天")
    args = parser.parse_args()

    # 确定目标日期
    target_date = date.today() - timedelta(days=1)  # 默认检查昨天的数据
    if args.target_date:
        from datetime import datetime
        target_date = datetime.strptime(args.target_date, "%Y-%m-%d").date()

    print("=" * 60)
    print(f"📊 数据质量监控 | 目标日期: {target_date}")
    print("=" * 60)

    # 加载配置
    config_path = PROJECT_ROOT / "config/settings.json"
    config = {}
    if config_path.exists():
        with open(config_path, encoding="utf-8") as f:
            config = json.load(f)

    conn = TiDBManager()

    # 执行检查
    results = []
    results.append(check_volume_anomaly(conn, target_date))
    results.append(check_biz_line_completeness(conn, target_date))
    results.append(check_data_freshness(conn))
    results.append(check_file_import_status(conn, target_date))

    # conn 由连接池管理，无需手动关闭

    # 输出结果
    print("\n📋 检查结果：")
    has_anomaly = False
    for result in results:
        status = "❌ 异常" if result["is_anomaly"] else "✅ 正常"
        print(f"  {result['check']}: {status}")
        if result["is_anomaly"]:
            has_anomaly = True
            for alert in result["alerts"]:
                print(f"    - {alert}")

    # 生成诊断报告
    if has_anomaly:
        print("\n" + "=" * 60)
        print("🔍 诊断报告：")
        print("=" * 60)
        report = generate_diagnosis_report(results)
        print(report)

        # 推送告警
        if args.alert:
            webhook_key = config.get("wecom_webhook_key")
            if webhook_key:
                alert_msg = f"""⚠️ **数据质量异常告警** | {target_date}

{report}

<font color="comment">请检查数据导入流程</font>"""
                send_wecom_alert(webhook_key, alert_msg)
                print("\n✅ 已推送企微告警")
    else:
        print("\n✅ 数据质量正常，无需告警")

    print("\n" + "=" * 60)

    # 返回退出码（异常返回 1）
    sys.exit(1 if has_anomaly else 0)


if __name__ == "__main__":
    main()
