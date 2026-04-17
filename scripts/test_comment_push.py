#!/usr/bin/env python3
"""临时测试脚本：用评论数据看板的新格式推送到企微群"""
import json
import sys
from pathlib import Path
from datetime import date, datetime

# ── 项目路径 ──
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# ── 读取已有日报数据 ──
data_path = PROJECT_ROOT / "daily_data.json"
if not data_path.exists():
    print(f"❌ 找不到 {data_path}，请先运行 jobs/daily_report.py --dry-run 生成数据")
    sys.exit(1)

with open(data_path, "r", encoding="utf-8") as f:
    report = json.load(f)

if not report.get("has_data"):
    print("❌ 当日无数据")
    sys.exit(1)

# ── 复用 daily_report.py 的格式化逻辑 ──
# 导入 jobs 模块的格式化函数
sys.path.insert(0, str(PROJECT_ROOT / "jobs"))
from daily_report import (
    report_to_wecom_md, _group_by_mother, _build_actions,
    _acc_flag, _build_supplement, _build_risks,
)

wecom_md = report_to_wecom_md(report)
print("=" * 60)
print("📊 评论业务质检日报（新格式）预览:")
print("=" * 60)
print(wecom_md)
print("=" * 60)

# ── 推送到企微 ──
import requests
import yaml

# 读取 webhook 配置
config_path = PROJECT_ROOT / "config.yaml"
with open(config_path, "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

import os
webhook_key = os.environ.get("WECOM_WEBHOOK_KEY", "")
if not webhook_key:
    webhook_cfg = config.get("global", {}).get("wecom_webhook", {})
    webhook_key = webhook_cfg.get("key", "")

if not webhook_key:
    print("\n⚠️ 未配置 Webhook Key，仅打印预览（不推送）")
    sys.exit(0)

url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={webhook_key}"
payload = {
    "msgtype": "markdown",
    "markdown": {"content": wecom_md},
}

print(f"\n📤 推送到企微...")
resp = requests.post(url, json=payload, timeout=10)
result = resp.json()
if result.get("errcode") == 0:
    print(f"✅ 推送成功!")
else:
    print(f"❌ 推送失败: {result}")
