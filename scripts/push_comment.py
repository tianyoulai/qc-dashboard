#!/usr/bin/env python3
"""评论质量日报残留推送脚本。

注意：这个脚本服务于评论质量日报链路，不属于 TAO 看板的本地 launchd 任务入口。
"""
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "jobs"))

data_path = PROJECT_ROOT / "daily_data.json"
if not data_path.exists():
    print(f"❌ 找不到 {data_path}")
    sys.exit(1)

with open(data_path, "r", encoding="utf-8") as f:
    report = json.load(f)

if not report.get("has_data"):
    print("❌ 无数据")
    sys.exit(1)

from daily_report import report_to_wecom_md
wecom_md = report_to_wecom_md(report)

# 正确的评论看板 webhook
WEBHOOK_KEY = "b174ccf6-dd35-4165-9b3f-64925c45e215"
url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={WEBHOOK_KEY}"

import requests
print("📤 推送评论业务质检日报到目标群...")
resp = requests.post(url, json={"msgtype": "markdown", "markdown": {"content": wecom_md}}, timeout=10)
result = resp.json()
if result.get("errcode") == 0:
    print("✅ 推送成功!")
else:
    print(f"❌ 失败: {result}")
