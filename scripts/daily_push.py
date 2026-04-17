#!/usr/bin/env python3
"""
QC Dashboard — 每日数据推送
=============================
每日自动刷新后，汇总各队列指标 → DeepSeek 生成摘要 → 企微应用消息推送

流程:
  1. 从 SQLite 读取最新数据
  2. 组装结构化摘要
  3. (可选) 调用 DeepSeek API 生成自然语言日报
  4. 通过企微应用消息 API 推送给指定用户

用法:
  python scripts/daily_push.py                    # 正常推送
  python scripts/daily_push.py --no-ai            # 不调用 DeepSeek，直接推原始数据
  python scripts/daily_push.py --test             # 测试模式，只打印不推送
  python scripts/daily_push.py --user @all        # 推送给所有人
"""

import os
import sys
import json
import logging
import requests
from datetime import datetime, date, timedelta
from pathlib import Path

# ── 项目路径 ──
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# ── 日志 ──
LOG_DIR = PROJECT_ROOT / "data" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / f"push_{datetime.now().strftime('%Y%m%d')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("qc-daily-push")

DB_FILE = PROJECT_ROOT / "data" / "metrics.db"
CONFIG_FILE = PROJECT_ROOT / "config.yaml"


# ============================================================
#  队列配置（与 app.py 保持一致）
# ============================================================
QUEUES = [
    {"id": "q1_toufang", "name": "投放误漏", "icon": "📢",
     "metric_keys": ["violation_rate", "miss_rate"],
     "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
     "thresholds": {"violation_rate": {"min": 0.98}, "miss_rate": {"max": 0.02}}},
    {"id": "q2_erjiansimple", "name": "简单二审", "icon": "📋",
     "metric_keys": ["violation_rate", "miss_rate"],
     "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
     "thresholds": {"violation_rate": {"min": 0.98}, "miss_rate": {"max": 0.02}}},
    {"id": "q3_erjian_4qi_gt", "name": "四期-二审GT", "icon": "🔄",
     "metric_keys": ["violation_rate", "miss_rate"],
     "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
     "thresholds": {"violation_rate": {"min": 0.99}, "miss_rate": {"max": 0.01}}},
    {"id": "q3b_erjian_4qi_qiepian", "name": "四期-切片GT", "icon": "🔪",
     "metric_keys": ["violation_rate", "miss_rate"],
     "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
     "thresholds": {"violation_rate": {"min": 0.99}, "miss_rate": {"max": 0.01}}},
    {"id": "q4_jubao_4qi", "name": "四期-举报", "icon": "🚨",
     "metric_keys": ["pre_violation_rate", "pre_miss_rate", "post_violation_rate", "post_miss_rate"],
     "metric_labels": {"pre_violation_rate": "申诉前违规率", "pre_miss_rate": "申诉前漏率",
                       "post_violation_rate": "申诉后违规率", "post_miss_rate": "申诉后漏率"},
     "thresholds": {"pre_violation_rate": {"min": 0.99}, "pre_miss_rate": {"max": 0.01},
                    "post_violation_rate": {"min": 0.99}, "post_miss_rate": {"max": 0.01}}},
    {"id": "q5_lahei", "name": "拉黑误漏", "icon": "🚫",
     "metric_keys": ["violation_rate", "miss_rate"],
     "metric_labels": {"violation_rate": "违规准确率", "miss_rate": "漏率"},
     "thresholds": {"violation_rate": {"min": 0.98}, "miss_rate": {"max": 0.02}}},
    {"id": "q6_shangqiang", "name": "上墙文本", "icon": "📝",
     "metric_keys": ["audit_accuracy"],
     "metric_labels": {"audit_accuracy": "审核准确率"},
     "thresholds": {"audit_accuracy": {"min": 0.98}}},
]


def check_threshold(q, metric_key, value):
    """检查指标是否达标（与 app.py 同逻辑）"""
    thresholds = q.get("thresholds", {})
    rule = thresholds.get(metric_key)
    if not rule or value is None:
        return True, ""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return True, ""
    if metric_key in ("violation_rate", "audit_accuracy", "pre_violation_rate", "post_violation_rate"):
        min_val = rule.get("min")
        if min_val is not None and v < min_val:
            return False, f"低于底线{min_val*100:.0f}%"
    elif metric_key in ("miss_rate", "pre_miss_rate", "post_miss_rate"):
        max_val = rule.get("max")
        if max_val is not None and v > max_val:
            return False, f"超出上限{max_val*100:.0f}%"
    return True, ""


# ============================================================
#  数据读取
# ============================================================
def get_latest_metrics():
    """从 SQLite 读取各队列最新一天的非零指标"""
    if not DB_FILE.exists():
        log.error(f"数据库不存在: {DB_FILE}")
        return []

    # 使用 collector 的加密连接（自动处理 SQLCipher 密码）
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
    from collector import get_db_connection
    conn = get_db_connection()
    c = conn.cursor()

    results = []
    for q in QUEUES:
        qid = q["id"]
        # 获取该队列最新非零数据的日期
        c.execute("""
            SELECT date, metric_key, metric_value 
            FROM daily_metrics 
            WHERE queue_id=? 
            ORDER BY date DESC
        """, (qid,))
        
        rows = c.fetchall()
        if not rows:
            results.append({
                "queue": q["name"],
                "icon": q["icon"],
                "date": "--",
                "metrics": {},
                "alerts": [],
            })
            continue

        # 找最新非零日期（用索引访问，兼容 sqlcipher3）
        dates_vals = {}
        for r in rows:
            d = r[0]   # date
            k = r[1]   # metric_key
            v = r[2]   # metric_value
            if d not in dates_vals:
                dates_vals[d] = {}
            dates_vals[d][k] = v

        latest_date = None
        latest_metrics = {}
        for d in sorted(dates_vals.keys(), reverse=True):
            has_nonzero = any(
                v and isinstance(v, (int, float)) and v != 0
                for v in dates_vals[d].values()
            )
            if has_nonzero:
                latest_date = d
                latest_metrics = dates_vals[d]
                break

        if not latest_date:
            latest_date = sorted(dates_vals.keys(), reverse=True)[0]
            latest_metrics = dates_vals[latest_date]

        # 格式化指标 + 检查阈值
        formatted = {}
        alerts = []
        for mk in q["metric_keys"]:
            raw_val = latest_metrics.get(mk)
            if raw_val is not None and isinstance(raw_val, (int, float)):
                formatted[mk] = f"{raw_val * 100:.2f}%"
                is_ok, alert_txt = check_threshold(q, mk, raw_val)
                if not is_ok:
                    label = q["metric_labels"].get(mk, mk)
                    alerts.append(f"{label}{alert_txt}")
            else:
                formatted[mk] = "--"

        results.append({
            "queue": q["name"],
            "icon": q["icon"],
            "date": latest_date,
            "metrics": formatted,
            "metric_labels": q["metric_labels"],
            "alerts": alerts,
        })

    conn.close()
    return results


# ============================================================
#  DeepSeek AI 摘要生成
# ============================================================
def _load_gongfeng_auth():
    """从 OpenClaw auth-profiles 读取工蜂代理认证信息。"""
    auth_path = Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json"
    if not auth_path.exists():
        return None
    try:
        import json as _json
        with open(auth_path, "r", encoding="utf-8") as f:
            profiles = _json.load(f)
        gf = profiles.get("profiles", {}).get("gongfeng:default", {})
        if not gf or gf.get("type") != "oauth":
            return None
        return {
            "access": gf.get("access", ""),
            "username": gf.get("username", ""),
            "deviceId": gf.get("deviceId", ""),
        }
    except Exception:
        return None


def _call_llm(system_prompt, user_prompt, model="claude-sonnet-4-5",
              max_tokens=500, temperature=0.3):
    """调用 LLM API，优先工蜂代理（免费 Claude），fallback DeepSeek。"""
    # ── 优先：工蜂代理 ──
    gf_auth = _load_gongfeng_auth()
    if gf_auth and gf_auth["access"]:
        try:
            gf_url = "https://copilot.code.woa.com/server/openclaw/copilot-gateway/v1/chat/completions"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {gf_auth['access']}",
                "X-Model-Name": "Claude Sonnet 4.5",
                "X-Username": gf_auth["username"],
                "OAUTH-TOKEN": gf_auth["access"],
                "DEVICE-ID": gf_auth["deviceId"],
            }
            resp = requests.post(
                gf_url,
                headers=headers,
                json={
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    "max_tokens": max_tokens,
                    "temperature": temperature,
                },
                timeout=60,
            )
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"].strip()
            if content.startswith("```"):
                lines = content.split("\n")
                content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
            log.info("✅ 工蜂代理 Claude 调用成功")
            return content
        except Exception as e:
            log.warning(f"⚠️ 工蜂代理调用失败: {e}，fallback DeepSeek")

    # ── Fallback：DeepSeek ──
    if not api_key:
        log.warning("⚠️ 无可用 LLM（工蜂代理和 DeepSeek 均不可用）")
        return None
    try:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        resp = requests.post(
            f"{base_url}/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"].strip()
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        log.info("✅ DeepSeek fallback 调用成功")
        return content
    except requests.exceptions.RequestException as e:
        log.error(f"❌ DeepSeek API 调用失败: {e}")
        return None
    except (KeyError, IndexError) as e:
        log.error(f"❌ DeepSeek API 返回格式异常: {e}")
        return None


def generate_ai_summary(metrics_data, api_key=None, base_url="https://api.deepseek.com"):
    """调用 LLM 生成日报摘要（优先 Claude，fallback DeepSeek）。"""
    # 组装 prompt
    data_text = ""
    alert_queues = []
    for item in metrics_data:
        data_text += f"\n{item['icon']} {item['queue']}（{item['date']}）:\n"
        for mk, val in item["metrics"].items():
            label = item["metric_labels"].get(mk, mk)
            data_text += f"  - {label}: {val}\n"
        if item["alerts"]:
            alert_queues.append(item["queue"])
            data_text += f"  ⚠️ 不达标: {', '.join(item['alerts'])}\n"

    alert_summary = ""
    if alert_queues:
        alert_summary = f"\n特别关注：以下队列未达标 - {', '.join(alert_queues)}"

    system_prompt = "你是质检数据分析师，擅长从数据中提取关键信息并给出专业建议。"
    user_prompt = f"""根据以下各队列最新指标数据，生成一份简洁的日报摘要。

要求：
1. 用 3-5 句话概括整体情况
2. 重点标注不达标的队列和指标
3. 给出简短的改善建议（如有不达标项）
4. 语气专业简洁，不要啰嗦
5. 使用 Markdown 格式

今日数据：{data_text}
{alert_summary}"""

    return _call_llm(system_prompt, user_prompt)


# ============================================================
#  企微 Webhook 机器人推送
# ============================================================
def send_webhook_markdown(webhook_key, content):
    """通过企微群机器人 Webhook 发送 Markdown 消息"""
    url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={webhook_key}"
    payload = {
        "msgtype": "markdown",
        "markdown": {"content": content},
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode") == 0:
            log.info("✅ 企微 Webhook Markdown 推送成功")
            return True
        else:
            log.error(f"❌ 企微 Webhook 推送失败: {data.get('errmsg', '未知错误')}")
            return False
    except Exception as e:
        log.error(f"❌ 企微 Webhook 请求失败: {e}")
        return False


def send_webhook_text(webhook_key, content):
    """通过企微群机器人 Webhook 发送纯文本消息（Markdown 失败时的 fallback）"""
    url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={webhook_key}"
    payload = {
        "msgtype": "text",
        "text": {"content": content},
    }
    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode") == 0:
            log.info("✅ 企微 Webhook 纯文本推送成功")
            return True
        else:
            log.error(f"❌ 企微 Webhook 纯文本推送失败: {data.get('errmsg', '未知错误')}")
            return False
    except Exception as e:
        log.error(f"❌ 企微 Webhook 请求失败: {e}")
        return False


def build_markdown_message(metrics_data, ai_summary=None):
    """组装企微 Markdown 格式消息"""
    today = date.today().strftime("%Y-%m-%d")
    
    lines = [
        f"# 📊 QC 质检日报 {today}",
        "",
    ]

    # 不达标队列汇总（放最前面）
    alert_items = [item for item in metrics_data if item["alerts"]]
    if alert_items:
        lines.append("## ⚠️ 不达标队列")
        for item in alert_items:
            alert_str = "、".join(item["alerts"])
            lines.append(f"> **{item['icon']} {item['queue']}**：{alert_str}")
        lines.append("")

    # 各队列明细
    lines.append("## 📋 各队列指标")
    for item in metrics_data:
        metrics_str = " | ".join(
            f"{item['metric_labels'].get(mk, mk)}: {val}"
            for mk, val in item["metrics"].items()
        )
        alert_marker = " ⚠️" if item["alerts"] else " ✅"
        lines.append(f"- {item['icon']} **{item['queue']}**（{item['date']}）{alert_marker}")
        lines.append(f"  {metrics_str}")
    
    lines.append("")

    # AI 摘要
    if ai_summary:
        lines.append("## 🤖 AI 分析")
        lines.append(ai_summary)
        lines.append("")

    lines.append("---")
    lines.append(f"<font color=\"comment\">QC Dashboard · 自动推送 · {datetime.now().strftime('%H:%M')}</font>")

    return "\n".join(lines)


def build_text_message(metrics_data, ai_summary=None):
    """组装纯文本格式消息（Markdown 不支持时的 fallback）"""
    today = date.today().strftime("%Y-%m-%d")
    
    lines = [
        f"📊 QC 质检日报 {today}",
        "=" * 30,
        "",
    ]

    alert_items = [item for item in metrics_data if item["alerts"]]
    if alert_items:
        lines.append("⚠️ 不达标队列：")
        for item in alert_items:
            alert_str = "、".join(item["alerts"])
            lines.append(f"  {item['icon']} {item['queue']}：{alert_str}")
        lines.append("")

    for item in metrics_data:
        metrics_str = " | ".join(
            f"{item['metric_labels'].get(mk, mk)}:{val}"
            for mk, val in item["metrics"].items()
        )
        marker = "⚠️" if item["alerts"] else "✅"
        lines.append(f"{item['icon']} {item['queue']}({item['date']}) {marker}")
        lines.append(f"  {metrics_str}")

    if ai_summary:
        lines.append("")
        lines.append("🤖 AI 分析：")
        lines.append(ai_summary)

    return "\n".join(lines)


# ============================================================
#  主流程
# ============================================================
def main():
    test_mode = "--test" in sys.argv
    no_ai = "--no-ai" in sys.argv
    user_override = None
    if "--user" in sys.argv:
        idx = sys.argv.index("--user")
        if idx + 1 < len(sys.argv):
            user_override = sys.argv[idx + 1]

    log.info("=" * 60)
    log.info("📤 QC Dashboard 每日数据推送")
    log.info(f"   时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"   模式: {'测试' if test_mode else '正式'} | AI: {'关闭' if no_ai else '开启'}")
    log.info("=" * 60)

    try:
        # ── Step 1: 读取配置 ──
        import yaml
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        webhook_cfg = config.get("global", {}).get("wecom_webhook", {})
        webhook_key = os.environ.get("WECOM_WEBHOOK_KEY", "") or webhook_cfg.get("key", "")

        # DeepSeek 配置
        ds_cfg = config.get("global", {}).get("deepseek", {})
        deepseek_key = os.environ.get("DEEPSEEK_API_KEY", "") or ds_cfg.get("api_key", "")
        deepseek_url = os.environ.get("DEEPSEEK_BASE_URL", "") or ds_cfg.get("base_url", "https://api.deepseek.com")

        # ── Step 2: 读取最新数据 ──
        log.info("📊 [1/4] 读取最新指标数据...")
        metrics_data = get_latest_metrics()
        
        if not metrics_data:
            log.error("❌ 无数据可推送")
            return 1

        alert_count = sum(1 for item in metrics_data if item["alerts"])
        log.info(f"   ✅ 读取完成：{len(metrics_data)} 个队列，{alert_count} 个不达标")

        # ── Step 3: AI 摘要（可选）──
        ai_summary = None
        if not no_ai:
            log.info("🤖 [2/4] 生成 AI 摘要...")
            if deepseek_key:
                ai_summary = generate_ai_summary(metrics_data, deepseek_key, deepseek_url)
            else:
                log.warning("   ⚠️ 未配置 DEEPSEEK_API_KEY，跳过 AI 摘要")
        else:
            log.info("🤖 [2/4] AI 摘要已关闭 (--no-ai)")

        # ── Step 4: 组装消息 ──
        log.info("📝 [3/4] 组装推送消息...")
        md_content = build_markdown_message(metrics_data, ai_summary)
        text_content = build_text_message(metrics_data, ai_summary)

        if test_mode:
            log.info("🏁 测试模式，消息内容如下：")
            print("\n" + "=" * 50)
            print("Markdown 格式：")
            print("=" * 50)
            print(md_content)
            print("\n" + "=" * 50)
            print("纯文本格式：")
            print("=" * 50)
            print(text_content)
            return 0

        # ── Step 5: 推送 ──
        log.info("📤 [4/4] 推送企微 Webhook 消息...")

        if not webhook_key:
            log.error("❌ 缺少企微 Webhook Key！请设置环境变量 WECOM_WEBHOOK_KEY 或在 config.yaml 中配置 wecom_webhook.key")
            log.info("💡 获取方式：企微群 → 添加群机器人 → 复制 Webhook 地址中的 key 参数")
            return 1

        # 优先发 Markdown，失败 fallback 到文本
        success = send_webhook_markdown(webhook_key, md_content)
        if not success:
            log.info("   尝试 fallback 纯文本格式...")
            success = send_webhook_text(webhook_key, text_content)

        if success:
            log.info("=" * 60)
            log.info("🎉 每日数据推送完成！")
            log.info("=" * 60)
            return 0
        else:
            return 1

    except Exception as e:
        log.error(f"❌ 推送失败: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
