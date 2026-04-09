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
import sqlite3
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
    if metric_key in ("violation_rate", "audit_accuracy"):
        min_val = rule.get("min")
        if min_val is not None and v < min_val:
            return False, f"低于底线{min_val*100:.0f}%"
    elif metric_key in ("miss_rate",):
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

    conn = sqlite3.connect(str(DB_FILE))
    conn.row_factory = sqlite3.Row
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

        # 找最新非零日期
        dates_vals = {}
        for r in rows:
            d = r["date"]
            k = r["metric_key"]
            v = r["metric_value"]
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
def generate_ai_summary(metrics_data, api_key=None, base_url="https://api.deepseek.com"):
    """调用 DeepSeek API 生成日报摘要"""
    if not api_key:
        log.warning("未配置 DeepSeek API Key，跳过 AI 摘要生成")
        return None

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

    prompt = f"""你是质检数据分析师。根据以下各队列最新指标数据，生成一份简洁的日报摘要。

要求：
1. 用 3-5 句话概括整体情况
2. 重点标注不达标的队列和指标
3. 给出简短的改善建议（如有不达标项）
4. 语气专业简洁，不要啰嗦
5. 使用 Markdown 格式

今日数据：{data_text}
{alert_summary}"""

    try:
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": "deepseek-chat",
            "messages": [
                {"role": "system", "content": "你是质检数据分析师，擅长从数据中提取关键信息并给出专业建议。"},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.3,
            "max_tokens": 500,
        }

        resp = requests.post(
            f"{base_url}/v1/chat/completions",
            headers=headers,
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        result = resp.json()
        summary = result["choices"][0]["message"]["content"].strip()
        log.info("✅ DeepSeek AI 摘要生成成功")
        return summary

    except requests.exceptions.RequestException as e:
        log.error(f"❌ DeepSeek API 调用失败: {e}")
        return None
    except (KeyError, IndexError) as e:
        log.error(f"❌ DeepSeek API 返回格式异常: {e}")
        return None


# ============================================================
#  企微应用消息推送
# ============================================================
def get_wecom_token(corpid, secret):
    """获取企微应用 access_token"""
    url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
    params = {"corpid": corpid, "corpsecret": secret}
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode") != 0:
            log.error(f"获取 token 失败: {data.get('errmsg', '未知错误')}")
            return None
        return data.get("access_token")
    except Exception as e:
        log.error(f"获取 token 请求失败: {e}")
        return None


def send_wecom_message(token, agent_id, content, touser="@all", msg_type="markdown"):
    """通过企微应用消息 API 发送消息"""
    url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={token}"
    
    payload = {
        "touser": touser,
        "msgtype": msg_type,
        "agentid": int(agent_id),
    }

    if msg_type == "markdown":
        payload["markdown"] = {"content": content}
    elif msg_type == "text":
        payload["text"] = {"content": content}

    try:
        resp = requests.post(url, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("errcode") == 0:
            log.info(f"✅ 企微消息推送成功 (touser={touser})")
            return True
        else:
            log.error(f"❌ 企微消息推送失败: {data.get('errmsg', '未知错误')}")
            return False
    except Exception as e:
        log.error(f"❌ 企微消息推送请求失败: {e}")
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

        wcom_cfg = config.get("global", {}).get("wcom_api", {})
        corpid = os.environ.get("WECOM_CORPID", "") or wcom_cfg.get("corpid", "")
        secret = wcom_cfg.get("secret", "")
        agent_id = wcom_cfg.get("bot_id", "")

        # DeepSeek 配置
        ds_cfg = config.get("global", {}).get("deepseek", {})
        deepseek_key = os.environ.get("DEEPSEEK_API_KEY", "") or ds_cfg.get("api_key", "")
        deepseek_url = os.environ.get("DEEPSEEK_BASE_URL", "") or ds_cfg.get("base_url", "https://api.deepseek.com")

        # 推送目标用户
        touser = user_override or os.environ.get("WECOM_TOUSER", "") or wcom_cfg.get("touser", "@all")

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
        log.info(f"📤 [4/4] 推送企微消息 (touser={touser})...")

        if not corpid:
            log.error("❌ 缺少企微 CorpID！请设置环境变量 WECOM_CORPID 或在 config.yaml 中配置")
            log.info("💡 获取方式：企微管理后台 → 我的企业 → 企业信息 → 企业ID")
            return 1

        if not secret or not agent_id:
            log.error("❌ 缺少企微应用 Secret 或 AgentID！请检查 config.yaml")
            return 1

        # 获取 access_token
        token = get_wecom_token(corpid, secret)
        if not token:
            return 1

        # 优先发 Markdown，失败 fallback 到文本
        success = send_wecom_message(token, agent_id, md_content, touser, "markdown")
        if not success:
            log.info("   尝试 fallback 纯文本格式...")
            success = send_wecom_message(token, agent_id, text_content, touser, "text")

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
