#!/usr/bin/env python3
"""质检日报：按母业务/子业务两层汇总，生成 Markdown 日报并通过企微机器人推送。

日报结构（五段式，结论先行）：
  1. 一、今日结论       — 一句话说清大盘 + 核心风险点
  2. 二、分组表现       — A组/B组 各自的数据 + 结论判断
  3. 三、重点风险       — 未达标板块、问题集中队列、归因方向
  4. 四、处理动作       — 交付侧 / 质培侧 / 次日关注
  5. 五、补充风险       — 告警联动判断

业务线映射规则（BUSINESS_LINE_RULES）：
  - 基于入库时的 group_name（公司字段）和 queue_name 推导母业务/子业务
  - 对齐老系统 identify_business_line 的逻辑

错判/漏判识别：从 qa_result / qa_note / error_type 文本中关键词提取（对齐老代码）。

PMT（Prompt Template）：
  日报生成逻辑按固定模板输出，结构与提示词模板对齐，确保每天输出一致。
  PMT 模板存储于 config/report_pmt.md，可独立维护。
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage.repository import DashboardRepository

# ── 正确率目标值 ──
ACC_TARGET = 99.00  # %
# "达标但需关注"的缓冲区间
WATCH_BUFFER = 0.50  # 99.00% ~ 99.50% 为关注区间

# ── 已知业务组（用于检测缺席组） ──
KNOWN_GROUPS = ["A组", "B组"]

# ═══════════════════════════════════════════════════════════════
#  业务线映射规则
# ═══════════════════════════════════════════════════════════════

BUSINESS_LINE_RULES = [
    {
        "match": "queue_contains",
        "keyword": "账号",
        "母业务": "B组",
        "子业务": "B组-账号",
    },
    {
        "match": "group_contains",
        "keyword": "长沙",
        "母业务": "A组",
        "子业务": "A组-评论",
    },
    {
        "match": "group_contains",
        "keyword": "重庆",
        "母业务": "B组",
        "子业务": "B组-评论",
    },
    {
        "match": "group_contains",
        "keyword": "视频号安全",
        "母业务": "视频号安全",
        "子业务": "视频号安全-评论",
    },
]


def classify_business_line_sql() -> tuple[str, str]:
    """生成两个独立的 SQL CASE 表达式，分别返回母业务和子业务。"""
    mother_cases = []
    sub_cases = []
    for rule in BUSINESS_LINE_RULES:
        match_type = rule["match"]
        keyword = rule["keyword"].replace("'", "''")
        if match_type == "queue_contains":
            mother_cases.append(f"WHEN COALESCE(queue_name, '') LIKE '%{keyword}%' THEN '{rule['母业务']}'")
            sub_cases.append(f"WHEN COALESCE(queue_name, '') LIKE '%{keyword}%' THEN '{rule['子业务']}'")
        elif match_type == "group_contains":
            mother_cases.append(f"WHEN COALESCE(group_name, '') LIKE '%{keyword}%' THEN '{rule['母业务']}'")
            sub_cases.append(f"WHEN COALESCE(group_name, '') LIKE '%{keyword}%' THEN '{rule['子业务']}'")
    fb = "ELSE COALESCE(NULLIF(TRIM(group_name), ''), '未识别')"
    mother_sql = "CASE " + " ".join(mother_cases) + " " + fb + " END"
    sub_sql = "CASE " + " ".join(sub_cases) + " " + fb + " END"
    return mother_sql, sub_sql


# ═══════════════════════════════════════════════════════════════
#  工具函数
# ═══════════════════════════════════════════════════════════════

def load_settings() -> dict:
    with open(PROJECT_ROOT / "config" / "settings.json", "r", encoding="utf-8") as f:
        return json.load(f)


def _si(val) -> int:
    if val is None:
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def _sf(val, decimals: int = 2) -> float:
    """安全转 float（兼容 decimal.Decimal / int / str 等类型）。"""
    if val is None:
        return 0.0
    try:
        # decimal.Decimal 和 int 都能被 float() 转换
        return round(float(val), decimals)
    except (ValueError, TypeError):
        return 0.0


def _safe_mul(a, b):
    """安全乘法（兼容 decimal.Decimal * float）。"""
    try:
        return float(a) * float(b)
    except (ValueError, TypeError):
        return 0.0


def _acc_flag(acc: float) -> str:
    """达标/关注/未达标标志。"""
    if acc >= ACC_TARGET:
        return "✅"
    if acc >= ACC_TARGET - 0.5:
        return "🟡"
    return "🔴"


def _acc_label(acc: float) -> str:
    """文字标签。"""
    if acc >= ACC_TARGET + WATCH_BUFFER:
        return "达标"
    if acc >= ACC_TARGET:
        return "达标但需关注"
    if acc >= ACC_TARGET - 0.5:
        return "接近达标"
    return "未达标"


# ═══════════════════════════════════════════════════════════════
#  数据查询层
# ═══════════════════════════════════════════════════════════════

def _bl_case() -> tuple[str, str]:
    """返回 (母业务 CASE SQL, 子业务 CASE SQL)。"""
    return classify_business_line_sql()


def _cuopan_expr() -> str:
    return (
        "(CASE WHEN is_raw_correct = 0 AND ("
        "  COALESCE(raw_judgement, '') LIKE '%错判%' OR "
        "  COALESCE(qa_result, '') LIKE '%错判%' OR "
        "  COALESCE(qa_note, '') LIKE '%错判%' OR "
        "  COALESCE(error_type, '') LIKE '%错判%' OR "
        "  COALESCE(error_reason, '') LIKE '%错判%'"
        ") THEN 1 ELSE 0 END)"
    )


def _loupan_expr() -> str:
    return (
        "(CASE WHEN is_raw_correct = 0 AND ("
        "  COALESCE(raw_judgement, '') LIKE '%漏判%' OR "
        "  COALESCE(qa_result, '') LIKE '%漏判%' OR "
        "  COALESCE(qa_note, '') LIKE '%漏判%' OR "
        "  COALESCE(error_type, '') LIKE '%漏判%' OR "
        "  COALESCE(error_reason, '') LIKE '%漏判%'"
        ") THEN 1 ELSE 0 END)"
    )


def _query_sub_biz(conn, d: str) -> list[dict]:
    cp = _cuopan_expr()
    lp = _loupan_expr()
    sql = f"""
        SELECT
            mother_biz,
            sub_biz,
            COUNT(*) AS qa_cnt,
            SUM(CASE WHEN is_raw_correct = 1 THEN 1 ELSE 0 END) AS raw_correct,
            SUM({cp}) AS cuopan,
            SUM({lp}) AS loupan,
            SUM(CASE WHEN is_raw_correct = 0 THEN 1 ELSE 0 END) AS error_total,
            SUM(CASE WHEN is_appealed = 1 THEN 1 ELSE 0 END) AS appealed,
            SUM(CASE WHEN is_appeal_reversed = 1 THEN 1 ELSE 0 END) AS appeal_reversed
        FROM vw_qa_base
        WHERE biz_date = %s
          AND COALESCE(mother_biz, '') != ''
          AND COALESCE(sub_biz, '') != ''
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    df = conn.fetch_df(sql, [d])
    result = []
    for _, r in df.iterrows():
        cnt = _si(r["qa_cnt"])
        raw_acc = _sf(_safe_mul(r["raw_correct"], 100.0) / cnt) if cnt > 0 else 0.0
        result.append({
            "mother_biz": r["mother_biz"],
            "sub_biz": r["sub_biz"],
            "qa_cnt": cnt,
            "raw_correct": _si(r["raw_correct"]),
            "cuopan": _si(r["cuopan"]),
            "loupan": _si(r["loupan"]),
            "error_total": _si(r["error_total"]),
            "appealed": _si(r["appealed"]),
            "appeal_reversed": _si(r["appeal_reversed"]),
            "raw_acc": raw_acc,
        })
    return result


def _aggregate_mother(sub_list: list[dict]) -> list[dict]:
    from collections import OrderedDict
    mothers: OrderedDict[str, dict] = OrderedDict()
    for s in sub_list:
        mb = s["mother_biz"]
        if mb not in mothers:
            mothers[mb] = {
                "mother_biz": mb,
                "qa_cnt": 0, "raw_correct": 0,
                "cuopan": 0, "loupan": 0, "error_total": 0,
                "appealed": 0, "appeal_reversed": 0,
            }
        m = mothers[mb]
        for k in ["qa_cnt", "raw_correct", "cuopan", "loupan", "error_total", "appealed", "appeal_reversed"]:
            m[k] += s[k]

    result = []
    for m in mothers.values():
        cnt = m["qa_cnt"]
        m["raw_acc"] = _sf(_safe_mul(m["raw_correct"], 100.0) / cnt) if cnt > 0 else 0.0
        result.append(m)
    return result


def _query_top_error_queues(conn, d: str) -> list[dict]:
    cp = _cuopan_expr()
    lp = _loupan_expr()
    sql = f"""
        SELECT
            mother_biz, sub_biz, queue_name,
            COUNT(*) AS err_cnt,
            SUM({cp}) AS cuopan,
            SUM({lp}) AS loupan
        FROM vw_qa_base
        WHERE biz_date = %s
          AND is_raw_correct = 0
          AND COALESCE(mother_biz, '') != ''
          AND COALESCE(sub_biz, '') != ''
        GROUP BY 1, 2, 3
        ORDER BY 4 DESC LIMIT 10
    """
    df = conn.fetch_df(sql, [d])
    return [
        {
            "mother_biz": r["mother_biz"], "sub_biz": r["sub_biz"], "queue": r["queue_name"],
            "err_cnt": _si(r["err_cnt"]), "cuopan": _si(r["cuopan"]), "loupan": _si(r["loupan"]),
        }
        for _, r in df.iterrows()
    ]


def _query_top_error_types(conn, d: str) -> list[dict]:
    """错误类型 TOP N（用于重点队列归因说明）。"""
    sql = f"""
        SELECT COALESCE(NULLIF(TRIM(error_type), ''), '未标注') AS err_type,
               COUNT(*) AS cnt
        FROM vw_qa_base
        WHERE biz_date = %s AND is_raw_correct = 0
        GROUP BY 1 ORDER BY 2 DESC LIMIT 5
    """
    df = conn.fetch_df(sql, [d])
    return [{"error_type": r["err_type"], "cnt": _si(r["cnt"])} for _, r in df.iterrows()]


def _query_alerts(conn, d: str) -> dict:
    df = conn.fetch_df("""
        SELECT severity, COUNT(*) AS cnt FROM fact_alert_event
        WHERE alert_date = %s GROUP BY 1
    """, [d])
    p0 = _si(next((r["cnt"] for _, r in df.iterrows() if r["severity"] == "P0"), 0))
    p1 = _si(next((r["cnt"] for _, r in df.iterrows() if r["severity"] == "P1"), 0))
    p2 = _si(next((r["cnt"] for _, r in df.iterrows() if r["severity"] == "P2"), 0))
    return {"P0": p0, "P1": p1, "P2": p2, "total": p0 + p1 + p2}


def _query_yesterday_sub(conn, d: str) -> list[dict] | None:
    yesterday = str(date.fromisoformat(d) - timedelta(days=1))
    sql = """
        SELECT
            sub_biz,
            COUNT(*) AS qa_cnt,
            ROUND(SUM(CASE WHEN is_raw_correct = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS raw_acc
        FROM vw_qa_base
        WHERE biz_date = %s
          AND COALESCE(mother_biz, '') != ''
          AND COALESCE(sub_biz, '') != ''
        GROUP BY 1
    """
    df = conn.fetch_df(sql, [yesterday])
    if df.empty:
        return None
    return [{"sub_biz": r["sub_biz"], "qa_cnt": _si(r["qa_cnt"]), "raw_acc": _sf(r["raw_acc"])} for _, r in df.iterrows()]


def _query_yesterday_overall(conn, d: str) -> dict | None:
    yesterday = str(date.fromisoformat(d) - timedelta(days=1))
    df = conn.fetch_df("""
        SELECT COUNT(*) AS total_qa, ROUND(SUM(CASE WHEN is_raw_correct = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS raw_acc
        FROM vw_qa_base WHERE biz_date = %s
    """, [yesterday])
    if df.empty or _si(df.iloc[0]["total_qa"]) == 0:
        return None
    return {"total_qa": _si(df.iloc[0]["total_qa"]), "raw_acc": _sf(df.iloc[0]["raw_acc"])}


def _query_yesterday_alerts(conn, d: str) -> dict:
    """查询昨日告警数量，用于补充风险回顾。"""
    yesterday = str(date.fromisoformat(d) - timedelta(days=1))
    df = conn.fetch_df("""
        SELECT severity, COUNT(*) AS cnt FROM fact_alert_event
        WHERE alert_date = %s GROUP BY 1
    """, [yesterday])
    p0 = _si(next((r["cnt"] for _, r in df.iterrows() if r["severity"] == "P0"), 0))
    p1 = _si(next((r["cnt"] for _, r in df.iterrows() if r["severity"] == "P1"), 0))
    p2 = _si(next((r["cnt"] for _, r in df.iterrows() if r["severity"] == "P2"), 0))
    return {"P0": p0, "P1": p1, "P2": p2, "total": p0 + p1 + p2}


def _query_watch_queues(conn, d: str) -> list[dict]:
    """查询每个子业务下各队列的质检量和正确率，用于判断是否有观察队列。"""
    sql = """
        SELECT
            mother_biz, sub_biz, queue_name,
            COUNT(*) AS qa_cnt,
            ROUND(SUM(CASE WHEN is_raw_correct = 1 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 2) AS raw_acc
        FROM vw_qa_base
        WHERE biz_date = %s
          AND COALESCE(mother_biz, '') != ''
          AND COALESCE(sub_biz, '') != ''
        GROUP BY 1, 2, 3
        ORDER BY mother_biz, sub_biz, 5 ASC
    """
    df = conn.fetch_df(sql, [d])
    return [
        {
            "mother_biz": r["mother_biz"], "sub_biz": r["sub_biz"], "queue": r["queue_name"],
            "qa_cnt": _si(r["qa_cnt"]), "raw_acc": _sf(r["raw_acc"]),
        }
        for _, r in df.iterrows()
    ]


# ═══════════════════════════════════════════════════════════════
#  日报数据构建
# ═══════════════════════════════════════════════════════════════

def build_daily_report(conn, report_date: date) -> dict:
    d = str(report_date)

    sub_list = _query_sub_biz(conn, d)
    if not sub_list:
        return {"report_date": d, "has_data": False, "message": f"{d} 无质检数据。"}

    mother_list = _aggregate_mother(sub_list)
    top_error_queues = _query_top_error_queues(conn, d)
    top_error_types = _query_top_error_types(conn, d)
    alerts = _query_alerts(conn, d)
    yesterday_sub = _query_yesterday_sub(conn, d)
    yesterday_overall = _query_yesterday_overall(conn, d)
    yesterday_alerts = _query_yesterday_alerts(conn, d)
    watch_queues = _query_watch_queues(conn, d)

    total_qa = sum(s["qa_cnt"] for s in sub_list)
    total_correct = sum(s["raw_correct"] for s in sub_list)
    total_error = sum(s["error_total"] for s in sub_list)
    total_cuopan = sum(s["cuopan"] for s in sub_list)
    total_loupan = sum(s["loupan"] for s in sub_list)
    total_appeal = sum(s["appealed"] for s in sub_list)
    total_appeal_rev = sum(s["appeal_reversed"] for s in sub_list)
    raw_acc = _sf(_safe_mul(total_correct, 100.0) / total_qa) if total_qa > 0 else 0.0
    appeal_rev_rate = _sf(_safe_mul(total_appeal_rev, 100.0) / total_appeal) if total_appeal > 0 else 0.0

    # 质检量环比
    volume_change_pct = None
    if yesterday_overall and yesterday_overall["total_qa"] > 0:
        volume_change_pct = _sf((total_qa - yesterday_overall["total_qa"]) / yesterday_overall["total_qa"] * 100)

    overview = {
        "total_qa": total_qa,
        "raw_correct": total_correct,
        "error_total": total_error,
        "cuopan": total_cuopan,
        "loupan": total_loupan,
        "appealed": total_appeal,
        "appeal_reversed": total_appeal_rev,
        "raw_acc": raw_acc,
        "appeal_rev_rate": appeal_rev_rate,
        "volume_change_pct": volume_change_pct,
    }

    return {
        "report_date": d,
        "has_data": True,
        "target": ACC_TARGET,
        "overview": overview,
        "mother_list": mother_list,
        "sub_list": sub_list,
        "top_error_queues": top_error_queues,
        "top_error_types": top_error_types,
        "alerts": alerts,
        "yesterday_overall": yesterday_overall,
        "yesterday_sub": yesterday_sub,
        "yesterday_alerts": yesterday_alerts,
        "watch_queues": watch_queues,
    }


# ═══════════════════════════════════════════════════════════════
#  五段式日报格式化
# ═══════════════════════════════════════════════════════════════

def _group_by_mother(report: dict) -> dict:
    """将子业务按母业务归集，返回结构化数据。"""
    from collections import OrderedDict

    groups: OrderedDict[str, dict] = OrderedDict()
    mother_map = {m["mother_biz"]: m for m in report["mother_list"]}

    for s in report["sub_list"]:
        mb = s["mother_biz"]
        if mb not in groups:
            groups[mb] = {
                "mother_biz": mb,
                "mother": mother_map.get(mb, {}),
                "subs": [],
            }
        groups[mb]["subs"].append(s)

    # 按母业务汇总
    queues_by_sub: dict[str, list[dict]] = {}
    for q in report["top_error_queues"]:
        queues_by_sub.setdefault(q["sub_biz"], []).append(q)

    watch_by_sub: dict[str, list[dict]] = {}
    for w in report["watch_queues"]:
        watch_by_sub.setdefault(w["sub_biz"], []).append(w)

    result = {}
    for mb_name, g in groups.items():
        m = g["mother"]
        flag = _acc_flag(m.get("raw_acc", 0))
        total_qa = m.get("qa_cnt", 0)
        total_err = m.get("error_total", 0)
        raw_acc = m.get("raw_acc", 0)

        # 找出该组未达标 / 承压的子业务
        unqual_subs = [s for s in g["subs"] if s["raw_acc"] < ACC_TARGET]
        watch_subs = [s for s in g["subs"]
                      if ACC_TARGET <= s["raw_acc"] < ACC_TARGET + WATCH_BUFFER
                      and s["qa_cnt"] > 1000]

        # 找出该组正确率最低的队列（观察队列）
        all_queues = watch_by_sub.get(mb_name, [])
        # 展开所有子业务的队列
        sub_names = [s["sub_biz"] for s in g["subs"]]
        low_queues = [
            w for w in all_queues
            if w["sub_biz"] in sub_names and w["raw_acc"] < ACC_TARGET + WATCH_BUFFER
        ]

        result[mb_name] = {
            "mother_biz": mb_name,
            "flag": flag,
            "total_qa": total_qa,
            "total_err": total_err,
            "raw_acc": raw_acc,
            "subs": g["subs"],
            "unqual_subs": unqual_subs,
            "watch_subs": watch_subs,
            "low_queues": low_queues[:3],  # 最多展示 3 个观察队列
        }

    return result


def _build_conclusion(report: dict, grouped: dict) -> str:
    """一、今日结论：一句话说清大盘 + 核心风险点。"""
    o = report["overview"]
    acc = o["raw_acc"]
    target = report["target"]

    # 判断各组状态
    overall_ok = acc >= target
    has_unqual = any(g["unqual_subs"] for g in grouped.values())
    has_watch = any(g["watch_subs"] for g in grouped.values())

    if not has_unqual and not has_watch:
        conclusion = "今日整体质量达标，各业务线表现平稳，暂无明显风险点。"
    else:
        # 有风险
        parts = []
        if has_unqual:
            unqual_names = []
            for g in grouped.values():
                for s in g["unqual_subs"]:
                    unqual_names.append(f"{s['sub_biz']}（{s['raw_acc']:.2f}%）")
            risk_desc = "、".join(unqual_names)
            parts.append(f"其中{'，'.join([n for n in unqual_names[:2]])}未达标，为当日核心风险点")
        elif has_watch:
            for g in grouped.values():
                for s in g["watch_subs"]:
                    parts.append(f"{s['sub_biz']}正确率 {s['raw_acc']:.2f}%，处于关注区间")

        for g in grouped.values():
            if g["unqual_subs"]:
                worst = g["unqual_subs"][0]
                worst_queues = [q for q in report["top_error_queues"] if q["sub_biz"] == worst["sub_biz"]]
                if worst_queues:
                    parts.append(f"问题集中在「{worst_queues[0]['queue']}」")

        # 环比
        yesterday = report.get("yesterday_overall")
        pp_change = ""
        if yesterday:
            pp = acc - yesterday["raw_acc"]
            if pp < -0.3:
                pp_change = f"，较昨日下降 {abs(pp):.2f}pp"
            elif pp > 0.3:
                pp_change = f"，较昨日上升 {pp:.2f}pp"

        if overall_ok:
            first = f"今日整体质量达标{pp_change}，"
        else:
            gap = target - acc
            first = f"今日整体正确率 {acc:.2f}%，低于目标 {gap:.2f}pp{pp_change}，"

        conclusion = first + "；".join(parts) + "，需优先处理。"

    # 附加质检量环比提醒
    vol_pct = o.get("volume_change_pct")
    if vol_pct is not None and abs(vol_pct) >= 15:
        if vol_pct < -15:
            conclusion += f"质检量较昨日下降 {abs(vol_pct):.0f}%，请确认是否为业务正常波动。"
        else:
            conclusion += f"质检量较昨日上升 {vol_pct:.0f}%。"

    return conclusion


def _build_group_performance(grouped: dict) -> list[str]:
    """二、分组表现：每组的数据 + 结论判断。"""
    lines: list[str] = []

    for mb_name, g in grouped.items():
        flag = g["flag"]
        acc = g["raw_acc"]

        # 组概览行
        lines.append(f"【{mb_name}】")
        lines.append(f"正确率 {acc:.2f}% {flag}｜质检量 {g['total_qa']}｜出错 {g['total_err']}")

        # 子业务拆行（多子业务时展示）
        if len(g["subs"]) > 1:
            sub_parts = []
            for s in g["subs"]:
                s_flag = _acc_flag(s["raw_acc"])
                sub_parts.append(f"{s['sub_biz']} {s['raw_acc']:.2f}% {s_flag}")
            lines.append("其中：" + "｜".join(sub_parts))

        # 结论行
        conclusion = _build_group_conclusion(g)
        lines.append(f"结论：{conclusion}")
        lines.append("")

    # 缺席组提示（紧跟最后一组结论，无多余空行）
    present_groups = set(grouped.keys())
    absent_groups = [g for g in KNOWN_GROUPS if g not in present_groups]
    if absent_groups:
        # 移除末尾空行，让提示紧凑
        while lines and lines[-1] == "":
            lines.pop()
        lines.append(f"注：{'、'.join(absent_groups)}当日无质检数据。")
        lines.append("")

    return lines


def _build_group_conclusion(g: dict) -> str:
    """为单组生成结论判断。"""
    unqual = g["unqual_subs"]
    watch = g["watch_subs"]
    low_queues = g["low_queues"]

    if unqual:
        # 有未达标子业务
        worst = unqual[0]
        gap = ACC_TARGET - worst["raw_acc"]
        # 判断拖累方向
        if len(unqual) == 1 and len(g["subs"]) > 1:
            other_ok = [s["sub_biz"] for s in g["subs"] if s["raw_acc"] >= ACC_TARGET]
            other_str = "、".join(other_ok)
            return (
                f"{worst['sub_biz']}明显拖低表现（{worst['raw_acc']:.2f}%，低于目标 {gap:.2f}pp），"
                f"风险集中且方向明确，应优先处理。"
            )
        else:
            return (
                f"多个板块未达标，正确率最低为 {worst['sub_biz']}（{worst['raw_acc']:.2f}%），"
                f"需逐板块排查。"
            )

    if watch:
        # 承压
        worst_watch = watch[0]
        queue_note = ""
        if low_queues:
            queue_names = [q["queue"] for q in low_queues[:2]]
            queue_note = f"，偏低队列如「{'、'.join(queue_names)}」需持续观察"
        return (
            f"整体达标但承压（{worst_watch['sub_biz']} {worst_watch['raw_acc']:.2f}%），"
            f"非明显失控但需关注局部波动{queue_note}。"
        )

    # 全部稳定
    if low_queues:
        queue_names = [q["queue"] for q in low_queues[:2]]
        return (
            f"整体稳定，暂未发现明显失控队列。"
            f"当前偏低队列如「{'、'.join(queue_names)}」可纳入日常观察。"
        )
    return "整体稳定，当前未见明显失控队列，可维持当前审核标准并持续日常监控。"


def _build_risks(report: dict, grouped: dict) -> list[str]:
    """三、重点风险：未达标板块、集中队列、归因方向。"""
    risks: list[str] = []
    risk_idx = 0

    for mb_name, g in grouped.items():
        for s in g["unqual_subs"]:
            risk_idx += 1
            gap = ACC_TARGET - s["raw_acc"]
            risks.append(f"{risk_idx}）{s['sub_biz']}未达标")
            risks.append(
                f"正确率 {s['raw_acc']:.2f}%，低于目标 {gap:.2f}pp，需升级关注。"
            )

            # 集中队列
            related_queues = [q for q in report["top_error_queues"] if q["sub_biz"] == s["sub_biz"]]
            if related_queues:
                risk_idx += 1
                q_names = "、".join([q["queue"] for q in related_queues[:3]])
                risks.append(f"{risk_idx}）问题集中在{q_names}")
                # 判断错判/漏判方向
                cp = s.get("cuopan", 0)
                lp = s.get("loupan", 0)
                total_err = s.get("error_total", 0)
                if cp > 0 and lp > 0:
                    cp_pct = cp * 100 // total_err if total_err > 0 else 0
                    lp_pct = lp * 100 // total_err if total_err > 0 else 0
                    risks.append(
                        f"当前风险呈单点集中，错判占比约 {cp_pct}%、漏判约 {lp_pct}%，"
                        f"需重点判断是个体人员波动、场景识别能力不足还是标签边界理解不一致。"
                    )
                else:
                    risks.append(
                        "当前风险呈单点集中，不是面上扩散，"
                        "需重点判断是个体人员波动还是场景识别能力不足。"
                    )
            else:
                risk_idx += 1
                risks.append(f"{risk_idx}）问题类型待进一步定位")
                risks.append("需排查是否为个体人员波动或场景识别能力不足。")

    if not risks:
        risks.append("当日无未达标板块，暂无升级风险。")

    return risks


def _build_actions(report: dict, grouped: dict) -> dict:
    """四、处理动作：交付侧 / 质培侧 / 次日关注。"""
    delivery: list[str] = []
    training: list[str] = []
    next_focus: list[str] = []

    for mb_name, g in grouped.items():
        for s in g["unqual_subs"]:
            related_queues = [q for q in report["top_error_queues"] if q["sub_biz"] == s["sub_biz"]]
            if related_queues:
                worst_q = related_queues[0]
                delivery.append(
                    f"优先复盘「{worst_q['queue']}」当日问题样本及责任人员，"
                    f"明确问题归因是「个体波动」还是「队列共性问题」。"
                )
                scene = s["sub_biz"].split("-")[-1] if "-" in s["sub_biz"] else s["sub_biz"]
                training.append(
                    f"补充{scene}场景专项案例，重点校准标签边界，"
                    f"统一{scene}场景下的判定口径，避免同类问题重复发生。"
                )
                next_focus.append(
                    f"持续跟踪{worst_q['queue']}正确率变化及问题复发情况。"
                )
            else:
                delivery.append(
                    f"优先复盘{s['sub_biz']}问题样本，定位是局部人员波动还是场景能力不足。"
                )
                scene = s["sub_biz"].split("-")[-1] if "-" in s["sub_biz"] else s["sub_biz"]
                training.append(
                    f"围绕「{scene}」场景补充专项案例复盘，明确高频问题标签边界。"
                )
                next_focus.append(f"持续跟踪{s['sub_biz']}正确率变化趋势。")

        for s in g["watch_subs"]:
            related_queues = [q for q in report["top_error_queues"] if q["sub_biz"] == s["sub_biz"]]
            if related_queues:
                q_names = "、".join([q["queue"] for q in related_queues[:2]])
                delivery.append(
                    f"对{s['sub_biz']}高频出错队列（{q_names}）增加班中抽查，避免大盘达标掩盖局部风险。"
                )
                next_focus.append(f"关注{s['sub_biz']}是否从承压转为达标或恶化。")

    # 申诉改判相关
    o = report["overview"]
    if o["appealed"] > 10 and o["appeal_rev_rate"] >= 70:
        delivery.append(
            f"对申诉改判样本逐条回看，确认是否存在重复性误判（当日改判 {o['appeal_reversed']} 条）。"
        )
        training.append(
            f"将当日 {o['appealed']} 条申诉样本单独沉淀为复盘池，用于后续培训和尺度校准。"
        )

    # 兜底
    if not delivery:
        delivery.append("当日整体质量稳定，建议保持当前审核标准，关注队列间正确率差异。")
    if not training:
        training.append("无紧急培训需求，建议持续积累案例库。")
    if not next_focus:
        next_focus.append("次日持续关注整体正确率及各队列波动情况。")

    return {"交付侧": delivery, "质培侧": training, "次日关注": next_focus}


def _build_supplement(report: dict, grouped: dict) -> list[str]:
    """五、补充风险：告警联动判断 + 申诉 + 环比。"""
    lines: list[str] = []
    o = report["overview"]
    a = report["alerts"]

    # 告警（与正文联动）
    if a["total"] > 0:
        lines.append(f"告警：P0 {a['P0']}条 / P1 {a['P1']}条 / P2 {a['P2']}条")

        has_unqual = any(g["unqual_subs"] for g in grouped.values())
        if a["P0"] > 0 and has_unqual:
            lines.append("判断：当日已出现升级风险信号（P0 级），且存在未达标板块，需重点关注是否从「质量波动」升级为「风险事件」。")
        elif a["P0"] > 0:
            lines.append("判断：当日已触发 P0 级告警，需重点关注相关板块后续表现。")
        elif a["P1"] > 3:
            lines.append("判断：当日 P1 级告警较多，说明部分指标已偏离正常区间，建议关注。")
        elif has_unqual:
            lines.append("判断：告警与未达标板块吻合，风险信号明确，需优先处理。")
        else:
            lines.append("判断：告警已触发，但大盘整体达标，建议持续观察。")

    # 昨日告警回顾（当日无告警时展示）
    yesterday_a = report.get("yesterday_alerts", {})
    if a["total"] == 0 and yesterday_a.get("total", 0) > 0:
        ya = yesterday_a
        lines.append(f"昨日告警回顾：P0 {ya['P0']}条 / P1 {ya['P1']}条 / P2 {ya['P2']}条（当日无新增告警）")

    # 申诉
    if o["appealed"] > 0:
        rev_rate = o["appeal_rev_rate"]
        lines.append(
            f"申诉 {o['appealed']} / 改判 {o['appeal_reversed']}（{rev_rate:.0f}%）："
            f"{'说明部分问题并非偶发，建议单独沉淀申诉改判样本复盘。' if rev_rate >= 70 else '改判率可控，暂无需升级处理。'}"
        )

    # 环比
    yesterday = report.get("yesterday_overall")
    if yesterday:
        pp = o["raw_acc"] - yesterday["raw_acc"]
        if pp < -0.3:
            lines.append(f"正确率环比下降 {abs(pp):.2f}pp（昨日 {yesterday['raw_acc']:.2f}%），需关注是否为趋势性下降。")
        elif pp > 0.3:
            lines.append(f"正确率环比上升 {pp:.2f}pp（昨日 {yesterday['raw_acc']:.2f}%），表现向好。")

    if not lines:
        lines.append("当日无补充风险信号。")

    return lines


# ═══════════════════════════════════════════════════════════════
#  日报输出
# ═══════════════════════════════════════════════════════════════

def report_to_wecom_md(report: dict) -> str:
    """企微 Markdown 格式（平衡版：保留问题指向，但不铺太长）。"""
    if not report["has_data"]:
        return f"📊 质检日报 ({report['report_date']})\n\n{report['message']}"

    d = report["report_date"]
    grouped = _group_by_mother(report)
    a = report["alerts"]
    o = report["overview"]

    lines: list[str] = [f"📊 **评论业务质检日报** {d}"]

    # 今日判断：不报总体数值，只说风险落点
    judgement_parts: list[str] = []
    for mb_name in ["A组", "B组"]:
        if mb_name not in grouped:
            continue
        g = grouped[mb_name]
        risky_subs = [s for s in g.get("subs", []) if s["raw_acc"] < ACC_TARGET]
        watch_subs = [s for s in g.get("subs", []) if ACC_TARGET <= s["raw_acc"] < ACC_TARGET + WATCH_BUFFER]
        if risky_subs:
            names = "、".join([s["sub_biz"] for s in risky_subs[:2]])
            judgement_parts.append(f"{names}未达标")
        elif watch_subs:
            names = "、".join([s["sub_biz"] for s in watch_subs[:2]])
            judgement_parts.append(f"{names}承压")
    if judgement_parts:
        lines.append(f"🔴 今日判断：{'；'.join(judgement_parts)}")
    else:
        lines.append("✅ 今日判断：整体达标，但仍需关注高频错误集中队列")

    # A组 / B组分开展示，B组保留评论/账号拆分
    for mb_name in ["A组", "B组"]:
        if mb_name not in grouped:
            continue

        g = grouped[mb_name]
        flag = _acc_flag(g["raw_acc"])
        lines.append(f"{flag} **{mb_name}** {g['raw_acc']:.2f}%（{g['total_qa']:,}条 / 错{g['total_err']}）")

        subs = sorted(g.get("subs", []), key=lambda x: x["sub_biz"])
        if mb_name == "B组" and len(subs) > 1:
            sub_parts = []
            for s in subs:
                short_name = s["sub_biz"].replace("B组-", "")
                sub_parts.append(f"{short_name}{_acc_flag(s['raw_acc'])}{s['raw_acc']:.2f}%")
            lines.append(f"　拆分：{'｜'.join(sub_parts)}")

        risky_or_watch = [s for s in subs if s["raw_acc"] < ACC_TARGET + WATCH_BUFFER]
        group_queues = [q for q in report.get("top_error_queues", []) if q.get("mother_biz") == mb_name]

        if risky_or_watch:
            problem_parts = []
            for s in risky_or_watch[:2]:
                sub_queues = [q["queue"] for q in report.get("top_error_queues", []) if q.get("sub_biz") == s["sub_biz"]]
                queue_text = f"（{sub_queues[0]}）" if sub_queues else ""
                problem_parts.append(f"{s['sub_biz']} {s['raw_acc']:.2f}%{queue_text}")
            lines.append(f"　问题：{'；'.join(problem_parts)}")
        elif group_queues:
            queue_names = "、".join([q["queue"] for q in group_queues[:2]])
            lines.append(f"　关注：错误集中在{queue_names}")
        else:
            lines.append("　关注：暂无明显异常")

    # 今日必做：每组只给 1 条，优先最差子业务/高频队列
    action_lines: list[str] = []
    for mb_name in ["A组", "B组"]:
        if mb_name not in grouped:
            continue
        g = grouped[mb_name]
        subs = sorted(g.get("subs", []), key=lambda x: x["raw_acc"])
        target_sub = next((s for s in subs if s["raw_acc"] < ACC_TARGET + WATCH_BUFFER), None)
        if target_sub is None:
            continue

        sub_queues = [q["queue"] for q in report.get("top_error_queues", []) if q.get("sub_biz") == target_sub["sub_biz"]]
        top_queue = sub_queues[0] if sub_queues else None
        label = target_sub["sub_biz"] if mb_name == "B组" else mb_name

        if top_queue:
            action_lines.append(f"👉 {label}：复盘「{top_queue}」高频错误样本。")
        else:
            action_lines.append(f"👉 {label}：复盘当日问题样本，确认问题归因。")

    if action_lines:
        lines.append("**今日动作**")
        lines.extend(action_lines[:2])

    tail_parts: list[str] = []
    if a["total"] > 0:
        tail_parts.append(f"🔔P0 {a['P0']} / P1 {a['P1']} / P2 {a['P2']}")
    if o["appealed"] > 0:
        tail_parts.append(f"📩申诉{o['appealed']} / 改判{o['appeal_reversed']}")
    if tail_parts:
        lines.append(" | ".join(tail_parts))

    lines.append(f"<font color=\"comment\">QC Dashboard · 自动推送 · {datetime.now().strftime('%H:%M')}</font>")
    return "\n".join(lines)


def report_to_markdown(report: dict, dashboard_url: str = "") -> str:
    """完整 Markdown（保存到 deliverables）。"""
    if not report["has_data"]:
        return f"📊 质检日报 ({report['report_date']})\n\n{report['message']}"

    d = report["report_date"]
    grouped = _group_by_mother(report)
    actions = _build_actions(report, grouped)

    lines: list[str] = []

    lines.append(f"# 📊 评论业务质检日报｜{d}")
    lines.append(f"正确率目标：{ACC_TARGET:.2f}%")

    # 一、今日结论
    lines.append("")
    lines.append("## 一、今日结论")
    lines.append(_build_conclusion(report, grouped))

    # 二、分组表现
    lines.append("")
    lines.append("## 二、分组表现")
    group_lines = _build_group_performance(grouped)
    while group_lines and group_lines[-1] == "":
        group_lines.pop()
    lines.extend(group_lines)

    # 三、重点风险
    lines.append("")
    lines.append("## 三、重点风险")
    risk_lines = _build_risks(report, grouped)
    for r in risk_lines:
        lines.append(f"- {r}")

    # 四、处理动作
    lines.append("")
    lines.append("## 四、处理动作")
    lines.append(f"- **交付侧**：{actions['交付侧'][0]}")
    for a in actions["交付侧"][1:]:
        lines.append(f"-  {a}")
    lines.append(f"- **质培侧**：{actions['质培侧'][0]}")
    for a in actions["质培侧"][1:]:
        lines.append(f"-  {a}")
    lines.append(f"- **次日关注**：{actions['次日关注'][0]}")
    for a in actions["次日关注"][1:]:
        lines.append(f"-  {a}")

    # 五、补充风险
    lines.append("")
    lines.append("## 五、补充风险")
    supplement_lines = _build_supplement(report, grouped)
    for s in supplement_lines:
        lines.append(f"- {s}")

    # 看板链接（可配置）
    if dashboard_url:
        lines.append("---")
        lines.append(f"🔗 [点击此处查看详细看板]({dashboard_url})")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════
#  DeepSeek AI 洞察分析
# ═══════════════════════════════════════════════════════════════

def _load_gongfeng_auth() -> dict[str, str] | None:
    """从 OpenClaw auth-profiles 读取工蜂代理认证信息。"""
    auth_path = Path.home() / ".openclaw" / "agents" / "main" / "agent" / "auth-profiles.json"
    if not auth_path.exists():
        return None
    try:
        with open(auth_path, "r", encoding="utf-8") as f:
            profiles = json.load(f)
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


def _call_llm(system_prompt: str, user_prompt: str, model: str = "claude-sonnet-4-5",
              max_tokens: int = 500, temperature: float = 0.3) -> str:
    """调用 LLM API，优先工蜂代理（免费 Claude），fallback DeepSeek。"""
    import os
    import requests

    # ── 优先：工蜂代理（免费 Claude）──
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
            content = resp.json()["choices"][0]["message"]["content"]
            content = content.strip()
            if content.startswith("```"):
                lines = content.split("\n")
                content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
            print("✅ 工蜂代理 Claude 调用成功")
            return content
        except Exception as e:
            print(f"⚠️ 工蜂代理调用失败: {e}，fallback DeepSeek")

    # ── Fallback：DeepSeek ──
    api_key = os.environ.get("DEEPSEEK_API_KEY", "")
    api_url = os.environ.get("DEEPSEEK_API_URL", "https://api.deepseek.com/v1/chat/completions")
    if not api_key:
        print("⚠️ 无可用 LLM（工蜂代理和 DeepSeek 均不可用）")
        return ""
    try:
        resp = requests.post(
            api_url,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "max_tokens": max_tokens,
                "temperature": temperature,
            },
            timeout=30,
        )
        resp.raise_for_status()
        content = resp.json()["choices"][0]["message"]["content"]
        content = content.strip()
        if content.startswith("```"):
            lines = content.split("\n")
            content = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        print("✅ DeepSeek fallback 调用成功")
        return content
    except Exception as e:
        print(f"⚠️ DeepSeek API 调用失败: {e}")
        return ""


def call_deepseek(report: dict) -> str:
    """调用 LLM 生成 AI 洞察分析（优先 Claude，fallback DeepSeek）。"""
    data_summary = json.dumps({
        "report_date": report["report_date"],
        "target": report.get("target", 99.0),
        "overview": report.get("overview", {}),
        "mother_list": report.get("mother_list", []),
        "sub_list": report.get("sub_list", []),
        "top_error_queues": report.get("top_error_queues", []),
        "top_error_types": report.get("top_error_types", []),
        "alerts": report.get("alerts", {}),
        "yesterday_overall": report.get("yesterday_overall"),
    }, ensure_ascii=False, indent=2)

    system_prompt = """你是评论业务质检数据分析师（质培 owner 视角）。根据给定的质检数据 JSON，生成一段简短的 AI 洞察分析。

要求：
1. **结论先行**：第一句话给出整体判断（达标/未达标/承压）
2. **归因分析**：分析错误集中在哪个业务组、哪个队列，错判还是漏判为主，可能的原因是什么
3. **趋势判断**：如果有昨日数据，对比环比变化，判断是改善还是恶化
4. **风险预警**：指出最需要关注的 1-2 个风险点
5. **行动建议**：给出 2-3 条具体可执行的建议

格式要求：
- 使用企微 Markdown 格式
- 总长度控制在 300 字以内
- 关键数据用 **加粗**
- 用 emoji 标识状态：✅ 达标 🟡 承压 🔴 风险
- 不要重复罗列数据，重点在分析和洞察
- 语气专业简洁，不说废话"""

    return _call_llm(system_prompt, f"以下是今日质检数据：\n{data_summary}\n\n请生成 AI 洞察分析。")


# ═══════════════════════════════════════════════════════════════
#  推送
# ═══════════════════════════════════════════════════════════════

def send_wecom_webhook(content: str, mentioned_list: list[str] | None = None) -> dict:
    settings = load_settings()
    webhook_url = settings["wecom_webhook_url"]
    payload: dict[str, Any] = {
        "msgtype": "markdown",
        "markdown": {"content": content},
    }
    if mentioned_list:
        payload["markdown"]["mentioned_list"] = mentioned_list

    import requests
    resp = requests.post(webhook_url, json=payload, timeout=10)
    resp.raise_for_status()
    return resp.json()


# ── 企微 Markdown 长度限制 ──
WECOM_MAX_LEN = 4096  # 企微 webhook 硬限制


def _split_for_wecom(content: str, max_len: int = WECOM_MAX_LEN) -> list[str]:
    """将超长内容拆分为多条消息，每条不超过 max_len 字符。

    策略：
    1. 内容 <= max_len → 原样返回单条
    2. 超长时按「标题+核心数据」优先，逐段截断
    """
    if len(content) <= max_len:
        return [content]

    lines = content.split("\n")
    parts: list[str] = []
    current: list[str] = []

    for line in lines:
        # 单行过长（如 AI 洞察段落），强制截断
        if len(line) > max_len - 50:
            line = line[:max_len - 80] + "...（内容过长已截断，查看完整日报）"

        if sum(len(l) + 1 for l in current) + len(line) + 1 > max_len and current:
            parts.append("\n".join(current))
            current = [line]
        else:
            current.append(line)

    if current:
        parts.append("\n".join(current))

    return parts


def send_wecom_webhook_with_split(content: str, mentioned_list: list[str] | None = None) -> tuple[bool, str]:
    """发送企微消息，自动处理超长内容的分片。

    Returns:
        (success: bool, message: str)
    """
    parts = _split_for_wecom(content)
    errors: list[str] = []

    for idx, part in enumerate(parts):
        # 最后一条追加提示
        if idx == len(parts) - 1 and len(parts) > 1:
            part += "\n\n> 📄 以上为日报摘要，[完整版](点击此处查看详情)请查看归档文件。"
        elif len(parts) > 1:
            header = f"📊 **评论业务质检日报** ({idx+1}/{len(parts)})\n---\n"
            part = header + part

        try:
            result = send_wecom_webhook(part, mentioned_list=mentioned_list)
            if result.get("errcode") != 0:
                errors.append(f"第{idx+1}段推送失败: {result}")
        except Exception as e:
            errors.append(f"第{idx+1}段异常: {e}")

    if errors:
        return False, "; ".join(errors)
    return True, f"共 {len(parts)} 条消息已推送"


# ═══════════════════════════════════════════════════════════════
#  入口
# ═══════════════════════════════════════════════════════════════

def _get_sent_stamp_path() -> Path:
    """已推送日期的锁文件路径"""
    return PROJECT_ROOT / ".cache" / "report_sent.txt"


def _load_sent_dates() -> set[str]:
    """读取已推送日期集合"""
    p = _get_sent_stamp_path()
    if p.exists():
        return {line.strip() for line in p.read_text("utf-8").splitlines() if line.strip()}
    return set()


def _mark_date_sent(d: date) -> None:
    """标记某日期已推送"""
    p = _get_sent_stamp_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    dates = _load_sent_dates()
    dates.add(str(d))
    p.write_text("\n".join(sorted(dates)), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="质检日报生成 & 企微群推送")
    parser.add_argument("--date", default=str(date.today() - timedelta(days=1)), help="报告日期 YYYY-MM-DD（默认 T-1）")
    parser.add_argument("--dry-run", action="store_true", help="只生成不推送")
    parser.add_argument("--output", "-o", default=None, help="保存 Markdown 到指定文件")
    parser.add_argument("--db-path", default=None, help="已废弃，保留兼容性")
    parser.add_argument("--mention", nargs="*", default=[], help="@指定成员 userId 列表")
    parser.add_argument("--force", action="store_true", help="强制推送（忽略去重检查）")
    args = parser.parse_args()

    settings = load_settings()
    report_date = date.fromisoformat(args.date)

    conn = DashboardRepository().connect()
    report = build_daily_report(conn, report_date)

    wecom_md = report_to_wecom_md(report)
    full_md = report_to_markdown(report, dashboard_url=settings.get("dashboard_url", ""))

    print(wecom_md)

    output_path = Path(args.output) if args.output else PROJECT_ROOT / "deliverables" / f"daily_report_{report_date}.md"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(full_md)
    print(f"\n📄 日报已保存到: {output_path}")

    # 生成 daily_data.json（供 Knot 智能体读取）
    json_path = PROJECT_ROOT / "daily_data.json"
    json_data = {
        "report_date": report["report_date"],
        "has_data": report["has_data"],
        "target": report.get("target", 99.0),
        "overview": report.get("overview", {}),
        "mother_list": report.get("mother_list", []),
        "sub_list": report.get("sub_list", []),
        "top_error_queues": report.get("top_error_queues", []),
        "top_error_types": report.get("top_error_types", []),
        "alerts": report.get("alerts", {}),
        "watch_queues": report.get("watch_queues", []),
        "yesterday_overall": report.get("yesterday_overall"),
        "yesterday_alerts": report.get("yesterday_alerts", {}),
    }
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(json_data, f, ensure_ascii=False, indent=2)
    print(f"📊 数据已保存到: {json_path}")

    if not args.dry_run:
        # 去重检查：同一报告日期只推送一次
        if not args.force:
            sent_dates = _load_sent_dates()
            if str(report_date) in sent_dates:
                print(f"⏭️ 报告日期 {report_date} 今日已推送过，跳过重复推送（可用 --force 强制推送）")
                return

        if not report["has_data"]:
            # 无数据时推送告警通知，而不是静默跳过
            alert_md = f"""📊 质检日报 ({report_date})

⚠️ **今日无质检数据**

请检查：
1. 企微缓存文件是否已同步
2. daily_refresh 是否正常执行
3. 数据源是否有延迟

> 此告警由日报任务自动推送"""
            result = send_wecom_webhook(alert_md)
            if result.get("errcode") == 0:
                print("⚠️ 无数据，已推送告警通知到企业微信群")
                _mark_date_sent(report_date)
            else:
                print(f"⚠️ 无数据，推送告警失败: {result}")
            return
        mentioned = args.mention if args.mention else None
        # 使用支持自动分片的推送（解决企微 4096 字符限制）
        success, msg = send_wecom_webhook_with_split(wecom_md, mentioned_list=mentioned)
        if success:
            print(f"✅ {msg}")
            _mark_date_sent(report_date)
        else:
            print(f"❌ 推送失败: {msg}")
            sys.exit(1)
    else:
        print("\n⏭️ dry-run 模式，未推送。")


if __name__ == "__main__":
    main()
