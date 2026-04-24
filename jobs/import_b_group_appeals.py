#!/usr/bin/env python3
"""B组申诉表专用导入脚本。

B组申诉表与A组的差异：
1. 一个xlsx包含多个sheet（评论/账号/图片/投诉等），每个sheet字段结构不同
2. 每行同时包含QC信息和申诉信息（不需要拆分为QA+Appeal两个文件）
3. 部分表有B组特有字段（申诉人uin、截图、链接等）
4. "投诉明细表"语义不同于申诉，需做字段名标准化

处理策略：
- 识别sheet名 → 匹配到对应的字段映射规则
- 每行拆为一条 fact_qa_event + 一条 fact_appeal_event（如有申诉信息）
- 非文本字段（截图）跳过，链接存入 appeal_note
- join_key 自动生成

用法：
    .venv/bin/python jobs/import_b_group_appeals.py --file path/to/b_group.xlsx
    .venv/bin/python jobs/import_b_group_appeals.py --file path/to/b_group.xlsx --channel 云雀联营3组
    .venv/bin/python jobs/import_b_group_appeals.py --file path/to/b_group.xlsx --skip-dedup
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from storage.repository import DashboardRepository

# 复用 A 组导入脚本的公共函数
from jobs.import_fact_data import (
    APPEAL_INSERT_COLUMNS,
    QA_INSERT_COLUMNS,
    build_join_key,
    build_row_hash,
    check_file_duplicate,
    clean_text,
    coalesce_series,
    compute_file_hash,
    extract_date_from_filename,
    has_meaningful_text,
    infer_correct,
    insert_new_rows,
    keyword_flag,
    normalize_name,
    parse_date_series,
    parse_timestamp_series,
    to_boolean,
    write_etl_log,
    write_upload_log,
)


# ==========================================================
# B组 sheet → 字段映射规则
# ==========================================================

# B组 sheet 类型识别规则（sheet名关键词 → sheet_type）
SHEET_TYPE_RULES = [
    (["评论质检申诉", "评论队列汇总申诉", "评论申诉", "健康度队列申诉"], "comment_appeal"),
    (["账号质检申诉", "账号正式队列质检申诉", "账号申诉"], "account_appeal"),
    (["图片质检申诉", "图片队列质检申诉", "图片申诉"], "image_appeal"),
    (["引流专项评论申诉", "引流申诉"], "drainage_appeal"),
    (["培训-评论质检申诉", "培训评论申诉", "培训申诉"], "training_appeal"),
    (["内外部投诉明细", "投诉明细", "投诉"], "complaint"),
]

# 各 sheet_type 的字段映射（中文表头 → DB字段）
# 关键：B组申诉表里同时有QC和申诉字段，需要映射到两个表

B_GROUP_QA_ALIASES = {
    # 公共字段
    "biz_date": ["质检日期", "质检时间", "日期", "业务日期"],
    "qa_time": ["质检日期", "质检时间", "日期", "审核时间"],
    "import_date": ["一审时间"],
    "qa_owner_name": ["质检员", "QC员"],
    "queue_name": ["队列"],
    "reviewer_name": ["一审人员", "审核员", "审核人员", "一审员", "姓名"],
    "raw_judgement": ["一审结果", "一审答案"],
    "error_type": ["错误类型"],
    "qa_result": ["质检结果", "QC结论"],
    "qa_note": ["质检备注", "QC备注"],
    "comment_text": ["评论文本", "图片评论", "评论内容"],
    "is_appealed": ["是否申诉"],
    # 评论类
    "dynamic_id": ["动态ID", "动态id"],
    "comment_id": ["评论ID", "评论id"],
    # 账号类
    "account_id": ["账号ID", "微信ID"],
    # 组织
    "channel_name": ["一审公司", "公司"],
    "final_judgement": ["正确答案"],
}

B_GROUP_APPEAL_ALIASES = {
    "biz_date": ["质检日期", "质检时间", "日期", "投诉时间"],
    "appeal_time": ["质检日期", "质检时间", "日期", "投诉时间"],
    "appeal_reason": ["申诉理由", "复盘原因", "申诉原因"],
    "appeal_result": ["申诉结果", "是否成功"],
    "appeal_operator": ["申诉员A", "申诉员B", "投诉人"],
    "appeal_note": ["备注", "原因"],
    "is_reversed": ["是否成功", "是否改判"],
    # 关联键（和QA一样）
    "dynamic_id": ["动态ID", "动态id"],
    "comment_id": ["评论ID", "评论id"],
    "account_id": ["账号ID", "微信ID"],
    "queue_name": ["队列"],
    "reviewer_name": ["一审人员", "审核员", "审核人员", "姓名"],
}


def identify_sheet_type(sheet_name: str) -> str:
    """根据 sheet 名识别 B 组表类型。"""
    normalized = normalize_name(sheet_name)
    for keywords, sheet_type in SHEET_TYPE_RULES:
        for kw in keywords:
            if normalize_name(kw) in normalized:
                return sheet_type
    return "unknown"


def identify_channel(file_path: Path, cli_channel: str | None = None) -> str:
    """识别渠道（云雀联营3组/长沙）。"""
    if cli_channel:
        return cli_channel
    name = file_path.name
    if "长沙" in name or "cs" in name.lower():
        return "长沙"
    if "云雀" in name or "联营" in name or "3组" in name:
        return "云雀联营3组"
    return "未知渠道"


def map_b_group_columns(df: pd.DataFrame, aliases: dict[str, list[str]]) -> pd.DataFrame:
    """B组字段映射（与 A 组 map_columns 逻辑相同）。"""
    normalized_source = {normalize_name(column): column for column in df.columns}
    mapped = pd.DataFrame(index=df.index)

    for target, candidate_aliases in aliases.items():
        source_column = None
        for candidate in [target, *candidate_aliases]:
            source_column = normalized_source.get(normalize_name(candidate))
            if source_column is not None:
                break
        mapped[target] = df[source_column] if source_column is not None else pd.NA

    return mapped


def prepare_b_group_qa_frame(
    raw_df: pd.DataFrame,
    sheet_type: str,
    channel: str,
    source_file_name: str,
    batch_id: str,
    import_day: date,
) -> tuple[pd.DataFrame, int]:
    """准备 B 组 fact_qa_event 数据。"""
    mapped = map_b_group_columns(raw_df, B_GROUP_QA_ALIASES)
    index = mapped.index

    # 业务日期
    biz_date_from_file = extract_date_from_filename(source_file_name)
    qa_time = parse_timestamp_series(mapped["qa_time"])
    if biz_date_from_file:
        biz_date = pd.Series(biz_date_from_file, index=index)
    else:
        biz_date = coalesce_series(
            parse_date_series(mapped["biz_date"]),
            qa_time.dt.date,
            pd.Series(import_day, index=index),
        )

    import_date = coalesce_series(parse_date_series(mapped["import_date"]), pd.Series(import_day, index=index))

    # B组业务线固定
    mother_biz = pd.Series("B组", index=index)
    # 根据 sheet_type 决定 sub_biz
    sub_biz_map = {
        "comment_appeal": "B组-评论",
        "account_appeal": "B组-账号",
        "image_appeal": "B组-图片",
        "drainage_appeal": "B组-引流",
        "training_appeal": "B组-培训",
        "complaint": "B组-投诉",
    }
    sub_biz_value = sub_biz_map.get(sheet_type, "B组-其他")
    sub_biz = pd.Series(sub_biz_value, index=index)
    group_name = pd.Series(sub_biz_value, index=index)

    # 内容类型推断
    content_type_map = {
        "comment_appeal": "评论",
        "account_appeal": "账号",
        "image_appeal": "图片",
        "drainage_appeal": "评论",
        "training_appeal": "评论",
        "complaint": "评论",
    }
    content_type = pd.Series(content_type_map.get(sheet_type, "未知"), index=index)

    queue_name = coalesce_series(clean_text(mapped["queue_name"]), pd.Series(sub_biz_value, index=index))
    reviewer_name = clean_text(mapped["reviewer_name"])
    qa_owner_name = clean_text(mapped["qa_owner_name"])
    raw_judgement = clean_text(mapped["raw_judgement"])
    final_judgement = clean_text(mapped["final_judgement"])
    qa_result = clean_text(mapped["qa_result"])
    error_type = clean_text(mapped["error_type"])

    # 正确性推断
    is_raw_correct = coalesce_series(
        infer_correct(index, mapped["qa_result"]),
        pd.Series(True, index=index),  # B组申诉表默认有错（被质检出错了才会进申诉表）
        dtype="boolean",
    ).fillna(True)

    # B组申诉表里的记录都是有问题的（被质检出来的），所以 is_raw_correct = False
    # 但要区分是错判还是漏判
    is_misjudge = keyword_flag(index, error_type, qa_result, keywords=["错判", "误判"]).fillna(False)
    is_missjudge = keyword_flag(index, error_type, qa_result, keywords=["漏判", "漏审"]).fillna(False)

    # B组申诉表里的记录默认 is_raw_correct = False（因为是被检出错误的）
    is_raw_correct = pd.Series(False, index=index, dtype="boolean")

    # is_final_correct：如果有正确答案/final_judgement，用那个；否则跟随 raw
    is_final_correct = coalesce_series(
        infer_correct(index, mapped["final_judgement"], mapped["qa_result"]),
        is_raw_correct,
        dtype="boolean",
    ).fillna(False)

    # 申诉标记
    is_appealed = coalesce_series(
        to_boolean(mapped["is_appealed"]),
        has_meaningful_text(index, mapped.get("appeal_reason", pd.Series(pd.NA, index=index))),
        pd.Series(True, index=index),  # 申诉表里的记录默认已申诉
        dtype="boolean",
    ).fillna(True)

    prepared = pd.DataFrame({
        "biz_date": biz_date,
        "qa_time": qa_time,
        "import_date": import_date,
        "mother_biz": mother_biz,
        "sub_biz": sub_biz,
        "group_name": group_name,
        "queue_name": queue_name,
        "scene_name": pd.NA,
        "channel_name": clean_text(mapped["channel_name"]),
    "content_type": content_type,
    "inspect_type": pd.Series("external", index=index, dtype="string"),  # B组默认外检
    "workforce_type": pd.Series("外检", index=index, dtype="string"),  # B组默认外检人员
    "reviewer_name": reviewer_name,
        "qa_owner_name": qa_owner_name,
        "trainer_name": pd.NA,
        "source_record_id": pd.NA,
        "comment_id": clean_text(mapped["comment_id"]),
        "dynamic_id": clean_text(mapped["dynamic_id"]),
        "account_id": clean_text(mapped["account_id"]),
        "raw_label": pd.NA,
        "final_label": pd.NA,
        "raw_judgement": raw_judgement,
        "final_judgement": final_judgement,
        "qa_result": qa_result,
        "error_type": error_type,
        "error_level": pd.NA,
        "error_reason": pd.NA,
        "risk_level": pd.NA,
        "training_topic": pd.NA,
        "is_raw_correct": is_raw_correct,
        "is_final_correct": is_final_correct,
        "is_misjudge": is_misjudge,
        "is_missjudge": is_missjudge,
        "is_appealed": is_appealed,
        "is_appeal_reversed": pd.Series(False, index=index, dtype="boolean"),  # 后续由 appeal 数据推断
        "appeal_status": pd.NA,
        "appeal_reason": pd.NA,
        "comment_text": clean_text(mapped["comment_text"]),
        "qa_note": clean_text(mapped["qa_note"]),
        "batch_id": batch_id,
        "source_file_name": source_file_name,
    })

    prepared["join_key"] = build_join_key(
        prepared["source_record_id"],
        prepared["comment_id"],
        prepared["dynamic_id"],
        prepared["account_id"],
    )
    prepared["row_hash"] = build_row_hash(prepared, [
        "biz_date", "qa_time", "group_name", "queue_name", "reviewer_name",
        "comment_id", "dynamic_id", "account_id",
        "raw_judgement", "error_type", "qa_result",
        "comment_text", "source_file_name",
    ])

    # event_id：用 join_key + row_hash 兜底
    event_id_seed = coalesce_series(
        prepared["comment_id"],
        prepared["dynamic_id"],
        prepared["account_id"],
    )
    prepared["event_id"] = clean_text(event_id_seed).fillna("bqa-" + prepared["row_hash"].str[:16])

    warning_rows = int(prepared["join_key"].isna().sum())
    return prepared[QA_INSERT_COLUMNS], warning_rows


def prepare_b_group_appeal_frame(
    raw_df: pd.DataFrame,
    sheet_type: str,
    channel: str,
    source_file_name: str,
    batch_id: str,
    import_day: date,
) -> tuple[pd.DataFrame, int]:
    """准备 B 组 fact_appeal_event 数据。"""
    mapped = map_b_group_columns(raw_df, B_GROUP_APPEAL_ALIASES)
    index = mapped.index

    # 过滤掉没有申诉信息的行
    appeal_reason = clean_text(mapped["appeal_reason"])
    appeal_result = clean_text(mapped["appeal_result"])
    has_appeal = appeal_reason.notna() | appeal_result.notna()

    if not has_appeal.any():
        return pd.DataFrame(columns=APPEAL_INSERT_COLUMNS), 0

    # 只保留有申诉信息的行
    mapped = mapped[has_appeal].copy()
    index = mapped.index

    # 业务日期
    biz_date_from_file = extract_date_from_filename(source_file_name)
    appeal_time = parse_timestamp_series(mapped["appeal_time"])
    if biz_date_from_file:
        biz_date = pd.Series(biz_date_from_file, index=index)
    else:
        biz_date = coalesce_series(
            parse_date_series(mapped["biz_date"]),
            appeal_time.dt.date,
            pd.Series(import_day, index=index),
        )

    sub_biz_map = {
        "comment_appeal": "B组-评论",
        "account_appeal": "B组-账号",
        "image_appeal": "B组-图片",
        "drainage_appeal": "B组-引流",
        "training_appeal": "B组-培训",
        "complaint": "B组-投诉",
    }
    sub_biz_value = sub_biz_map.get(sheet_type, "B组-其他")

    appeal_result_clean = clean_text(mapped["appeal_result"])
    raw_judgement = clean_text(mapped.get("raw_judgement", pd.Series(pd.NA, index=index)))

    # 推断 is_reversed
    is_reversed = coalesce_series(
        to_boolean(mapped["is_reversed"]),
        (raw_judgement.notna() & appeal_result_clean.notna() &
         (raw_judgement.str.strip() != appeal_result_clean.str.strip())).astype("boolean"),
        keyword_flag(index, mapped["appeal_result"], keywords=["改判", "申诉成功", "成功"]),
        dtype="boolean",
    ).fillna(False)

    # 处理 appeal_operator（可能有A/B双列，取第一个非空的）
    appeal_operator = clean_text(mapped["appeal_operator"])

    # 处理 appeal_note：如果有"链接"字段，追加到 appeal_note
    # 注意：截图字段跳过不入库

    prepared = pd.DataFrame({
        "biz_date": biz_date,
        "appeal_time": appeal_time,
        "source_record_id": pd.NA,
        "comment_id": clean_text(mapped["comment_id"]),
        "dynamic_id": clean_text(mapped["dynamic_id"]),
        "account_id": clean_text(mapped["account_id"]),
        "group_name": pd.Series(sub_biz_value, index=index),
        "queue_name": clean_text(mapped["queue_name"]),
        "reviewer_name": clean_text(mapped["reviewer_name"]),
        "appeal_status": pd.NA,
        "appeal_result": appeal_result_clean,
        "appeal_reason": appeal_reason,
        "appeal_operator": appeal_operator,
        "appeal_note": clean_text(mapped["appeal_note"]),
        "is_reversed": is_reversed,
        "batch_id": batch_id,
        "source_file_name": source_file_name,
    })

    prepared["join_key"] = build_join_key(
        prepared["source_record_id"],
        prepared["comment_id"],
        prepared["dynamic_id"],
        prepared["account_id"],
    )
    prepared["row_hash"] = build_row_hash(prepared, [
        "biz_date", "comment_id", "dynamic_id", "account_id",
        "appeal_result", "appeal_reason", "source_file_name",
    ])

    # appeal_event_id
    event_id_seed = coalesce_series(
        prepared["comment_id"],
        prepared["dynamic_id"],
        prepared["account_id"],
    )
    prepared["appeal_event_id"] = clean_text(event_id_seed).fillna("bapp-" + prepared["row_hash"].str[:16])

    warning_rows = int(prepared["join_key"].isna().sum())
    return prepared[APPEAL_INSERT_COLUMNS], warning_rows


def read_b_group_sheets(file_path: Path) -> list[tuple[str, str, pd.DataFrame]]:
    """读取 B 组 xlsx 的所有 sheet，返回 (sheet_name, sheet_type, DataFrame) 列表。"""
    xlsx = pd.ExcelFile(file_path)
    results = []
    for sheet_name in xlsx.sheet_names:
        df = pd.read_excel(xlsx, sheet_name=sheet_name, dtype=str)
        if df.empty:
            continue
        sheet_type = identify_sheet_type(sheet_name)
        if sheet_type == "unknown":
            print(f"  ⚠️ 跳过未识别的 sheet: {sheet_name}")
            continue
        results.append((sheet_name, sheet_type, df))
    return results


@dataclass
class ImportSummary:
    sheet_name: str
    sheet_type: str
    qa_inserted: int
    appeal_inserted: int
    warning_rows: int


def main() -> None:
    parser = argparse.ArgumentParser(description="导入 B 组申诉表（多 sheet）到 TiDB")
    parser.add_argument("--file", required=True, help="B 组申诉表 xlsx 文件路径")
    parser.add_argument("--channel", default=None, help="渠道名（云雀联营3组/长沙），不传则从文件名推断")
    parser.add_argument("--batch-id", default=None, help="批次号")
    parser.add_argument("--skip-dedup", action="store_true", help="跳过文件级去重")
    parser.add_argument("--skip-refresh", action="store_true", help="跳过 mart 刷新")
    args = parser.parse_args()

    file_path = Path(args.file).expanduser().resolve()
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")

    batch_id = args.batch_id or f"bgroup_{date.today():%Y%m%d}_{uuid.uuid4().hex[:8]}"
    run_id = uuid.uuid4().hex
    channel = identify_channel(file_path, args.channel)
    import_day = date.today()

    repo = DashboardRepository()
    repo.initialize_schema()
    conn = repo.connect()

    # 文件级去重
    if not args.skip_dedup:
        file_hash = compute_file_hash(file_path)
        upload_id = f"upload_{run_id}_bgroup"
        if check_file_duplicate(conn, file_hash, file_path.name, "b_group_appeal", upload_id):
            print(f"⏭️ 文件已导入过，跳过: {file_path.name}")
            sys.exit(0)

    # 读取所有 sheet
    sheets = read_b_group_sheets(file_path)
    if not sheets:
        print("⚠️ 未找到可识别的 B 组 sheet")
        sys.exit(0)

    print(f"\n📂 {file_path.name} (渠道: {channel})")
    print(f"   发现 {len(sheets)} 个可识别 sheet:")

    summaries: list[ImportSummary] = []

    for sheet_name, sheet_type, raw_df in sheets:
        print(f"\n  📄 {sheet_name} → {sheet_type} ({len(raw_df)} 行)")

        source_name = f"{file_path.name}::{sheet_name}"

        # 1. 写入 fact_qa_event
        try:
            qa_frame, qa_warnings = prepare_b_group_qa_frame(
                raw_df, sheet_type, channel, source_name, batch_id, import_day
            )
            qa_inserted, _ = insert_new_rows(conn, "fact_qa_event", QA_INSERT_COLUMNS, qa_frame)
            print(f"    ✅ fact_qa_event: {qa_inserted} 行")
        except Exception as e:
            print(f"    ❌ fact_qa_event 写入失败: {e}")
            qa_inserted = 0
            qa_warnings = 0

        # 2. 写入 fact_appeal_event
        try:
            appeal_frame, appeal_warnings = prepare_b_group_appeal_frame(
                raw_df, sheet_type, channel, source_name, batch_id, import_day
            )
            if appeal_frame.empty:
                appeal_inserted = 0
                print(f"    ℹ️ 无申诉数据需要写入")
            else:
                appeal_inserted, _ = insert_new_rows(conn, "fact_appeal_event", APPEAL_INSERT_COLUMNS, appeal_frame)
                print(f"    ✅ fact_appeal_event: {appeal_inserted} 行")
        except Exception as e:
            print(f"    ❌ fact_appeal_event 写入失败: {e}")
            appeal_inserted = 0

        # 记录 ETL 日志
        write_etl_log(
            conn,
            run_id=f"{run_id}_{sheet_type}",
            job_name=f"import_b_group:{source_name}",
            source_rows=len(raw_df),
            inserted_rows=qa_inserted + appeal_inserted,
            dedup_rows=0,
            warning_rows=qa_warnings,
            run_status="success" if qa_inserted > 0 else "skipped",
        )

        summaries.append(ImportSummary(
            sheet_name=sheet_name,
            sheet_type=sheet_type,
            qa_inserted=qa_inserted,
            appeal_inserted=appeal_inserted,
            warning_rows=qa_warnings,
        ))

    # 汇总
    total_qa = sum(s.qa_inserted for s in summaries)
    total_appeal = sum(s.appeal_inserted for s in summaries)
    print(f"\n{'='*60}")
    print(f"  B组导入完成: ✅ QA {total_qa} 行 | Appeal {total_appeal} 行")
    for s in summaries:
        print(f"    {s.sheet_name} ({s.sheet_type}): QA={s.qa_inserted} Appeal={s.appeal_inserted}")
    print(f"{'='*60}")

    # 刷新 mart
    if not args.skip_refresh:
        print("\n🔄 刷新 mart 聚合表...")
        from jobs.refresh_warehouse import main as refresh_main
        # 直接调用 refresh
        sys.argv = ["refresh_warehouse.py"]
        try:
            refresh_main()
        except SystemExit:
            pass

    # 输出 JSON 统计
    stats = {
        "run_id": run_id,
        "batch_id": batch_id,
        "source_file": str(file_path),
        "channel": channel,
        "sheets": [asdict(s) for s in summaries],
        "total_qa": total_qa,
        "total_appeal": total_appeal,
    }
    print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
