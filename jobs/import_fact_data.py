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


# ==========================================================
# 业务线映射规则（按文件名关键词匹配）
# ==========================================================
BUSINESS_LINE_RULES = [
    {
        "keyword": "长沙云雀",
        "业务线": "A组-评论",
        "母业务": "A组",
        "子业务": "A组-评论",
    },
    {
        "keyword": "迁移人力ilabel",
        "业务线": "B组-评论",
        "母业务": "B组",
        "子业务": "B组-评论",
    },
    {
        "keyword": "账号",
        "业务线": "B组-账号",
        "母业务": "B组",
        "子业务": "B组-账号",
    },
]


def identify_business_line(filename: str) -> tuple[str, str]:
    """根据文件名识别业务线。

    Returns:
        tuple[str, str]: (母业务, 子业务)，未匹配返回 ("未识别", "未识别")
    """
    normalized_name = re.sub(r"\s+", "", filename).lower()
    for rule in BUSINESS_LINE_RULES:
        keyword = re.sub(r"\s+", "", rule["keyword"]).lower()
        if keyword in normalized_name:
            return rule["母业务"], rule["子业务"]
    return "未识别", "未识别"


def extract_date_from_filename(filename: str, reference_year: int | None = None) -> date | None:
    """从文件名提取业务日期。

    支持格式：
    - 2026.3.6长沙云雀... → 2026-03-06
    - 2026.3.14长沙... → 2026-03-14
    - 0322迁移人力... → 智能推断年份-03-22
    - 3.20迁移人力... → 智能推断年份-03-20

    年份推断逻辑：
    - 如果没有年份信息，优先使用去年的日期（避免未来日期）
    - 如果去年的日期导致日期 > 今天，则使用前年
    - 最多回溯 2 年

    Args:
        filename: 文件名
        reference_year: 参考年份（用于测试），默认当前年份

    Returns:
        date | None: 提取成功返回日期，否则返回 None
    """
    import datetime

    today = datetime.date.today()
    current_year = reference_year or today.year

    # 匹配 YYYY.M.D 或 YYYY.MM.DD 格式（如 2026.3.6）
    match = re.search(r'(\d{4})\.(\d{1,2})\.(\d{1,2})', filename)
    if match:
        year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
        try:
            return datetime.date(year, month, day)
        except ValueError:
            pass

    # 智能年份推断函数
    def infer_year(month: int, day: int) -> int:
        """推断合理的年份，避免未来日期"""
        for year_offset in range(3):  # 尝试今年、去年、前年
            candidate_year = current_year - year_offset
            try:
                candidate_date = datetime.date(candidate_year, month, day)
                if candidate_date <= today:
                    return candidate_year
            except ValueError:
                continue
        return current_year - 1  # 默认去年

    # 匹配 MMDD 格式（如 0322、0314）
    match = re.search(r'(?:^|[^\d])(\d{2})(\d{2})(?:[^\d]|$)', filename)
    if match:
        month, day = int(match.group(1)), int(match.group(2))
        try:
            year = infer_year(month, day)
            return datetime.date(year, month, day)
        except ValueError:
            pass

    # 匹配 M.D 格式（如 3.20、3.6）
    match = re.search(r'(?:^|[^\d])(\d{1,2})\.(\d{1,2})(?:[^\d\.]|$)', filename)
    if match:
        month, day = int(match.group(1)), int(match.group(2))
        try:
            year = infer_year(month, day)
            return datetime.date(year, month, day)
        except ValueError:
            pass

    return None


QA_INSERT_COLUMNS = [
    "event_id",
    "biz_date",
    "qa_time",
    "import_date",
    "mother_biz",
    "sub_biz",
    "group_name",
    "queue_name",
    "scene_name",
    "channel_name",
    "content_type",
    "inspect_type",
    "workforce_type",
    "reviewer_name",
    "qa_owner_name",
    "trainer_name",
    "source_record_id",
    "comment_id",
    "dynamic_id",
    "account_id",
    "join_key",
    "raw_label",
    "final_label",
    "raw_judgement",
    "final_judgement",
    "qa_result",
    "error_type",
    "error_level",
    "error_reason",
    "risk_level",
    "training_topic",
    "is_raw_correct",
    "is_final_correct",
    "is_misjudge",
    "is_missjudge",
    "is_appealed",
    "is_appeal_reversed",
    "appeal_status",
    "appeal_reason",
    "comment_text",
    "qa_note",
    "batch_id",
    "source_file_name",
    "row_hash",
]

APPEAL_INSERT_COLUMNS = [
    "appeal_event_id",
    "biz_date",
    "appeal_time",
    "source_record_id",
    "comment_id",
    "dynamic_id",
    "account_id",
    "join_key",
    "group_name",
    "queue_name",
    "reviewer_name",
    "appeal_status",
    "appeal_result",
    "appeal_reason",
    "appeal_operator",
    "appeal_note",
    "is_reversed",
    "batch_id",
    "source_file_name",
    "row_hash",
]

QA_COLUMN_ALIASES = {
    "event_id": ["事件id", "eventid"],
    "biz_date": ["业务日期", "日期", "抽检日期", "质检日期", "qa日期"],
    "qa_time": ["质检时间", "抽检时间", "审核时间", "qa时间", "reviewtime", "质检完成时间"],
    "import_date": ["导入日期", "入仓日期"],
    "mother_biz": ["一级业务", "母业务", "业务线", "业务大类"],
    "sub_biz": ["二级业务", "子业务", "业务小类"],
    "group_name": ["组别", "团队", "组", "group", "母业务", "公司", "一审公司"],
    "queue_name": ["队列", "队列名", "queue", "子业务", "供应商"],
    "scene_name": ["场景", "场景名"],
    "channel_name": ["渠道", "渠道名"],
    "content_type": ["内容类型", "内容体裁"],
    "inspect_type": ["检查类型", "质检类型", "检验类型"],
    "workforce_type": ["用工类型", "人员类型", "员工类型"],
    "reviewer_name": ["审核人", "审核员", "reviewer", "审核账号", "一审员", "一审人员", "原始答案.operatorName", "operatorName"],
    "qa_owner_name": ["质检人", "质检员", "qa负责人", "qaowner", "抽检人"],
    "trainer_name": ["培训负责人", "培训人", "trainer"],
    "source_record_id": ["主键id", "主键ID", "源记录id", "记录id", "recordid", "sampleid", "样本id", "任务id", "objectId"],
    "comment_id": ["评论id", "评论ID", "commentid", "commentId"],
    "dynamic_id": ["动态id", "动态ID", "帖子id", "内容id", "dynamicid"],
    "account_id": ["账号id", "账号ID", "作者id", "accountid", "uid"],
    "raw_label": ["原始标签", "初判标签", "机审标签"],
    "final_label": ["最终标签", "复核标签", "终判标签"],
    "raw_judgement": ["原始判断", "初判结果", "机审结果", "一审结果"],
    "final_judgement": ["最终判断", "复核结果", "终判结果"],
    "qa_result": ["质检结果", "qa结果", "结果", "判定结果"],
    "error_type": ["错误类型", "问题类型", "差错类型"],
    "error_level": ["错误级别", "问题级别"],
    "error_reason": ["错误原因", "问题原因", "归因"],
    "risk_level": ["风险等级", "风险级别"],
    "training_topic": ["培训专题", "培训主题", "训练专题"],
    "is_raw_correct": ["原始正确", "原始是否正确", "初判是否正确", "质检判断"],
    "is_final_correct": ["最终正确", "最终是否正确", "复核是否正确"],
    "is_misjudge": ["是否错判", "错判", "误判"],
    "is_missjudge": ["是否漏判", "漏判", "漏审"],
    "is_appealed": ["是否申诉", "申诉标记"],
    "is_appeal_reversed": ["是否改判", "申诉改判", "改判标记"],
    "appeal_status": ["申诉状态"],
    "appeal_reason": ["申诉原因"],
    "comment_text": ["评论文本", "文本", "内容文本", "样本文本", "文案"],
    "qa_note": ["质检备注", "备注", "qa备注"],
    "batch_id": ["批次id", "批次号"],
}

APPEAL_COLUMN_ALIASES = {
    "appeal_event_id": ["申诉事件id", "appealeventid"],
    "biz_date": ["业务日期", "日期", "申诉日期", "质检日期"],
    "appeal_time": ["申诉时间", "处理时间", "质检日期"],
    "source_record_id": ["主键id", "主键ID", "源记录id", "记录id", "recordid", "样本id", "任务id"],
    "comment_id": ["评论id", "评论ID", "commentid"],
    "dynamic_id": ["动态id", "动态ID", "帖子id", "内容id", "dynamicid"],
    "account_id": ["账号id", "账号ID", "作者id", "accountid", "uid"],
    "group_name": ["组别", "团队", "组", "一审公司", "公司", "母业务"],
    "queue_name": ["队列", "队列名", "子业务", "供应商"],
    "reviewer_name": ["审核人", "审核员", "reviewer", "一审员", "一审人员"],
    "raw_judgement": ["一审结果", "初审结果"],
    "appeal_status": ["申诉状态"],
    "appeal_result": ["申诉结果", "处理结果"],
    "appeal_reason": ["申诉理由", "申诉原因"],
    "appeal_operator": ["处理人", "申诉处理人", "operator", "申诉员A", "申诉员B"],
    "appeal_note": ["申诉备注", "质检备注", "备注"],
    "is_reversed": ["是否改判", "改判标记", "申诉是否成功"],
    "batch_id": ["批次id", "批次号"],
}

TRUE_TOKENS = {
    "1",
    "true",
    "t",
    "yes",
    "y",
    "是",
    "正确",
    "通过",
    "pass",
    "success",
    "成功",
    "已申诉",
    "已改判",
}
FALSE_TOKENS = {
    "0",
    "false",
    "f",
    "no",
    "n",
    "否",
    "错误",
    "不通过",
    "未通过",
    "fail",
    "失败",
    "未申诉",
}


@dataclass
class ImportSummary:
    dataset: str
    source_file: str
    source_rows: int
    inserted_rows: int
    dedup_rows: int
    warning_rows: int


@dataclass
class RunStats:
    run_id: str
    batch_id: str
    db_path: str
    qa_files: list[dict[str, Any]]
    appeal_files: list[dict[str, Any]]
    refreshed_marts: bool


def normalize_name(value: str) -> str:
    return re.sub(r"[\s\-_:/\\（）()\[\]【】,.]+", "", str(value).strip().lower())


def clean_text(series: pd.Series) -> pd.Series:
    normalized = series.astype("string").str.strip()
    return normalized.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA, "null": pd.NA, "<NA>": pd.NA})


def parse_date_series(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, errors="coerce")
    return parsed.dt.date


def parse_timestamp_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def to_boolean(series: pd.Series) -> pd.Series:
    text = clean_text(series)
    lowered = text.str.lower()
    result = pd.Series(pd.NA, index=series.index, dtype="boolean")
    result = result.mask(lowered.isin(TRUE_TOKENS), True)
    result = result.mask(lowered.isin(FALSE_TOKENS), False)
    return result


def coalesce_series(*series_list: pd.Series, dtype: str | None = None) -> pd.Series:
    if not series_list:
        raise ValueError("series_list 不能为空")

    result = series_list[0].copy()
    for series in series_list[1:]:
        result = result.combine_first(series)

    if dtype:
        return result.astype(dtype)
    return result


def keyword_flag(index: pd.Index, *series_list: pd.Series, keywords: list[str]) -> pd.Series:
    result = pd.Series(pd.NA, index=index, dtype="boolean")
    pattern = "|".join(re.escape(keyword) for keyword in keywords)
    for series in series_list:
        text = clean_text(series)
        mask = text.str.contains(pattern, na=False, case=False)
        result = result.mask(result.isna() & mask, True)
    return result


def infer_correct(index: pd.Index, *series_list: pd.Series) -> pd.Series:
    result = pd.Series(pd.NA, index=index, dtype="boolean")
    true_pattern = r"正确|通过|pass|success"
    false_pattern = r"错误|不通过|未通过|fail|驳回"

    for series in series_list:
        text = clean_text(series)
        lowered = text.str.lower()
        result = result.mask(result.isna() & lowered.isin(TRUE_TOKENS), True)
        result = result.mask(result.isna() & lowered.isin(FALSE_TOKENS), False)
        result = result.mask(result.isna() & text.str.contains(true_pattern, na=False, case=False), True)
        result = result.mask(result.isna() & text.str.contains(false_pattern, na=False, case=False), False)
    return result


def has_meaningful_text(index: pd.Index, *series_list: pd.Series) -> pd.Series:
    result = pd.Series(pd.NA, index=index, dtype="boolean")
    for series in series_list:
        text = clean_text(series)
        result = result.mask(result.isna() & text.notna(), True)
    return result


def read_table_file(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()
    if suffix == ".csv":
        last_error: Exception | None = None
        for encoding in ["utf-8-sig", "utf-8", "gb18030"]:
            try:
                return pd.read_csv(file_path, encoding=encoding, dtype=str)
            except Exception as exc:  # noqa: PERF203
                last_error = exc
        raise RuntimeError(f"CSV 读取失败：{file_path}，最后错误：{last_error}")

    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(file_path, dtype=str)

    if suffix == ".parquet":
        return pd.read_parquet(file_path)

    raise ValueError(f"暂不支持的文件格式：{file_path.suffix}")


def map_columns(df: pd.DataFrame, aliases: dict[str, list[str]]) -> pd.DataFrame:
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


def build_row_hash(df: pd.DataFrame, key_columns: list[str]) -> pd.Series:
    text_frame = df[key_columns].copy()
    for column in key_columns:
        text_frame[column] = clean_text(text_frame[column]).fillna("")
    row_text = text_frame.agg("|".join, axis=1)
    return row_text.map(lambda value: hashlib.sha1(value.encode("utf-8")).hexdigest())


def build_join_key(
    source_record_id: pd.Series,
    comment_id: pd.Series,
    dynamic_id: pd.Series,
    account_id: pd.Series,
) -> pd.Series:
    source_record_id = clean_text(source_record_id)
    comment_id = clean_text(comment_id)
    dynamic_id = clean_text(dynamic_id)
    account_id = clean_text(account_id)

    join_key = pd.Series(pd.NA, index=source_record_id.index, dtype="string")
    join_key = join_key.mask(source_record_id.notna(), "record:" + source_record_id)

    comment_dynamic = comment_id.notna() & dynamic_id.notna()
    join_key = join_key.mask(join_key.isna() & comment_dynamic, "comment:" + comment_id.fillna("") + "|dynamic:" + dynamic_id.fillna(""))
    join_key = join_key.mask(join_key.isna() & comment_id.notna(), "comment:" + comment_id)
    join_key = join_key.mask(join_key.isna() & dynamic_id.notna(), "dynamic:" + dynamic_id)
    join_key = join_key.mask(join_key.isna() & account_id.notna(), "account:" + account_id)
    return join_key


def prepare_qa_frame(raw_df: pd.DataFrame, source_file_name: str, batch_id: str, import_day: date) -> tuple[pd.DataFrame, int]:
    mapped = map_columns(raw_df, QA_COLUMN_ALIASES)
    index = mapped.index

    # 从文件名提取业务日期（优先级最高）
    biz_date_from_file = extract_date_from_filename(source_file_name)

    qa_time = parse_timestamp_series(mapped["qa_time"])

    # biz_date 优先级：文件名 > 数据列 > qa_time > 导入日期
    if biz_date_from_file:
        biz_date = pd.Series(biz_date_from_file, index=index)
    else:
        biz_date = coalesce_series(parse_date_series(mapped["biz_date"]), qa_time.dt.date, pd.Series(import_day, index=index))

    import_date = coalesce_series(parse_date_series(mapped["import_date"]), pd.Series(import_day, index=index))

    # 根据文件名识别业务线（优先级高于数据内容）
    mother_biz_from_file, sub_biz_from_file = identify_business_line(source_file_name)

    # 业务线：优先用文件名匹配，否则用数据内容
    if sub_biz_from_file != "未识别":
        mother_biz = pd.Series(mother_biz_from_file, index=index)
        sub_biz = pd.Series(sub_biz_from_file, index=index)
        # 如果文件名识别成功，group_name 使用映射后的 sub_biz
        group_name = pd.Series(sub_biz_from_file, index=index)
    else:
        mother_biz = coalesce_series(clean_text(mapped["mother_biz"]), pd.Series("未识别", index=index))
        sub_biz = coalesce_series(clean_text(mapped["sub_biz"]), pd.Series("未识别", index=index))
        group_name = coalesce_series(clean_text(mapped["group_name"]), clean_text(mapped["mother_biz"]), clean_text(mapped["sub_biz"]))
    queue_name = coalesce_series(clean_text(mapped["queue_name"]), clean_text(mapped["sub_biz"]))
    reviewer_name = clean_text(mapped["reviewer_name"])
    qa_owner_name = clean_text(mapped["qa_owner_name"])

    # ── inspect_type / workforce_type 自动推断 ──
    # 规则（与 QC 看板4模块分类体系一致）：
    #   inspect_type: qa_owner_name 含"内部团队"前缀或在短名列表 → internal，否则 external
    #   workforce_type: queue_name 含"10816"→新人, 含"18365"→复培
    #                  其余按 inspect_type 分：internal→正式, external→外检
    INTERNAL_OWNER_NAMES = {"邓生位", "刘玉虹", "刘东旭", "张玺", "v_shgwedeng"}
    _owner_text = clean_text(mapped["qa_owner_name"]).fillna("").str.strip()
    _is_internal = (
        _owner_text.str.contains("内部团队", na=False, case=False) |
        _owner_text.isin(INTERNAL_OWNER_NAMES)
    )
    inspect_type = pd.Series("external", index=index, dtype="string")
    inspect_type = inspect_type.mask(_is_internal, "internal")
    # 如果数据里有显式字段，优先用
    inspect_type = coalesce_series(clean_text(mapped["inspect_type"]), inspect_type, dtype="string")

    _queue_text = clean_text(mapped["queue_name"]).fillna("").str.strip()
    workforce_type = pd.Series("正式", index=index, dtype="string")
    workforce_type = workforce_type.mask(_is_internal, "正式")  # 内部团队默认正式
    workforce_type = workforce_type.mask(_queue_text.str.contains("10816", na=False), "新人")
    workforce_type = workforce_type.mask(_queue_text.str.contains("18365", na=False), "复培")
    workforce_type = workforce_type.mask(~_is_internal & ~(_queue_text.str.contains("10816|18365", na=False)), "外检")
    workforce_type = coalesce_series(clean_text(mapped["workforce_type"]), workforce_type, dtype="string")

    raw_judgement = clean_text(mapped["raw_judgement"])
    final_judgement = clean_text(mapped["final_judgement"])
    qa_result = clean_text(mapped["qa_result"])
    qa_note = clean_text(mapped["qa_note"])
    error_type = clean_text(mapped["error_type"])
    error_reason = clean_text(mapped["error_reason"])

    # 质检判断字段：正确/漏判/错判
    qa_judgement = clean_text(mapped["is_raw_correct"])
    
    # 从质检判断字段提取漏判/错判标记
    is_misjudge_from_judgement = qa_judgement.str.contains("错判|误判", na=False, case=False)
    is_missjudge_from_judgement = qa_judgement.str.contains("漏判|漏审", na=False, case=False)
    
    is_raw_correct = coalesce_series(
        to_boolean(mapped["is_raw_correct"]),
        ~is_misjudge_from_judgement & ~is_missjudge_from_judgement,  # 不是错判也不是漏判，就是正确
        infer_correct(index, mapped["is_raw_correct"], mapped["final_judgement"]),
        dtype="boolean",
    ).fillna(False)
    is_final_correct = coalesce_series(
        to_boolean(mapped["is_final_correct"]),
        infer_correct(index, mapped["final_judgement"], mapped["final_label"]),
        is_raw_correct,
        dtype="boolean",
    ).fillna(False)
    is_misjudge = coalesce_series(
        to_boolean(mapped["is_misjudge"]),
        is_misjudge_from_judgement,  # 优先从质检判断字段提取
        keyword_flag(index, error_type, error_reason, qa_result, qa_note, keywords=["错判", "误判"]),
        dtype="boolean",
    ).fillna(False)
    is_missjudge = coalesce_series(
        to_boolean(mapped["is_missjudge"]),
        is_missjudge_from_judgement,  # 优先从质检判断字段提取
        keyword_flag(index, error_type, error_reason, qa_result, qa_note, keywords=["漏判", "漏审"]),
        dtype="boolean",
    ).fillna(False)
    is_appealed = coalesce_series(
        to_boolean(mapped["is_appealed"]),
        has_meaningful_text(index, mapped["appeal_status"], mapped["appeal_reason"]),
        dtype="boolean",
    ).fillna(False)
    is_appeal_reversed = coalesce_series(
        to_boolean(mapped["is_appeal_reversed"]),
        keyword_flag(index, mapped["appeal_status"], mapped["qa_result"], keywords=["改判", "申诉成功"]),
        dtype="boolean",
    ).fillna(False)

    prepared = pd.DataFrame(
        {
            "biz_date": biz_date,
            "qa_time": qa_time,
            "import_date": import_date,
            "mother_biz": mother_biz,  # 用文件名匹配的业务线
            "sub_biz": sub_biz,        # 用文件名匹配的业务线
            "group_name": group_name,
            "queue_name": queue_name,
            "scene_name": clean_text(mapped["scene_name"]),
            "channel_name": clean_text(mapped["channel_name"]),
            "content_type": clean_text(mapped["content_type"]),
            "inspect_type": inspect_type,
            "workforce_type": workforce_type,
            "reviewer_name": reviewer_name,
            "qa_owner_name": qa_owner_name,
            "trainer_name": clean_text(mapped["trainer_name"]),
            "source_record_id": clean_text(mapped["source_record_id"]),
            "comment_id": clean_text(mapped["comment_id"]),
            "dynamic_id": clean_text(mapped["dynamic_id"]),
            "account_id": clean_text(mapped["account_id"]),
            "raw_label": clean_text(mapped["raw_label"]),
            "final_label": clean_text(mapped["final_label"]),
            "raw_judgement": raw_judgement,
            "final_judgement": final_judgement,
            "qa_result": qa_result,
            "error_type": error_type,
            "error_level": clean_text(mapped["error_level"]),
            "error_reason": error_reason,
            "risk_level": clean_text(mapped["risk_level"]),
            "training_topic": clean_text(mapped["training_topic"]),
            "is_raw_correct": is_raw_correct,
            "is_final_correct": is_final_correct,
            "is_misjudge": is_misjudge,
            "is_missjudge": is_missjudge,
            "is_appealed": is_appealed,
            "is_appeal_reversed": is_appeal_reversed,
            "appeal_status": clean_text(mapped["appeal_status"]),
            "appeal_reason": clean_text(mapped["appeal_reason"]),
            "comment_text": clean_text(mapped["comment_text"]),
            "qa_note": qa_note,
            "batch_id": clean_text(mapped["batch_id"]).fillna(batch_id),
            "source_file_name": source_file_name,
        }
    )
    prepared["join_key"] = build_join_key(
        prepared["source_record_id"],
        prepared["comment_id"],
        prepared["dynamic_id"],
        prepared["account_id"],
    )
    prepared["row_hash"] = build_row_hash(
        prepared,
        [
            "biz_date",
            "qa_time",  # 新增：质检时间有较高唯一性
            "group_name",
            "queue_name",
            "reviewer_name",
            "source_record_id",
            "comment_id",
            "dynamic_id",
            "account_id",  # 新增：账号类业务的主键
            "raw_label",
            "final_label",
            "qa_result",
            "error_type",
            "qa_note",
            "comment_text",  # 新增：用评论文本区分没有主键的行
            "source_file_name",
        ],
    )
    event_id_seed = coalesce_series(
        clean_text(mapped["event_id"]),
        prepared["source_record_id"],
        prepared["comment_id"],
        prepared["dynamic_id"],
        prepared["account_id"],
    )
    prepared["event_id"] = clean_text(event_id_seed).fillna("qa-" + prepared["row_hash"].str[:16])

    warning_rows = int(parse_date_series(mapped["biz_date"]).isna().sum() + prepared["join_key"].isna().sum())
    return prepared[QA_INSERT_COLUMNS], warning_rows


def prepare_appeal_frame(raw_df: pd.DataFrame, source_file_name: str, batch_id: str, import_day: date) -> tuple[pd.DataFrame, int]:
    mapped = map_columns(raw_df, APPEAL_COLUMN_ALIASES)
    index = mapped.index

    # 从文件名提取业务日期（优先级最高）
    biz_date_from_file = extract_date_from_filename(source_file_name)

    appeal_time = parse_timestamp_series(mapped["appeal_time"])

    # biz_date 优先级：文件名 > 数据列 > appeal_time > 导入日期
    if biz_date_from_file:
        biz_date = pd.Series(biz_date_from_file, index=index)
    else:
        biz_date = coalesce_series(parse_date_series(mapped["biz_date"]), appeal_time.dt.date, pd.Series(import_day, index=index))

    source_record_id = clean_text(mapped["source_record_id"])
    comment_id = clean_text(mapped["comment_id"])
    dynamic_id = clean_text(mapped["dynamic_id"])
    account_id = clean_text(mapped["account_id"])
    appeal_result = clean_text(mapped["appeal_result"])
    raw_judgement = clean_text(mapped["raw_judgement"])
    appeal_status = clean_text(mapped["appeal_status"])

    is_reversed = coalesce_series(
        to_boolean(mapped["is_reversed"]),
        (raw_judgement.notna() & appeal_result.notna() & (raw_judgement.str.strip() != appeal_result.str.strip())).astype("boolean"),
        keyword_flag(index, mapped["appeal_status"], mapped["appeal_result"], keywords=["改判", "申诉成功"]),
        dtype="boolean",
    ).fillna(False)

    prepared = pd.DataFrame(
        {
            "biz_date": biz_date,
            "appeal_time": appeal_time,
            "source_record_id": source_record_id,
            "comment_id": comment_id,
            "dynamic_id": dynamic_id,
            "account_id": account_id,
            "group_name": coalesce_series(clean_text(mapped["group_name"]), clean_text(mapped["queue_name"])),
            "queue_name": clean_text(mapped["queue_name"]),
            "reviewer_name": clean_text(mapped["reviewer_name"]),
            "appeal_status": appeal_status,
            "appeal_result": appeal_result,
            "appeal_reason": clean_text(mapped["appeal_reason"]),
            "appeal_operator": clean_text(mapped["appeal_operator"]),
            "appeal_note": clean_text(mapped["appeal_note"]),
            "is_reversed": is_reversed,
            "batch_id": clean_text(mapped["batch_id"]).fillna(batch_id),
            "source_file_name": source_file_name,
        }
    )
    prepared["join_key"] = build_join_key(
        prepared["source_record_id"],
        prepared["comment_id"],
        prepared["dynamic_id"],
        prepared["account_id"],
    )
    prepared["row_hash"] = build_row_hash(
        prepared,
        [
            "biz_date",
            "source_record_id",
            "comment_id",
            "dynamic_id",
            "account_id",
            "appeal_status",
            "appeal_result",
            "appeal_reason",
            "source_file_name",
        ],
    )
    event_id_seed = coalesce_series(
        clean_text(mapped["appeal_event_id"]),
        prepared["source_record_id"],
        prepared["comment_id"],
        prepared["dynamic_id"],
        prepared["account_id"],
    )
    prepared["appeal_event_id"] = clean_text(event_id_seed).fillna("appeal-" + prepared["row_hash"].str[:16])

    warning_rows = int(parse_date_series(mapped["biz_date"]).isna().sum() + prepared["join_key"].isna().sum())
    return prepared[APPEAL_INSERT_COLUMNS], warning_rows


def insert_new_rows(
    conn: Any,
    table_name: str,
    insert_columns: list[str],
    stage_df: pd.DataFrame,
) -> tuple[int, int]:
    """插入数据到 fact 表，不做文件内去重，保持与原始文件一致。

    注意：仍保留文件级去重（通过 fact_file_dedup 表），避免同一文件重复上传。
    """
    if stage_df.empty:
        return 0, 0

    # 使用 TiDB insert_dataframe 批量插入
    inserted_rows = conn.insert_dataframe(table_name, stage_df)
    dedup_rows = 0
    return inserted_rows, dedup_rows


def write_etl_log(
    conn,
    *,
    run_id: str,
    job_name: str,
    source_rows: int,
    inserted_rows: int,
    dedup_rows: int,
    warning_rows: int,
    run_status: str,
    error_message: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO etl_run_log (
            run_id,
            job_name,
            start_time,
            end_time,
            run_status,
            source_rows,
            inserted_rows,
            dedup_rows,
            warning_rows,
            error_message
        )
        VALUES (%s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s)
        """,
        [run_id, job_name, run_status, source_rows, inserted_rows, dedup_rows, warning_rows, error_message],
    )


def write_upload_log(
    conn,
    *,
    upload_id: str,
    file_name: str,
    file_type: str,
    file_size_bytes: int,
    source_rows: int,
    inserted_rows: int,
    dedup_rows: int,
    business_line: str,
    upload_status: str,
    error_message: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO fact_upload_log (
            upload_id,
            file_name,
            file_type,
            file_size_bytes,
            source_rows,
            inserted_rows,
            dedup_rows,
            business_line,
            upload_status,
            error_message
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [upload_id, file_name, file_type, file_size_bytes, source_rows, inserted_rows, dedup_rows, business_line, upload_status, error_message],
    )


def check_file_duplicate(
    conn,
    file_hash: str,
    file_name: str,
    file_type: str,
    upload_id: str,
) -> bool:
    """检查文件是否已上传过（基于文件内容哈希）。

    Returns:
        True: 已上传过（跳过）
        False: 未上传过（继续导入）
    """
    result = conn.fetch_one(
        "SELECT file_hash, upload_count FROM fact_file_dedup WHERE file_hash = %s",
        [file_hash]
    )

    if result:
        conn.execute(
            "UPDATE fact_file_dedup SET upload_count = upload_count + 1 WHERE file_hash = %s",
            [file_hash]
        )
        return True

    conn.execute(
        """
        INSERT INTO fact_file_dedup (file_hash, file_name, file_type, first_upload_time, first_upload_id, upload_count)
        VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s, 1)
        """,
        [file_hash, file_name, file_type, upload_id]
    )
    return False


def compute_file_hash(file_path: Path) -> str:
    """计算文件内容哈希（用于去重）。"""
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def import_dataset(
    conn,
    *,
    dataset: str,
    file_path: Path,
    source_name: str,
    batch_id: str,
    import_day: date,
    upload_id: str,
    skip_dedup: bool = False,
) -> ImportSummary:
    raw_df = read_table_file(file_path)
    file_size = file_path.stat().st_size if file_path.exists() else 0
    file_hash = compute_file_hash(file_path) if skip_dedup is False else ""
    
    # 去重检测
    if not skip_dedup and check_file_duplicate(conn, file_hash, source_name, dataset, upload_id):
        # 文件已上传过，返回跳过信息
        mother_biz, sub_biz = identify_business_line(source_name)
        business_line = sub_biz if sub_biz != "未识别" else mother_biz
        write_upload_log(
            conn,
            upload_id=upload_id,
            file_name=source_name,
            file_type=dataset,
            file_size_bytes=file_size,
            source_rows=len(raw_df),
            inserted_rows=0,
            dedup_rows=len(raw_df),
            business_line=business_line,
            upload_status="skipped",
            error_message="文件内容已存在，跳过重复导入"
        )
        return ImportSummary(dataset, str(file_path), len(raw_df), 0, len(raw_df), 0)
    
    if dataset == "qa":
        prepared_df, warning_rows = prepare_qa_frame(raw_df, source_name, batch_id, import_day)
        inserted_rows, dedup_rows = insert_new_rows(conn, "fact_qa_event", QA_INSERT_COLUMNS, prepared_df)
        mother_biz, sub_biz = identify_business_line(source_name)
        business_line = sub_biz if sub_biz != "未识别" else mother_biz
        write_upload_log(
            conn,
            upload_id=upload_id,
            file_name=source_name,
            file_type="qa",
            file_size_bytes=file_size,
            source_rows=len(prepared_df),
            inserted_rows=inserted_rows,
            dedup_rows=dedup_rows,
            business_line=business_line,
            upload_status="success"
        )
        return ImportSummary("qa", str(file_path), len(prepared_df), inserted_rows, dedup_rows, warning_rows)

    prepared_df, warning_rows = prepare_appeal_frame(raw_df, source_name, batch_id, import_day)
    inserted_rows, dedup_rows = insert_new_rows(conn, "fact_appeal_event", APPEAL_INSERT_COLUMNS, prepared_df)
    write_upload_log(
        conn,
        upload_id=upload_id,
        file_name=source_name,
        file_type="appeal",
        file_size_bytes=file_size,
        source_rows=len(prepared_df),
        inserted_rows=inserted_rows,
        dedup_rows=dedup_rows,
        business_line="",
        upload_status="success"
    )
    return ImportSummary("appeal", str(file_path), len(prepared_df), inserted_rows, dedup_rows, warning_rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="把质检/申诉明细文件导入 TiDB fact 表，并可选刷新 mart。")
    parser.add_argument("--qa-file", dest="qa_files", action="append", default=[], help="质检明细文件路径，可重复传多个")
    parser.add_argument("--appeal-file", dest="appeal_files", action="append", default=[], help="申诉明细文件路径，可重复传多个")
    parser.add_argument("--source-name", dest="source_names", action="append", default=[], help="原始文件名（用于业务线识别），与 --qa-file/--appeal-file 一一对应")
    parser.add_argument("--db-path", default=None, help="已废弃，保留兼容性")
    parser.add_argument("--batch-id", default=None, help="本次导入批次号；不传则自动生成")
    parser.add_argument("--skip-refresh", action="store_true", help="只导入 fact，不刷新 mart / 规则维表")
    parser.add_argument("--skip-dedup", action="store_true", help="跳过文件级去重检测（默认启用去重）")
    return parser.parse_args()


def ensure_files_exist(paths: list[str]) -> list[Path]:
    resolved = [Path(path).expanduser().resolve() for path in paths]
    missing = [str(path) for path in resolved if not path.exists()]
    if missing:
        raise FileNotFoundError(f"以下文件不存在：{missing}")
    return resolved


def main() -> None:
    args = parse_args()
    if not args.qa_files and not args.appeal_files:
        raise SystemExit("至少传一个 --qa-file 或 --appeal-file")

    qa_files = ensure_files_exist(args.qa_files)
    appeal_files = ensure_files_exist(args.appeal_files)
    source_names = args.source_names if args.source_names else None
    batch_id = args.batch_id or f"batch_{date.today():%Y%m%d}_{uuid.uuid4().hex[:8]}"
    run_id = uuid.uuid4().hex
    repo = DashboardRepository()
    repo.initialize_schema()

    qa_summaries: list[ImportSummary] = []
    appeal_summaries: list[ImportSummary] = []

    conn = repo.connect()
    for idx, file_path in enumerate(qa_files):
        # 获取原始文件名（用于业务线识别）
        source_name = source_names[idx] if source_names and idx < len(source_names) else file_path.name
        upload_id = f"upload_{run_id}_qa_{idx}"
        try:
            summary = import_dataset(
                conn, dataset="qa", file_path=file_path, source_name=source_name,
                batch_id=batch_id, import_day=date.today(), upload_id=upload_id,
                skip_dedup=args.skip_dedup
            )
            qa_summaries.append(summary)
            write_etl_log(
                conn,
                run_id=f"{run_id}_qa_{len(qa_summaries)}",
                job_name=f"import_fact_qa_event:{source_name}",
                source_rows=summary.source_rows,
                inserted_rows=summary.inserted_rows,
                dedup_rows=summary.dedup_rows,
                warning_rows=summary.warning_rows,
                run_status="success" if summary.inserted_rows > 0 or summary.source_rows == 0 else "skipped",
            )
        except Exception as exc:
            write_etl_log(
                conn,
                run_id=f"{run_id}_qa_fail_{len(qa_summaries) + 1}",
                job_name=f"import_fact_qa_event:{source_name}",
                source_rows=0,
                inserted_rows=0,
                dedup_rows=0,
                warning_rows=0,
                run_status="failed",
                error_message=str(exc),
            )
            write_upload_log(
                conn,
                upload_id=upload_id,
                file_name=source_name,
                file_type="qa",
                file_size_bytes=file_path.stat().st_size if file_path.exists() else 0,
                source_rows=0,
                inserted_rows=0,
                dedup_rows=0,
                business_line="",
                upload_status="failed",
                error_message=str(exc)
            )
            raise

    for idx, file_path in enumerate(appeal_files):
        source_name = source_names[len(qa_files) + idx] if source_names and (len(qa_files) + idx) < len(source_names) else file_path.name
        upload_id = f"upload_{run_id}_appeal_{idx}"
        try:
            summary = import_dataset(
                conn, dataset="appeal", file_path=file_path, source_name=source_name,
                batch_id=batch_id, import_day=date.today(), upload_id=upload_id,
                skip_dedup=args.skip_dedup
            )
            appeal_summaries.append(summary)
            write_etl_log(
                conn,
                run_id=f"{run_id}_appeal_{len(appeal_summaries)}",
                job_name=f"import_fact_appeal_event:{source_name}",
                source_rows=summary.source_rows,
                inserted_rows=summary.inserted_rows,
                dedup_rows=summary.dedup_rows,
                warning_rows=summary.warning_rows,
                run_status="success" if summary.inserted_rows > 0 or summary.source_rows == 0 else "skipped",
            )
        except Exception as exc:
            write_etl_log(
                conn,
                run_id=f"{run_id}_appeal_fail_{len(appeal_summaries) + 1}",
                job_name=f"import_fact_appeal_event:{file_path.name}",
                source_rows=0,
                inserted_rows=0,
                dedup_rows=0,
                warning_rows=0,
                run_status="failed",
                error_message=str(exc),
            )
            write_upload_log(
                conn,
                upload_id=upload_id,
                file_name=source_name,
                file_type="appeal",
                file_size_bytes=file_path.stat().st_size if file_path.exists() else 0,
                source_rows=0,
                inserted_rows=0,
                dedup_rows=0,
                business_line="",
                upload_status="failed",
                error_message=str(exc)
            )
            raise

    refreshed_marts = False
    if not args.skip_refresh:
        repo.initialize_schema()
        refreshed_marts = True

    stats = RunStats(
        run_id=run_id,
        batch_id=batch_id,
        db_path="tidb",
        qa_files=[asdict(item) for item in qa_summaries],
        appeal_files=[asdict(item) for item in appeal_summaries],
        refreshed_marts=refreshed_marts,
    )
    print(json.dumps(asdict(stats), ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
