"""
DuckDB → TiDB 数据迁移脚本。

从本地 DuckDB (data/warehouse/qa.duckdb) 读取所有 fact/mart/dim 表数据，
分批写入 TiDB。

用法：
    python jobs/migrate_duckdb_to_tidb.py
    python jobs/migrate_duckdb_to_tidb.py --batch-size 5000
    python jobs/migrate_duckdb_to_tidb.py --tables fact_qa_event fact_appeal_event  # 只迁移指定表
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd

# 将项目根目录加入 sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from storage.tidb_manager import TiDBManager

DUCKDB_PATH = PROJECT_ROOT / "data" / "warehouse" / "qa.duckdb"

# 需要迁移的表（不含视图，视图由 TiDB schema.sql 定义）
MIGRATE_TABLES = [
    # fact 层
    "fact_qa_event",
    "fact_appeal_event",
    "fact_alert_event",
    "fact_file_dedup",
    "fact_upload_log",
    # mart 层
    "mart_day_group",
    "mart_day_queue",
    "mart_day_auditor",
    "mart_day_error_topic",
    "mart_week_group",
    "mart_week_queue",
    "mart_week_error_topic",
    "mart_month_group",
    "mart_month_queue",
    "mart_month_error_topic",
    "mart_training_action_recovery",
    # dim 层
    "dim_alert_rule",
    # 告警状态
    "fact_alert_status",
    "fact_alert_status_history",
    # ETL 日志
    "etl_run_log",
]


def migrate_table(
    ddb_conn: duckdb.DuckDBPyConnection,
    tidb: TiDBManager,
    table_name: str,
    batch_size: int = 2000,
) -> dict:
    """迁移单张表。返回统计信息。"""
    print(f"\n{'='*60}")
    print(f"迁移表: {table_name}")
    print(f"{'='*60}")

    # 1. 从 DuckDB 读取总行数
    total = ddb_conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"  DuckDB 总行数: {total}")

    if total == 0:
        print(f"  跳过（空表）")
        return {"table": table_name, "source": 0, "inserted": 0, "time": 0}

    # 2. 检查 TiDB 是否已有数据
    tidb_cnt = tidb.fetch_one(f"SELECT COUNT(*) AS cnt FROM `{table_name}`")
    existing = tidb_cnt["cnt"] if tidb_cnt else 0
    if existing > 0:
        print(f"  ⚠️  TiDB 已有 {existing} 行，将清空后重新写入")

    # 3. 清空 TiDB 目标表
    tidb.execute(f"DELETE FROM `{table_name}`")
    print(f"  已清空 TiDB 表")

    # 4. 分批读取 DuckDB 并写入 TiDB
    offset = 0
    inserted = 0
    start_time = time.time()

    while offset < total:
        batch_end = min(offset + batch_size, total)
        print(f"  读取 DuckDB 行 {offset + 1}~{batch_end} ...", end=" ", flush=True)

        df = ddb_conn.execute(
            f"SELECT * FROM {table_name} LIMIT {batch_size} OFFSET {offset}"
        ).fetchdf()

        # 类型转换：DuckDB BOOLEAN → int (TiDB TINYINT(1))
        for col in df.columns:
            if df[col].dtype == "object" and df[col].apply(lambda x: isinstance(x, bool)).any():
                df[col] = df[col].astype(int)

        # 处理 NaN → None
        df = df.replace({np.nan: None})

        # 写入 TiDB
        rows_inserted = tidb.insert_dataframe(table_name, df)
        inserted += rows_inserted
        offset = batch_end
        print(f"写入 {rows_inserted} 行 ✅")

    elapsed = time.time() - start_time
    print(f"  ✅ 完成: {inserted} 行, 耗时 {elapsed:.1f}s")

    return {"table": table_name, "source": total, "inserted": inserted, "time": round(elapsed, 1)}


def main():
    parser = argparse.ArgumentParser(description="DuckDB → TiDB 数据迁移")
    parser.add_argument("--batch-size", type=int, default=2000, help="每批写入行数 (默认 2000)")
    parser.add_argument("--tables", nargs="+", default=None, help="只迁移指定表")
    args = parser.parse_args()

    tables = args.tables or MIGRATE_TABLES

    print("=" * 60)
    print("  DuckDB → TiDB 数据迁移")
    print(f"  源: {DUCKDB_PATH}")
    print(f"  批大小: {args.batch_size}")
    print(f"  迁移表: {len(tables)} 张")
    print("=" * 60)

    # 连接
    if not DUCKDB_PATH.exists():
        print(f"❌ DuckDB 文件不存在: {DUCKDB_PATH}")
        sys.exit(1)

    ddb_conn = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    tidb = TiDBManager()

    # 验证 TiDB 连接
    try:
        tidb.fetch_one("SELECT 1 AS ping")
    except Exception as e:
        print(f"❌ TiDB 连接失败: {e}")
        sys.exit(1)
    print("✅ TiDB 连接正常")

    # 逐表迁移
    results = []
    total_start = time.time()

    for table in tables:
        try:
            result = migrate_table(ddb_conn, tidb, table, args.batch_size)
            results.append(result)
        except Exception as e:
            print(f"  ❌ 迁移失败: {e}")
            results.append({"table": table, "source": -1, "inserted": -1, "time": 0, "error": str(e)})

    total_elapsed = time.time() - total_start

    # 汇总
    print(f"\n{'='*60}")
    print("  迁移汇总")
    print(f"{'='*60}")
    ok_count = sum(1 for r in results if r.get("inserted", -1) >= 0)
    err_count = len(results) - ok_count
    total_rows = sum(r.get("inserted", 0) for r in results)
    print(f"  成功: {ok_count} 表, 失败: {err_count} 表")
    print(f"  总行数: {total_rows}")
    print(f"  总耗时: {total_elapsed:.1f}s")
    print()

    for r in results:
        status = "✅" if r.get("inserted", -1) >= 0 else "❌"
        src = r.get("source", "?")
        ins = r.get("inserted", "?")
        t = r.get("time", "?")
        err = r.get("error", "")
        line = f"  {status} {r['table']}: 源={src} 写入={ins} 耗时={t}s"
        if err:
            line += f" 错误={err}"
        print(line)

    ddb_conn.close()
    print(f"\n✅ 迁移完成！")


if __name__ == "__main__":
    main()
