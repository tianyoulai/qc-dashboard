"""Google Sheet 直连拉取申诉数据并入库。"""
from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import uuid
from datetime import date
from pathlib import Path

import requests
from google.oauth2 import service_account
from google.auth.transport.requests import AuthorizedSession

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def load_settings() -> dict:
    settings_path = PROJECT_ROOT / "config" / "settings.json"
    with open(settings_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_service_account() -> dict:
    settings = load_settings()
    sa_file = PROJECT_ROOT / settings["google_sheet"]["service_account_file"]
    with open(sa_file, "r", encoding="utf-8") as f:
        return json.load(f)


def pull_google_sheet_csv(sheet_id: str, gid: str, output_path: Path | None = None) -> tuple[str, int]:
    """从 Google Sheet 导出 CSV，返回 (csv_content, row_count)。
    
    使用 Google Sheets API v4 values.get 拉取数据，再转成 CSV 格式。
    比 /export 端点对服务账号更可靠。
    """
    import pandas as pd

    sa_info = load_service_account()
    credentials = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )
    session = AuthorizedSession(credentials)

    # 先用 sheets metadata API 找到 sheet name
    meta_url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}?fields=sheets.properties"
    meta_resp = session.get(meta_url)
    meta_resp.raise_for_status()
    sheets_meta = meta_resp.json().get("sheets", [])
    
    target_sheet = None
    for s in sheets_meta:
        props = s.get("properties", {})
        if props.get("sheetId") == int(gid):
            target_sheet = props.get("title")
            break
    
    if not target_sheet:
        print(f"⚠️ 未找到 gid={gid} 对应的 sheet 名称，尝试用 gid={gid} 直接读取...")
        # 回退：用 gid 作为 sheetId 传参
        values_url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{gid}"
    else:
        values_url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/{target_sheet}"

    print(f"  读取 sheet: {target_sheet or gid}")
    
    resp = session.get(values_url)
    resp.raise_for_status()

    data = resp.json()
    values = data.get("values", [])
    
    if not values:
        raise RuntimeError(f"Sheet {target_sheet or gid} 为空或无数据")

    # 把二维数组转成 CSV
    df = pd.DataFrame(values[1:], columns=values[0])
    csv_content = df.to_csv(index=False, encoding="utf-8-sig")
    
    if output_path:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8-sig") as f:
            f.write(csv_content)

    return csv_content, len(df)


def main() -> None:
    parser = argparse.ArgumentParser(description="从 Google Sheet 拉取申诉数据并保存为 CSV。")
    parser.add_argument("--output", "-o", default=None, help="输出 CSV 路径，默认 deliverables/appeal_gsheet_{date}.csv")
    parser.add_argument("--import-to-db", action="store_true", help="拉取后直接导入到 TiDB")
    parser.add_argument("--db-path", default=None, help="已废弃，保留兼容性")
    args = parser.parse_args()

    settings = load_settings()
    sheet_id = settings["google_sheet"]["spreadsheet_id"]
    gid = settings["google_sheet"]["gid"]

    output_path = Path(args.output) if args.output else PROJECT_ROOT / "deliverables" / f"appeal_gsheet_{date.today():%Y%m%d}.csv"

    print(f"正在从 Google Sheet 拉取申诉数据...")
    csv_content, row_count = pull_google_sheet_csv(sheet_id, gid, output_path)
    print(f"拉取完成：{row_count} 行 → {output_path}")

    if args.import_to_db:
        from jobs.import_fact_data import main as import_main
        print(f"\n正在导入到 TiDB...")
        sys.argv = ["import_fact_data.py", "--appeal-file", str(output_path)]
        import_main()

    print("\n完成。")


if __name__ == "__main__":
    main()
