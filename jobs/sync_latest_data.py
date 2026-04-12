#!/usr/bin/env python3
"""
每日数据同步脚本 - 增量同步最新质检数据
支持：历史规律对比、异常检测、企微通知
"""

import argparse
import hashlib
import json
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from storage.tidb_manager import TiDBManager

# ============== 文件导入规则 ==============
# 1. 目标文件：长沙云雀、迁移人力ilabel、迁移人力账号
# 2. 排除文件：图片质检、新人培训、带数字编号（如10816）
# 3. 业务线映射：
#    - 长沙云雀 → A组-评论
#    - 迁移人力ilabel → B组-评论
#    - 迁移人力账号 → B组-账号
# 4. 日期提取优先级：文件名 > 数据列 > qa_time > 导入日期
# =========================================

WEWORK_CACHE_ROOT = Path.home() / 'Library/Containers/com.tencent.WeWorkMac/Data/Documents/Profiles'
DB_PATH = ""  # TiDB, 不再需要本地路径
PROJECT_ROOT = Path(__file__).parent.parent

# 排除关键词
EXCLUDE_KEYWORDS = ["图片", "模板", "~$", "新人培训", "新人"]

# 目标关键词（必须包含其一）
TARGET_KEYWORDS = ["长沙云雀", "迁移人力ilabel", "迁移人力账号"]


def has_number_code(filename: str) -> bool:
    """检查文件名是否包含5位以上纯数字编号（排除日期）"""
    import re
    matches = re.findall(r'\d{5,}', filename)
    return len(matches) > 0


def extract_date_from_filename(filename: str) -> Optional[date]:
    """从文件名提取日期"""
    import re
    
    # 1. 优先匹配 YYYY.M.D 格式（如 2026.3.28）
    match = re.search(r'(\d{4})\.(\d{1,2})\.(\d{1,2})', filename)
    if match:
        year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
        try:
            return date(year, month, day)
        except ValueError:
            pass
    
    # 2. 匹配 MMDD 格式（如 0326）
    match = re.search(r'(?<!\d)(\d{2})(\d{2})(?!\d)', filename)
    if match:
        month, day = int(match.group(1)), int(match.group(2))
        # 假设是当前年份
        year = date.today().year
        try:
            parsed = date(year, month, day)
            # 如果日期在未来，则是去年
            if parsed > date.today():
                parsed = date(year - 1, month, day)
            return parsed
        except ValueError:
            pass
    
    # 3. 匹配 M.D 格式（如 3.28）
    match = re.search(r'(?<!\d)(\d{1,2})\.(\d{1,2})(?!\d)', filename)
    if match:
        month, day = int(match.group(1)), int(match.group(2))
        year = date.today().year
        try:
            parsed = date(year, month, day)
            if parsed > date.today():
                parsed = date(year - 1, month, day)
            return parsed
        except ValueError:
            pass
    
    return None


def scan_new_files(days_back: int = 7) -> List[Tuple[Path, date]]:
    """扫描最近 N 天的新文件（支持跨月份）"""
    cutoff_date = date.today() - timedelta(days=days_back)
    new_files = []
    
    for profile_dir in WEWORK_CACHE_ROOT.iterdir():
        if not profile_dir.is_dir():
            continue
        cache_dir = profile_dir / 'Caches' / 'Files'
        if not cache_dir.exists():
            continue
        
        for month_dir in cache_dir.iterdir():
            if not month_dir.is_dir():
                continue
            for file_dir in month_dir.iterdir():
                if not file_dir.is_dir():
                    continue
                for file_path in file_dir.glob('*.xlsx'):
                    file_name = file_path.name
                    
                    # 排除不需要的文件
                    if any(exclude in file_name for exclude in EXCLUDE_KEYWORDS):
                        continue
                    
                    # 排除带数字编号的文件
                    if has_number_code(file_name):
                        continue
                    
                    # 必须包含目标关键词
                    if not any(target in file_name for target in TARGET_KEYWORDS):
                        continue
                    
                    # 提取日期
                    file_date = extract_date_from_filename(file_name)
                    if file_date and file_date >= cutoff_date:
                        new_files.append((file_path, file_date))
    
    # 按日期排序
    new_files.sort(key=lambda x: x[1])
    return new_files


def check_file_dedup(file_path: Path) -> bool:
    """检查文件是否已导入（去重）"""
    file_hash = hashlib.sha256(file_path.read_bytes()).hexdigest()
    
    conn = TiDBManager()
    try:
        result = conn.fetch_one(
            "SELECT 1 AS c FROM fact_file_dedup WHERE file_hash = %s LIMIT 1",
            [file_hash]
        )
        return result is not None
    except Exception:
        return False


def analyze_historical_pattern() -> dict:
    """分析历史数据规律"""
    conn = TiDBManager()
    try:
        # 最近 14 天的数据统计
        df = conn.fetch_df('''
            SELECT 
                biz_date,
                mother_biz,
                sub_biz,
                COUNT(*) as cnt
            FROM fact_qa_event
            WHERE biz_date >= CURRENT_DATE - INTERVAL 14 DAY
            GROUP BY 1, 2, 3
            ORDER BY 1, 2, 3
        ''')

        if df.empty:
            return {}

        # 按业务线统计
        daily_by_biz = df.groupby(['biz_date', 'sub_biz'])['cnt'].sum().unstack(fill_value=0)
        
        # 计算统计指标
        stats = {
            'daily_avg': float(daily_by_biz.sum(axis=1).mean()),
            'daily_std': float(daily_by_biz.sum(axis=1).std()),
            'daily_min': int(daily_by_biz.sum(axis=1).min()),
            'daily_max': int(daily_by_biz.sum(axis=1).max()),
            'biz_avg': {k: float(v) for k, v in daily_by_biz.mean().to_dict().items()},
            'biz_std': {k: float(v) for k, v in daily_by_biz.std().to_dict().items()},
        }
        
        return stats
    finally:
        conn.close()


def check_anomalies(today_data: dict, historical_stats: dict) -> List[dict]:
    """检查数据异常"""
    anomalies = []
    
    if not historical_stats:
        return anomalies
    
    # 1. 总量异常（低于均值-2σ 或高于均值+2σ）
    today_total = sum(today_data.values())
    avg = historical_stats['daily_avg']
    std = historical_stats['daily_std']
    
    if std > 0:
        z_score = (today_total - avg) / std
        if abs(z_score) > 2:
            anomalies.append({
                'type': '总量异常',
                'severity': 'P1' if abs(z_score) > 3 else 'P2',
                'message': f'今日总量 {today_total:,.0f} 条，偏离历史均值 {avg:,.0f} 条达 {z_score:.1f}σ',
                'z_score': z_score,
            })
    
    # 2. 业务线缺失（应该有但实际没有）
    expected_biz = set(historical_stats['biz_avg'].keys())
    actual_biz = set(today_data.keys())
    missing_biz = expected_biz - actual_biz
    
    if missing_biz:
        anomalies.append({
            'type': '业务线缺失',
            'severity': 'P0',
            'message': f'缺失业务线: {", ".join(missing_biz)}',
            'missing': list(missing_biz),
        })
    
    # 3. 业务线数据量异常
    for biz, today_cnt in today_data.items():
        if biz in historical_stats['biz_avg']:
            biz_avg = historical_stats['biz_avg'][biz]
            biz_std = historical_stats['biz_std'][biz]
            
            if biz_std > 0:
                biz_z = (today_cnt - biz_avg) / biz_std
                if abs(biz_z) > 2:
                    anomalies.append({
                        'type': f'{biz} 数据异常',
                        'severity': 'P2',
                        'message': f'{biz} 今日 {today_cnt:,.0f} 条，偏离均值 {biz_avg:,.0f} 条达 {biz_z:.1f}σ',
                        'z_score': biz_z,
                    })
    
    return anomalies


def send_alert_to_wework(message: str):
    """发送告警到企业微信群"""
    import requests
    
    # 读取配置
    config_path = PROJECT_ROOT / 'config' / 'settings.json'
    with open(config_path) as f:
        settings = json.load(f)
    webhook_key = settings.get('wework_webhook_key')
    
    if not webhook_key:
        print("⚠️ 未配置企微 Webhook Key，跳过通知")
        return
    
    webhook_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={webhook_key}"
    
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": message
        }
    }
    
    try:
        response = requests.post(webhook_url, json=payload, timeout=10)
        if response.status_code == 200:
            print("✅ 企微通知发送成功")
        else:
            print(f"❌ 企微通知发送失败: {response.text}")
    except Exception as e:
        print(f"❌ 企微通知发送异常: {e}")


def get_today_data() -> dict:
    """获取今日已导入的数据统计"""
    conn = TiDBManager()
    try:
        df = conn.fetch_df('''
            SELECT 
                sub_biz,
                COUNT(*) as cnt
            FROM fact_qa_event
            WHERE biz_date = CURRENT_DATE
            GROUP BY 1
        ''')
        
        return dict(zip(df['sub_biz'], df['cnt']))
    except Exception:
        return {}


def import_file(file_path: Path) -> int:
    """导入单个文件"""
    result = subprocess.run(
        ['.venv/bin/python', 'jobs/import_fact_data.py', '--qa-file', str(file_path)],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise Exception(result.stderr)
    
    # 解析输出获取导入行数
    for line in result.stdout.split('\n'):
        if 'inserted' in line.lower() or '导入' in line:
            import re
            match = re.search(r'(\d+)', line)
            if match:
                return int(match.group(1))
    
    return 0


def main():
    parser = argparse.ArgumentParser(description='每日数据同步脚本')
    parser.add_argument('--dry-run', action='store_true', help='仅扫描，不导入')
    parser.add_argument('--days', type=int, default=7, help='扫描最近 N 天的文件（默认 7 天）')
    args = parser.parse_args()
    
    print("=" * 60)
    print("📊 每日数据同步")
    print("=" * 60)
    
    # 1. 扫描新文件
    print(f"\n🔍 扫描最近 {args.days} 天的文件...")
    new_files = scan_new_files(args.days)
    
    if not new_files:
        print("✅ 没有发现新文件")
        return
    
    print(f"\n发现 {len(new_files)} 个目标文件：")
    for file_path, file_date in new_files:
        dedup = "✓ 已导入" if check_file_dedup(file_path) else "○ 待导入"
        print(f"  {dedup} {file_path.name} ({file_date})")
    
    if args.dry_run:
        print("\n🔍 Dry-run 模式，跳过导入")
        return
    
    # 2. 导入文件
    print(f"\n📥 开始导入...")
    imported = 0
    skipped = 0
    total_rows = 0
    
    for file_path, file_date in new_files:
        if check_file_dedup(file_path):
            print(f"  ⊘ 跳过 {file_path.name}（已导入）")
            skipped += 1
            continue
        
        try:
            rows = import_file(file_path)
            print(f"  ✓ {file_path.name}: {rows:,} 条")
            imported += 1
            total_rows += rows
        except Exception as e:
            print(f"  ✗ {file_path.name}: {e}")
    
    print(f"\n导入完成: {imported} 个文件，{total_rows:,} 条数据")
    
    # 3. 刷新数仓
    if imported > 0:
        print("\n🔄 刷新数仓...")
        result = subprocess.run(
            ['.venv/bin/python', 'jobs/refresh_warehouse.py'],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True
        )
        if result.returncode == 0:
            print("✅ 数仓刷新完成")
        else:
            print(f"❌ 数仓刷新失败: {result.stderr}")
    
    # 4. 异常检测
    print("\n🔍 异常检测...")
    
    # 获取今日数据
    today_data = get_today_data()
    
    if today_data:
        print(f"今日数据: {today_data}")
        
        # 分析历史规律
        historical_stats = analyze_historical_pattern()
        
        if historical_stats:
            print(f"历史均值: {historical_stats['daily_avg']:,.0f} 条")
            
            # 检查异常
            anomalies = check_anomalies(today_data, historical_stats)
            
            if anomalies:
                print(f"\n⚠️ 发现 {len(anomalies)} 个异常：")
                for a in anomalies:
                    print(f"  [{a['severity']}] {a['type']}: {a['message']}")
                
                # 发送企微通知
                today_lines = [f'- {k}: {v:,} 条' for k, v in today_data.items()]
                anomaly_lines = [f'- [{a["severity"]}] {a["message"]}' for a in anomalies]
                
                alert_msg = f"""📊 **数据同步异常告警**

**日期**: {date.today()}

**今日数据**: {sum(today_data.values()):,} 条
{chr(10).join(today_lines)}

**历史均值**: {historical_stats['daily_avg']:,.0f} 条

**异常详情**:
{chr(10).join(anomaly_lines)}

请检查数据源是否正常。
"""
                send_alert_to_wework(alert_msg)
            else:
                print("✅ 未发现异常")
        else:
            print("⚠️ 历史数据不足，无法进行异常检测")
    else:
        print("⚠️ 今日暂无数据")
    
    print("\n" + "=" * 60)
    print("✅ 同步完成")


if __name__ == '__main__':
    main()
