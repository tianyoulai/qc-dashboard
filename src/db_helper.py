"""
QC Dashboard — 数据库连接助手（明文 SQLite）
======================================================
所有脚本和看板页面都通过此模块连接数据库。

用法:
    from src.db_helper import get_db
    conn = get_db()
"""

import os
import sqlite3
from pathlib import Path

# 项目根目录（兼容不同调用路径）
_PROJECT_ROOT = Path(__file__).parent.parent
_DB_FILE = _PROJECT_ROOT / "data" / "metrics.db"


def get_db(db_path=None):
    """
    获取数据库连接（明文 SQLite）。

    Args:
        db_path: 可选自定义数据库路径，默认 data/metrics.db

    Returns:
        sqlite3 connection 对象
    """
    path = db_path or str(_DB_FILE)
    conn = sqlite3.connect(path)
    return conn


# 兼容旧代码的别名
get_db_connection = get_db
DB_FILE = _DB_FILE
