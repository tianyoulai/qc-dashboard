"""
QC Dashboard — 数据库连接助手（统一加密/明文切换）
======================================================
所有脚本和看板页面都通过此模块连接数据库，
自动处理 SQLCipher 加密（密码从环境变量 QC_DB_PASSWORD 读取）。

用法:
    from src.db_helper import get_db
    conn = get_db()
"""

import os
import sys
from pathlib import Path

# 项目根目录（兼容不同调用路径）
_PROJECT_ROOT = Path(__file__).parent.parent
_DB_FILE = _PROJECT_ROOT / "data" / "metrics.db"

# SQLCipher 支持（回退到明文 sqlite3）
try:
    from sqlcipher3 import dbapi2 as _sqlite
    _USE_CIPHER = True
except ImportError:
    import sqlite3 as _sqlite
    _USE_CIPHER = False


def _get_password():
    """获取数据库加密密码"""
    pwd = os.environ.get('QC_DB_PASSWORD') or os.environ.get('DB_PASSWORD')
    if pwd:
        return pwd
    # 尝试 .env 文件
    env_file = _PROJECT_ROOT / '.env'
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, _, val = line.partition('=')
                if key.strip() in ('QC_DB_PASSWORD', 'DB_PASSWORD'):
                    return val.strip('"').strip("'")
    return None


def get_db(db_path=None):
    """
    获取数据库连接。
    
    - 自动尝试 SQLCipher 加密（如果设置了 QC_DB_PASSWORD 环境变量）
    - 未设置密码时以明文模式打开（向后兼容）
    - 密码错误时抛出 RuntimeError
    
    Args:
        db_path: 可选自定义数据库路径，默认 data/metrics.db
    
    Returns:
        sqlite connection 对象
    """
    path = db_path or str(_DB_FILE)
    conn = _sqlite.connect(path)

    password = _get_password()
    if password and _USE_CIPHER:
        conn.execute(f"PRAGMA key = '{password}'")
        # 验证密码是否正确
        try:
            conn.execute("SELECT count(*) FROM sqlite_master")
        except _sqlite.DatabaseError as e:
            err_msg = str(e).lower()
            if "file is not a database" in err_msg or "decryption" in err_msg:
                raise RuntimeError(
                    f"\n❌ 数据库密码错误或文件未加密！\n"
                    f"   请设置正确的环境变量: export QC_DB_PASSWORD='你的密码'\n"
                    f"   或在项目根目录创建 .env 文件写入: QC_DB_PASSWORD=你的密码\n"
                ) from e
            raise

    return conn


# 兼容旧代码的别名
get_db_connection = get_db
DB_FILE = _DB_FILE
