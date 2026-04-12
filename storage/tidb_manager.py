"""TiDB 连接管理器：基于 mysql-connector-python 的连接池与基础操作。

配置读取优先级：st.secrets → 环境变量 → 本地 settings.json
  - Streamlit Cloud 部署时，凭据存储在 st.secrets 中
  - 本地开发时，从 config/settings.json 读取（文件不提交 Git）
"""
from __future__ import annotations

import json
import os
import ssl
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generator, Iterable

import mysql.connector
from mysql.connector import pooling
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _get_secret(key: str, default: str | None = None) -> str | None:
    """从 st.secrets / 环境变量 / settings.json 三级读取配置值。
    
    支持嵌套格式：key="tidb.host" → st.secrets["tidb"]["host"] 或 env["tidb.host"]
    """
    # 优先级 1: Streamlit Cloud Secrets（支持嵌套字典）
    try:
        import streamlit as st
        if hasattr(st, "secrets"):
            # 尝试嵌套访问：st.secrets["tidb"]["host"]
            if "." in key:
                parts = key.split(".")
                val = st.secrets
                for p in parts:
                    # Secrets 对象不支持 isinstance(val, dict)，用 try-except 更安全
                    try:
                        val = val[p]
                    except (KeyError, TypeError):
                        val = None
                        break
                if val is not None:
                    return str(val)
            # 尝试扁平访问：st.secrets["tidb.host"]
            elif key in st.secrets:
                return str(st.secrets[key])
    except Exception:
        pass
    
    # 优先级 2: 环境变量（扁平格式）
    env_val = os.environ.get(key)
    if env_val is not None:
        return env_val
    
    # 优先级 3: 本地 settings.json（嵌套格式）
    settings_path = PROJECT_ROOT / "config" / "settings.json"
    if settings_path.exists():
        with open(settings_path, "r", encoding="utf-8") as f:
            settings = json.load(f)
        if "." in key:
            parts = key.split(".")
            val = settings
            for p in parts:
                if isinstance(val, dict):
                    val = val.get(p)
                else:
                    val = None
                    break
            if val is not None:
                return str(val)
        elif key in settings:
            return str(settings[key])
    return default


@dataclass
class TiDBConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    charset: str = "utf8mb4"
    pool_name: str = "tidb_pool"
    pool_size: int = 3  # 降低连接数，适配 TiDB Serverless 免费版限制

    @classmethod
    def from_settings(cls) -> "TiDBConfig":
        """从 st.secrets / 环境变量 / settings.json 读取 TiDB 配置。"""
        return cls(
            host=_get_secret("tidb.host", ""),
            port=int(_get_secret("tidb.port", "4000")),
            user=_get_secret("tidb.user", ""),
            password=_get_secret("tidb.password", ""),
            database=_get_secret("tidb.database") or _get_secret("tidb.db", "test"),
            charset=_get_secret("tidb.charset", "utf8mb4"),
        )


def _create_pool(config: TiDBConfig) -> pooling.MySQLConnectionPool:
    """创建 TiDB 连接池。TiDB Serverless 需要 SSL。"""
    return pooling.MySQLConnectionPool(
        pool_name=config.pool_name,
        pool_size=config.pool_size,
        autocommit=True,
        host=config.host,
        port=config.port,
        user=config.user,
        password=config.password,
        database=config.database,
        charset=config.charset,
        ssl_ca=None,
        ssl_verify_cert=False,
        connect_timeout=10,
        connection_timeout=30,
        pool_reset_session=True,
    )


class TiDBManager:
    """TiDB 数据库操作封装，提供连接池、查询、执行等基础方法。
    
    连接池延迟初始化：只有在首次使用时才创建，确保 st.secrets 已就绪。
    """

    def __init__(self, config: TiDBConfig | None = None):
        self.config = config
        self._pool: pooling.MySQLConnectionPool | None = None

    def _ensure_pool(self) -> pooling.MySQLConnectionPool:
        """延迟初始化连接池。"""
        if self._pool is None:
            config = self.config or TiDBConfig.from_settings()
            if not config.host or not config.user or not config.password:
                raise ValueError(f"TiDB 配置不完整: host={config.host}, user={config.user}, "
                                 f"password={'已设置' if config.password else '未设置'}")
            self.config = config  # 回写，确保 table_exists 等方法能读取
            self._pool = _create_pool(config)
        return self._pool

    @contextmanager
    def get_connection(self) -> Generator[mysql.connector.MySQLConnection, None, None]:
        """从连接池获取一个连接，使用后自动归还。"""
        pool = self._ensure_pool()
        conn = pool.get_connection()
        try:
            yield conn
        finally:
            conn.close()

    def fetch_df(self, sql: str, params: Iterable[Any] | None = None) -> pd.DataFrame:
        """执行 SQL 并返回 DataFrame。
        
        安全防护：对无 LIMIT 的纯 SELECT（非聚合/非 GROUP BY）自动追加上限，
        防止全表扫描 122 万行数据。
        """
        safe_sql = sql.strip().rstrip(";")
        upper = safe_sql.upper()
        # 只对无 LIMIT 的纯行查询追加限制，排除聚合/GROUP BY/子查询 COUNT
        if (upper.startswith("SELECT") and " LIMIT " not in upper
                and "OFFSET" not in upper
                and "GROUP BY" not in upper
                and "COUNT(" not in upper
                and "SUM(" not in upper
                and "AVG(" not in upper
                and "MAX(" not in upper
                and "MIN(" not in upper):
            safe_sql += " LIMIT 50000"
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(safe_sql, tuple(params) if params else ())
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                rows = cursor.fetchall()
                return pd.DataFrame(rows, columns=columns) if columns else pd.DataFrame()
            finally:
                cursor.close()

    def fetch_one(self, sql: str, params: Iterable[Any] | None = None) -> dict[str, Any] | None:
        """执行 SQL 并返回第一行。直接使用 fetchone，不加 LIMIT。"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, tuple(params) if params else ())
                row = cursor.fetchone()
                if row is None:
                    return None
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                return dict(zip(columns, row)) if columns else None
            finally:
                cursor.close()

    def execute(self, sql: str, params: Iterable[Any] | None = None) -> None:
        """执行 SQL（INSERT/UPDATE/DELETE 等）。"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql, tuple(params) if params else ())
            finally:
                cursor.close()

    def execute_many(self, sql: str, params_list: list[Iterable[Any]]) -> int:
        """批量执行同一条 SQL（executemany）。返回影响的行数。"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.executemany(sql, [tuple(p) for p in params_list])
                return cursor.rowcount
            finally:
                cursor.close()

    def execute_in_transaction(self, sql_list: list[tuple[str, Iterable[Any] | None]]) -> None:
        """在事务中批量执行 SQL，失败时自动回滚。"""
        with self.get_connection() as conn:
            conn.autocommit = False
            cursor = conn.cursor()
            try:
                for sql, params in sql_list:
                    cursor.execute(sql, tuple(params) if params else ())
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.autocommit = True
                cursor.close()

    def insert_dataframe(self, table_name: str, df: pd.DataFrame,
                         batch_size: int = 3000) -> int:
        """将 DataFrame 分批插入到指定表。返回插入行数。"""
        if df.empty:
            return 0
        columns = list(df.columns)
        placeholders = ", ".join(["%s"] * len(columns))
        col_sql = ", ".join(f"`{c}`" for c in columns)
        sql = f"INSERT INTO `{table_name}` ({col_sql}) VALUES ({placeholders})"

        # 将 DataFrame 转换为元组列表，处理 NaN/NaT
        all_rows = []
        for _, row in df.iterrows():
            converted = []
            for val in row:
                if pd.isna(val):
                    converted.append(None)
                elif hasattr(val, "to_pydatetime"):
                    converted.append(val.to_pydatetime())
                else:
                    converted.append(val)
            all_rows.append(tuple(converted))

        # 分批提交，避免 TiDB Serverless 大事务超时
        total_inserted = 0
        for i in range(0, len(all_rows), batch_size):
            batch = all_rows[i:i + batch_size]
            with self.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.executemany(sql, batch)
                    total_inserted += cursor.rowcount
                finally:
                    cursor.close()
        return total_inserted

    def table_exists(self, table_name: str) -> bool:
        """检查表是否存在。"""
        sql = """
        SELECT COUNT(*) FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """
        result = self.fetch_one(sql, [self.config.database, table_name])
        return bool(result and result.get("COUNT(*)", 0) > 0)
