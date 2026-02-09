from __future__ import annotations

import sqlite3
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from .bar_transformer import BarRecord


class VnpySQLiteWriter:
    """写入 vn.py SQLite 格式（dbbardata）。"""

    def __init__(self, db_path: str):
        self.db_path = str(Path(db_path).expanduser())
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_schema(self):
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS dbbardata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    exchange TEXT NOT NULL,
                    datetime TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    volume REAL NOT NULL,
                    turnover REAL NOT NULL,
                    open_interest REAL NOT NULL,
                    open_price REAL NOT NULL,
                    high_price REAL NOT NULL,
                    low_price REAL NOT NULL,
                    close_price REAL NOT NULL,
                    gateway_name TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_dbbardata_unique
                ON dbbardata(symbol, exchange, interval, datetime)
                """
            )
            conn.commit()

    @staticmethod
    def _dt_to_str(dt: datetime) -> str:
        # vn.py sqlite 常用朴素 datetime 存储格式
        if dt.tzinfo:
            dt = dt.astimezone().replace(tzinfo=None)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def upsert_bars(self, bars: Iterable[BarRecord]) -> int:
        rows = [
            (
                b.symbol,
                b.exchange,
                self._dt_to_str(b.datetime),
                b.interval,
                b.volume,
                b.turnover,
                b.open_interest,
                b.open_price,
                b.high_price,
                b.low_price,
                b.close_price,
                b.gateway_name,
            )
            for b in bars
        ]
        if not rows:
            return 0

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO dbbardata
                (symbol, exchange, datetime, interval, volume, turnover, open_interest,
                 open_price, high_price, low_price, close_price, gateway_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()
        return len(rows)

    def get_latest_date(self, symbol: str, exchange: str, interval: str = "1d") -> Optional[str]:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(datetime) FROM dbbardata
                WHERE symbol = ? AND exchange = ? AND interval = ?
                """,
                (symbol, exchange, interval),
            ).fetchone()
        if row and row[0]:
            return row[0][:10]
        return None

    def get_count(self, symbol: str, exchange: str, interval: str = "1d") -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM dbbardata WHERE symbol=? AND exchange=? AND interval=?",
                (symbol, exchange, interval),
            ).fetchone()
        return int(row[0] if row else 0)
