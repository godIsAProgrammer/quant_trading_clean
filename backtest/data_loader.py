from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sqlite3
from typing import Dict, Iterable, List, Optional, Sequence

import pandas as pd


@dataclass(frozen=True)
class VnpyBarDataLoader:
    """读取 vn.py SQLite(dbbardata) 日线数据。"""

    db_path: str = "vnpy_data.db"

    def _connect(self) -> sqlite3.Connection:
        path = Path(self.db_path).expanduser().resolve()
        if not path.exists():
            raise FileNotFoundError(f"vn.py 数据库不存在: {path}")
        return sqlite3.connect(str(path))

    def list_symbols(self, interval: str = "1d") -> List[str]:
        sql = """
        SELECT DISTINCT symbol || '.' || exchange AS vt_symbol
        FROM dbbardata
        WHERE interval = ?
        ORDER BY vt_symbol
        """
        with self._connect() as conn:
            rows = conn.execute(sql, (interval,)).fetchall()
        return [r[0] for r in rows]

    def load_symbol(
        self,
        vt_symbol: str,
        interval: str = "1d",
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        symbol, exchange = self._split_vt_symbol(vt_symbol)

        sql = """
        SELECT
            datetime,
            symbol,
            exchange,
            interval,
            open_price AS open,
            high_price AS high,
            low_price AS low,
            close_price AS close,
            volume,
            turnover
        FROM dbbardata
        WHERE symbol = ?
          AND exchange = ?
          AND interval = ?
        """
        params: List[str] = [symbol, exchange, interval]

        if start:
            sql += " AND datetime >= ?"
            params.append(f"{start} 00:00:00")
        if end:
            sql += " AND datetime <= ?"
            params.append(f"{end} 23:59:59")

        sql += " ORDER BY datetime"

        with self._connect() as conn:
            df = pd.read_sql_query(sql, conn, params=params)

        if df.empty:
            return df

        df["datetime"] = pd.to_datetime(df["datetime"])
        df["date"] = df["datetime"].dt.date
        df["vt_symbol"] = df["symbol"] + "." + df["exchange"]
        return df

    def load_many(
        self,
        vt_symbols: Sequence[str],
        interval: str = "1d",
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        out: Dict[str, pd.DataFrame] = {}
        for vt_symbol in vt_symbols:
            out[vt_symbol] = self.load_symbol(
                vt_symbol=vt_symbol,
                interval=interval,
                start=start,
                end=end,
            )
        return out

    @staticmethod
    def _split_vt_symbol(vt_symbol: str) -> tuple[str, str]:
        if "." not in vt_symbol:
            raise ValueError(f"vt_symbol 格式错误，期望如 600519.SSE，实际: {vt_symbol}")
        symbol, exchange = vt_symbol.split(".", 1)
        symbol, exchange = symbol.strip(), exchange.strip().upper()
        if not symbol or not exchange:
            raise ValueError(f"vt_symbol 格式错误: {vt_symbol}")
        return symbol, exchange


def validate_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """返回每行 OHLC 是否通过校验的结果。"""
    if df.empty:
        return pd.DataFrame(columns=["datetime", "vt_symbol", "ok", "reason"])

    out = pd.DataFrame(index=df.index)
    out["datetime"] = df["datetime"]
    out["vt_symbol"] = df["vt_symbol"]

    high_ge_low = df["high"] >= df["low"]
    open_in_range = (df["open"] >= df["low"]) & (df["open"] <= df["high"])
    close_in_range = (df["close"] >= df["low"]) & (df["close"] <= df["high"])

    out["ok"] = high_ge_low & open_in_range & close_in_range
    out["reason"] = ""
    out.loc[~high_ge_low, "reason"] += "high<low;"
    out.loc[~open_in_range, "reason"] += "open_out_of_range;"
    out.loc[~close_in_range, "reason"] += "close_out_of_range;"
    return out
