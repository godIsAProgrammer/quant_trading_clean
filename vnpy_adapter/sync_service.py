from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text

from .bar_transformer import BarTransformer
from .database_writer import VnpySQLiteWriter
from .symbol_mapper import SymbolMapper

logger = logging.getLogger(__name__)


@dataclass
class SyncResult:
    symbol: str
    source_rows: int
    written_rows: int
    mode: str  # full / incremental


class SyncService:
    """akshare/quant_trading SQLite -> vn.py SQLite 同步服务。"""

    def __init__(
        self,
        source_db_path: str,
        target_db_path: str,
        volume_unit: str = "share",
    ):
        self.source_db_path = str(Path(source_db_path).expanduser())
        self.target_db_path = str(Path(target_db_path).expanduser())
        self.source_engine = create_engine(f"sqlite:///{self.source_db_path}", echo=False)
        self.transformer = BarTransformer(volume_unit=volume_unit)
        self.writer = VnpySQLiteWriter(self.target_db_path)

    def list_source_symbols(self) -> List[str]:
        with self.source_engine.connect() as conn:
            rows = conn.execute(text("SELECT DISTINCT symbol FROM daily_data ORDER BY symbol")).fetchall()
        return [r[0] for r in rows]

    def fetch_source_data(self, symbol: str, start_date: Optional[str] = None) -> pd.DataFrame:
        sql = """
            SELECT symbol, date, open, high, low, close, volume, amount, turnover_rate
            FROM daily_data
            WHERE symbol = :symbol
        """
        params = {"symbol": SymbolMapper.to_akshare(symbol)}
        if start_date:
            sql += " AND date >= :start_date"
            params["start_date"] = start_date
        sql += " ORDER BY date"
        with self.source_engine.connect() as conn:
            df = pd.read_sql(text(sql), conn, params=params)
        return df

    def sync_symbol(self, symbol: str, incremental: bool = True) -> SyncResult:
        info = SymbolMapper.to_vnpy(symbol)
        mode = "incremental" if incremental else "full"

        start_date = None
        if incremental:
            latest = self.writer.get_latest_date(info.symbol, info.exchange, interval="1d")
            if latest:
                start_date = latest

        df = self.fetch_source_data(symbol, start_date=start_date)
        if df.empty:
            return SyncResult(symbol=info.symbol, source_rows=0, written_rows=0, mode=mode)

        # 增量时去掉“已存在的最新日期”避免重复转换（DB 层也有去重）
        if incremental and start_date:
            df = df[df["date"] > start_date]

        bars = self.transformer.transform(symbol, df)
        written = self.writer.upsert_bars(bars)
        return SyncResult(symbol=info.symbol, source_rows=len(df), written_rows=written, mode=mode)

    def sync(self, symbols: Optional[List[str]] = None, incremental: bool = True, limit: Optional[int] = None) -> Dict[str, SyncResult]:
        if symbols is None:
            symbols = self.list_source_symbols()
        if limit:
            symbols = symbols[:limit]

        result: Dict[str, SyncResult] = {}
        for i, s in enumerate(symbols, 1):
            r = self.sync_symbol(s, incremental=incremental)
            result[s] = r
            logger.info("[%s/%s] %s mode=%s source=%s written=%s", i, len(symbols), s, r.mode, r.source_rows, r.written_rows)
        return result
