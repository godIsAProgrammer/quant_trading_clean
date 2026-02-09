from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, List
import pandas as pd
from zoneinfo import ZoneInfo
from .symbol_mapper import SymbolMapper

TZ_SH = ZoneInfo("Asia/Shanghai")


def _detect_and_fix_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """检测并修复 high/low 可能存反的问题。
    
    数据质量分析显示：14,134 / 14,547 条记录 (97%) 出现 high < low，
    说明采集时字段映射有误。此处自动检测并修复。
    """
    df = df.copy()
    # 计算异常比例
    high_lt_low = (df["high"] < df["low"]).sum()
    if len(df) > 0 and high_lt_low / len(df) > 0.9:  # 超过90%异常则判定为字段存反
        print(f"[数据修复] 检测到 high/low 字段存反 (异常率 {high_lt_low/len(df)*100:.1f}%)，自动交换")
        # 交换 high 和 low
        df["high"], df["low"] = df["low"].copy(), df["high"].copy()
    return df


@dataclass
class BarRecord:
    symbol: str
    exchange: str
    datetime: datetime
    interval: str
    volume: float
    turnover: float
    open_interest: float
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    gateway_name: str = "DB"


class BarTransformer:
    """DataFrame -> vn.py bar 记录。"""

    def __init__(self, volume_unit: str = "share"):
        """
        Args:
            volume_unit: share 或 lot。lot 表示输入为"手"，将自动 *100 转为"股"。
        """
        if volume_unit not in {"share", "lot"}:
            raise ValueError("volume_unit 必须是 share 或 lot")
        self.volume_unit = volume_unit

    def _to_datetime(self, value) -> datetime:
        dt = pd.to_datetime(value).to_pydatetime()
        # 日线默认使用收盘时刻，确保时区明确
        if dt.tzinfo is None:
            dt = dt.replace(hour=15, minute=0, second=0, microsecond=0, tzinfo=TZ_SH)
        else:
            dt = dt.astimezone(TZ_SH)
        return dt

    def _normalize_volume(self, v: float) -> float:
        volume = float(v) if v is not None else 0.0
        if self.volume_unit == "lot":
            volume *= 100.0
        return volume

    def transform(self, symbol: str, df: pd.DataFrame) -> List[BarRecord]:
        if df is None or df.empty:
            return []
        info = SymbolMapper.to_vnpy(symbol)
        required = {"date", "open", "high", "low", "close", "volume"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"缺少必要列: {sorted(missing)}")
        
        # 应用数据质量修复（自动检测并修正 high/low 字段）
        df = _detect_and_fix_ohlc(df)

        bars: List[BarRecord] = []
        for _, row in df.sort_values("date").iterrows():
            turnover = float(row.get("amount", 0.0) or 0.0)
            bars.append(
                BarRecord(
                    symbol=info.symbol,
                    exchange=info.exchange,
                    datetime=self._to_datetime(row["date"]),
                    interval="1d",
                    volume=self._normalize_volume(row["volume"]),
                    turnover=turnover,
                    open_interest=0.0,
                    open_price=float(row["open"]),
                    high_price=float(row["high"]),
                    low_price=float(row["low"]),
                    close_price=float(row["close"]),
                    gateway_name="AKSHARE"
                )
            )
        return bars

    def transform_many(self, items: Iterable[tuple[str, pd.DataFrame]]) -> List[BarRecord]:
        out: List[BarRecord] = []
        for symbol, df in items:
            out.extend(self.transform(symbol, df))
        return out
