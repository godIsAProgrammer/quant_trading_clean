"""
数据模型定义
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class DailyData:
    """股票日线数据模型"""
    symbol: str              # 股票代码
    date: str               # 交易日期 (YYYY-MM-DD)
    open: float             # 开盘价
    high: float             # 最高价
    low: float              # 最低价
    close: float            # 收盘价
    volume: float           # 成交量
    amount: float           # 成交额
    turnover_rate: Optional[float] = None  # 换手率
    
    def __repr__(self):
        return f"<DailyData {self.symbol} {self.date}>"
