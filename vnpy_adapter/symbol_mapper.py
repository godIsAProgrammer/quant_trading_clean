from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SymbolInfo:
    """标准化后的股票代码信息。"""

    raw_symbol: str
    symbol: str
    exchange: str  # SSE / SZSE

    @property
    def vt_symbol(self) -> str:
        return f"{self.symbol}.{self.exchange}"


class SymbolMapper:
    """股票代码映射工具。

    支持输入：
    - 600000
    - 600000.SH / 000001.SZ
    - 600000.SSE / 000001.SZSE
    """

    EXCHANGE_ALIAS = {
        "SH": "SSE",
        "SZ": "SZSE",
        "SSE": "SSE",
        "SZSE": "SZSE",
    }

    @classmethod
    def _normalize_symbol(cls, symbol: str) -> str:
        s = symbol.strip().upper()
        if "." in s:
            left, right = s.split(".", 1)
            ex = cls.EXCHANGE_ALIAS.get(right)
            if not ex:
                raise ValueError(f"不支持的交易所后缀: {symbol}")
            return f"{left}.{ex}"
        return s

    @classmethod
    def infer_exchange(cls, symbol: str) -> str:
        code = symbol.strip().split(".", 1)[0]
        if not code.isdigit() or len(code) != 6:
            raise ValueError(f"无效股票代码: {symbol}")

        if code.startswith(("5", "6", "9")):
            return "SSE"
        if code.startswith(("0", "1", "2", "3")):
            return "SZSE"

        raise ValueError(f"无法根据代码推断交易所: {symbol}")

    @classmethod
    def to_vnpy(cls, symbol: str) -> SymbolInfo:
        s = cls._normalize_symbol(symbol)
        if "." in s:
            code, ex = s.split(".", 1)
            return SymbolInfo(raw_symbol=symbol, symbol=code, exchange=ex)
        return SymbolInfo(raw_symbol=symbol, symbol=s, exchange=cls.infer_exchange(s))

    @classmethod
    def to_akshare(cls, symbol: str) -> str:
        """akshare 常用 6 位纯代码。"""
        return cls.to_vnpy(symbol).symbol
