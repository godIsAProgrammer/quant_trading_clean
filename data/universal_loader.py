"""
通用数据加载器 - 支持本地数据库或 AKShare
"""
from __future__ import annotations

from typing import Optional
import pandas as pd
import os


class UniversalDataLoader:
    """
    通用数据加载器
    优先使用本地数据库，如不存在则尝试 AKShare
    """
    
    def __init__(self, db_path: str = "vnpy_data.db"):
        self.db_path = db_path
        self._vnpy_loader = None
        self._akshare_available = self._check_akshare()
    
    def _check_akshare(self) -> bool:
        """检查 AKShare 是否可用"""
        try:
            import akshare as ak
            return True
        except ImportError:
            return False
    
    def load_symbol(
        self,
        vt_symbol: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        interval: str = "1d",
        prefer_akshare: bool = False,  # 是否优先使用 AKShare
    ) -> pd.DataFrame:
        """
        加载股票数据
        
        Args:
            vt_symbol: 如 "600519.SSE"
            start: 开始日期
            end: 结束日期
            interval: 时间周期
            prefer_akshare: 是否优先使用 AKShare（默认使用本地数据库）
        """
        # 如果优先使用 AKShare 且 AKShare 可用
        if prefer_akshare and self._akshare_available:
            return self._load_from_akshare(vt_symbol, start, end, interval)
        
        # 否则先尝试本地数据库
        if os.path.exists(self.db_path):
            return self._load_from_local(vt_symbol, start, end, interval)
        
        # 本地没有则尝试 AKShare
        if self._akshare_available:
            print(f"本地数据库不存在，尝试从 AKShare 获取...")
            return self._load_from_akshare(vt_symbol, start, end, interval)
        
        # 都没有则报错
        raise FileNotFoundError(
            f"无法加载数据: 本地数据库 {self.db_path} 不存在，"
            f"且 AKShare 未安装。\n"
            f"请安装 AKShare: pip install akshare"
        )
    
    def _load_from_local(
        self,
        vt_symbol: str,
        start: Optional[str],
        end: Optional[str],
        interval: str,
    ) -> pd.DataFrame:
        """从本地数据库加载"""
        from backtest.data_loader import VnpyBarDataLoader
        
        if self._vnpy_loader is None:
            self._vnpy_loader = VnpyBarDataLoader(db_path=self.db_path)
        
        return self._vnpy_loader.load_symbol(vt_symbol, interval, start, end)
    
    def _load_from_akshare(
        self,
        vt_symbol: str,
        start: Optional[str],
        end: Optional[str],
        interval: str,
    ) -> pd.DataFrame:
        """从 AKShare 加载"""
        from data.akshare_loader import AkshareDataLoader
        
        loader = AkshareDataLoader(adjust="qfq")
        return loader.load_symbol(vt_symbol, start, end, interval)


def test_universal_loader():
    """测试通用数据加载器"""
    print("="*50)
    print("通用数据加载器测试")
    print("="*50)
    
    loader = UniversalDataLoader(db_path="vnpy_data.db")
    
    # 测试加载数据
    vt_symbol = "600519.SSE"
    df = loader.load_symbol(
        vt_symbol=vt_symbol,
        start="2024-01-01",
        end="2024-12-31",
    )
    
    if not df.empty:
        print(f"\n数据预览 ({vt_symbol}):")
        print(df.head())
        print(f"\n数据统计:")
        print(f"  数据条数: {len(df)}")
        print(f"  日期范围: {df['datetime'].min()} ~ {df['datetime'].max()}")
    else:
        print("数据加载失败")


if __name__ == "__main__":
    test_universal_loader()
