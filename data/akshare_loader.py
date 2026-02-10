"""
AKShare 实时数据适配器
用于从 AKShare 获取真实行情数据替换本地数据库
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, List
import pandas as pd


@dataclass
class AkshareDataLoader:
    """AKShare 数据加载器"""
    
    adjust: str = "qfq"  # qfq=前复权, hfq=后复权, 不复权=None
    
    def _convert_symbol(self, vt_symbol: str) -> str:
        """转换 vt_symbol 为 AKShare 格式"""
        if "." not in vt_symbol:
            return vt_symbol
        
        symbol, exchange = vt_symbol.split(".", 1)
        symbol = symbol.strip()
        exchange = exchange.strip().upper()
        
        # 转换为 AKShare 格式
        if exchange in ['SSE', 'SH']:
            return f"{symbol}.sh"
        elif exchange in ['SZSE', 'SZ', 'SZE']:
            return f"{symbol}.sz"
        else:
            return symbol
    
    def load_symbol(
        self,
        vt_symbol: str,
        start: Optional[str] = None,
        end: Optional[str] = None,
        interval: str = "1d",
    ) -> pd.DataFrame:
        """
        从 AKShare 加载数据
        
        Args:
            vt_symbol: 如 "600519.SSE"
            start: 开始日期 "2020-01-01"
            end: 结束日期 "2024-12-31"
            interval: 时间周期，目前只支持 "1d"
        """
        try:
            import akshare as ak
        except ImportError:
            raise ImportError("请先安装 akshare: pip install akshare")
        
        symbol = self._convert_symbol(vt_symbol)
        
        # 默认日期范围
        if not start:
            start = (datetime.now() - timedelta(days=365*5)).strftime("%Y%m%d")
        else:
            start = start.replace("-", "")
        
        if not end:
            end = datetime.now().strftime("%Y%m%d")
        else:
            end = end.replace("-", "")
        
        print(f"从 AKShare 加载 {vt_symbol} ({symbol}) 数据...")
        
        try:
            # 使用 AKShare 获取历史数据
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=start,
                end_date=end,
                adjust=self.adjust,
            )
            
            if df.empty:
                print(f"警告: {vt_symbol} 没有返回数据")
                return pd.DataFrame()
            
            # 转换列名
            df = df.rename(columns={
                '日期': 'datetime',
                '开盘': 'open',
                '收盘': 'close',
                '最高': 'high',
                '最低': 'low',
                '成交量': 'volume',
                '成交额': 'turnover',
                '振幅': 'amplitude',
                '涨跌幅': 'change_pct',
                '涨跌额': 'change',
                '换手率': 'turnover_rate',
            })
            
            # 转换数据类型
            df['datetime'] = pd.to_datetime(df['datetime'])
            df['date'] = df['datetime'].dt.date
            
            # 提取 symbol 和 exchange
            df['symbol'] = symbol.split('.')[0]
            df['exchange'] = 'SSE' if symbol.endswith('.sh') else 'SZSE'
            df['vt_symbol'] = df['symbol'] + '.' + df['exchange']
            df['interval'] = interval
            
            # 添加停牌标记（成交量为0）
            df['is_suspended'] = df['volume'] == 0
            
            print(f"成功加载 {len(df)} 条K线数据")
            
            return df
            
        except Exception as e:
            print(f"获取数据失败: {e}")
            return pd.DataFrame()
    
    def get_realtime_quote(self, vt_symbol: str) -> Optional[pd.Series]:
        """获取实时行情（用于模拟盘）"""
        try:
            import akshare as ak
        except ImportError:
            raise ImportError("请先安装 akshare: pip install akshare")
        
        symbol = self._convert_symbol(vt_symbol)
        symbol_code = symbol.split('.')[0]
        
        try:
            # 获取实时行情
            df = ak.stock_zh_a_spot_em()
            
            # 查找对应股票
            stock_row = df[df['代码'] == symbol_code]
            
            if stock_row.empty:
                print(f"未找到 {vt_symbol} 的实时行情")
                return None
            
            return stock_row.iloc[0]
            
        except Exception as e:
            print(f"获取实时行情失败: {e}")
            return None
    
    def get_stock_list(self) -> pd.DataFrame:
        """获取股票列表"""
        try:
            import akshare as ak
            return ak.stock_zh_a_spot_em()
        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return pd.DataFrame()


def test_akshare_loader():
    """测试 AKShare 数据加载器"""
    print("="*50)
    print("AKShare 数据加载器测试")
    print("="*50)
    
    loader = AkshareDataLoader(adjust="qfq")
    
    # 测试加载贵州茅台数据
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
        print(f"  收盘价范围: {df['close'].min():.2f} ~ {df['close'].max():.2f}")
        
        # 测试实时行情
        print(f"\n实时行情测试:")
        quote = loader.get_realtime_quote(vt_symbol)
        if quote is not None:
            print(f"  股票: {quote.get('名称', 'N/A')}")
            print(f"  最新价: {quote.get('最新价', 'N/A')}")
            print(f"  涨跌幅: {quote.get('涨跌幅', 'N/A')}%")
    else:
        print("数据加载失败")


if __name__ == "__main__":
    test_akshare_loader()
