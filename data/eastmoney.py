"""
东方财富A股日线数据采集器
免费API，无需token
"""
import requests
import pandas as pd
from typing import Optional, List
import time


class EastMoneyCollector:
    """东方财富数据采集器"""
    
    def __init__(self, delay: float = 0.3):
        """
        初始化
        
        Args:
            delay: 请求间隔（秒），避免触发频率限制
        """
        self.delay = delay
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        })
    
    def _get_secid(self, symbol: str) -> str:
        """获取东方财富的secid"""
        if symbol.startswith('6'):
            return f'1.{symbol}'  # 上海
        elif symbol.startswith('0') or symbol.startswith('3'):
            return f'0.{symbol}'  # 深圳
        else:
            raise ValueError(f'无效的股票代码: {symbol}')
    
    def get_daily_data(
        self, 
        symbol: str, 
        start_date: str = '20190101', 
        end_date: str = '20241231',
        adjust: str = 'qfq'  # qfq=前复权, hfq=后复权, ''=不复权
    ) -> Optional[pd.DataFrame]:
        """
        获取单只股票的日线数据
        
        Args:
            symbol: 股票代码 (如: '600519', '000001')
            start_date: 开始日期 (YYYYMMDD)
            end_date: 结束日期 (YYYYMMDD)
            adjust: 复权类型 ('qfq'=前复权, 'hfq'=后复权, ''=不复权)
        
        Returns:
            DataFrame或None
        """
        secid = self._get_secid(symbol)
        
        # 复权参数
        fqt_map = {'qfq': '1', 'hfq': '2', '': '0'}
        fqt = fqt_map.get(adjust, '1')
        
        url = 'http://push2his.eastmoney.com/api/qt/stock/kline/get'
        params = {
            'fields1': 'f1,f2,f3,f4,f5,f6',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
            'klt': '101',  # 日K
            'fqt': fqt,
            'secid': secid,
            'beg': start_date,
            'end': end_date,
            'lmt': '1000'
        }
        
        try:
            time.sleep(self.delay)
            response = self.session.get(url, params=params, timeout=10)
            data = response.json()
            
            if data['data']['klines']:
                klines = data['data']['klines']
                records = []
                for kline in klines:
                    parts = kline.split(',')
                    records.append({
                        'symbol': symbol,
                        'date': parts[0],
                        'open': float(parts[1]),
                        'high': float(parts[2]),
                        'low': float(parts[3]),
                        'close': float(parts[4]),
                        'volume': float(parts[5]),
                        'amount': float(parts[6])
                    })
                return pd.DataFrame(records)
            else:
                return None
                
        except Exception as e:
            print(f'获取{symbol}数据失败: {e}')
            return None
    
    def get_all_stocks(self) -> List[str]:
        """获取所有A股股票列表（简化的方法）"""
        # 这里返回常用股票列表作为演示
        # 实际使用时可以从东方财富获取完整列表
        return [
            '600519',  # 贵州茅台
            '000001',  # 平安银行
            '600036',  # 招商银行
            '000858',  # 五粮液
            '300750',  # 宁德时代
            '600276',  # 恒瑞医药
            '600887',  # 伊利股份
            '600030',  # 中信证券
            '601318',  # 中国平安
            '600016',  # 民生银行
        ]
    
    def collect_all(
        self, 
        start_date: str = '20190101', 
        end_date: str = '20241231',
        symbols: Optional[List[str]] = None
    ) -> dict:
        """
        采集多只股票数据
        
        Returns:
            dict: {symbol: DataFrame}
        """
        if symbols is None:
            symbols = self.get_all_stocks()
        
        results = {}
        for symbol in symbols:
            print(f'下载 {symbol}...', end=' ')
            df = self.get_daily_data(symbol, start_date, end_date)
            if df is not None and len(df) > 0:
                results[symbol] = df
                print(f'✅ {len(df)} 条')
            else:
                print('❌ 失败')
        
        return results


if __name__ == '__main__':
    # 测试
    collector = EastMoneyCollector(delay=0.3)
    
    # 获取单只股票
    print('测试获取贵州茅台...')
    df = collector.get_daily_data('600519', '20240101', '20240131')
    if df is not None:
        print(f'✅ 获取 {len(df)} 条数据')
        print(df.head())
    else:
        print('❌ 获取失败')
