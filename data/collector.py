"""
数据采集模块
使用 akshare 获取 A 股日线数据
"""
import time
import logging
from typing import List, Optional, Dict
from datetime import datetime, timedelta

import akshare as ak
import pandas as pd

from .storage import StockStorage
from .models import DailyData

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockCollector:
    """股票数据采集器"""
    
    # A股市场前缀映射
    MARKET_PREFIX = {
        'sh': '沪市',
        'sz': '深市'
    }
    
    def __init__(
        self, 
        storage: StockStorage,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        delay: float = 0.5
    ):
        """
        初始化采集器实例
        
        Args:
            storage: StockStorage 实例
            start_date: 可选，起始日期 (格式: YYYY-MM-DD)
            end_date: 可选，结束日期 (格式: YYYY-MM-DD)
            delay: 请求间隔时间（秒）
        """
        self.storage = storage
        self.start_date = start_date
        self.end_date = end_date or datetime.now().strftime('%Y-%m-%d')
        self.delay = delay
        
        if start_date and end_date:
            logger.info(f"设置日期范围: {start_date} 至 {end_date}")
    
    def get_stock_list(self) -> List[str]:
        """
        获取所有A股股票列表
        
        Returns:
            股票代码列表
        """
        logger.info("正在获取A股股票列表...")
        
        try:
            # 使用 akshare 获取沪市和深市股票列表
            stock_info_sh_df = ak.stock_info_a_code_name(symbol="沪市")
            stock_info_sz_df = ak.stock_info_a_code_name(symbol="深市")
            
            # 合并并提取代码
            all_stocks = pd.concat([stock_info_sh_df, stock_info_sz_df], ignore_index=True)
            symbols = all_stocks['code'].tolist()
            
            logger.info(f"共获取 {len(symbols)} 只A股股票")
            return symbols
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            # 备用方案：返回一些常用股票
            return self._get_common_stocks()
    
    def _get_common_stocks(self) -> List[str]:
        """
        获取常用股票列表（备用方案）
        
        Returns:
            常用股票代码列表
        """
        common_stocks = [
            '600519', '000001', '600036', '601398', '601988',
            '601857', '600000', '600015', '600030', '600016',
            '000002', '000009', '000010', '000011', '000012',
            '300001', '300002', '300003', '300004', '300005'
        ]
        logger.warning(f"使用备用股票列表，共 {len(common_stocks)} 只")
        return common_stocks
    
    def _format_date(self, date_str: str) -> str:
        """
        格式化日期字符串
        
        Args:
            date_str: 原始日期字符串
            
        Returns:
            格式化后的日期 (YYYY-MM-DD)
        """
        if pd.isna(date_str):
            return None
        
        try:
            # 处理各种日期格式
            if isinstance(date_str, str):
                if '-' in date_str:
                    return date_str[:10]
                elif '/' in date_str:
                    return date_str.replace('/', '-')[:10]
                elif len(date_str) == 8:  # YYYYMMDD
                    return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            
            # 尝试转换为 datetime
            dt = pd.to_datetime(date_str)
            return dt.strftime('%Y-%m-%d')
            
        except Exception:
            return None
    
    def _process_data(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        处理和清洗数据
        
        Args:
            df: 原始数据 DataFrame
            symbol: 股票代码
            
        Returns:
            处理后的 DataFrame
        """
        if df.empty:
            return df
        
        # 确保必要的列存在
        required_columns = ['日期', '开盘', '最高', '最低', '收盘', '成交量', '成交额']
        available_columns = [col for col in required_columns if col in df.columns]
        
        if not available_columns:
            logger.warning(f"股票 {symbol} 数据缺少必要列")
            return pd.DataFrame()
        
        # 重命名列
        column_mapping = {
            '日期': 'date',
            '开盘': 'open',
            '最高': 'high',
            '最低': 'low',
            '收盘': 'close',
            '成交量': 'volume',
            '成交额': 'amount',
            '换手率': 'turnover_rate'
        }
        
        df = df.rename(columns=column_mapping)
        
        # 选择需要的列
        columns = ['date', 'open', 'high', 'low', 'close', 'volume', 'amount', 'turnover_rate']
        available = [col for col in columns if col in df.columns]
        df = df[available].copy()
        
        # 格式化日期
        df['date'] = df['date'].apply(self._format_date)
        
        # 移除无效数据
        df = df.dropna(subset=['date', 'close'])
        df = df[df['date'].notna()]
        
        # 确保数值列为数值类型
        numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'amount', 'turnover_rate']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def collect_stock(self, symbol: str) -> int:
        """
        采集单个股票的数据
        
        Args:
            symbol: 股票代码
            
        Returns:
            采集的数据条数
        """
        try:
            # 确定市场类型
            if symbol.startswith('6'):
                market = 'sh'
            elif symbol.startswith(['0', '3']):
                market = 'sz'
            else:
                market = 'sh'
            
            # 获取增量数据：检查已存储的最新日期
            latest_date = self.storage.get_latest_date(symbol)
            
            if latest_date:
                # 从最新日期的下一天开始获取
                start = (datetime.strptime(latest_date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            elif self.start_date:
                start = self.start_date
            else:
                start = '2020-01-01'  # 默认起始日期
            
            end = self.end_date
            
            logger.info(f"采集股票 {symbol} ({self.MARKET_PREFIX.get(market, market)}) 数据: {start} 至 {end}")
            
            # 使用 akshare 获取日线数据
            if market == 'sh':
                df = ak.stock_zh_a_hist(symbol=symbol, period="daily", 
                                       start_date=start, end_date=end, adjust="qfq")
            else:
                df = ak.stock_zh_a_hist(symbol=symbol, period="daily",
                                       start_date=start, end_date=end, adjust="qfq")
            
            # 处理数据
            df = self._process_data(df, symbol)
            
            if df.empty:
                logger.info(f"股票 {symbol} 无新数据")
                return 0
            
            # 保存到数据库
            self.storage.save_daily_data(symbol, df)
            
            logger.info(f"股票 {symbol} 采集完成，新增 {len(df)} 条数据")
            return len(df)
            
        except Exception as e:
            logger.error(f"采集股票 {symbol} 失败: {e}")
            return 0
    
    def collect_all_stocks(self, symbols: Optional[List[str]] = None) -> Dict[str, int]:
        """
        采集所有股票的数据（支持增量更新）
        
        Args:
            symbols: 可选，指定股票列表
            
        Returns:
            采集结果字典 {symbol: data_count}
        """
        if symbols is None:
            symbols = self.get_stock_list()
        
        logger.info(f"开始采集 {len(symbols)} 只股票的数据...")
        
        results = {}
        success_count = 0
        fail_count = 0
        
        for i, symbol in enumerate(symbols, 1):
            logger.info(f"进度: {i}/{len(symbols)}")
            
            count = self.collect_stock(symbol)
            
            if count >= 0:
                results[symbol] = count
                success_count += 1
            else:
                results[symbol] = 0
                fail_count += 1
            
            # 避免请求过快
            if i < len(symbols):
                time.sleep(self.delay)
        
        logger.info(f"采集完成! 成功: {success_count}, 失败: {fail_count}")
        return results
    
    def get_stock_info(self, symbol: str) -> Optional[Dict]:
        """
        获取股票基本信息
        
        Args:
            symbol: 股票代码
            
        Returns:
            股票信息字典
        """
        try:
            if symbol.startswith('6'):
                info = ak.stock_info_sh_symbol(symbol=symbol)
            else:
                info = ak.stock_info_sz_symbol(symbol=symbol)
            
            return info.to_dict('records')[0] if not info.empty else None
            
        except Exception as e:
            logger.error(f"获取股票 {symbol} 信息失败: {e}")
            return None
    
    def get_market_overview(self) -> Dict:
        """
        获取市场概况
        
        Returns:
            市场概况字典
        """
        try:
            # 沪市概况
            sh_overview = ak.stock_market_summary_sh()
            # 深市概况  
            sz_overview = ak.stock_market_summary_sz()
            
            return {
                'sh': sh_overview,
                'sz': sz_overview
            }
            
        except Exception as e:
            logger.error(f"获取市场概况失败: {e}")
            return {}


if __name__ == "__main__":
    # 测试代码
    import sys
    
    # 创建存储和采集器
    storage = StockStorage("test_stocks.db")
    collector = StockCollector(storage)
    
    # 采集单只股票测试
    if len(sys.argv) > 1:
        symbol = sys.argv[1]
        collector.collect_stock(symbol)
    else:
        collector.collect_stock("600519")  # 测试贵州茅台
