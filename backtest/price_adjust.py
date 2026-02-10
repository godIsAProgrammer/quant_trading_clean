"""复权处理模块"""
import pandas as pd
from typing import Optional


class AdjustFactorLoader:
    """复权因子加载器 - 从 AKShare 获取复权数据"""
    
    @staticmethod
    def get_adjust_factor(symbol: str, exchange: str) -> pd.DataFrame:
        """
        获取复权因子
        从 AKShare 获取股票的除权除息数据
        """
        try:
            import akshare as ak
            
            # 转换代码格式
            if exchange.upper() in ['SSE', 'SH']:
                stock_code = f"{symbol}.sh"
            elif exchange.upper() in ['SZSE', 'SZ', 'SZE']:
                stock_code = f"{symbol}.sz"
            else:
                stock_code = symbol
            
            # 获取除权除息数据
            df = ak.stock_zh_a_hist(symbol=stock_code, period="daily", 
                                   start_date="20000101", adjust="qfq")
            
            if df.empty:
                return pd.DataFrame()
            
            df['date'] = pd.to_datetime(df['日期'])
            df['close'] = df['收盘']
            df['close_raw'] = df['收盘']
            df['factor'] = df['close'] / df['close_raw']
            df['factor'] = df['factor'].fillna(1.0)
            
            return df[['date', 'factor']]
            
        except ImportError:
            print("警告: 未安装 akshare，使用简单复权因子")
            return pd.DataFrame()
        except Exception as e:
            print(f"获取复权因子失败: {e}")
            return pd.DataFrame()


def adjust_prices(df: pd.DataFrame, adjust_type: str = "backward") -> pd.DataFrame:
    """
    对价格数据进行复权处理
    
    Args:
        df: DataFrame with columns ['datetime', 'open', 'high', 'low', 'close']
        adjust_type: "none", "forward" (前复权), "backward" (后复权)
    
    Returns:
        DataFrame with adjusted prices
    """
    if adjust_type == "none" or df.empty:
        return df
    
    if 'adj_factor' not in df.columns:
        # 如果没有复权因子列，尝试计算简单的复权因子
        # 基于价格连续性检测除权点
        df['adj_factor'] = calculate_simple_adjust_factor(df, adjust_type)
    
    # 应用复权因子
    for col in ['open', 'high', 'low', 'close']:
        if col in df.columns:
            df[f'{col}_original'] = df[col]  # 保存原始价格
            df[col] = df[col] * df['adj_factor']
    
    return df


def calculate_simple_adjust_factor(df: pd.DataFrame, adjust_type: str) -> pd.Series:
    """
    基于价格连续性检测计算简单复权因子
    
    使用价格变化率来检测除权除息导致的跳空
    """
    df = df.copy()
    df = df.sort_values('datetime').reset_index(drop=True)
    
    # 基于收盘价的变化计算复权因子
    df['close_prev'] = df['close'].shift(1)
    df['return'] = df['close'] / df['close_prev'] - 1
    
    # 检测异常价格跳空 (> 30% 或 < -20%)
    jump_threshold = 0.30
    df['is_gap'] = (df['return'].abs() > jump_threshold) | df['return'].isna()
    
    # 计算累积复权因子
    factor = 1.0
    factors = [1.0]
    
    for i in range(1, len(df)):
        if df.loc[i, 'is_gap']:
            # 发现除权点，调整因子
            prev_close = df.loc[i-1, 'close']
            curr_open = df.loc[i, 'open']
            gap_factor = curr_open / prev_close if prev_close > 0 else 1.0
            
            if adjust_type == "backward":
                # 后复权：保持最新价格不变，调整历史价格
                factor *= gap_factor
                factors.append(factor)
            else:  # forward
                # 前复权：保持最早价格不变，调整后续价格
                # 前复权需要从后往前计算
                factors.append(factor)
        else:
            factors.append(factor)
    
    if adjust_type == "forward":
        # 前复权：需要从后向前逆推
        factor = 1.0
        for i in range(len(factors) - 2, -1, -1):
            if df.loc[i+1, 'is_gap']:
                prev_close = df.loc[i, 'close']
                curr_open = df.loc[i+1, 'open']
                gap_factor = curr_open / prev_close if prev_close > 0 else 1.0
                factor /= gap_factor
            factors[i] = factor
    
    return pd.Series(factors, index=df.index)


def should_use_adjusted_price(gap_pct: float) -> bool:
    """
    判断是否复权价格（根据价格跳空幅度）
    
    Args:
        gap_pct: 价格跳空幅度百分比 (-1.0 to +inf)
    
    Returns:
        True 如果应该使用复权价格
    """
    # 价格跳空超过 30% 需要关注
    return abs(gap_pct) > 0.30


class PriceValidator:
    """价格数据验证器"""
    
    @staticmethod
    def check_for_exdiv_gap(df: pd.DataFrame, threshold: float = 0.30) -> pd.DataFrame:
        """
        检查除权除息导致的跳空
        
        Returns:
            DataFrame with gap indicators
        """
        df = df.copy()
        df = df.sort_values('datetime').reset_index(drop=True)
        
        df['prev_close'] = df['close'].shift(1)
        df['overnight_return'] = (df['open'] - df['prev_close']) / df['prev_close']
        df['is_exdiv'] = df['overnight_return'].abs() > threshold
        
        return df
