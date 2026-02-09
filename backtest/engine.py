from __future__ import annotations

from datetime import datetime
from typing import Dict, List, Optional
import pandas as pd
import numpy as np

from backtest.strategy_template import (
    CtaTemplate, BarData, Order, Trade, Direction
)
from backtest.data_loader import VnpyBarDataLoader


class BacktestEngine:
    """简化版回测引擎 - 支持日线回测"""
    
    def __init__(self):
        self.data_loader: Optional[VnpyBarDataLoader] = None
        self.strategy: Optional[CtaTemplate] = None
        
        # 回测参数
        self.start_date: Optional[str] = None
        self.end_date: Optional[str] = None
        self.initial_capital: float = 1_000_000.0
        
        # 手续费和滑点
        self.commission_rate: float = 0.0003  # 万分之3
        self.slippage: float = 0.01  # 滑点（价格跳动单位）
        
        # 回测数据
        self.bars: List[BarData] = []
        self.current_idx: int = 0
        
        # 资金和持仓
        self.capital: float = 0.0
        self.positions: Dict[str, int] = {}
        self.trades: List[Trade] = []
        self.orders: List[Order] = []
        
        # 每日记录
        self.daily_results: List[Dict] = []
        
        # 当前日期
        self.current_date: Optional[datetime] = None
    
    def set_parameters(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        initial_capital: float = 1_000_000.0,
        commission_rate: float = 0.0003,
        slippage: float = 0.01,
    ):
        """设置回测参数"""
        self.start_date = start_date
        self.end_date = end_date
        self.initial_capital = initial_capital
        self.commission_rate = commission_rate
        self.slippage = slippage
        self.capital = initial_capital
    
    def add_data(self, db_path: str, vt_symbol: str):
        """添加数据"""
        if self.data_loader is None:
            self.data_loader = VnpyBarDataLoader(db_path=db_path)
        
        df = self.data_loader.load_symbol(
            vt_symbol=vt_symbol,
            start=self.start_date,
            end=self.end_date,
        )
        
        if df.empty:
            print(f"警告: {vt_symbol} 没有数据")
            return
        
        # 转换为BarData
        for _, row in df.iterrows():
            bar = BarData(
                vt_symbol=vt_symbol,
                datetime=row['datetime'],
                open_price=row['open'],
                high_price=row['high'],
                low_price=row['low'],
                close_price=row['close'],
                volume=row['volume'],
                turnover=row.get('turnover', 0),
            )
            self.bars.append(bar)
        
        # 按时间排序
        self.bars.sort(key=lambda x: x.datetime)
        print(f"加载 {vt_symbol} 数据: {len(df)} 条K线")
    
    def add_strategy(self, strategy_class, strategy_name: str, vt_symbol: str, setting: Dict = None):
        """添加策略"""
        self.strategy = strategy_class(
            strategy_name=strategy_name,
            vt_symbol=vt_symbol,
            setting=setting or {},
        )
        
        # 设置回调
        self.strategy.send_order_callback = self._handle_order
        self.strategy.write_log_callback = lambda msg: print(f"[策略日志] {msg}")
        
        print(f"添加策略: {strategy_name} ({strategy_class.__name__})")
    
    def _handle_order(self, order: Order):
        """处理订单"""
        self.orders.append(order)
        
        # 简化处理：市价单立即成交
        if order.status == "pending":
            bar = self.bars[self.current_idx] if self.current_idx < len(self.bars) else None
            if bar is None:
                return
            
            # 使用当前bar的收盘价作为成交价
            trade_price = bar.close_price
            
            # 应用滑点
            if order.direction == Direction.LONG:
                trade_price += self.slippage
            else:
                trade_price -= self.slippage
            
            # 计算手续费
            trade_value = trade_price * order.volume
            commission = trade_value * self.commission_rate
            
            # 创建成交
            trade = Trade(
                vt_symbol=order.vt_symbol,
                direction=order.direction,
                price=trade_price,
                volume=order.volume,
                trade_time=bar.datetime,
                trade_id=f"trade_{len(self.trades)}",
            )
            
            self.trades.append(trade)
            order.status = "filled"
            order.filled_volume = order.volume
            order.filled_price = trade_price
            
            # 更新策略持仓
            self.strategy.on_trade(trade)
            
            # 更新资金
            if order.direction == Direction.LONG:
                self.capital -= (trade_value + commission)
            else:
                self.capital += (trade_value - commission)
            
            print(f"  成交: {order.direction.value} {order.volume} @ {trade_price:.2f}, 手续费: {commission:.2f}")
    
    def run_backtesting(self):
        """运行回测"""
        if not self.strategy:
            print("错误: 未设置策略")
            return
        
        if not self.bars:
            print("错误: 没有加载数据")
            return
        
        print(f"\n开始回测...")
        print(f"资金: {self.initial_capital:,.2f}")
        print(f"K线数量: {len(self.bars)}")
        
        # 初始化策略
        self.strategy.inited = True
        self.strategy.on_init()
        
        # 启动策略
        self.strategy.trading = True
        self.strategy.on_start()
        
        # 遍历K线
        for i, bar in enumerate(self.bars):
            self.current_idx = i
            self.current_date = bar.datetime
            self.strategy.bar = bar
            self.strategy.bars.append(bar)
            
            # 策略处理
            self.strategy.on_bar(bar)
            
            # 记录每日结果
            if i == len(self.bars) - 1 or self.bars[i+1].datetime.date() != bar.datetime.date():
                self._record_daily_result(bar)
        
        # 停止策略
        self.strategy.trading = False
        self.strategy.on_stop()
        
        print(f"\n回测完成")
    
    def _record_daily_result(self, bar: BarData):
        """记录每日结果"""
        # 计算当前持仓市值
        position_value = 0
        if self.strategy and self.strategy.position.volume != 0:
            position_value = self.strategy.position.volume * bar.close_price
        
        total_value = self.capital + position_value
        
        self.daily_results.append({
            'date': bar.datetime.date(),
            'capital': self.capital,
            'position_value': position_value,
            'total_value': total_value,
            'position': self.strategy.position.volume if self.strategy else 0,
        })
    
    def calculate_result(self) -> pd.DataFrame:
        """计算回测结果"""
        if not self.daily_results:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.daily_results)
        df['return'] = df['total_value'].pct_change()
        df['cum_return'] = (1 + df['return']).cumprod() - 1
        
        return df
    
    def get_statistics(self) -> Dict:
        """获取统计指标"""
        if not self.daily_results:
            return {}
        
        df = self.calculate_result()
        
        total_return = (self.daily_results[-1]['total_value'] / self.initial_capital) - 1
        
        # 年化收益率（假设252个交易日）
        n_days = len(df)
        annual_return = (1 + total_return) ** (252 / n_days) - 1 if n_days > 0 else 0
        
        # 波动率
        volatility = df['return'].std() * np.sqrt(252) if len(df) > 1 else 0
        
        # 夏普比率（假设无风险利率3%）
        sharpe_ratio = (annual_return - 0.03) / volatility if volatility != 0 else 0
        
        # 最大回撤
        cummax = df['total_value'].cummax()
        drawdown = (df['total_value'] - cummax) / cummax
        max_drawdown = drawdown.min()
        
        # 交易次数
        trade_count = len(self.trades)
        
        return {
            'initial_capital': self.initial_capital,
            'final_value': self.daily_results[-1]['total_value'] if self.daily_results else self.initial_capital,
            'total_return': total_return,
            'annual_return': annual_return,
            'volatility': volatility,
            'sharpe_ratio': sharpe_ratio,
            'max_drawdown': max_drawdown,
            'trade_count': trade_count,
            'days': n_days,
        }
    
    def print_report(self):
        """打印回测报告"""
        stats = self.get_statistics()
        
        print("\n" + "="*50)
        print("回测报告")
        print("="*50)
        print(f"初始资金: {stats.get('initial_capital', 0):,.2f}")
        print(f"最终价值: {stats.get('final_value', 0):,.2f}")
        print(f"总收益率: {stats.get('total_return', 0)*100:.2f}%")
        print(f"年化收益: {stats.get('annual_return', 0)*100:.2f}%")
        print(f"年化波动: {stats.get('volatility', 0)*100:.2f}%")
        print(f"夏普比率: {stats.get('sharpe_ratio', 0):.2f}")
        print(f"最大回撤: {stats.get('max_drawdown', 0)*100:.2f}%")
        print(f"交易次数: {stats.get('trade_count', 0)}")
        print(f"回测天数: {stats.get('days', 0)}")
        print("="*50)
