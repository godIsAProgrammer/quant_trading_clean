"""
双均线策略示例
短期均线上穿长期均线时买入，下穿时卖出
"""
from backtest.strategy_template import CtaTemplate, BarData


class DoubleMaStrategy(CtaTemplate):
    """双均线策略"""
    
    # 策略参数
    fast_window: int = 10  # 短期均线窗口
    slow_window: int = 20  # 长期均线窗口
    
    # 策略变量
    fast_ma: float = 0.0
    slow_ma: float = 0.0
    
    def __init__(self, strategy_name, vt_symbol, setting=None):
        super().__init__(strategy_name, vt_symbol, setting)
        
        # 保存收盘价用于计算均线
        self.close_prices: list = []
    
    def on_init(self):
        """策略初始化"""
        self.write_log(f"策略初始化，参数: fast={self.fast_window}, slow={self.slow_window}")
    
    def on_start(self):
        """策略启动"""
        self.write_log("策略启动")
    
    def on_stop(self):
        """策略停止"""
        self.write_log("策略停止")
    
    def on_bar(self, bar: BarData):
        """收到K线数据"""
        # 保存收盘价
        self.close_prices.append(bar.close_price)
        
        # 数据不足时直接返回
        if len(self.close_prices) < self.slow_window:
            return
        
        # 只保留需要的数量
        if len(self.close_prices) > self.slow_window * 2:
            self.close_prices = self.close_prices[-self.slow_window * 2:]
        
        # 计算均线
        self.fast_ma = sum(self.close_prices[-self.fast_window:]) / self.fast_window
        self.slow_ma = sum(self.close_prices[-self.slow_window:]) / self.slow_window
        
        # 获取前一日的均线（用于判断交叉）
        if len(self.close_prices) >= self.slow_window + 1:
            prev_fast_ma = sum(self.close_prices[-self.fast_window-1:-1]) / self.fast_window
            prev_slow_ma = sum(self.close_prices[-self.slow_window-1:-1]) / self.slow_window
            
            # 判断金叉（短期均线上穿长期均线）
            if prev_fast_ma <= prev_slow_ma and self.fast_ma > self.slow_ma:
                self.write_log(f"[{bar.datetime}] 金叉信号: 快线={self.fast_ma:.2f}, 慢线={self.slow_ma:.2f}")
                
                # 如果没有持仓，买入
                if self.pos == 0:
                    self.buy(bar.close_price, 100)  # 买入100股
                # 如果做空，先平仓再买入
                elif self.pos < 0:
                    self.cover(bar.close_price, abs(self.pos))
                    self.buy(bar.close_price, 100)
            
            # 判断死叉（短期均线下穿长期均线）
            elif prev_fast_ma >= prev_slow_ma and self.fast_ma < self.slow_ma:
                self.write_log(f"[{bar.datetime}] 死叉信号: 快线={self.fast_ma:.2f}, 慢线={self.slow_ma:.2f}")
                
                # 如果持有多头，卖出
                if self.pos > 0:
                    self.sell(bar.close_price, self.pos)
