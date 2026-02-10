"""
布林带 (Bollinger Bands) 策略示例

布林带指标说明：
- 中轨 = N日简单移动平均线 (SMA)
- 上轨 = 中轨 + K * N日标准差
- 下轨 = 中轨 - K * N日标准差

策略逻辑：
- 价格突破上轨 → 卖出（超买）
- 价格跌破下轨 → 买入（超卖）
- 回归中轨 → 平仓
"""
from backtest.strategy_template import CtaTemplate, BarData


class BollingerBandsStrategy(CtaTemplate):
    """布林带策略"""
    
    # 策略参数
    bb_period: int = 20      # 布林带周期（默认20日）
    bb_dev: float = 2.0      # 标准差倍数（默认2倍）
    
    # 策略变量
    upper_band: float = 0.0   # 上轨
    middle_band: float = 0.0  # 中轨
    lower_band: float = 0.0   # 下轨
    
    def __init__(self, strategy_name, vt_symbol, setting=None):
        super().__init__(strategy_name, vt_symbol, setting)
        
        # 保存收盘价用于计算
        self.close_prices: list = []
    
    def on_init(self):
        """策略初始化"""
        self.write_log(f"布林带策略初始化，参数: period={self.bb_period}, dev={self.bb_dev}")
    
    def on_start(self):
        """策略启动"""
        self.write_log("布林带策略启动")
    
    def on_stop(self):
        """策略停止"""
        self.write_log("布林带策略停止")
    
    def on_bar(self, bar: BarData):
        """收到K线数据"""
        # 保存收盘价
        self.close_prices.append(bar.close_price)
        
        # 数据不足时直接返回
        if len(self.close_prices) < self.bb_period:
            return
        
        # 只保留需要的数量
        if len(self.close_prices) > self.bb_period * 2:
            self.close_prices = self.close_prices[-self.bb_period * 2:]
        
        # 计算布林带
        recent_prices = self.close_prices[-self.bb_period:]
        self.middle_band = sum(recent_prices) / len(recent_prices)
        
        # 计算标准差
        variance = sum((p - self.middle_band) ** 2 for p in recent_prices) / len(recent_prices)
        std_dev = variance ** 0.5
        
        self.upper_band = self.middle_band + self.bb_dev * std_dev
        self.lower_band = self.middle_band - self.bb_dev * std_dev
        
        # 获取前一日收盘价
        if len(self.close_prices) < 2:
            return
        
        prev_close = self.close_prices[-2]
        curr_close = bar.close_price
        
        # 策略逻辑：均值回归
        
        # 1. 价格跌破下轨 → 买入信号（超卖）
        if prev_close >= self.lower_band and curr_close < self.lower_band:
            self.write_log(f"[{bar.datetime}] 跌破下轨: 收盘价={curr_close:.2f}, 下轨={self.lower_band:.2f}")
            
            if self.pos == 0:
                self.buy(bar.close_price, 100)
                self.write_log(f"买入 100 股 @ {bar.close_price:.2f}")
            elif self.pos < 0:
                self.cover(bar.close_price, abs(self.pos))
                self.buy(bar.close_price, 100)
                self.write_log(f"平空并买入 100 股 @ {bar.close_price:.2f}")
        
        # 2. 价格突破上轨 → 卖出信号（超买）
        elif prev_close <= self.upper_band and curr_close > self.upper_band:
            self.write_log(f"[{bar.datetime}] 突破上轨: 收盘价={curr_close:.2f}, 上轨={self.upper_band:.2f}")
            
            if self.pos > 0:
                self.sell(bar.close_price, self.pos)
                self.write_log(f"卖出 {self.pos} 股 @ {bar.close_price:.2f}")
            elif self.pos == 0:
                self.short(bar.close_price, 100)
                self.write_log(f"做空 100 股 @ {bar.close_price:.2f}")
        
        # 3. 回归中轨 → 平仓（止盈）
        # 持有多头且价格从上方向下穿过中轨
        elif self.pos > 0 and prev_close >= self.middle_band and curr_close < self.middle_band:
            self.write_log(f"[{bar.datetime}] 多头回归中轨: 收盘价={curr_close:.2f}, 中轨={self.middle_band:.2f}")
            self.sell(bar.close_price, self.pos)
            self.write_log(f"止盈卖出 {self.pos} 股 @ {bar.close_price:.2f}")
        
        # 持有空头且价格从下方向上穿过中轨
        elif self.pos < 0 and prev_close <= self.middle_band and curr_close > self.middle_band:
            self.write_log(f"[{bar.datetime}] 空头回归中轨: 收盘价={curr_close:.2f}, 中轨={self.middle_band:.2f}")
            self.cover(bar.close_price, abs(self.pos))
            self.write_log(f"止盈平仓 {abs(self.pos)} 股 @ {bar.close_price:.2f}")
