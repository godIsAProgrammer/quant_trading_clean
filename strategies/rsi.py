"""
RSI (Relative Strength Index) 策略示例

RSI 指标说明：
- RSI = 100 - 100 / (1 + RS)
- RS = N日平均上涨幅度 / N日平均下跌幅度

策略逻辑：
- RSI < 30 (超卖) → 买入
- RSI > 70 (超买) → 卖出
- RSI 回归 50 → 平仓
"""
from backtest.strategy_template import CtaTemplate, BarData


class RsiStrategy(CtaTemplate):
    """RSI 策略"""
    
    # 策略参数
    rsi_period: int = 14      # RSI 计算周期（默认14日）
    oversold: int = 30        # 超卖阈值
    overbought: int = 70      # 超买阈值
    
    # 策略变量
    rsi: float = 0.0          # 当前 RSI 值
    
    def __init__(self, strategy_name, vt_symbol, setting=None):
        super().__init__(strategy_name, vt_symbol, setting)
        
        # 保存收盘价用于计算
        self.close_prices: list = []
    
    def on_init(self):
        """策略初始化"""
        self.write_log(f"RSI策略初始化，参数: period={self.rsi_period}, oversold={self.oversold}, overbought={self.overbought}")
    
    def on_start(self):
        """策略启动"""
        self.write_log("RSI策略启动")
    
    def on_stop(self):
        """策略停止"""
        self.write_log("RSI策略停止")
    
    def on_bar(self, bar: BarData):
        """收到K线数据"""
        # 保存收盘价
        self.close_prices.append(bar.close_price)
        
        # 数据不足时直接返回（需要至少 period+1 个数据点来计算涨跌幅）
        if len(self.close_prices) < self.rsi_period + 1:
            return
        
        # 只保留需要的数量
        if len(self.close_prices) > self.rsi_period * 3:
            self.close_prices = self.close_prices[-self.rsi_period * 3:]
        
        # 计算 RSI
        self.rsi = self._calculate_rsi()
        
        # 获取前一日的 RSI
        if len(self.close_prices) < self.rsi_period + 2:
            return
        
        prev_close_prices = self.close_prices[:-1]
        prev_rsi = self._calculate_rsi_for_prices(prev_close_prices)
        
        # 策略逻辑
        
        # 1. RSI 从超卖区向上突破 → 买入信号
        if prev_rsi <= self.oversold and self.rsi > self.oversold:
            self.write_log(f"[{bar.datetime}] RSI突破超卖区: RSI={self.rsi:.2f} (>{self.oversold})")
            
            if self.pos == 0:
                self.buy(bar.close_price, 100)
                self.write_log(f"买入 100 股 @ {bar.close_price:.2f}")
            elif self.pos < 0:
                self.cover(bar.close_price, abs(self.pos))
                self.buy(bar.close_price, 100)
                self.write_log(f"平空并买入 100 股 @ {bar.close_price:.2f}")
        
        # 2. RSI 从超买区向下突破 → 卖出信号
        elif prev_rsi >= self.overbought and self.rsi < self.overbought:
            self.write_log(f"[{bar.datetime}] RSI跌破超买区: RSI={self.rsi:.2f} (<{self.overbought})")
            
            if self.pos > 0:
                self.sell(bar.close_price, self.pos)
                self.write_log(f"卖出 {self.pos} 股 @ {bar.close_price:.2f}")
            elif self.pos == 0:
                self.short(bar.close_price, 100)
                self.write_log(f"做空 100 股 @ {bar.close_price:.2f}")
        
        # 3. RSI 回归 50 → 平仓（止盈/止损）
        # 持有多头且 RSI 从上方跌破 50
        elif self.pos > 0 and prev_rsi >= 50 and self.rsi < 50:
            self.write_log(f"[{bar.datetime}] RSI多头回归50: RSI={self.rsi:.2f}")
            self.sell(bar.close_price, self.pos)
            self.write_log(f"止盈卖出 {self.pos} 股 @ {bar.close_price:.2f}")
        
        # 持有空头且 RSI 从下方突破 50
        elif self.pos < 0 and prev_rsi <= 50 and self.rsi > 50:
            self.write_log(f"[{bar.datetime}] RSI空头回归50: RSI={self.rsi:.2f}")
            self.cover(bar.close_price, abs(self.pos))
            self.write_log(f"止盈平仓 {abs(self.pos)} 股 @ {bar.close_price:.2f}")
    
    def _calculate_rsi(self) -> float:
        """计算当前 RSI"""
        return self._calculate_rsi_for_prices(self.close_prices)
    
    def _calculate_rsi_for_prices(self, prices: list) -> float:
        """根据价格列表计算 RSI"""
        if len(prices) < self.rsi_period + 1:
            return 50.0  # 默认中性值
        
        # 计算涨跌幅
        gains = []
        losses = []
        
        for i in range(1, len(prices)):
            change = prices[i] - prices[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        # 取最近 period 个涨跌幅
        recent_gains = gains[-self.rsi_period:]
        recent_losses = losses[-self.rsi_period:]
        
        avg_gain = sum(recent_gains) / self.rsi_period
        avg_loss = sum(recent_losses) / self.rsi_period
        
        if avg_loss == 0:
            return 100.0  # 没有下跌，RSI = 100
        
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
