"""
MACD 策略示例
DIF 上穿 DEA (金叉) 时买入，下穿 (死叉) 时卖出

MACD 指标说明：
- DIF = EMA(12) - EMA(26)
- DEA = EMA(DIF, 9)  
- MACD = 2 * (DIF - DEA)
"""
from backtest.strategy_template import CtaTemplate, BarData


class MacdStrategy(CtaTemplate):
    """MACD 策略"""
    
    # 策略参数
    fast_period: int = 12   # 快速 EMA 周期
    slow_period: int = 26   # 慢速 EMA 周期
    signal_period: int = 9  # 信号线 DEA 周期
    
    # 策略变量
    dif: float = 0.0        # DIF 值
    dea: float = 0.0        # DEA 值
    macd: float = 0.0       # MACD 柱状图值
    
    def __init__(self, strategy_name, vt_symbol, setting=None):
        super().__init__(strategy_name, vt_symbol, setting)
        
        # 保存收盘价用于计算 EMA
        self.close_prices: list = []
        
        # 保存历史 DIF 和 DEA 用于判断交叉
        self.dif_history: list = []
        self.dea_history: list = []
    
    def on_init(self):
        """策略初始化"""
        self.write_log(f"MACD策略初始化，参数: fast={self.fast_period}, slow={self.slow_period}, signal={self.signal_period}")
    
    def on_start(self):
        """策略启动"""
        self.write_log("MACD策略启动")
    
    def on_stop(self):
        """策略停止"""
        self.write_log("MACD策略停止")
    
    def on_bar(self, bar: BarData):
        """收到K线数据"""
        # 保存收盘价
        self.close_prices.append(bar.close_price)
        
        # 数据不足时直接返回
        min_bars = self.slow_period + self.signal_period + 10
        if len(self.close_prices) < min_bars:
            return
        
        # 只保留需要的数量
        if len(self.close_prices) > min_bars * 2:
            self.close_prices = self.close_prices[-min_bars * 2:]
        
        # 计算 EMA
        ema_fast = self._calculate_ema(self.close_prices, self.fast_period)
        ema_slow = self._calculate_ema(self.close_prices, self.slow_period)
        
        # 计算 DIF
        self.dif = ema_fast - ema_slow
        
        # 计算 DEA (DIF 的 EMA)
        self.dea = self._calculate_ema(self.dif_history + [self.dif], self.signal_period)
        
        # 计算 MACD 柱状图
        self.macd = 2 * (self.dif - self.dea)
        
        # 保存历史值
        self.dif_history.append(self.dif)
        self.dea_history.append(self.dea)
        
        # 限制历史长度
        if len(self.dif_history) > self.signal_period + 10:
            self.dif_history = self.dif_history[-(self.signal_period + 10):]
            self.dea_history = self.dea_history[-(self.signal_period + 10):]
        
        # 判断金叉死叉（需要至少2个数据点）
        if len(self.dif_history) < 2:
            return
        
        prev_dif = self.dif_history[-2]
        prev_dea = self.dea_history[-2]
        
        # 判断金叉（DIF 上穿 DEA）
        if prev_dif <= prev_dea and self.dif > self.dea:
            self.write_log(f"[{bar.datetime}] MACD金叉: DIF={self.dif:.4f}, DEA={self.dea:.4f}, MACD={self.macd:.4f}")
            
            # 如果没有持仓，买入
            if self.pos == 0:
                self.buy(bar.close_price, 100)
                self.write_log(f"买入 100 股 @ {bar.close_price:.2f}")
            # 如果做空，先平仓再买入
            elif self.pos < 0:
                self.cover(bar.close_price, abs(self.pos))
                self.buy(bar.close_price, 100)
                self.write_log(f"平空并买入 100 股 @ {bar.close_price:.2f}")
        
        # 判断死叉（DIF 下穿 DEA）
        elif prev_dif >= prev_dea and self.dif < self.dea:
            self.write_log(f"[{bar.datetime}] MACD死叉: DIF={self.dif:.4f}, DEA={self.dea:.4f}, MACD={self.macd:.4f}")
            
            # 如果持有多头，卖出
            if self.pos > 0:
                self.sell(bar.close_price, self.pos)
                self.write_log(f"卖出 {self.pos} 股 @ {bar.close_price:.2f}")
    
    def _calculate_ema(self, prices: list, period: int) -> float:
        """
        计算指数移动平均 EMA
        EMA(t) = Price(t) * k + EMA(t-1) * (1-k)
        k = 2 / (period + 1)
        """
        if len(prices) < period:
            return sum(prices) / len(prices) if prices else 0.0
        
        k = 2 / (period + 1)
        
        # 初始值使用简单移动平均
        ema = sum(prices[:period]) / period
        
        # 递归计算 EMA
        for price in prices[period:]:
            ema = price * k + ema * (1 - k)
        
        return ema
