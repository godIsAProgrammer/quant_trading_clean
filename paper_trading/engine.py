"""
模拟盘交易引擎
用于实盘前的策略验证
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import json
import os

from backtest.strategy_template import CtaTemplate, BarData, Order, Trade, Direction


class SimulatedOrderStatus(Enum):
    """模拟订单状态"""
    PENDING = "待成交"
    FILLED = "已成交"
    CANCELLED = "已撤销"
    REJECTED = "已拒绝"


@dataclass
class SimulatedOrder:
    """模拟订单"""
    order_id: str
    vt_symbol: str
    direction: Direction
    price: float
    volume: int
    status: SimulatedOrderStatus = SimulatedOrderStatus.PENDING
    filled_volume: int = 0
    filled_price: float = 0.0
    create_time: datetime = field(default_factory=datetime.now)
    update_time: Optional[datetime] = None


@dataclass
class SimulatedPosition:
    """模拟持仓"""
    vt_symbol: str
    volume: int = 0
    avg_price: float = 0.0
    today_bought: Dict[str, int] = field(default_factory=dict)
    
    def get_sellable_volume(self, current_date: str) -> int:
        """获取当日可卖出的持仓量（T+1）"""
        if self.volume <= 0:
            return 0
        today_bought = self.today_bought.get(current_date, 0)
        return max(0, self.volume - today_bought)


@dataclass
class SimulatedAccount:
    """模拟账户"""
    account_id: str = "sim_001"
    total_capital: float = 100_000.0  # 总资金
    available: float = 100_000.0      # 可用资金
    frozen: float = 0.0               # 冻结资金
    
    # 持仓
    positions: Dict[str, SimulatedPosition] = field(default_factory=dict)
    
    # 当日记录
    daily_pnl: float = 0.0            # 当日盈亏
    total_trades: int = 0             # 总交易次数
    
    def update_position(self, vt_symbol: str, volume_change: int, price: float, current_date: str):
        """更新持仓"""
        if vt_symbol not in self.positions:
            self.positions[vt_symbol] = SimulatedPosition(vt_symbol=vt_symbol)
        
        pos = self.positions[vt_symbol]
        
        if volume_change > 0:
            # 买入
            new_volume = pos.volume + volume_change
            total_cost = pos.volume * pos.avg_price + volume_change * price
            pos.avg_price = total_cost / new_volume if new_volume > 0 else 0
            pos.volume = new_volume
            
            # T+1 记录
            pos.today_bought[current_date] = pos.today_bought.get(current_date, 0) + volume_change
        else:
            # 卖出
            pos.volume += volume_change  # volume_change 为负数
            if pos.volume == 0:
                pos.avg_price = 0.0
        
        self.total_trades += 1
    
    def get_total_value(self, prices: Dict[str, float]) -> float:
        """计算账户总价值"""
        position_value = sum(
            pos.volume * prices.get(vt_symbol, pos.avg_price)
            for vt_symbol, pos in self.positions.items()
        )
        return self.available + self.frozen + position_value


class PaperTradingEngine:
    """模拟盘交易引擎"""
    
    def __init__(self, initial_capital: float = 100_000.0):
        self.initial_capital = initial_capital
        self.account = SimulatedAccount(total_capital=initial_capital, available=initial_capital)
        
        # 策略
        self.strategy: Optional[CtaTemplate] = None
        
        # 订单管理
        self.orders: Dict[str, SimulatedOrder] = {}
        self.order_counter: int = 0
        
        # 手续费
        self.commission_rate: float = 0.0003  # 万分之3
        self.min_commission: float = 5.0      # 最低手续费5元
        
        # 数据
        self.current_bar: Optional[BarData] = None
        self.prices: Dict[str, float] = {}    # 当前价格
        
        # 运行状态
        self.is_running: bool = False
        self.start_time: Optional[datetime] = None
        
        # 日志回调
        self.log_callback: Optional[Callable] = None
    
    def _log(self, msg: str):
        """记录日志"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{timestamp}] {msg}"
        print(log_msg)
        if self.log_callback:
            self.log_callback(log_msg)
    
    def add_strategy(self, strategy_class, strategy_name: str, vt_symbol: str, setting: Dict = None):
        """添加策略"""
        self.strategy = strategy_class(
            strategy_name=strategy_name,
            vt_symbol=vt_symbol,
            setting=setting or {},
        )
        
        # 设置回调
        self.strategy.send_order_callback = self._handle_order
        self.strategy.write_log_callback = lambda msg: self._log(f"[策略] {msg}")
        
        self._log(f"添加策略: {strategy_name} ({strategy_class.__name__})")
    
    def _handle_order(self, order: Order):
        """处理订单"""
        self.order_counter += 1
        order_id = f"sim_{self.order_counter:06d}"
        
        sim_order = SimulatedOrder(
            order_id=order_id,
            vt_symbol=order.vt_symbol,
            direction=order.direction,
            price=order.price,
            volume=order.volume,
        )
        
        self.orders[order_id] = sim_order
        
        # T+1 检查
        if order.direction == Direction.SHORT:
            current_date = datetime.now().strftime('%Y-%m-%d')
            pos = self.account.positions.get(order.vt_symbol)
            if pos:
                sellable = pos.get_sellable_volume(current_date)
                if order.volume > sellable:
                    sim_order.status = SimulatedOrderStatus.REJECTED
                    self._log(f"[T+1阻止] 订单 {order_id}: 试图卖出 {order.volume}，但可卖仅 {sellable}")
                    return
        
        # 资金检查
        if order.direction == Direction.LONG:
            required_fund = order.price * order.volume * 1.001  # 预留手续费
            if required_fund > self.account.available:
                sim_order.status = SimulatedOrderStatus.REJECTED
                self._log(f"[资金不足] 订单 {order_id}: 需要 {required_fund:.2f}，可用 {self.account.available:.2f}")
                return
        
        # 模拟成交（市价单立即成交）
        current_price = self.prices.get(order.vt_symbol, order.price)
        self._fill_order(sim_order, current_price)
    
    def _fill_order(self, order: SimulatedOrder, price: float):
        """订单成交"""
        order.filled_price = price
        order.filled_volume = order.volume
        order.status = SimulatedOrderStatus.FILLED
        order.update_time = datetime.now()
        
        # 计算手续费
        trade_value = price * order.volume
        commission = max(trade_value * self.commission_rate, self.min_commission)
        
        # 更新账户
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        if order.direction == Direction.LONG:
            # 买入
            cost = trade_value + commission
            self.account.available -= cost
            self.account.update_position(order.vt_symbol, order.volume, price, current_date)
        else:
            # 卖出
            revenue = trade_value - commission
            self.account.available += revenue
            self.account.update_position(order.vt_symbol, -order.volume, price, current_date)
        
        self._log(f"[成交] {order.order_id}: {order.direction.value} {order.volume} @ {price:.2f}, 手续费: {commission:.2f}")
        
        # 通知策略
        trade = Trade(
            vt_symbol=order.vt_symbol,
            direction=order.direction,
            price=price,
            volume=order.volume,
            trade_time=datetime.now(),
            trade_id=order.order_id,
        )
        self.strategy.on_trade(trade)
    
    def on_bar(self, bar: BarData):
        """处理新K线（模拟盘用）"""
        self.current_bar = bar
        self.prices[bar.vt_symbol] = bar.close_price
        
        if self.strategy and self.strategy.trading:
            self.strategy.bar = bar
            self.strategy.bars.append(bar)
            self.strategy.on_bar(bar)
    
    def on_tick(self, vt_symbol: str, price: float):
        """处理实时 tick（模拟盘用）"""
        self.prices[vt_symbol] = price
        # 可以在这里触发条件单等
    
    def start(self):
        """启动模拟盘"""
        if not self.strategy:
            self._log("错误: 未设置策略")
            return
        
        self.is_running = True
        self.start_time = datetime.now()
        
        self.strategy.inited = True
        self.strategy.on_init()
        self.strategy.trading = True
        self.strategy.on_start()
        
        self._log("="*50)
        self._log("模拟盘启动")
        self._log(f"初始资金: {self.account.total_capital:,.2f}")
        self._log("="*50)
    
    def stop(self):
        """停止模拟盘"""
        if self.strategy:
            self.strategy.trading = False
            self.strategy.on_stop()
        
        self.is_running = False
        
        self._log("="*50)
        self._log("模拟盘停止")
        self._print_report()
        self._log("="*50)
    
    def _print_report(self):
        """打印报告"""
        # 计算当前持仓市值
        position_value = sum(
            pos.volume * self.prices.get(vt_symbol, pos.avg_price)
            for vt_symbol, pos in self.account.positions.items()
        )
        
        total_value = self.account.available + self.account.frozen + position_value
        pnl = total_value - self.initial_capital
        pnl_pct = (pnl / self.initial_capital) * 100
        
        self._log(f"账户总值: {total_value:,.2f}")
        self._log(f"可用资金: {self.account.available:,.2f}")
        self._log(f"持仓市值: {position_value:,.2f}")
        self._log(f"总盈亏: {pnl:,.2f} ({pnl_pct:+.2f}%)")
        self._log(f"交易次数: {self.account.total_trades}")
    
    def get_status(self) -> Dict:
        """获取当前状态"""
        position_value = sum(
            pos.volume * self.prices.get(vt_symbol, pos.avg_price)
            for vt_symbol, pos in self.account.positions.items()
        )
        
        return {
            'is_running': self.is_running,
            'available': self.account.available,
            'position_value': position_value,
            'total_value': self.account.available + position_value,
            'positions': {
                vt_symbol: {'volume': pos.volume, 'avg_price': pos.avg_price}
                for vt_symbol, pos in self.account.positions.items()
            },
            'orders': len(self.orders),
            'trades': self.account.total_trades,
        }
    
    def save_state(self, filepath: str):
        """保存模拟盘状态"""
        state = {
            'account': {
                'total_capital': self.account.total_capital,
                'available': self.account.available,
                'positions': {
                    vt_symbol: {
                        'volume': pos.volume,
                        'avg_price': pos.avg_price,
                    }
                    for vt_symbol, pos in self.account.positions.items()
                },
            },
            'orders': [
                {
                    'order_id': o.order_id,
                    'vt_symbol': o.vt_symbol,
                    'direction': o.direction.value,
                    'price': o.price,
                    'volume': o.volume,
                    'status': o.status.value,
                }
                for o in self.orders.values()
            ],
        }
        
        with open(filepath, 'w') as f:
            json.dump(state, f, indent=2)
        
        self._log(f"状态已保存: {filepath}")
