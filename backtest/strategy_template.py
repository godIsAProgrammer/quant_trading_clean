from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Callable

import pandas as pd


class Direction(Enum):
    """方向"""
    LONG = "多"
    SHORT = "空"


class OrderType(Enum):
    """订单类型"""
    MARKET = "市价"
    LIMIT = "限价"


@dataclass
class Order:
    """订单"""
    vt_symbol: str
    direction: Direction
    price: float
    volume: int
    order_type: OrderType = OrderType.MARKET
    order_id: str = ""
    status: str = "pending"  # pending, filled, cancelled
    filled_volume: int = 0
    filled_price: float = 0.0
    create_time: Optional[datetime] = None
    
    def __post_init__(self):
        if self.create_time is None:
            self.create_time = datetime.now()


@dataclass
class Trade:
    """成交"""
    vt_symbol: str
    direction: Direction
    price: float
    volume: int
    trade_time: datetime
    trade_id: str = ""


@dataclass
class Position:
    """持仓"""
    vt_symbol: str
    volume: int = 0  # 正数=多头，负数=空头
    avg_price: float = 0.0
    
    @property
    def is_long(self) -> bool:
        return self.volume > 0
    
    @property
    def is_short(self) -> bool:
        return self.volume < 0
    
    @property
    def is_flat(self) -> bool:
        return self.volume == 0


@dataclass
class BarData:
    """K线数据"""
    vt_symbol: str
    datetime: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    turnover: float


class CtaTemplate(ABC):
    """CTA策略模板"""
    
    def __init__(
        self,
        strategy_name: str,
        vt_symbol: str,
        setting: Dict = None,
    ):
        self.strategy_name = strategy_name
        self.vt_symbol = vt_symbol
        self.setting = setting or {}
        
        # 状态
        self.inited: bool = False
        self.trading: bool = False
        
        # 数据
        self.bar: Optional[BarData] = None
        self.bars: List[BarData] = []
        
        # 持仓和交易
        self.pos: int = 0
        self.position = Position(vt_symbol=vt_symbol)
        self.orders: Dict[str, Order] = {}
        self.trades: List[Trade] = []
        
        # 回调
        self.send_order_callback: Optional[Callable] = None
        self.write_log_callback: Optional[Callable] = None
        
        # 应用设置
        self.apply_setting()
    
    def apply_setting(self):
        """应用策略参数设置"""
        for key, value in self.setting.items():
            if hasattr(self, key):
                setattr(self, key, value)
    
    @abstractmethod
    def on_init(self):
        """策略初始化"""
        pass
    
    @abstractmethod
    def on_start(self):
        """策略启动"""
        pass
    
    @abstractmethod
    def on_stop(self):
        """策略停止"""
        pass
    
    @abstractmethod
    def on_bar(self, bar: BarData):
        """收到K线数据"""
        pass
    
    def buy(self, price: float, volume: int) -> List[str]:
        """买入开仓"""
        return self.send_order(Direction.LONG, price, volume)
    
    def sell(self, price: float, volume: int) -> List[str]:
        """卖出平仓"""
        return self.send_order(Direction.SHORT, price, volume)
    
    def short(self, price: float, volume: int) -> List[str]:
        """卖出开仓（做空）"""
        return self.send_order(Direction.SHORT, price, volume)
    
    def cover(self, price: float, volume: int) -> List[str]:
        """买入平仓（平空）"""
        return self.send_order(Direction.LONG, price, volume)
    
    def send_order(
        self,
        direction: Direction,
        price: float,
        volume: int,
        order_type: OrderType = OrderType.MARKET,
    ) -> List[str]:
        """发送订单"""
        if not self.trading:
            self.write_log("策略未启动，无法下单")
            return []
        
        if volume <= 0:
            self.write_log("下单量必须大于0")
            return []
        
        order = Order(
            vt_symbol=self.vt_symbol,
            direction=direction,
            price=price,
            volume=volume,
            order_type=order_type,
        )
        
        self.orders[order.order_id] = order
        
        # 回调给引擎处理
        if self.send_order_callback:
            self.send_order_callback(order)
        
        return [order.order_id]
    
    def cancel_order(self, order_id: str):
        """撤单"""
        if order_id in self.orders:
            self.orders[order_id].status = "cancelled"
    
    def cancel_all(self):
        """撤销所有订单"""
        for order in self.orders.values():
            if order.status == "pending":
                order.status = "cancelled"
    
    def write_log(self, msg: str):
        """记录日志"""
        log_msg = f"[{self.strategy_name}] {msg}"
        print(log_msg)
        if self.write_log_callback:
            self.write_log_callback(log_msg)
    
    def get_positions(self) -> Dict[str, Position]:
        """获取持仓信息"""
        return {self.vt_symbol: self.position}
    
    def on_order(self, order: Order):
        """订单状态更新回调"""
        pass
    
    def on_trade(self, trade: Trade):
        """成交回调"""
        self.trades.append(trade)
        
        # 更新持仓
        if trade.direction == Direction.LONG:
            new_volume = self.position.volume + trade.volume
            if self.position.volume >= 0:
                # 加仓或多头加仓
                total_cost = self.position.volume * self.position.avg_price + trade.volume * trade.price
                self.position.avg_price = total_cost / new_volume if new_volume > 0 else 0
            else:
                # 平空
                if trade.volume >= abs(self.position.volume):
                    # 完全平仓并可能转多
                    remaining = trade.volume + self.position.volume
                    self.position.avg_price = trade.price if remaining > 0 else 0
                else:
                    self.position.avg_price = self.position.avg_price  # 部分平仓，成本不变
            self.position.volume = new_volume
        else:  # SHORT
            new_volume = self.position.volume - trade.volume
            if self.position.volume <= 0:
                # 加空或空头加仓
                total_cost = abs(self.position.volume) * self.position.avg_price + trade.volume * trade.price
                self.position.avg_price = total_cost / abs(new_volume) if new_volume < 0 else 0
            else:
                # 平多
                if trade.volume >= self.position.volume:
                    remaining = self.position.volume - trade.volume
                    self.position.avg_price = trade.price if remaining < 0 else 0
                else:
                    self.position.avg_price = self.position.avg_price
            self.position.volume = new_volume
        
        self.pos = self.position.volume
