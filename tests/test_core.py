"""
单元测试套件 - 量化交易系统核心功能测试
"""
import unittest
import sys
from pathlib import Path
from datetime import datetime, timedelta

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from backtest.strategy_template import Position, Direction, Order, Trade
from paper_trading.engine import PaperTradingEngine, SimulatedPosition


class TestT1Position(unittest.TestCase):
    """测试 T+1 持仓管理"""
    
    def test_get_sellable_volume_empty(self):
        """测试空仓时可卖数量为0"""
        pos = Position(vt_symbol="600519.SSE", volume=0)
        self.assertEqual(pos.get_sellable_volume("2024-01-01"), 0)
    
    def test_get_sellable_volume_no_today_buy(self):
        """测试没有当日买入时，可卖数量等于总持仓"""
        pos = Position(vt_symbol="600519.SSE", volume=100)
        pos.today_bought = {"2024-01-01": 100}  # 昨天买的
        
        # 今天可卖全部持仓（因为都是昨天买的）
        self.assertEqual(pos.get_sellable_volume("2024-01-02"), 100)
    
    def test_get_sellable_volume_with_today_buy(self):
        """测试有当日买入时，可卖数量正确计算"""
        pos = Position(vt_symbol="600519.SSE", volume=300)
        pos.today_bought = {
            "2024-01-01": 100,  # 昨天买的
            "2024-01-02": 200,  # 今天买的
        }
        
        # 今天只能卖昨天买的100股
        self.assertEqual(pos.get_sellable_volume("2024-01-02"), 100)
    
    def test_get_sellable_volume_all_today_buy(self):
        """测试全部持仓都是今天买的，可卖数量为0"""
        pos = Position(vt_symbol="600519.SSE", volume=100)
        pos.today_bought = {"2024-01-02": 100}
        
        self.assertEqual(pos.get_sellable_volume("2024-01-02"), 0)
    
    def test_short_position(self):
        """测试空头持仓时，可卖数量为0"""
        pos = Position(vt_symbol="600519.SSE", volume=-100)
        self.assertEqual(pos.get_sellable_volume("2024-01-01"), 0)


class TestSimulatedPosition(unittest.TestCase):
    """测试模拟盘持仓"""
    
    def test_update_position_buy(self):
        """测试买入更新持仓"""
        pos = SimulatedPosition(vt_symbol="600519.SSE")
        
        # 首次买入100股 @ 1000元
        pos.volume = 100
        pos.avg_price = 1000.0
        pos.today_bought["2024-01-01"] = 100
        
        self.assertEqual(pos.volume, 100)
        self.assertEqual(pos.avg_price, 1000.0)
        self.assertEqual(pos.get_sellable_volume("2024-01-01"), 0)
    
    def test_update_position_sell(self):
        """测试卖出更新持仓"""
        pos = SimulatedPosition(vt_symbol="600519.SSE", volume=200, avg_price=1000.0)
        # 持仓200股，其中100股是昨天(2024-01-01)买的，100股是今天(2024-01-02)买的
        pos.today_bought = {"2024-01-01": 100, "2024-01-02": 100}
        
        # 今天只能卖昨天买的100股
        self.assertEqual(pos.get_sellable_volume("2024-01-02"), 100)


class TestPaperTradingEngine(unittest.TestCase):
    """测试模拟盘引擎"""
    
    def setUp(self):
        """测试前准备"""
        self.engine = PaperTradingEngine(initial_capital=1_000_000.0)
    
    def test_initial_capital(self):
        """测试初始资金"""
        self.assertEqual(self.engine.account.total_capital, 1_000_000.0)
        self.assertEqual(self.engine.account.available, 1_000_000.0)
    
    def test_get_status(self):
        """测试获取状态"""
        status = self.engine.get_status()
        
        self.assertIn('is_running', status)
        self.assertIn('available', status)
        self.assertIn('total_value', status)
        self.assertEqual(status['available'], 1_000_000.0)


class TestDirection(unittest.TestCase):
    """测试方向枚举"""
    
    def test_direction_values(self):
        """测试方向值"""
        self.assertEqual(Direction.LONG.value, "多")
        self.assertEqual(Direction.SHORT.value, "空")


class TestOrder(unittest.TestCase):
    """测试订单"""
    
    def test_order_creation(self):
        """测试订单创建"""
        order = Order(
            vt_symbol="600519.SSE",
            direction=Direction.LONG,
            price=1000.0,
            volume=100,
        )
        
        self.assertEqual(order.vt_symbol, "600519.SSE")
        self.assertEqual(order.direction, Direction.LONG)
        self.assertEqual(order.price, 1000.0)
        self.assertEqual(order.volume, 100)
        self.assertEqual(order.status, "pending")


class TestTrade(unittest.TestCase):
    """测试成交"""
    
    def test_trade_creation(self):
        """测试成交创建"""
        trade_time = datetime.now()
        trade = Trade(
            vt_symbol="600519.SSE",
            direction=Direction.LONG,
            price=1000.0,
            volume=100,
            trade_time=trade_time,
            trade_id="T001",
        )
        
        self.assertEqual(trade.trade_id, "T001")
        self.assertEqual(trade.volume, 100)


def run_tests():
    """运行所有测试"""
    # 创建测试套件
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # 添加测试类
    suite.addTests(loader.loadTestsFromTestCase(TestT1Position))
    suite.addTests(loader.loadTestsFromTestCase(TestSimulatedPosition))
    suite.addTests(loader.loadTestsFromTestCase(TestPaperTradingEngine))
    suite.addTests(loader.loadTestsFromTestCase(TestDirection))
    suite.addTests(loader.loadTestsFromTestCase(TestOrder))
    suite.addTests(loader.loadTestsFromTestCase(TestTrade))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
