"""
模拟盘运行脚本 - 使用历史数据模拟实盘
"""
import sys
sys.path.insert(0, '.')

from datetime import datetime, timedelta
import time

from paper_trading.engine import PaperTradingEngine
from strategies.macd import MacdStrategy
from data.universal_loader import UniversalDataLoader


def run_paper_trading_simulation():
    """
    运行模拟盘（使用历史数据按时间顺序模拟）
    """
    print("="*60)
    print("模拟盘交易系统")
    print("="*60)
    
    # 创建模拟盘引擎（茅台股价约1500元，100股需要15万，设初始资金为200万）
    engine = PaperTradingEngine(initial_capital=2_000_000.0)
    
    # 添加 MACD 策略
    engine.add_strategy(
        MacdStrategy,
        strategy_name="MACD模拟盘",
        vt_symbol="600519.SSE",
        setting={"fast_period": 12, "slow_period": 26, "signal_period": 9}
    )
    
    # 加载历史数据
    print("\n加载历史数据...")
    loader = UniversalDataLoader(db_path="vnpy_data.db")
    df = loader.load_symbol(
        vt_symbol="600519.SSE",
        start="2024-10-01",
        end="2024-12-31",
    )
    
    if df.empty:
        print("数据加载失败")
        return
    
    print(f"加载数据: {len(df)} 条K线")
    print(f"日期范围: {df['datetime'].min()} ~ {df['datetime'].max()}")
    
    # 启动模拟盘
    engine.start()
    
    # 按时间顺序处理K线（模拟实时行情）
    print("\n开始模拟交易...")
    print("-"*60)
    
    from backtest.strategy_template import BarData
    
    for idx, row in df.iterrows():
        bar = BarData(
            vt_symbol=row['vt_symbol'],
            datetime=row['datetime'],
            open_price=row['open'],
            high_price=row['high'],
            low_price=row['low'],
            close_price=row['close'],
            volume=row['volume'],
            turnover=row.get('turnover', 0),
        )
        
        # 处理K线
        engine.on_bar(bar)
        
        # 模拟延迟（实际运行时可选）
        # time.sleep(0.01)
    
    print("-"*60)
    
    # 停止模拟盘
    engine.stop()
    
    # 保存状态
    engine.save_state("paper_trading/state.json")
    
    # 显示最终状态
    print("\n最终状态:")
    status = engine.get_status()
    for key, value in status.items():
        print(f"  {key}: {value}")


if __name__ == "__main__":
    run_paper_trading_simulation()
