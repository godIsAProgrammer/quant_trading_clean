"""
MACD 策略回测脚本
"""
import sys
sys.path.insert(0, '.')

from backtest.engine import BacktestEngine
from strategies.macd import MacdStrategy


def run_macd_backtest():
    """运行 MACD 策略回测"""
    
    # 创建回测引擎
    engine = BacktestEngine()
    
    # 设置参数
    engine.set_parameters(
        start_date="2020-01-01",
        end_date="2024-12-31",
        initial_capital=100_000.0,
        commission_rate=0.0003,
        slippage=0.01,
    )
    
    # 加载数据（使用现有数据库）
    import os
    db_paths = [
        "data/vnpy_database.db",
        "vnpy_data.db",
        "data/vnpy_data.db",
    ]
    
    db_path = None
    for p in db_paths:
        if os.path.exists(p):
            db_path = p
            break
    
    if not db_path:
        print("错误: 未找到数据库文件")
        print("请确保存在以下文件之一:", db_paths)
        return
    
    print(f"使用数据库: {db_path}")
    
    # 添加数据
    vt_symbol = "600519.SSE"  # 贵州茅台
    engine.add_data(db_path, vt_symbol)
    
    if not engine.bars:
        print(f"错误: 无法加载 {vt_symbol} 数据")
        return
    
    # 添加 MACD 策略
    engine.add_strategy(
        MacdStrategy,
        strategy_name="MACD策略",
        vt_symbol=vt_symbol,
        setting={
            "fast_period": 12,
            "slow_period": 26,
            "signal_period": 9,
        }
    )
    
    # 运行回测
    engine.run_backtesting()
    
    # 打印报告
    engine.print_report()
    
    # 保存结果
    df = engine.calculate_result()
    if not df.empty:
        output_path = "backtest/reports/macd_result.csv"
        import os
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"\n详细结果已保存: {output_path}")


if __name__ == "__main__":
    run_macd_backtest()
