"""
RSI 策略回测脚本
"""
import sys
sys.path.insert(0, '.')

from backtest.engine import BacktestEngine
from strategies.rsi import RsiStrategy


def run_rsi_backtest():
    """运行 RSI 策略回测"""
    
    engine = BacktestEngine()
    engine.set_parameters(
        start_date="2020-01-01",
        end_date="2024-12-31",
        initial_capital=100_000.0,
        commission_rate=0.0003,
        slippage=0.01,
    )
    
    import os
    db_paths = ["data/vnpy_database.db", "vnpy_data.db", "data/vnpy_data.db"]
    db_path = next((p for p in db_paths if os.path.exists(p)), None)
    
    if not db_path:
        print("错误: 未找到数据库文件")
        return
    
    print(f"使用数据库: {db_path}")
    
    vt_symbol = "600519.SSE"
    engine.add_data(db_path, vt_symbol)
    
    if not engine.bars:
        print(f"错误: 无法加载 {vt_symbol} 数据")
        return
    
    engine.add_strategy(
        RsiStrategy,
        strategy_name="RSI策略",
        vt_symbol=vt_symbol,
        setting={"rsi_period": 14, "oversold": 30, "overbought": 70}
    )
    
    engine.run_backtesting()
    engine.print_report()
    
    df = engine.calculate_result()
    if not df.empty:
        output_path = "backtest/reports/rsi_result.csv"
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df.to_csv(output_path, index=False)
        print(f"\n详细结果已保存: {output_path}")


if __name__ == "__main__":
    run_rsi_backtest()
