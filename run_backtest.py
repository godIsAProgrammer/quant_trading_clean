"""
回测运行脚本
"""
from __future__ import annotations

from pathlib import Path
import sys

# 添加项目根目录到路径
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest.engine import BacktestEngine
from strategies.double_ma import DoubleMaStrategy


def main():
    """运行回测"""
    # 创建回测引擎
    engine = BacktestEngine()
    
    # 设置参数
    engine.set_parameters(
        start_date="2020-01-01",
        end_date="2024-12-31",
        initial_capital=100_000.0,  # 10万初始资金
        commission_rate=0.0003,      # 万分之3手续费
        slippage=0.01,               # 滑点
    )
    
    # 添加数据
    db_path = PROJECT_ROOT / "vnpy_data.db"
    vt_symbol = "600519.SSE"  # 贵州茅台
    
    print(f"加载数据: {vt_symbol}")
    engine.add_data(str(db_path), vt_symbol)
    
    # 添加策略
    engine.add_strategy(
        strategy_class=DoubleMaStrategy,
        strategy_name="DoubleMA_600519",
        vt_symbol=vt_symbol,
        setting={
            "fast_window": 10,
            "slow_window": 20,
        }
    )
    
    # 运行回测
    engine.run_backtesting()
    
    # 打印报告
    engine.print_report()
    
    # 保存详细结果
    df = engine.calculate_result()
    if not df.empty:
        output_path = PROJECT_ROOT / "backtest" / "reports" / "backtest_result.csv"
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"\n详细结果已保存: {output_path}")


if __name__ == "__main__":
    main()
