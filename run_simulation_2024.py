"""
2024全年模拟盘运行脚本
使用MACD策略跑完整年度数据
"""
import sys
sys.path.insert(0, '.')

from datetime import datetime
from paper_trading.engine import PaperTradingEngine
from strategies.macd import MacdStrategy
from data.universal_loader import UniversalDataLoader


def run_2024_simulation():
    """运行2024年全年模拟盘"""
    print("="*70)
    print("2024年模拟盘交易 - MACD策略")
    print("="*70)
    
    # 创建模拟盘引擎（200万初始资金）
    engine = PaperTradingEngine(initial_capital=2_000_000.0)
    
    # 添加MACD策略
    engine.add_strategy(
        MacdStrategy,
        strategy_name="MACD-2024",
        vt_symbol="600519.SSE",
        setting={"fast_period": 12, "slow_period": 26, "signal_period": 9}
    )
    
    # 加载2024年全年数据
    print("\n加载2024年数据...")
    loader = UniversalDataLoader(db_path="vnpy_data.db")
    df = loader.load_symbol(
        vt_symbol="600519.SSE",
        start="2024-01-01",
        end="2024-12-31",
    )
    
    if df.empty:
        print("错误: 无法加载数据")
        return
    
    print(f"数据条数: {len(df)}")
    print(f"日期范围: {df['datetime'].min()} ~ {df['datetime'].max()}")
    
    # 启动模拟盘
    engine.start()
    
    # 处理每根K线
    print("\n开始模拟交易...")
    print("-"*70)
    
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
        engine.on_bar(bar)
    
    print("-"*70)
    
    # 停止并输出报告
    engine.stop()
    
    # 保存结果
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    engine.save_state(f"paper_trading/simulation_2024_{timestamp}.json")
    
    # 详细统计
    status = engine.get_status()
    
    # 计算收益
    total_value = status['total_value']
    initial = 2_000_000.0
    pnl = total_value - initial
    pnl_pct = (pnl / initial) * 100
    
    print("\n" + "="*70)
    print("2024年模拟盘最终报告")
    print("="*70)
    print(f"初始资金:     {initial:>15,.2f}")
    print(f"最终总值:     {total_value:>15,.2f}")
    print(f"盈亏金额:     {pnl:>15,.2f} ({pnl_pct:+.2f}%)")
    print(f"交易次数:     {status['trades']:>15}")
    print(f"持仓数量:     {len(status['positions']):>15}")
    
    for vt_symbol, pos in status['positions'].items():
        print(f"  {vt_symbol}: {pos['volume']}股 @ 成本{pos['avg_price']:.2f}")
    
    print("="*70)
    
    # 评估是否达到实盘标准
    print("\n📊 实盘 readiness 评估:")
    print(f"  收益率: {pnl_pct:+.2f}%", end="")
    if pnl_pct > 0:
        print(" ✅ 盈利")
    else:
        print(" ❌ 亏损")
    
    # 保存评估结果
    report = {
        'date': timestamp,
        'initial_capital': initial,
        'final_value': total_value,
        'pnl': pnl,
        'pnl_pct': pnl_pct,
        'trades': status['trades'],
        'positions': status['positions'],
        'pass_criteria': pnl_pct > 0
    }
    
    import json
    with open(f"paper_trading/report_2024_{timestamp}.json", 'w') as f:
        json.dump(report, f, indent=2)
    
    return report


if __name__ == "__main__":
    run_2024_simulation()
