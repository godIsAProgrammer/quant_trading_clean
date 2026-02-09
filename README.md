# quant_trading

A股量化交易系统的数据采集模块

## 功能特性

- 基于 akshare 库获取 A 股日线数据
- 支持增量更新（只获取新数据）
- 数据存储到 SQLite 数据库
- 完整的目录结构和测试用例

## 目录结构

```
quant_trading/
├── data/
│   ├── __init__.py
│   ├── collector.py      # 数据采集器
│   ├── storage.py        # 数据存储
│   └── models.py         # 数据模型
├── tests/
│   └── test_collector.py  # 测试用例
├── requirements.txt      # 依赖
└── README.md             # 说明文档
```

## 安装依赖

```bash
pip install -r requirements.txt
```

## 使用方法

### 基本使用

```python
from data.collector import StockCollector
from data.storage import StockStorage

# 创建存储实例
storage = StockStorage("stocks.db")

# 创建采集器实例
collector = StockCollector(storage)

# 获取所有A股日线数据（增量更新）
collector.collect_all_stocks()

# 获取单个股票数据
collector.collect_stock("600519")  # 贵州茅台
```

### 自定义配置

```python
from data.collector import StockCollector
from data.storage import StockStorage

# 自定义数据库路径
storage = StockStorage("custom_path/stocks.db")

# 自定义采集器配置
collector = StockCollector(
    storage=storage,
    start_date="2020-01-01",  # 起始日期
    end_date="2024-12-31"     # 结束日期
)

# 获取单个股票数据
collector.collect_stock("000001")
```

## API 说明

### StockCollector

主要类，用于采集股票数据。

参数:
- `storage`: StockStorage 实例
- `start_date`: 可选，起始日期 (格式: YYYY-MM-DD)
- `end_date`: 可选，结束日期 (格式: YYYY-MM-DD)

方法:
- `collect_stock(symbol)`: 采集单个股票的数据
- `collect_all_stocks()`: 采集所有股票的数据（支持增量更新）

### StockStorage

用于管理 SQLite 数据库存储。

参数:
- `db_path`: 数据库文件路径

方法:
- `save_daily_data(symbol, data)`: 保存日线数据
- `get_latest_date(symbol)`: 获取指定股票的最新数据日期
- `get_all_symbols()`: 获取所有已存储的股票代码
- `create_tables()`: 创建数据库表

## 数据库结构

### daily_data 表

| 字段 | 类型 | 描述 |
|------|------|------|
| symbol | TEXT | 股票代码 |
| date | TEXT | 交易日期 |
| open | REAL | 开盘价 |
| high | REAL | 最高价 |
| low | REAL | 最低价 |
| close | REAL | 收盘价 |
| volume | REAL | 成交量 |
| amount | REAL | 成交额 |
| turnover_rate | REAL | 换手率 |
| PRIMARY KEY | (symbol, date) | |

## vn.py 数据迁移（Phase 1）

已新增 `vnpy_adapter/` 模块，用于把 `daily_data` 表迁移到 vn.py SQLite 标准表 `dbbardata`。

快速开始：

```bash
./venv/bin/python migrate_to_vnpy.py \
  --source-db stocks_eastmoney.db \
  --target-db vnpy_data.db \
  --mode full \
  --limit 10 \
  --verify
```

详情见：`vnpy_adapter/README.md`


## 依赖

- akshare>=1.11.0
- pandas>=1.5.0
- sqlalchemy>=2.0.0
- pytest>=7.0.0
