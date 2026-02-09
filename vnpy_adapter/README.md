# vnpy_adapter 使用说明

将 `quant_trading` 项目的 `daily_data`（akshare/东方财富采集）迁移到 vn.py SQLite 标准表 `dbbardata`。

## 1. 模块结构

- `symbol_mapper.py`：股票代码和交易所映射
- `bar_transformer.py`：DataFrame -> BarRecord（vn.py bar字段）
- `database_writer.py`：写入 SQLite `dbbardata`（带唯一索引）
- `sync_service.py`：全量/增量同步服务
- `migrate_to_vnpy.py`：可执行迁移脚本

## 2. 数据约定

- 时区：`Asia/Shanghai`
- 周期：日线，`interval=1d`
- 交易所：
  - 代码 `5/6/9` 开头 -> `SSE`
  - 代码 `0/1/2/3` 开头 -> `SZSE`
- 复权：源数据使用前复权（由采集层保证）
- 成交量单位：默认 `share`（股）
  - 若源数据为“手”，执行脚本时加 `--volume-unit lot`

## 3. 运行示例

### 全量迁移（前10只）

```bash
./venv/bin/python migrate_to_vnpy.py \
  --source-db stocks_eastmoney.db \
  --target-db vnpy_data.db \
  --mode full \
  --limit 10 \
  --verify
```

### 增量同步（全部股票）

```bash
./venv/bin/python migrate_to_vnpy.py \
  --source-db stocks_eastmoney.db \
  --target-db vnpy_data.db \
  --mode incremental \
  --verify
```

### 指定股票同步

```bash
./venv/bin/python migrate_to_vnpy.py \
  --source-db stocks_eastmoney.db \
  --target-db vnpy_data.db \
  --symbols 600519 000001.SZ 300750 \
  --mode full \
  --verify
```

## 4. vn.py 配置

本项目提供 `vt_setting.json` 示例：

```json
{
  "database.timezone": "Asia/Shanghai",
  "database.name": "sqlite",
  "database.database": "vnpy_data.db"
}
```

若你要复用现有库，也可把 `database.database` 指向已有数据库文件路径。

## 5. 验证 SQL

```sql
-- 查看总条数
SELECT COUNT(*) FROM dbbardata;

-- 查看单只股票范围
SELECT symbol, exchange, MIN(datetime), MAX(datetime), COUNT(*)
FROM dbbardata
WHERE symbol='600519' AND exchange='SSE' AND interval='1d';
```
