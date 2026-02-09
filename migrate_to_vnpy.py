#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from vnpy_adapter.sync_service import SyncService
from vnpy_adapter.symbol_mapper import SymbolMapper


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="将 quant_trading 的 daily_data 同步到 vn.py SQLite(dbbardata)")
    p.add_argument("--source-db", default="stocks_eastmoney.db", help="源数据库路径（含 daily_data 表）")
    p.add_argument("--target-db", default="vnpy_data.db", help="目标 vn.py SQLite 路径")
    p.add_argument("--symbols", nargs="*", help="指定股票列表，如 600519 000001.SZ")
    p.add_argument("--limit", type=int, default=None, help="仅同步前 N 只股票")
    p.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    p.add_argument("--volume-unit", choices=["share", "lot"], default="share", help="源成交量单位")
    p.add_argument("--verify", action="store_true", help="输出每只股票目标库记录数")
    return p


def main() -> None:
    args = build_parser().parse_args()

    service = SyncService(
        source_db_path=args.source_db,
        target_db_path=args.target_db,
        volume_unit=args.volume_unit,
    )

    symbols = args.symbols
    results = service.sync(
        symbols=symbols,
        incremental=(args.mode == "incremental"),
        limit=args.limit,
    )

    summary = {
        "symbols": len(results),
        "source_rows": sum(r.source_rows for r in results.values()),
        "written_rows": sum(r.written_rows for r in results.values()),
        "mode": args.mode,
        "target_db": str(Path(args.target_db).resolve()),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))

    if args.verify:
        rows = {}
        for s in results.keys():
            info = SymbolMapper.to_vnpy(s)
            rows[f"{info.symbol}.{info.exchange}"] = service.writer.get_count(info.symbol, info.exchange, "1d")
        print(json.dumps(rows, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
