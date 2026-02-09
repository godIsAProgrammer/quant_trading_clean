from __future__ import annotations

from pathlib import Path
import json
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backtest.data_loader import VnpyBarDataLoader, validate_ohlc


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    db_path = project_root / "vnpy_data.db"

    loader = VnpyBarDataLoader(db_path=str(db_path))
    all_symbols = loader.list_symbols(interval="1d")
    sample_symbols = all_symbols[:10]

    report = {
        "db_path": str(db_path),
        "symbol_count_total": len(all_symbols),
        "symbol_count_checked": len(sample_symbols),
        "symbols": sample_symbols,
        "rows": {},
        "violations": {},
    }

    total_rows = 0
    total_bad = 0

    for vt_symbol in sample_symbols:
        df = loader.load_symbol(vt_symbol)
        checks = validate_ohlc(df)

        row_count = len(df)
        bad = checks.loc[~checks["ok"]]
        bad_count = len(bad)

        total_rows += row_count
        total_bad += bad_count

        report["rows"][vt_symbol] = row_count
        report["violations"][vt_symbol] = {
            "count": bad_count,
            "sample": bad.head(3).to_dict(orient="records"),
        }

    report["rows_total_checked"] = total_rows
    report["violations_total_checked"] = total_bad
    report["pass"] = total_bad == 0

    out_dir = project_root / "backtest" / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / "data_loader_validation.json"
    out_file.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(f"\n报告已写入: {out_file}")


if __name__ == "__main__":
    main()
