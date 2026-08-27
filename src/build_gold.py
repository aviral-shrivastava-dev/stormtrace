"""Run the first StormTrace SQL transformation with DuckDB."""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("DuckDB is not installed yet. Run: python -m pip install duckdb")
    raise SystemExit(1)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE_PATH = PROJECT_ROOT / "data" / "stormtrace.duckdb"
SQL_PATH = PROJECT_ROOT / "sql" / "lesson3_gold.sql"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
GOLD_PATH = GOLD_DIR / "space_weather_minute.csv"


def main() -> int:
    silver_dir = PROJECT_ROOT / "data" / "silver"
    required_files = [
        silver_dir / "noaa_magnetic_field_latest.csv",
        silver_dir / "noaa_plasma_latest.csv",
    ]
    missing_files = [path for path in required_files if not path.exists()]
    if missing_files:
        print("Run Lesson 2 first. Missing:", file=sys.stderr)
        for path in missing_files:
            print(f"  {path.relative_to(PROJECT_ROOT)}", file=sys.stderr)
        return 1

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(DATABASE_PATH))
    try:
        connection.execute(SQL_PATH.read_text(encoding="utf-8"))
        connection.execute(
            "COPY gold_space_weather_minute TO ? (HEADER, DELIMITER ',')",
            [str(GOLD_PATH)],
        )
        count = connection.sql("SELECT COUNT(*) FROM gold_space_weather_minute").fetchone()[0]
        matched = connection.sql(
            """
            SELECT COUNT(*)
            FROM gold_space_weather_minute
            WHERE plasma_observed_at_utc IS NOT NULL
            """
        ).fetchone()[0]
    finally:
        connection.close()

    print("StormTrace lesson 3")
    print(f"Gold rows: {count:,}")
    print(f"Rows with plasma match: {matched:,}")
    print(f"Database: {DATABASE_PATH.relative_to(PROJECT_ROOT)}")
    print(f"Gold CSV: {GOLD_PATH.relative_to(PROJECT_ROOT)}")
    print("Next lesson: query this Gold table with SQL.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
