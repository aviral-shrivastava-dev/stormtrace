"""Build the minute-level Gold space-weather table with DuckDB."""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import duckdb
except ImportError as error:
    print("DuckDB is not installed yet. Run: python -m pip install duckdb")
    raise SystemExit(1) from error



PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE_PATH = PROJECT_ROOT / "data" / "stormtrace.duckdb"
SQL_PATH = PROJECT_ROOT / "sql" / "lesson3_gold.sql"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
GOLD_PATH = GOLD_DIR / "space_weather_minute.csv"

REQUIRED_TABLES = ["noaa_magnetic_history", "noaa_plasma_history"]


def table_exists(connection: duckdb.DuckDBPyConnection, name: str) -> bool:
    return (
        connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
        ).fetchone()[0]
        > 0
    )


def main() -> int:
    if not DATABASE_PATH.exists():
        print(
            "The DuckDB database is missing. Run src\\load_history.py first.",
            file=sys.stderr,
        )
        return 1

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(DATABASE_PATH))
    try:
        # Gold reads the accumulated history tables, not the Silver
        # latest-CSV files: NOAA's feed is a 24-hour rolling window, so
        # Silver can never describe more than one day.
        missing = [t for t in REQUIRED_TABLES if not table_exists(connection, t)]
        if missing:
            print(
                "Run src\\load_history.py first. Missing history tables: "
                + ", ".join(missing),
                file=sys.stderr,
            )
            return 1

        connection.execute(SQL_PATH.read_text(encoding="utf-8"))
        connection.execute(
            "COPY gold_space_weather_minute TO ? (HEADER, DELIMITER ',')",
            [str(GOLD_PATH)],
        )
        count, matched, first_minute, last_minute = connection.execute(
            """
            SELECT
                COUNT(*),
                COUNT(*) FILTER (WHERE plasma_observed_at_utc IS NOT NULL),
                MIN(observation_minute_utc),
                MAX(observation_minute_utc)
            FROM gold_space_weather_minute
            """
        ).fetchone()
        source_rows = connection.execute(
            "SELECT (SELECT COUNT(*) FROM noaa_magnetic_history) "
            "+ (SELECT COUNT(*) FROM noaa_plasma_history)"
        ).fetchone()[0]
    except duckdb.Error as error:
        print(f"Gold build error: {error}", file=sys.stderr)
        return 1
    finally:
        connection.close()

    print("StormTrace Gold space weather (minute level)")
    print(f"Gold rows: {count:,}")
    print(f"Rows with plasma match: {matched:,}")
    print(f"Coverage: {first_minute} .. {last_minute}")
    print(
        f"Deduplicated from {source_rows:,} history rows "
        "(overlapping rolling-window snapshots)."
    )
    print(f"Database: {DATABASE_PATH.relative_to(PROJECT_ROOT)}")
    print(f"Gold CSV: {GOLD_PATH.relative_to(PROJECT_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

