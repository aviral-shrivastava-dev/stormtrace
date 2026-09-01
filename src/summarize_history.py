"""Create Gold summaries from the accumulated Bronze snapshot history."""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data" / "stormtrace.duckdb"
SQL_PATH = ROOT / "sql" / "lesson5_history.sql"
GOLD_DIR = ROOT / "data" / "gold"

EXPORTS = [
    ("gold_orbit_snapshot_summary", "orbit_snapshot_summary.csv"),
    ("gold_space_weather_snapshot_summary", "space_weather_snapshot_summary.csv"),
]


def main() -> int:
    if not DATABASE.exists():
        print(
            "The DuckDB database is missing. Run src\\load_history.py first.",
            file=sys.stderr,
        )
        return 1

    connection = duckdb.connect(str(DATABASE))
    try:
        connection.execute(SQL_PATH.read_text(encoding="utf-8"))
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        counts = {}
        for table, filename in EXPORTS:
            connection.execute(
                f"COPY {table} TO ? (HEADER, DELIMITER ',')",
                [str(GOLD_DIR / filename)],
            )
            counts[table] = connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
    except duckdb.Error as error:
        print(f"History summary error: {error}", file=sys.stderr)
        return 1
    finally:
        connection.close()

    print("Created historical summary Gold tables.")
    print(f"Orbit snapshots summarized: {counts['gold_orbit_snapshot_summary']:,}")
    print(
        "Space-weather snapshots summarized: "
        f"{counts['gold_space_weather_snapshot_summary']:,}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

