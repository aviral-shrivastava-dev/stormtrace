"""Build research-ready satellite orbit features with DuckDB."""

from __future__ import annotations

import sys
from pathlib import Path

try:
    import duckdb
except ImportError as error:
    print("DuckDB is not installed. Run: python -m pip install duckdb")
    raise SystemExit(1) from error



PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
DATABASE_PATH = PROJECT_ROOT / "data" / "stormtrace.duckdb"
SQL_PATH = PROJECT_ROOT / "sql" / "lesson4_orbit_features.sql"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
FEATURES_PATH = GOLD_DIR / "satellite_orbit_features.csv"
SUMMARY_PATH = GOLD_DIR / "orbit_band_summary.csv"


def main() -> int:
    silver_files = sorted(SILVER_DIR.glob("*_satellites_latest.csv"))
    if not silver_files:
        print("Run Lesson 1 first. No satellite Silver files found.", file=sys.stderr)
        return 1

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(DATABASE_PATH))
    try:
        connection.execute(SQL_PATH.read_text(encoding="utf-8"))
        connection.execute(
            "COPY gold_satellite_orbit_features TO ? (HEADER, DELIMITER ',')",
            [str(FEATURES_PATH)],
        )
        connection.execute(
            "COPY gold_orbit_band_summary TO ? (HEADER, DELIMITER ',')",
            [str(SUMMARY_PATH)],
        )
        object_count = connection.sql(
            "SELECT COUNT(*) FROM gold_satellite_orbit_features"
        ).fetchone()[0]
        stale_count = connection.sql(
            """
            SELECT COUNT(*) FROM gold_satellite_orbit_features
            WHERE is_stale_over_24_hours
            """
        ).fetchone()[0]
        bands = connection.sql(
            """
            SELECT altitude_band, object_count, average_altitude_km
            FROM gold_orbit_band_summary
            ORDER BY average_altitude_km
            """
        ).fetchall()
        groups = connection.sql(
            """
            SELECT source_group, COUNT(*) AS object_count
            FROM gold_satellite_orbit_features
            GROUP BY source_group
            ORDER BY source_group
            """
        ).fetchall()
    finally:
        connection.close()

    print("StormTrace lesson 4")
    print(f"Objects with valid orbital features: {object_count:,}")
    for group, count in groups:
        print(f"  {group} group: {count:,} objects")
    print(f"Objects stale by more than 24 hours: {stale_count:,}")
    for band, count, altitude in bands:
        print(f"  {band}: {count} objects, average {altitude:.2f} km")
    print(f"Features: {FEATURES_PATH.relative_to(PROJECT_ROOT)}")
    print(f"Summary: {SUMMARY_PATH.relative_to(PROJECT_ROOT)}")
    print("Next lesson: collect snapshots so changes can be measured over time.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
