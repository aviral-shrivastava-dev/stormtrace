"""Create Gold summaries from the accumulated Bronze snapshot history."""

from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "stormtrace.duckdb"
SQL = ROOT / "sql" / "lesson5_history.sql"
GOLD = ROOT / "data" / "gold"


connection = duckdb.connect(str(DB))
try:
    connection.execute(SQL.read_text(encoding="utf-8"))
    GOLD.mkdir(parents=True, exist_ok=True)
    connection.execute("COPY gold_orbit_snapshot_summary TO ? (HEADER, DELIMITER ',')", [str(GOLD / "orbit_snapshot_summary.csv")])
    connection.execute("COPY gold_space_weather_snapshot_summary TO ? (HEADER, DELIMITER ',')", [str(GOLD / "space_weather_snapshot_summary.csv")])
    print("Created historical summary Gold tables.")
    orbit_snapshots = connection.sql(
        "SELECT COUNT(*) FROM gold_orbit_snapshot_summary"
    ).fetchone()[0]
    weather_snapshots = connection.sql(
        "SELECT COUNT(*) FROM gold_space_weather_snapshot_summary"
    ).fetchone()[0]
    print(f"Orbit snapshots summarized: {orbit_snapshots}")
    print(f"Space-weather snapshots summarized: {weather_snapshots}")
finally:
    connection.close()
