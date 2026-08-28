"""StormTrace status API: expose research outputs as JSON.

Run from the project root:

    python -m uvicorn src.api:app --port 8000

Then open http://127.0.0.1:8000/docs for interactive documentation.

Design notes:

- Every endpoint opens its own read-only DuckDB connection. DuckDB allows
  either one writer OR multiple readers, so while the hourly pipeline holds
  the write lock, endpoints return 503 with a clear message instead of
  crashing. This is the honest behavior for a single-file database.
- Queries never fetch TIMESTAMPTZ values into Python (timezone('UTC', ...)
  casts keep timestamps naive UTC), avoiding the pytz dependency issue.
- The API is read-only and adds no new state; it is a view of the Gold
  layer plus the latest quality report.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data" / "stormtrace.duckdb"
QUALITY_REPORT = ROOT / "data" / "quality" / "latest_report.json"

app = FastAPI(
    title="StormTrace Status API",
    description=(
        "Public orbit reliability and space-weather research outputs. "
        "The Orbit Reliability Index is a trust signal for public orbit "
        "data; it is NOT collision probability and NOT a measurement of "
        "true position error."
    ),
    version="0.1.0",
)


class DatabaseBusy(HTTPException):
    def __init__(self) -> None:
        super().__init__(
            status_code=503,
            detail=(
                "The pipeline is currently writing to the database "
                "(DuckDB allows one writer at a time). Retry in a few seconds."
            ),
        )


@contextmanager
def database() -> Iterator[duckdb.DuckDBPyConnection]:
    """Yield a read-only connection or raise 503 when the writer holds it."""
    if not DATABASE.exists():
        raise HTTPException(status_code=503, detail="Database not built yet.")
    try:
        connection = duckdb.connect(str(DATABASE), read_only=True)
    except duckdb.Error:
        raise DatabaseBusy()
    try:
        yield connection
    finally:
        connection.close()


def table_exists(connection: duckdb.DuckDBPyConnection, name: str) -> bool:
    return (
        connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [name],
        ).fetchone()[0]
        > 0
    )


def rows(connection: duckdb.DuckDBPyConnection, sql: str) -> list[tuple]:
    return connection.execute(sql).fetchall()


def now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "service": "StormTrace Status API",
        "description": (
            "Space-weather-aware reliability of public satellite orbit data."
        ),
        "endpoints": [
            "/health",
            "/quality",
            "/space-weather",
            "/population",
            "/reliability",
            "/reliability/{{norad_catalog_id}}",
            "/disagreement",
            "/validation",
        ],
        "docs": "/docs",
        "disclaimer": (
            "Research outputs from public data. Not collision probability. "
            "Not a replacement for official conjunction warnings."
        ),
    }


@app.get("/health")
def health() -> dict[str, Any]:
    try:
        with database() as connection:
            counts: dict[str, int] = {}
            for table in [
                "orbital_snapshot_history",
                "noaa_magnetic_history",
                "noaa_plasma_history",
                "gold_propagation_disagreement",
                "gold_ori_validation_pairs",
            ]:
                counts[table] = (
                    connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    if table_exists(connection, table)
                    else 0
                )
            return {
                "status": "ok",
                "checked_at_utc": now_utc(),
                "table_rows": counts,
            }
    except DatabaseBusy:
        raise
    except duckdb.Error as error:
        return JSONResponse(
            status_code=503,
            content={"status": "degraded", "error": str(error)},
        )


@app.get("/quality")
def quality() -> dict[str, Any]:
    if not QUALITY_REPORT.exists():
        raise HTTPException(
            status_code=404,
            detail="No quality report yet. Run the pipeline first.",
        )
    report = json.loads(QUALITY_REPORT.read_text(encoding="utf-8"))
    failures = [c for c in report if c["status"] == "fail"]
    warnings = [c for c in report if c["status"] == "warn"]
    return {
        "checked_at_utc": report[0]["checked_at_utc"] if report else None,
        "status": "fail" if failures else ("warn" if warnings else "pass"),
        "checks_total": len(report),
        "failures": len(failures),
        "warnings": len(warnings),
        "checks": report,
    }


@app.get("/space-weather")
def space_weather() -> dict[str, Any]:
    with database() as connection:
        if not table_exists(connection, "gold_space_weather_hourly"):
            raise HTTPException(status_code=404, detail="No space-weather data yet.")
        relation = connection.execute(
            """
            SELECT
                COUNT(*) AS hour_count,
                COUNT(*) FILTER (WHERE disturbance_level = 'quiet') AS quiet_hours,
                COUNT(*) FILTER (WHERE disturbance_level = 'southward_bz') AS southward_bz_hours,
                COUNT(*) FILTER (WHERE disturbance_level = 'fast_wind') AS fast_wind_hours,
                ROUND(MIN(minimum_bz_gsm), 2) AS minimum_bz_nt,
                ROUND(MAX(average_proton_speed), 2) AS max_avg_proton_speed_km_s,
                CAST(MAX(hour_utc) AS VARCHAR) AS through_hour_utc
            FROM gold_space_weather_hourly
            """
        )
        columns = [d[0] for d in relation.description]
        row = relation.fetchone()
        return dict(zip(columns, row))


@app.get("/population")
def population() -> dict[str, Any]:
    with database() as connection:
        groups = [
            {
                "source_group": row[0],
                "object_count": row[1],
                "median_element_age_hours": row[2],
                "stale_percent": row[3],
            }
            for row in connection.execute(
                """
                SELECT source_group, object_count, median_age_hours, stale_percent
                FROM gold_freshness_by_group
                ORDER BY source_group
                """
            ).fetchall()
        ]
        snapshots = connection.execute(
            "SELECT COUNT(DISTINCT snapshot_at_utc) FROM orbital_snapshot_history"
        ).fetchone()[0]
        tracked = connection.execute(
            "SELECT COUNT(DISTINCT norad_catalog_id) FROM orbital_snapshot_history"
        ).fetchone()[0]
        return {
            "orbital_snapshots": snapshots,
            "distinct_objects_ever_tracked": tracked,
            "groups": groups,
        }


@app.get("/reliability")
def reliability() -> dict[str, Any]:
    with database() as connection:
        if not table_exists(connection, "gold_orbit_reliability_index"):
            raise HTTPException(status_code=404, detail="No reliability data yet.")
        classes = [
            {"reliability_class": row[0], "object_count": row[1], "median_index": row[2]}
            for row in connection.execute(
                """
                SELECT reliability_class, object_count, median_index
                FROM gold_reliability_class_summary
                ORDER BY median_index
                """
            ).fetchall()
        ]
        groups = [
            {
                "source_group": row[0],
                "object_count": row[1],
                "median_index": row[2],
                "low_count": row[3],
                "reduced_count": row[4],
            }
            for row in connection.execute(
                """
                SELECT source_group, object_count, median_index, low_count, reduced_count
                FROM gold_reliability_group_summary
                ORDER BY source_group
                """
            ).fetchall()
        ]
        least_reliable = [
            {
                "object_name": row[0],
                "norad_catalog_id": row[1],
                "orbit_reliability_index": row[2],
                "element_age_hours": row[3],
                "reliability_class": row[4],
            }
            for row in connection.execute(
                """
                SELECT object_name, norad_catalog_id, orbit_reliability_index,
                       element_age_hours, reliability_class
                FROM gold_orbit_reliability_index
                ORDER BY orbit_reliability_index
                LIMIT 10
                """
            ).fetchall()
        ]
        environment = connection.execute(
            "SELECT environment_factor FROM gold_orbit_reliability_index LIMIT 1"
        ).fetchone()
        return {
            "generated_at_utc": now_utc(),
            "environment_factor": environment[0] if environment else None,
            "formula": (
                "ORI = (0.55 * freshness_score + 0.45 * drag_safety) "
                "* environment_factor"
            ),
            "class_distribution": classes,
            "groups": groups,
            "least_reliable_objects": least_reliable,
            "disclaimer": (
                "Prototype indicator of public-orbit trustworthiness. "
                "Not collision probability. Weights are documented "
                "prototype choices."
            ),
        }


@app.get("/reliability/{norad_catalog_id}")
def reliability_object(norad_catalog_id: int) -> dict[str, Any]:
    with database() as connection:
        if not table_exists(connection, "gold_orbit_reliability_index"):
            raise HTTPException(status_code=404, detail="No reliability data yet.")
        relation = connection.execute(
            """
            SELECT object_name, norad_catalog_id, source_group,
                   element_age_hours, mean_altitude_km, freshness_score,
                   drag_safety_score, base_score, orbit_reliability_index,
                   reliability_class
            FROM gold_orbit_reliability_index
            WHERE norad_catalog_id = ?
            """,
            [norad_catalog_id],
        )
        columns = [d[0] for d in relation.description]
        row = relation.fetchone()
        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"Object {norad_catalog_id} is not currently tracked.",
            )
        return dict(zip(columns, row))


@app.get("/disagreement")
def disagreement() -> dict[str, Any]:
    with database() as connection:
        if not table_exists(connection, "gold_propagation_disagreement"):
            raise HTTPException(
                status_code=404,
                detail="No propagation disagreement measurements yet.",
            )
        relation = connection.execute(
            """
            SELECT
                COUNT(*) AS measured_pairs,
                ROUND(MEDIAN(propagation_span_hours), 2) AS median_span_hours,
                ROUND(MAX(propagation_span_hours), 2) AS max_span_hours,
                ROUND(MEDIAN(total_km), 3) AS median_total_km,
                ROUND(MAX(total_km), 3) AS max_total_km,
                ROUND(MEDIAN(along_track_km), 3) AS median_along_track_km,
                ROUND(MEDIAN(radial_km), 3) AS median_radial_km,
                ROUND(MEDIAN(cross_track_km), 3) AS median_cross_track_km
            FROM gold_propagation_disagreement
            """
        )
        columns = [d[0] for d in relation.description]
        stats = dict(zip(columns, relation.fetchone()))
        largest = [
            {
                "object_name": row[0],
                "source_group": row[1],
                "span_hours": row[2],
                "total_km": row[3],
            }
            for row in connection.execute(
                """
                SELECT object_name, source_group,
                       ROUND(propagation_span_hours, 1), ROUND(total_km, 2)
                FROM gold_propagation_disagreement
                ORDER BY total_km DESC
                LIMIT 5
                """
            ).fetchall()
        ]
        return {
            "definition": (
                "SGP4 drift between consecutive public element sets; "
                "the later element is not perfect ground truth."
            ),
            **stats,
            "largest_measurements": largest,
        }


@app.get("/validation")
def validation() -> dict[str, Any]:
    with database() as connection:
        if not table_exists(connection, "gold_ori_validation_stats"):
            raise HTTPException(
                status_code=404,
                detail="No validation data yet. Refreshed element sets required.",
            )
        stats = dict(
            connection.execute(
                "SELECT metric, value FROM gold_ori_validation_stats"
            ).fetchall()
        )
        bins = [
            {
                "predicted_class": row[0],
                "pairs": row[1],
                "median_total_km": row[2],
                "p90_total_km": row[3],
                "median_km_per_hour": row[4],
            }
            for row in connection.execute(
                """
                SELECT reliability_class, pair_count, median_total_km,
                       p90_total_km, median_km_per_hour
                FROM gold_ori_validation_bins
                ORDER BY pair_count
                """
            ).fetchall()
        ]
        return {
            "method": (
                "Point-in-time correct: scores reconstructed as they stood "
                "at the earlier element's snapshot, then compared with the "
                "measured disagreement when the refresh arrived."
            ),
            "spearman_correlations": {
                key: value
                for key, value in stats.items()
                if key.startswith("spearman")
            },
            "drag_like_pairs": stats.get("drag_like_pairs"),
            "validated_pairs": stats.get("pairs"),
            "class_bins": bins,
        }
