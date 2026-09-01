"""Run StormTrace data-quality checks and block unsafe Gold builds.

Each check is a named dataclass rather than a positional tuple, and its
comparison operator is looked up in OPERATORS. The previous version
inferred the operator with `value <= threshold if operator ==
"less_than_or_equal" else value > threshold`, so any typo in the operator
string silently inverted the check instead of failing loudly.

Severity decides consequences: an 'error' failure stops the pipeline
before Gold is rebuilt, a 'warning' is reported and allowed through.
"""

from __future__ import annotations

import csv
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from load_history import unloadable_files

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data" / "stormtrace.duckdb"
REPORT_DIR = ROOT / "data" / "quality"
JSON_REPORT = REPORT_DIR / "latest_report.json"
CSV_REPORT = REPORT_DIR / "latest_report.csv"

# Comparison operators, by name. An unknown name raises rather than
# defaulting to some other comparison.
OPERATORS = {
    "greater_than": lambda value, threshold: value > threshold,
    "greater_than_or_equal": lambda value, threshold: value >= threshold,
    "less_than": lambda value, threshold: value < threshold,
    "less_than_or_equal": lambda value, threshold: value <= threshold,
    "equals": lambda value, threshold: value == threshold,
}

# Every history table that Bronze registry rows must reconcile against.
HISTORY_TABLES = [
    "orbital_snapshot_history",
    "noaa_magnetic_history",
    "noaa_plasma_history",
    "space_weather_index_history",
    "satnogs_observation_history",
]


@dataclass(frozen=True)
class Check:
    name: str
    severity: str  # "error" stops the pipeline; "warning" only reports
    sql: str
    operator: str
    threshold: float
    description: str


def orphan_rows_sql() -> str:
    """Rows in any history table with no registered Bronze file."""
    parts = [
        f"(SELECT COUNT(*) FROM {table} h WHERE NOT EXISTS "
        f"(SELECT 1 FROM bronze_file_registry r WHERE r.file_path = h.source_file))"
        for table in HISTORY_TABLES
    ]
    return "SELECT " + " + ".join(parts)


def unreferenced_files_sql() -> str:
    """Registered Bronze files whose rows have vanished from history."""
    conditions = " AND ".join(
        f"NOT EXISTS (SELECT 1 FROM {table} h WHERE h.source_file = r.file_path)"
        for table in HISTORY_TABLES
    )
    return f"SELECT COUNT(*) FROM bronze_file_registry r WHERE {conditions}"

CHECKS = [
    Check(
        name="orbital_history_has_rows",
        severity="error",
        sql="SELECT COUNT(*) FROM orbital_snapshot_history",
        operator="greater_than",
        threshold=0,
        description="At least one orbital history record exists.",
    ),
    Check(
        name="duplicate_orbital_snapshot_objects",
        severity="error",
        sql="""
            SELECT COUNT(*) FROM (
                SELECT snapshot_at_utc, norad_catalog_id
                FROM orbital_snapshot_history
                GROUP BY 1, 2 HAVING COUNT(*) > 1
            )
        """,
        operator="less_than_or_equal",
        threshold=0,
        description="A snapshot has no duplicate NORAD identifiers.",
    ),
    Check(
        name="invalid_mean_motion_rows",
        severity="error",
        sql="""
            SELECT COUNT(*) FROM orbital_snapshot_history
            WHERE mean_motion_revolutions_per_day IS NULL
               OR mean_motion_revolutions_per_day <= 0
               OR mean_motion_revolutions_per_day > 20
        """,
        operator="less_than_or_equal",
        threshold=0,
        description="Mean motion is physically plausible for Earth orbit.",
    ),
    Check(
        name="invalid_eccentricity_rows",
        severity="error",
        sql="""
            SELECT COUNT(*) FROM orbital_snapshot_history
            WHERE eccentricity IS NULL OR eccentricity < 0 OR eccentricity >= 1
        """,
        operator="less_than_or_equal",
        threshold=0,
        description="Elliptical-orbit eccentricity is between zero and one.",
    ),
    Check(
        name="invalid_inclination_rows",
        severity="error",
        sql="""
            SELECT COUNT(*) FROM orbital_snapshot_history
            WHERE inclination_degrees IS NULL
               OR inclination_degrees < 0
               OR inclination_degrees > 180
        """,
        operator="less_than_or_equal",
        threshold=0,
        description="Inclination is between zero and 180 degrees.",
    ),
    Check(
        name="modified_bronze_paths",
        severity="error",
        sql="""
            SELECT COUNT(*) FROM (
                SELECT file_path FROM bronze_file_registry
                GROUP BY 1 HAVING COUNT(DISTINCT sha256) > 1
            )
        """,
        operator="less_than_or_equal",
        threshold=0,
        description="Each immutable Bronze path has one checksum.",
    ),
    Check(
        name="orphan_history_rows",
        severity="error",
        sql=orphan_rows_sql(),
        operator="less_than_or_equal",
        threshold=0,
        description=(
            "Every history row belongs to a registered Bronze file "
            "(no partial loads)."
        ),
    ),
    Check(
        name="registered_files_missing_rows",
        severity="error",
        sql=unreferenced_files_sql(),
        operator="less_than_or_equal",
        threshold=0,
        description="Every registered Bronze file still has its rows in history.",
    ),
    Check(
        name="unloadable_bronze_files",
        severity="warning",
        sql="SELECT 0",  # replaced at runtime by count_unloadable_bronze()
        operator="less_than_or_equal",
        threshold=0,
        description=(
            "Every file under data/bronze matches a loader pattern "
            "(no silently ignored evidence)."
        ),
    ),
    Check(
        name="magnetic_missing_bz_percent",
        severity="warning",
        sql="""
            SELECT COALESCE(
                100.0 * COUNT(*) FILTER (WHERE bz_gsm IS NULL)
                / NULLIF(COUNT(*), 0), 100.0)
            FROM noaa_magnetic_history
        """,
        operator="less_than_or_equal",
        threshold=10.0,
        description="No more than 10% of magnetic rows are missing Bz.",
    ),
    Check(
        name="plasma_missing_speed_percent",
        severity="warning",
        sql="""
            SELECT COALESCE(
                100.0 * COUNT(*) FILTER (WHERE proton_speed IS NULL)
                / NULLIF(COUNT(*), 0), 100.0)
            FROM noaa_plasma_history
        """,
        operator="less_than_or_equal",
        threshold=10.0,
        description="No more than 10% of plasma rows are missing proton speed.",
    ),
    Check(
        name="active_magnetic_rows",
        severity="warning",
        sql="SELECT COUNT(*) FROM noaa_magnetic_history WHERE is_active_source",
        operator="greater_than",
        threshold=0,
        description="NOAA identifies an active magnetic source.",
    ),
    Check(
        name="active_plasma_rows",
        severity="warning",
        sql="SELECT COUNT(*) FROM noaa_plasma_history WHERE is_active_source",
        operator="greater_than",
        threshold=0,
        description="NOAA identifies an active plasma source.",
    ),
]


def is_pass(value: float, operator: str, threshold: float) -> bool:
    """Evaluate one check. An unknown operator is a programming error."""
    try:
        comparison = OPERATORS[operator]
    except KeyError:
        raise ValueError(
            f"Unknown check operator {operator!r}. Known operators: "
            + ", ".join(sorted(OPERATORS))
        ) from None
    return comparison(value, threshold)



def observe(connection: duckdb.DuckDBPyConnection, check: Check) -> float:
    """Measure one check.

    The Bronze-coverage check is measured on the filesystem rather than in
    SQL, because a file that matches no loader pattern never reaches the
    database at all -- that is precisely why it went unnoticed.
    """
    if check.name == "unloadable_bronze_files":
        return float(len(unloadable_files()))
    return float(connection.sql(check.sql).fetchone()[0])


def evaluate(connection: duckdb.DuckDBPyConnection, checked_at: str) -> list[dict]:
    results = []
    for check in CHECKS:
        value = observe(connection, check)
        passed = is_pass(value, check.operator, check.threshold)
        status = "pass" if passed else ("fail" if check.severity == "error" else "warn")
        results.append(
            {
                "checked_at_utc": checked_at,
                "check_name": check.name,
                "status": status,
                "severity": check.severity,
                "observed_value": round(value, 6),
                "operator": check.operator,
                "threshold": check.threshold,
                "description": check.description,
            }
        )
    return results


def main() -> int:
    if not DATABASE.exists():
        print(
            "The DuckDB database is missing. Run src\\load_history.py first.",
            file=sys.stderr,
        )
        return 1

    checked_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    connection = duckdb.connect(str(DATABASE), read_only=True)
    try:
        results = evaluate(connection, checked_at)
    except duckdb.Error as error:
        print(f"Quality check error: {error}", file=sys.stderr)
        return 1
    finally:
        connection.close()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    JSON_REPORT.write_text(json.dumps(results, indent=2), encoding="utf-8")
    with CSV_REPORT.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(results[0]))
        writer.writeheader()
        writer.writerows(results)

    failures = [result for result in results if result["status"] == "fail"]
    warnings = [result for result in results if result["status"] == "warn"]
    print("StormTrace quality gate")
    for result in results:
        print(
            f"[{str(result['status']).upper():4}] {result['check_name']}: "
            f"{result['observed_value']}"
        )
    print(f"Checks: {len(results)}, failures: {len(failures)}, warnings: {len(warnings)}")
    print(f"JSON report: {JSON_REPORT.relative_to(ROOT)}")
    print(f"CSV report: {CSV_REPORT.relative_to(ROOT)}")
    if failures:
        print("Quality gate failed. Gold tables were not approved.", file=sys.stderr)
        return 1
    print("Quality gate passed. Gold tables may be built.")
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
