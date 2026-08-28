"""Run StormTrace data-quality checks and block unsafe Gold builds."""

from __future__ import annotations

import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data" / "stormtrace.duckdb"
REPORT_DIR = ROOT / "data" / "quality"
JSON_REPORT = REPORT_DIR / "latest_report.json"
CSV_REPORT = REPORT_DIR / "latest_report.csv"

CHECKS = [
    ("orbital_history_has_rows", "error", "SELECT COUNT(*) FROM orbital_snapshot_history", "greater_than", 0, "At least one orbital history record exists."),
    ("duplicate_orbital_snapshot_objects", "error", "SELECT COUNT(*) FROM (SELECT snapshot_at_utc, norad_catalog_id FROM orbital_snapshot_history GROUP BY 1, 2 HAVING COUNT(*) > 1)", "less_than_or_equal", 0, "A snapshot has no duplicate NORAD identifiers."),
    ("invalid_mean_motion_rows", "error", "SELECT COUNT(*) FROM orbital_snapshot_history WHERE mean_motion_revolutions_per_day IS NULL OR mean_motion_revolutions_per_day <= 0 OR mean_motion_revolutions_per_day > 20", "less_than_or_equal", 0, "Mean motion is physically plausible for Earth orbit."),
    ("invalid_eccentricity_rows", "error", "SELECT COUNT(*) FROM orbital_snapshot_history WHERE eccentricity IS NULL OR eccentricity < 0 OR eccentricity >= 1", "less_than_or_equal", 0, "Elliptical-orbit eccentricity is between zero and one."),
    ("invalid_inclination_rows", "error", "SELECT COUNT(*) FROM orbital_snapshot_history WHERE inclination_degrees IS NULL OR inclination_degrees < 0 OR inclination_degrees > 180", "less_than_or_equal", 0, "Inclination is between zero and 180 degrees."),
    ("modified_bronze_paths", "error", "SELECT COUNT(*) FROM (SELECT file_path FROM bronze_file_registry GROUP BY 1 HAVING COUNT(DISTINCT sha256) > 1)", "less_than_or_equal", 0, "Each immutable Bronze path has one checksum."),
    ("orphan_history_rows", "error", "SELECT (SELECT COUNT(*) FROM orbital_snapshot_history h WHERE NOT EXISTS (SELECT 1 FROM bronze_file_registry r WHERE r.file_path = h.source_file)) + (SELECT COUNT(*) FROM noaa_magnetic_history h WHERE NOT EXISTS (SELECT 1 FROM bronze_file_registry r WHERE r.file_path = h.source_file)) + (SELECT COUNT(*) FROM noaa_plasma_history h WHERE NOT EXISTS (SELECT 1 FROM bronze_file_registry r WHERE r.file_path = h.source_file))", "less_than_or_equal", 0, "Every history row belongs to a registered Bronze file (no partial loads)."),
    ("registered_files_missing_rows", "error", "SELECT COUNT(*) FROM bronze_file_registry r WHERE NOT EXISTS (SELECT 1 FROM noaa_magnetic_history h WHERE h.source_file = r.file_path) AND NOT EXISTS (SELECT 1 FROM noaa_plasma_history h WHERE h.source_file = r.file_path) AND NOT EXISTS (SELECT 1 FROM orbital_snapshot_history h WHERE h.source_file = r.file_path)", "less_than_or_equal", 0, "Every registered Bronze file still has its rows in history."),
    ("magnetic_missing_bz_percent", "warning", "SELECT COALESCE(100.0 * COUNT(*) FILTER (WHERE bz_gsm IS NULL) / NULLIF(COUNT(*), 0), 100.0) FROM noaa_magnetic_history", "less_than_or_equal", 10.0, "No more than 10% of magnetic rows are missing Bz."),
    ("plasma_missing_speed_percent", "warning", "SELECT COALESCE(100.0 * COUNT(*) FILTER (WHERE proton_speed IS NULL) / NULLIF(COUNT(*), 0), 100.0) FROM noaa_plasma_history", "less_than_or_equal", 10.0, "No more than 10% of plasma rows are missing proton speed."),
    ("active_magnetic_rows", "warning", "SELECT COUNT(*) FROM noaa_magnetic_history WHERE is_active_source", "greater_than", 0, "NOAA identifies an active magnetic source."),
    ("active_plasma_rows", "warning", "SELECT COUNT(*) FROM noaa_plasma_history WHERE is_active_source", "greater_than", 0, "NOAA identifies an active plasma source."),
]


def is_pass(value: float, operator: str, threshold: float) -> bool:
    return value <= threshold if operator == "less_than_or_equal" else value > threshold


def main() -> int:
    if not DATABASE.exists():
        print("Run Lesson 5 first. The DuckDB database is missing.", file=sys.stderr)
        return 1

    checked_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    results = []
    connection = duckdb.connect(str(DATABASE), read_only=True)
    try:
        for name, severity, sql, operator, threshold, description in CHECKS:
            value = float(connection.sql(sql).fetchone()[0])
            passed = is_pass(value, operator, threshold)
            status = "pass" if passed else ("fail" if severity == "error" else "warn")
            results.append({
                "checked_at_utc": checked_at,
                "check_name": name,
                "status": status,
                "severity": severity,
                "observed_value": round(value, 6),
                "operator": operator,
                "threshold": threshold,
                "description": description,
            })
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
    print("StormTrace lesson 7 quality gate")
    for result in results:
        print(f"[{str(result['status']).upper():4}] {result['check_name']}: {result['observed_value']}")
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
