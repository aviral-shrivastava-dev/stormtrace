"""Measure SGP4 propagation disagreement between consecutive element sets.

For each object, when a refreshed element set (a later element with a
different epoch) becomes available, this script:

1. Builds an SGP4 satellite from the EARLIER element.
2. Propagates it forward to the LATER element's epoch.
3. Builds an SGP4 satellite from the LATER element and evaluates its
   position at its own epoch.
4. Measures the difference, decomposed into radial, along-track, and
   cross-track components in the reference orbit's RIC frame.

The result is the *public orbit propagation disagreement*: how far the old
public element set's prediction drifted from where the newer public element
set places the object. The later element is NOT perfect ground truth; this
measures disagreement between successive public estimates, which is the
honest, measurable quantity available from public data.

Pairing happens in SQL (a LAG window over each object's snapshots) rather
than by loading the whole history into Python.

Every measurement is labelled with `measurement_quality`: 'ok' inside the
SGP4 analysis envelope, 'long_span' when propagated too far to trust, and
'extreme' when the disagreement indicates a bad element rather than
physical drag. Consumers analyse 'ok' rows only; the rest stay in the
table as evidence.
"""

from __future__ import annotations

import math
import sys
from datetime import datetime
from pathlib import Path

import duckdb
from sgp4.api import WGS72, Satrec, jday

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data" / "stormtrace.duckdb"
GOLD_DIR = ROOT / "data" / "gold"
CSV_PATH = GOLD_DIR / "propagation_disagreement.csv"

TWO_PI = 2.0 * math.pi
MINUTES_PER_DAY = 1440.0

# Analysis-grade envelope for SGP4 disagreement measurements.
#
# SGP4 is a near-Earth model whose error grows quickly with propagation
# span, and public catalogs occasionally republish elements that make a
# pair physically meaningless. Silently dropping such pairs would hide
# evidence; averaging them in corrupts every downstream statistic (one
# 5,000 km pair moved the reported median and skewed the ML target). Each
# measurement therefore carries an explicit quality label:
#
#   ok         inside the envelope; used by research, validation, and ML
#   long_span  propagated further than SGP4 can be trusted
#   extreme    disagreement so large it indicates a bad element or model
#              breakdown, not a measurement of orbital drag
#
# Every consumer filters to 'ok' and reports how many pairs it excluded.
MAX_ANALYSIS_SPAN_HOURS = 72.0
MAX_ANALYSIS_TOTAL_KM = 500.0

# Element fields needed to rebuild an SGP4 satellite from a snapshot row.
ELEMENT_FIELDS = [
    "element_epoch_utc",
    "inclination_degrees",
    "eccentricity",
    "mean_motion_revolutions_per_day",
    "bstar_drag_term",
    "ra_of_asc_node_degrees",
    "arg_of_pericenter_degrees",
    "mean_anomaly_degrees",
    "mean_motion_dot",
    "mean_motion_ddot",
]


def measurement_quality(span_hours: float, total_km: float) -> str:
    """Label a measurement against the analysis envelope."""
    if total_km > MAX_ANALYSIS_TOTAL_KM:
        return "extreme"
    if span_hours > MAX_ANALYSIS_SPAN_HOURS:
        return "long_span"
    return "ok"


def pair_query() -> str:
    """Consecutive element-set pairs per object, paired inside DuckDB.

    A window function does the pairing that a Python dict of the whole
    history used to do: for every snapshot row, LAG carries the previous
    snapshot of the same object. Keeping only rows where the element epoch
    changed yields exactly the consecutive refreshed pairs, and the
    'earlier' side is the last snapshot that still carried the old
    element -- the point-in-time correct moment just before the refresh.
    """
    later_columns = ",\n        ".join(ELEMENT_FIELDS)
    earlier_columns = ",\n        ".join(
        f"LAG({field}) OVER w AS earlier_{field}" for field in ELEMENT_FIELDS
    )
    return f"""
    WITH ordered AS (
        SELECT
            object_name,
            norad_catalog_id,
            source_group,
            timezone('UTC', snapshot_at_utc) AS snapshot_at_utc,
            LAG(timezone('UTC', snapshot_at_utc)) OVER w
                AS earlier_snapshot_at_utc,
            {later_columns},
            {earlier_columns}
        FROM orbital_snapshot_history
        WHERE element_epoch_utc IS NOT NULL
          AND norad_catalog_id IS NOT NULL
        WINDOW w AS (PARTITION BY norad_catalog_id ORDER BY snapshot_at_utc)
    )
    SELECT *
    FROM ordered
    WHERE earlier_element_epoch_utc IS NOT NULL
      AND earlier_element_epoch_utc <> element_epoch_utc
    ORDER BY norad_catalog_id, snapshot_at_utc
    """



def epoch_to_julian(epoch: datetime) -> tuple[float, float]:
    return jday(
        epoch.year,
        epoch.month,
        epoch.day,
        epoch.hour,
        epoch.minute,
        epoch.second + epoch.microsecond / 1e6,
    )


def build_satellite(record: dict) -> Satrec | None:
    """Build an SGP4 satellite from an element record.

    Returns None when the element is unusable (missing fields or
    non-positive mean motion). Unit conversions:
      angles: degrees -> radians
      mean motion: rev/day -> rad/min
      derivatives: rev/day^n -> rad/min^n
      epoch: days since 1949-12-31
    """
    try:
        mean_motion = float(record["mean_motion_revolutions_per_day"])
        eccentricity = float(record["eccentricity"])
        inclination = float(record["inclination_degrees"])
        raan = float(record["ra_of_asc_node_degrees"])
        arg_per = float(record["arg_of_pericenter_degrees"])
        mean_anomaly = float(record["mean_anomaly_degrees"])
        bstar = float(record["bstar_drag_term"] or 0.0)
        ndot = float(record["mean_motion_dot"] or 0.0)
        nddot = float(record["mean_motion_ddot"] or 0.0)
        norad = int(record["norad_catalog_id"])
    except (TypeError, ValueError):
        return None

    if mean_motion <= 0 or not 0 <= eccentricity < 1:
        return None
    if not 0 <= inclination <= 180:
        return None

    epoch = record["element_epoch_utc"]
    jd, fr = epoch_to_julian(epoch)

    satellite = Satrec()
    satellite.sgp4init(
        WGS72,
        "i",
        norad,
        jd + fr - 2433281.5,
        bstar,
        ndot * TWO_PI / (MINUTES_PER_DAY**2),
        nddot * TWO_PI / (MINUTES_PER_DAY**3),
        eccentricity,
        math.radians(arg_per),
        math.radians(inclination),
        math.radians(mean_anomaly),
        mean_motion * TWO_PI / MINUTES_PER_DAY,
        math.radians(raan),
    )
    return satellite


def ric_components(
    reference_position: list[float],
    reference_velocity: list[float],
    difference: list[float],
) -> tuple[float, float, float]:
    """Decompose a position difference into radial/along/cross-track km."""
    r = reference_position
    v = reference_velocity

    r_mag = math.sqrt(r[0] * r[0] + r[1] * r[1] + r[2] * r[2])
    radial_unit = [x / r_mag for x in r]

    cross = [
        r[1] * v[2] - r[2] * v[1],
        r[2] * v[0] - r[0] * v[2],
        r[0] * v[1] - r[1] * v[0],
    ]
    cross_mag = math.sqrt(cross[0] ** 2 + cross[1] ** 2 + cross[2] ** 2)
    cross_unit = [x / cross_mag for x in cross]

    along_unit = [
        cross_unit[1] * radial_unit[2] - cross_unit[2] * radial_unit[1],
        cross_unit[2] * radial_unit[0] - cross_unit[0] * radial_unit[2],
        cross_unit[0] * radial_unit[1] - cross_unit[1] * radial_unit[0],
    ]

    radial = sum(d * u for d, u in zip(difference, radial_unit, strict=True))
    along = sum(d * u for d, u in zip(difference, along_unit, strict=True))
    cross_track = sum(d * u for d, u in zip(difference, cross_unit, strict=True))
    return radial, along, cross_track



def main() -> int:
    if not DATABASE.exists():
        print("Run earlier lessons first. The DuckDB database is missing.", file=sys.stderr)
        return 1

    connection = duckdb.connect(str(DATABASE))
    try:
        relation = connection.execute(pair_query())
        columns = [description[0] for description in relation.description]
        pairs = [dict(zip(columns, row, strict=True)) for row in relation.fetchall()]

        tracked_objects = connection.execute(
            "SELECT COUNT(DISTINCT norad_catalog_id) FROM orbital_snapshot_history "
            "WHERE element_epoch_utc IS NOT NULL AND norad_catalog_id IS NOT NULL"
        ).fetchone()[0]
        awaiting = connection.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT norad_catalog_id
                FROM orbital_snapshot_history
                WHERE element_epoch_utc IS NOT NULL
                  AND norad_catalog_id IS NOT NULL
                GROUP BY norad_catalog_id
                HAVING COUNT(DISTINCT element_epoch_utc) < 2
            )
            """
        ).fetchone()[0]

        measurements: list[tuple] = []
        skipped = 0
        retrograde_pairs = 0
        quality_counts = {"ok": 0, "long_span": 0, "extreme": 0}

        for pair in pairs:
            later = {field: pair[field] for field in ELEMENT_FIELDS}
            earlier = {
                field: pair[f"earlier_{field}"] for field in ELEMENT_FIELDS
            }
            earlier["norad_catalog_id"] = pair["norad_catalog_id"]
            later["norad_catalog_id"] = pair["norad_catalog_id"]

            span_hours = (
                later["element_epoch_utc"] - earlier["element_epoch_utc"]
            ).total_seconds() / 3600.0
            if span_hours <= 0:
                # The catalog republished an element whose epoch is older
                # than the previously seen element (a retrograde update).
                # There is no forward prediction to evaluate, so the pair
                # is not a measurement; it is counted and reported as a
                # catalog quirk instead of silently dropped.
                retrograde_pairs += 1
                continue

            earlier_satellite = build_satellite(earlier)
            later_satellite = build_satellite(later)
            if earlier_satellite is None or later_satellite is None:
                skipped += 1
                continue

            jd_later, fr_later = epoch_to_julian(later["element_epoch_utc"])
            error_a, position_predicted, _ = earlier_satellite.sgp4(jd_later, fr_later)
            error_b, position_reference, velocity_reference = later_satellite.sgp4(
                jd_later, fr_later
            )
            if error_a != 0 or error_b != 0:
                skipped += 1
                continue

            difference = [
                position_predicted[i] - position_reference[i] for i in range(3)
            ]
            radial, along, cross = ric_components(
                position_reference, velocity_reference, difference
            )
            total = math.sqrt(sum(d * d for d in difference))
            quality = measurement_quality(span_hours, total)
            quality_counts[quality] += 1

            measurements.append(
                (
                    pair["object_name"],
                    pair["norad_catalog_id"],
                    pair["source_group"],
                    earlier["element_epoch_utc"],
                    later["element_epoch_utc"],
                    round(span_hours, 3),
                    round(radial, 4),
                    round(along, 4),
                    round(cross, 4),
                    round(total, 4),
                    pair["earlier_snapshot_at_utc"],
                    pair["snapshot_at_utc"],
                    quality,
                )
            )

        connection.execute(
            """
            CREATE OR REPLACE TABLE gold_propagation_disagreement (
                object_name VARCHAR,
                norad_catalog_id BIGINT,
                source_group VARCHAR,
                earlier_element_epoch_utc TIMESTAMP,
                later_element_epoch_utc TIMESTAMP,
                propagation_span_hours DOUBLE,
                radial_km DOUBLE,
                along_track_km DOUBLE,
                cross_track_km DOUBLE,
                total_km DOUBLE,
                earlier_snapshot_at_utc TIMESTAMP,
                later_snapshot_at_utc TIMESTAMP,
                measurement_quality VARCHAR
            )
            """
        )
        if measurements:
            connection.executemany(
                "INSERT INTO gold_propagation_disagreement VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                measurements,
            )
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        connection.execute(
            "COPY gold_propagation_disagreement TO ? (HEADER, DELIMITER ',')",
            [str(CSV_PATH)],
        )
    except duckdb.Error as error:
        print(f"Propagation disagreement error: {error}", file=sys.stderr)
        return 1
    finally:
        connection.close()

    analysis_grade = [m for m in measurements if m[12] == "ok"]

    print("StormTrace lesson 15 propagation disagreement")
    print(f"Objects with element history: {tracked_objects:,}")
    print(f"Objects awaiting a refreshed element set: {awaiting:,}")
    print(f"Measurable element-set pairs: {len(measurements)}")
    if retrograde_pairs:
        print(
            f"Retrograde epoch pairs (republished with older epoch, not "
            f"measurable): {retrograde_pairs}"
        )
    if skipped:
        print(f"Pairs skipped (unusable elements or SGP4 errors): {skipped}")
    if quality_counts["long_span"] or quality_counts["extreme"]:
        print(
            f"Outside the analysis envelope: {quality_counts['long_span']} "
            f"long_span (> {MAX_ANALYSIS_SPAN_HOURS:.0f} h), "
            f"{quality_counts['extreme']} extreme "
            f"(> {MAX_ANALYSIS_TOTAL_KM:.0f} km). Kept in the table as "
            "evidence, excluded from analysis."
        )
    if analysis_grade:
        totals = sorted(m[9] for m in analysis_grade)
        median = totals[len(totals) // 2]
        spans = [m[5] for m in analysis_grade]
        along = sorted(m[7] for m in analysis_grade)
        print(f"Analysis-grade pairs: {len(analysis_grade)}")
        print(f"Disagreement total km: min {totals[0]:.3f}, median {median:.3f}, max {totals[-1]:.3f}")
        print(f"Along-track km: median {along[len(along) // 2]:.3f}, max {along[-1]:.3f}")
        print(f"Propagation spans: {min(spans):.2f} h to {max(spans):.2f} h")
        print("Gold table and CSV updated with measurements.")
    else:
        print("No analysis-grade pairs yet: all consecutive element sets are")
        print("identical republished data or fall outside the SGP4 envelope.")
        print("This instrument reports nothing rather than reporting a fake")
        print("zero. Once the catalog refreshes element epochs, real")
        print("disagreement measurements appear here automatically.")
    print(f"CSV: {CSV_PATH.relative_to(ROOT)}")
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
