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
"""

from __future__ import annotations

import math
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb

from sgp4.api import Satrec, WGS72, jday

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data" / "stormtrace.duckdb"
GOLD_DIR = ROOT / "data" / "gold"
CSV_PATH = GOLD_DIR / "propagation_disagreement.csv"

TWO_PI = 2.0 * math.pi
MINUTES_PER_DAY = 1440.0


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

    radial = sum(d * u for d, u in zip(difference, radial_unit))
    along = sum(d * u for d, u in zip(difference, along_unit))
    cross_track = sum(d * u for d, u in zip(difference, cross_unit))
    return radial, along, cross_track


def main() -> int:
    if not DATABASE.exists():
        print("Run earlier lessons first. The DuckDB database is missing.", file=sys.stderr)
        return 1

    connection = duckdb.connect(str(DATABASE))
    try:
        records = connection.sql(
            """
            SELECT
                object_name,
                norad_catalog_id,
                source_group,
                timezone('UTC', snapshot_at_utc) AS snapshot_at_utc,
                element_epoch_utc,
                inclination_degrees,
                eccentricity,
                mean_motion_revolutions_per_day,
                bstar_drag_term,
                ra_of_asc_node_degrees,
                arg_of_pericenter_degrees,
                mean_anomaly_degrees,
                mean_motion_dot,
                mean_motion_ddot
            FROM orbital_snapshot_history
            WHERE element_epoch_utc IS NOT NULL
            ORDER BY norad_catalog_id, snapshot_at_utc
            """
        ).fetchall()
        columns = [
            "object_name",
            "norad_catalog_id",
            "source_group",
            "snapshot_at_utc",
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

        history: dict[int, list[dict]] = {}
        for row in records:
            record = dict(zip(columns, row))
            norad = record["norad_catalog_id"]
            if norad is None:
                continue
            history.setdefault(norad, []).append(record)

        measurements: list[tuple] = []
        skipped = 0
        retrograde_pairs = 0
        for norad, entries in history.items():
            for earlier, later in zip(entries, entries[1:]):
                if earlier["element_epoch_utc"] == later["element_epoch_utc"]:
                    continue
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

                measurements.append(
                    (
                        later["object_name"],
                        norad,
                        later["source_group"],
                        earlier["element_epoch_utc"],
                        later["element_epoch_utc"],
                        round(span_hours, 3),
                        round(radial, 4),
                        round(along, 4),
                        round(cross, 4),
                        round(total, 4),
                        earlier["snapshot_at_utc"],
                        later["snapshot_at_utc"],
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
                later_snapshot_at_utc TIMESTAMP
            )
            """
        )
        if measurements:
            placeholders = "(" + ", ".join("?" for _ in measurements[0]) + ")"
            values_sql = ", ".join([placeholders] * len(measurements))
            parameters = [value for row in measurements for value in row]
            connection.execute(
                f"INSERT INTO gold_propagation_disagreement VALUES {values_sql}",
                parameters,
            )
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        connection.execute(
            "COPY gold_propagation_disagreement TO ? (HEADER, DELIMITER ',')",
            [str(CSV_PATH)],
        )
        tracked_objects = len(history)
        awaiting = sum(
            1 for entries in history.values() if len({e["element_epoch_utc"] for e in entries}) < 2
        )
    except duckdb.Error as error:
        print(f"Propagation disagreement error: {error}", file=sys.stderr)
        return 1
    finally:
        connection.close()

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
    if measurements:
        totals = sorted(m[9] for m in measurements)
        median = totals[len(totals) // 2]
        spans = [m[5] for m in measurements]
        along = sorted(m[7] for m in measurements)
        print(f"Disagreement total km: min {totals[0]:.3f}, median {median:.3f}, max {totals[-1]:.3f}")
        print(f"Along-track km: median {along[len(along) // 2]:.3f}, max {along[-1]:.3f}")
        print(f"Propagation spans: {min(spans):.2f} h to {max(spans):.2f} h")
        print("Gold table and CSV updated with measurements.")
    else:
        print("No measurable pairs yet: all consecutive element sets are")
        print("identical republished data. This instrument reports nothing")
        print("rather than reporting a fake zero. Once the catalog refreshes")
        print("element epochs, real disagreement measurements appear here")
        print("automatically on the next pipeline run.")
    print(f"CSV: {CSV_PATH.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
