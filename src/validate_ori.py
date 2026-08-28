"""Validate the Orbit Reliability Index against measured disagreement.

For every measured propagation-disagreement pair, the ORI components are
reconstructed exactly as they would have stood at the EARLIER element's
snapshot time (point-in-time correctness):

  element_age_at_prediction = earlier_snapshot_at - earlier_element_epoch
  altitude_at_prediction    = derived from the earlier element's mean motion
  freshness_score           = 100 * clamp(1 - age_hours / 48)
  drag_safety_score         = 100 * clamp((altitude_km - 300) / 500)
  base_score                = 0.55 * freshness + 0.45 * drag_safety

The measured outcome is the SGP4 disagreement when the refreshed element
arrived. Validation questions:

  1. Do lower-scored objects actually show larger measured disagreement?
  2. Does the disagreement grow with propagation span?
  3. Does the along-track-dominant subset (drag-like disagreements) follow
     the predicted reliability better than maneuver-contaminated pairs?

The environment factor is NOT validated here: current data is entirely
quiet-weather, so the factor is constant and cannot discriminate. That
validation must wait for a disturbed period.

Spearman rank correlation is computed with a pure-stdlib implementation
(average ranks for ties, Pearson on ranks).
"""

from __future__ import annotations

import math
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data" / "stormtrace.duckdb"
GOLD_DIR = ROOT / "data" / "gold"
CSV_PAIRS = GOLD_DIR / "ori_validation_pairs.csv"
CSV_BINS = GOLD_DIR / "ori_validation_bins.csv"
CSV_STATS = GOLD_DIR / "ori_validation_stats.csv"

MU = 398600.4418
EARTH_RADIUS_KM = 6378.137


def average_ranks(values: list[float]) -> list[float]:
    """Ranks with ties assigned the average of the tied positions."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    start = 0
    while start < len(order):
        end = start
        while end + 1 < len(order) and values[order[end + 1]] == values[order[start]]:
            end += 1
        average = (start + end) / 2.0 + 1.0
        for position in range(start, end + 1):
            ranks[order[position]] = average
        start = end + 1
    return ranks


def pearson(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    mean_x = sum(xs) / n
    mean_y = sum(ys) / n
    covariance = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs, ys))
    variance_x = sum((x - mean_x) ** 2 for x in xs)
    variance_y = sum((y - mean_y) ** 2 for y in ys)
    if variance_x == 0 or variance_y == 0:
        return 0.0
    return covariance / math.sqrt(variance_x * variance_y)


def spearman(xs: list[float], ys: list[float]) -> float:
    return pearson(average_ranks(xs), average_ranks(ys))


def median(values: list[float]) -> float:
    ordered = sorted(values)
    n = len(ordered)
    if n == 0:
        return float("nan")
    if n % 2 == 1:
        return ordered[n // 2]
    return (ordered[n // 2 - 1] + ordered[n // 2]) / 2.0


QUERY = """
SELECT
    d.norad_catalog_id,
    d.object_name,
    d.source_group,
    d.propagation_span_hours,
    d.radial_km,
    d.along_track_km,
    d.cross_track_km,
    d.total_km,
    date_diff('minute', h.element_epoch_utc, d.earlier_snapshot_at_utc) / 60.0
        AS element_age_hours_at_prediction,
    POWER(:mu: / POWER(h.mean_motion_revolutions_per_day * 2.0 * PI() / 86400.0, 2), 1.0/3.0)
        - :earth_radius:
        AS mean_altitude_km_at_prediction,
    h.inclination_degrees AS inclination_degrees_at_prediction,
    h.eccentricity AS eccentricity_at_prediction,
    h.bstar_drag_term AS bstar_at_prediction
FROM gold_propagation_disagreement d
JOIN orbital_snapshot_history h
  ON h.norad_catalog_id = d.norad_catalog_id
 AND h.element_epoch_utc = d.earlier_element_epoch_utc
 AND timezone('UTC', h.snapshot_at_utc) = d.earlier_snapshot_at_utc
"""


def main() -> int:
    if not DATABASE.exists():
        print("Run earlier lessons first. The DuckDB database is missing.", file=sys.stderr)
        return 1

    connection = duckdb.connect(str(DATABASE))
    try:
        query = QUERY.replace(":mu:", str(MU)).replace(":earth_radius:", str(EARTH_RADIUS_KM))
        rows = connection.execute(query).fetchall()
        if not rows:
            print("StormTrace lesson 17 ORI validation")
            print("No measurable pairs available yet; nothing to validate.")
            print("Once refreshed element sets appear, run this script again.")
            return 0

        scored: list[dict] = []
        for row in rows:
            (
                norad,
                name,
                group,
                span,
                radial,
                along,
                cross,
                total,
                age_hours,
                altitude,
                inclination,
                eccentricity,
                bstar,
            ) = row
            if age_hours is None or altitude is None or span is None or total is None:
                continue
            freshness = 100.0 * max(0.0, min(1.0, 1.0 - age_hours / 48.0))
            drag_safety = 100.0 * max(0.0, min(1.0, (altitude - 300.0) / 500.0))
            base_score = 0.55 * freshness + 0.45 * drag_safety
            if base_score >= 80:
                reliability_class = "high"
            elif base_score >= 60:
                reliability_class = "moderate"
            elif base_score >= 40:
                reliability_class = "reduced"
            else:
                reliability_class = "low"
            along_dominant = (
                abs(along or 0.0) > abs(radial or 0.0)
                and abs(along or 0.0) > abs(cross or 0.0)
            )
            scored.append(
                {
                    "norad": norad,
                    "name": name,
                    "group": group,
                    "base_score": round(base_score, 1),
                    "freshness": round(freshness, 1),
                    "drag_safety": round(drag_safety, 1),
                    "reliability_class": reliability_class,
                    "age_hours": round(age_hours, 2),
                    "altitude": round(altitude, 2),
                    "inclination": inclination,
                    "eccentricity": eccentricity,
                    "bstar": bstar,
                    "span_hours": round(span, 2),
                    "radial_km": radial,
                    "along_km": along,
                    "cross_km": cross,
                    "total_km": round(total, 4),
                    "km_per_hour": round(total / span, 4) if span > 0 else None,
                    "along_dominant": along_dominant,
                }
            )

        connection.execute(
            """
            CREATE OR REPLACE TABLE gold_ori_validation_pairs (
                norad_catalog_id BIGINT,
                object_name VARCHAR,
                source_group VARCHAR,
                base_score DOUBLE,
                freshness_score DOUBLE,
                drag_safety_score DOUBLE,
                reliability_class VARCHAR,
                element_age_hours_at_prediction DOUBLE,
                mean_altitude_km_at_prediction DOUBLE,
                inclination_degrees_at_prediction DOUBLE,
                eccentricity_at_prediction DOUBLE,
                bstar_at_prediction DOUBLE,
                propagation_span_hours DOUBLE,
                radial_km DOUBLE,
                along_track_km DOUBLE,
                cross_track_km DOUBLE,
                total_km DOUBLE,
                km_per_hour DOUBLE,
                is_along_track_dominant BOOLEAN
            )
            """
        )
        if scored:
            tuples = [
                (
                    s["norad"], s["name"], s["group"], s["base_score"],
                    s["freshness"], s["drag_safety"], s["reliability_class"],
                    s["age_hours"], s["altitude"], s["inclination"],
                    s["eccentricity"], s["bstar"], s["span_hours"],
                    s["radial_km"], s["along_km"], s["cross_km"],
                    s["total_km"], s["km_per_hour"], s["along_dominant"],
                )
                for s in scored
            ]
            placeholders = "(" + ", ".join("?" for _ in tuples[0]) + ")"
            values_sql = ", ".join([placeholders] * len(tuples))
            parameters = [value for row in tuples for value in row]
            connection.execute(
                f"INSERT INTO gold_ori_validation_pairs VALUES {values_sql}",
                parameters,
            )

        # Bin by predicted class; medians are robust to maneuver outliers.
        class_order = ["high", "moderate", "reduced", "low"]
        bins = []
        for reliability_class in class_order:
            members = [s for s in scored if s["reliability_class"] == reliability_class]
            if not members:
                continue
            totals = [s["total_km"] for s in members]
            rates = [s["km_per_hour"] for s in members if s["km_per_hour"] is not None]
            along = [abs(s["along_km"] or 0.0) for s in members]
            sorted_totals = sorted(totals)
            bins.append(
                {
                    "class": reliability_class,
                    "count": len(members),
                    "median_total": round(median(totals), 4),
                    "p90_total": round(
                        sorted_totals[min(len(sorted_totals) - 1, int(0.9 * len(sorted_totals)))],
                        4,
                    ),
                    "median_rate": round(median(rates), 4) if rates else None,
                    "median_along": round(median(along), 4),
                }
            )

        connection.execute(
            """
            CREATE OR REPLACE TABLE gold_ori_validation_bins (
                reliability_class VARCHAR,
                pair_count INTEGER,
                median_total_km DOUBLE,
                p90_total_km DOUBLE,
                median_km_per_hour DOUBLE,
                median_along_track_km DOUBLE
            )
            """
        )
        if bins:
            tuples = [
                (b["class"], b["count"], b["median_total"], b["p90_total"],
                 b["median_rate"], b["median_along"])
                for b in bins
            ]
            placeholders = "(" + ", ".join("?" for _ in tuples[0]) + ")"
            values_sql = ", ".join([placeholders] * len(tuples))
            parameters = [value for row in tuples for value in row]
            connection.execute(
                f"INSERT INTO gold_ori_validation_bins VALUES {values_sql}",
                parameters,
            )

        # Rank correlations: negative means lower score -> larger error,
        # which is exactly what the index predicts.
        scores = [s["base_score"] for s in scored]
        stats: list[tuple[str, float]] = []
        stats.append(("pairs", float(len(scored))))
        stats.append(("spearman_score_vs_total_km", round(spearman(scores, [s["total_km"] for s in scored]), 4)))
        stats.append(("spearman_score_vs_km_per_hour", round(spearman(scores, [s["km_per_hour"] for s in scored if s["km_per_hour"] is not None]), 4) if any(s["km_per_hour"] is not None for s in scored) else 0.0))
        stats.append(("spearman_age_vs_total_km", round(spearman([s["age_hours"] for s in scored], [s["total_km"] for s in scored]), 4)))
        drag_like = [s for s in scored if s["along_dominant"]]
        if len(drag_like) >= 3:
            stats.append(("drag_like_pairs", float(len(drag_like))))
            stats.append(("spearman_score_vs_total_km_drag_like", round(spearman([s["base_score"] for s in drag_like], [s["total_km"] for s in drag_like]), 4)))
            stats.append(("spearman_altitude_vs_total_km_drag_like", round(spearman([s["altitude"] for s in drag_like], [s["total_km"] for s in drag_like]), 4)))

        connection.execute(
            "CREATE OR REPLACE TABLE gold_ori_validation_stats (metric VARCHAR, value DOUBLE)"
        )
        if stats:
            placeholders = "(" + ", ".join("?" for _ in stats[0]) + ")"
            values_sql = ", ".join([placeholders] * len(stats))
            parameters = [value for row in stats for value in row]
            connection.execute(
                f"INSERT INTO gold_ori_validation_stats VALUES {values_sql}",
                parameters,
            )

        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        connection.execute(
            "COPY gold_ori_validation_pairs TO ? (HEADER, DELIMITER ',')", [str(CSV_PAIRS)]
        )
        connection.execute(
            "COPY gold_ori_validation_bins TO ? (HEADER, DELIMITER ',')", [str(CSV_BINS)]
        )
        connection.execute(
            "COPY gold_ori_validation_stats TO ? (HEADER, DELIMITER ',')", [str(CSV_STATS)]
        )
    except duckdb.Error as error:
        print(f"ORI validation error: {error}", file=sys.stderr)
        return 1
    finally:
        connection.close()

    print("StormTrace lesson 17 ORI validation")
    print(f"Validated pairs: {len(scored)}")
    print()
    print("Predicted class -> measured disagreement (medians, robust to maneuvers):")
    for b in bins:
        rate_text = f", {b['median_rate']} km/h" if b["median_rate"] is not None else ""
        print(
            f"  {b['class']:>8}: n={b['count']:>3}  median {b['median_total']:>8} km"
            f"{rate_text}  P90 {b['p90_total']} km"
        )
    print()
    for metric, value in stats:
        if metric != "pairs":
            print(f"  {metric}: {value}")
    print()
    print("Reading the correlations: negative score-vs-error means the index")
    print("predicts correctly (lower score, larger error). Positive age-vs-error")
    print("means element age drives error as designed.")
    print("The environment factor is NOT validated: all current data is quiet.")
    print(f"CSV: {CSV_PAIRS.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
