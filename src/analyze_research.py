"""StormTrace research analysis: orbit change, space weather, freshness."""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data" / "stormtrace.duckdb"
SQL_PATH = ROOT / "sql" / "lesson8_research.sql"
FRESHNESS_SQL_PATH = ROOT / "sql" / "lesson11_freshness.sql"
RELIABILITY_SQL_PATH = ROOT / "sql" / "lesson12_reliability.sql"
PILLARS_SQL_PATH = ROOT / "sql" / "lesson23_pillars.sql"
REPORTS_DIR = ROOT / "data" / "reports"
GOLD_DIR = ROOT / "data" / "gold"


def fetch_rows(connection: duckdb.DuckDBPyConnection, sql: str) -> list[tuple]:
    return connection.sql(sql).fetchall()


def to_datetime(epoch_seconds: float) -> datetime:
    return datetime.fromtimestamp(epoch_seconds, tz=UTC)


def chart_space_weather(connection: duckdb.DuckDBPyConnection) -> Path | None:
    rows = fetch_rows(
        connection,
        """
        SELECT
            epoch(observation_minute_utc),
            bz_gsm_nanotesla,
            proton_speed_km_per_second
        FROM gold_space_weather_minute
        ORDER BY observation_minute_utc
        """,
    )
    if not rows:
        return None

    times = [to_datetime(row[0]) for row in rows]
    bz_values = [row[1] for row in rows]
    speed_values = [row[2] for row in rows]

    figure, (axis_bz, axis_speed) = plt.subplots(2, 1, figsize=(12, 7), sharex=True)

    axis_bz.plot(times, bz_values, linewidth=0.6, color="tab:blue")
    axis_bz.axhline(y=-5.0, color="tab:red", linestyle="--", linewidth=1, label="-5 nT guide")
    axis_bz.axhline(y=0.0, color="gray", linewidth=0.5)
    axis_bz.set_ylabel("Bz GSM (nT)")
    axis_bz.set_title("Interplanetary Magnetic Field Bz (GSM), last 24 hours")
    axis_bz.legend(loc="upper right")
    axis_bz.grid(alpha=0.3)

    axis_speed.plot(times, speed_values, linewidth=0.6, color="tab:green")
    axis_speed.axhline(y=500.0, color="tab:orange", linestyle="--", linewidth=1, label="500 km/s guide")
    axis_speed.set_ylabel("Proton speed (km/s)")
    axis_speed.set_xlabel("Time (UTC)")
    axis_speed.set_title("Solar Wind Proton Speed")
    axis_speed.legend(loc="upper right")
    axis_speed.grid(alpha=0.3)

    figure.autofmt_xdate()
    figure.tight_layout()
    path = REPORTS_DIR / "space_weather_timeline.png"
    figure.savefig(path, dpi=120)
    plt.close(figure)
    return path


def chart_sw_index_daily(connection: duckdb.DuckDBPyConnection) -> Path | None:
    if not table_exists(connection, "gold_sw_index_daily"):
        return None
    rows = fetch_rows(
        connection,
        """
        SELECT
            observation_date,
            kp_sum,
            ap_avg,
            f10_7_observed
        FROM gold_sw_index_daily
        WHERE f10_7_observed IS NOT NULL OR kp_sum IS NOT NULL
        ORDER BY observation_date
        """,
    )
    if not rows:
        return None


    dates = [row[0] for row in rows]
    kp_sums = [float(row[1]) if row[1] is not None else None for row in rows]
    f10_7_values = [
        float(row[3]) if row[3] is not None else None for row in rows
    ]

    figure, axis_f10 = plt.subplots(figsize=(12, 6))
    axis_kp = axis_f10.twinx()

    axis_kp.bar(dates, kp_sums, width=1.0, color="tab:blue", alpha=0.55, label="Daily Kp sum")
    axis_f10.plot(
        dates, f10_7_values, color="tab:red", linewidth=1.2, label="F10.7 (observed)"
    )
    axis_kp.axhline(y=30, color="gray", linestyle="--", linewidth=1, label="Kp sum 30 guide")
    axis_kp.set_ylabel("Daily Kp sum")
    axis_f10.set_ylabel("F10.7 solar flux (sfu)")
    axis_f10.set_xlabel("Date (UTC)")
    axis_f10.set_title("Daily Space-Weather Indices: Kp and Solar Radio Flux F10.7")
    lines_kp, labels_kp = axis_kp.get_legend_handles_labels()
    lines_f10, labels_f10 = axis_f10.get_legend_handles_labels()
    axis_f10.legend(lines_kp + lines_f10, labels_kp + labels_f10, loc="upper left")
    axis_kp.grid(alpha=0.3)

    figure.tight_layout()
    path = REPORTS_DIR / "sw_indices_timeline.png"
    figure.savefig(path, dpi=120)
    plt.close(figure)
    return path


def chart_orbit_population(connection: duckdb.DuckDBPyConnection) -> Path | None:
    rows = fetch_rows(
        connection,
        """
        SELECT object_name, ROUND(mean_altitude_km, 1)
        FROM gold_satellite_orbit_features
        ORDER BY mean_altitude_km DESC
        """,
    )
    if not rows:
        return None

    altitudes = [float(row[1]) for row in rows]
    path = REPORTS_DIR / "orbit_altitude_distribution.png"

    if len(rows) > 30:
        # With a large population, a histogram of altitudes is far more
        # readable than one bar per object.
        figure, axis = plt.subplots(figsize=(12, 6))
        axis.hist(altitudes, bins=40, color="tab:blue", edgecolor="white")
        axis.axvline(x=300, color="tab:red", linestyle="--", linewidth=1, label="300 km boundary")
        axis.axvline(x=600, color="tab:orange", linestyle="--", linewidth=1, label="600 km boundary")
        axis.axvline(x=2000, color="tab:green", linestyle="--", linewidth=1, label="2000 km boundary")
        axis.set_xlabel("Mean altitude (km)")
        axis.set_ylabel("Object count")
        axis.set_title(f"Mean Orbital Altitude Distribution ({len(rows):,} objects)")
        axis.legend()
        axis.grid(alpha=0.3)
    else:
        names = [row[0] for row in rows]
        figure, axis = plt.subplots(figsize=(10, 8))
        axis.barh(names, altitudes, color="tab:blue")
        axis.axvline(x=300, color="tab:red", linestyle="--", linewidth=1, label="300 km boundary")
        axis.axvline(x=600, color="tab:orange", linestyle="--", linewidth=1, label="600 km boundary")
        axis.set_xlabel("Mean altitude (km)")
        axis.set_title("Station-Group Objects: Mean Orbital Altitude")
        axis.legend()
        axis.grid(alpha=0.3, axis="x")

    figure.tight_layout()
    figure.savefig(path, dpi=120)
    plt.close(figure)
    return path


def chart_orbit_change(connection: duckdb.DuckDBPyConnection) -> Path | None:
    rows = fetch_rows(
        connection,
        """
        SELECT object_name, decay_rate_km_per_day
        FROM gold_orbit_change
        WHERE decay_rate_km_per_day IS NOT NULL
        ORDER BY ABS(decay_rate_km_per_day) DESC
        LIMIT 25
        """,
    )
    if not rows:
        return None

    names = [row[0] for row in rows]
    decay_rates = [float(row[1]) for row in rows]

    figure, axis = plt.subplots(figsize=(10, max(4, len(names) * 0.35)))
    colors = ["tab:red" if rate > 0 else "tab:blue" for rate in decay_rates]
    axis.barh(names, decay_rates, color=colors)
    axis.axvline(x=0, color="black", linewidth=0.8)
    axis.set_xlabel("Orbit decay rate (km per day, positive = altitude loss)")
    axis.set_title("Measured Orbit Change Between Snapshots")
    axis.grid(alpha=0.3, axis="x")
    figure.tight_layout()
    path = REPORTS_DIR / "orbit_decay_rates.png"
    figure.savefig(path, dpi=120)
    plt.close(figure)
    return path


def chart_element_freshness(connection: duckdb.DuckDBPyConnection) -> Path | None:
    group_rows = fetch_rows(
        connection,
        "SELECT source_group, element_age_hours FROM gold_element_freshness",
    )
    band_rows = fetch_rows(
        connection,
        """
        SELECT altitude_band, median_age_hours, stale_percent
        FROM gold_freshness_by_band
        ORDER BY median_age_hours
        """,
    )
    if not group_rows:
        return None

    ages_by_group: dict[str, list[float]] = {}
    for group, age in group_rows:
        if age is not None:
            ages_by_group.setdefault(str(group), []).append(float(age))
    if not ages_by_group:
        return None

    figure, (axis_hist, axis_band) = plt.subplots(1, 2, figsize=(14, 6))

    for group, ages in sorted(ages_by_group.items()):
        axis_hist.hist(ages, bins=30, alpha=0.6, label=f"{group} (n={len(ages):,})")
    axis_hist.axvline(x=24, color="tab:red", linestyle="--", linewidth=1, label="24 h guide")
    axis_hist.set_xlabel("Element age (hours)")
    axis_hist.set_ylabel("Object count")
    axis_hist.set_title("Public Element Age by Group")
    axis_hist.legend()
    axis_hist.grid(alpha=0.3)

    if band_rows:
        bands = [str(row[0]) for row in band_rows]
        medians = [float(row[1]) for row in band_rows]
        axis_band.bar(bands, medians, color="tab:purple")
        axis_band.axhline(y=24, color="tab:red", linestyle="--", linewidth=1, label="24 h guide")
        axis_band.set_xlabel("Altitude band")
        axis_band.set_ylabel("Median element age (hours)")
        axis_band.set_title("Median Element Age by Altitude Band")
        axis_band.legend()
        axis_band.grid(alpha=0.3, axis="y")
    else:
        axis_band.set_visible(False)

    figure.tight_layout()
    path = REPORTS_DIR / "element_freshness.png"
    figure.savefig(path, dpi=120)
    plt.close(figure)
    return path


def chart_orbit_reliability(connection: duckdb.DuckDBPyConnection) -> Path | None:
    rows = fetch_rows(
        connection,
        """
        SELECT source_group, element_age_hours, orbit_reliability_index,
               reliability_class
        FROM gold_orbit_reliability_index
        """,
    )
    if not rows:
        return None

    figure, (axis_hist, axis_scatter) = plt.subplots(1, 2, figsize=(14, 6))

    class_colors = {
        "high": "tab:green",
        "moderate": "tab:blue",
        "reduced": "tab:orange",
        "low": "tab:red",
    }
    for reliability_class, color in class_colors.items():
        indexes = [
            float(row[2])
            for row in rows
            if row[3] == reliability_class and row[2] is not None
        ]
        if indexes:
            axis_hist.hist(
                indexes,
                bins=20,
                alpha=0.7,
                color=color,
                label=f"{reliability_class} (n={len(indexes):,})",
            )
    axis_hist.set_xlabel("Orbit Reliability Index (0-100)")
    axis_hist.set_ylabel("Object count")
    axis_hist.set_title("Reliability Score Distribution")
    axis_hist.legend()
    axis_hist.grid(alpha=0.3)

    groups = sorted({str(row[0]) for row in rows})
    scatter_colors = ["tab:blue", "tab:purple", "tab:cyan", "tab:olive"]
    for index, group in enumerate(groups):
        group_rows = [row for row in rows if str(row[0]) == group]
        ages = [float(row[1]) for row in group_rows if row[1] is not None]
        indexes = [float(row[2]) for row in group_rows if row[2] is not None]
        axis_scatter.scatter(
            ages,
            indexes,
            alpha=0.6,
            s=25,
            color=scatter_colors[index % len(scatter_colors)],
            label=group,
        )
    axis_scatter.set_xlabel("Element age (hours)")
    axis_scatter.set_ylabel("Orbit Reliability Index")
    axis_scatter.set_title("Reliability Versus Element Age")
    axis_scatter.legend()
    axis_scatter.grid(alpha=0.3)

    figure.tight_layout()
    path = REPORTS_DIR / "orbit_reliability_index.png"
    figure.savefig(path, dpi=120)
    plt.close(figure)
    return path


def table_exists(
    connection: duckdb.DuckDBPyConnection, table_name: str
) -> bool:
    return (
        connection.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table_name],
        ).fetchone()[0]
        > 0
    )


def chart_propagation_disagreement(connection: duckdb.DuckDBPyConnection) -> Path | None:
    if not table_exists(connection, "gold_propagation_disagreement"):
        return None
    rows = fetch_rows(
        connection,
        """
        SELECT source_group, propagation_span_hours, total_km
        FROM gold_propagation_disagreement
        WHERE measurement_quality = 'ok'
        """,
    )
    if not rows:
        return None

    figure, axis = plt.subplots(figsize=(12, 6))
    groups = sorted({str(row[0]) for row in rows})
    colors = ["tab:blue", "tab:purple", "tab:cyan", "tab:olive"]
    for index, group in enumerate(groups):
        group_rows = [row for row in rows if str(row[0]) == group]
        axis.scatter(
            [float(row[1]) for row in group_rows],
            [float(row[2]) for row in group_rows],
            alpha=0.6,
            s=30,
            color=colors[index % len(colors)],
            label=f"{group} (n={len(group_rows):,})",
        )
    axis.set_xlabel("Propagation span (hours)")
    axis.set_ylabel("Total disagreement (km)")
    axis.set_title("SGP4 Propagation Disagreement Between Consecutive Element Sets")
    axis.set_yscale("log")
    axis.legend()
    axis.grid(alpha=0.3, which="both")
    figure.tight_layout()
    path = REPORTS_DIR / "propagation_disagreement.png"
    figure.savefig(path, dpi=120)
    plt.close(figure)
    return path


def chart_ori_validation(connection: duckdb.DuckDBPyConnection) -> Path | None:
    if not table_exists(connection, "gold_ori_validation_pairs"):
        return None
    rows = fetch_rows(
        connection,
        """
        SELECT base_score, total_km, reliability_class
        FROM gold_ori_validation_pairs
        """,
    )
    if not rows:
        return None

    figure, axis = plt.subplots(figsize=(12, 6))
    class_colors = {
        "high": "tab:green",
        "moderate": "tab:blue",
        "reduced": "tab:orange",
        "low": "tab:red",
    }
    for reliability_class, color in class_colors.items():
        class_rows = [row for row in rows if row[2] == reliability_class]
        if not class_rows:
            continue
        axis.scatter(
            [float(row[0]) for row in class_rows],
            [float(row[1]) for row in class_rows],
            alpha=0.6,
            s=30,
            color=color,
            label=f"{reliability_class} (n={len(class_rows):,})",
        )
    axis.set_xlabel("Predicted base score at earlier element (higher = more reliable)")
    axis.set_ylabel("Measured disagreement (km)")
    axis.set_title("ORI Validation: Predicted Reliability vs Measured Disagreement")
    axis.set_yscale("log")
    axis.legend()
    axis.grid(alpha=0.3, which="both")
    figure.tight_layout()
    path = REPORTS_DIR / "ori_validation.png"
    figure.savefig(path, dpi=120)
    plt.close(figure)
    return path


def sw_index_summary(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    if not table_exists(connection, "gold_sw_index_daily"):
        return {}
    relation = connection.sql(
        """
        SELECT
            MIN(observation_date) AS first_date,
            MAX(observation_date) AS last_date,
            COUNT(*) AS day_count,
            ROUND(MAX(kp_sum), 2) AS max_kp_sum,
            ROUND(MAX(ap_avg), 2) AS max_ap_avg,
            ROUND(MIN(f10_7_observed), 2) AS min_f10_7,
            ROUND(MAX(f10_7_observed), 2) AS max_f10_7
        FROM gold_sw_index_daily
        """
    )
    row = relation.fetchone()
    return dict(zip(relation.columns, row, strict=True))


def satnogs_summary(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    if not table_exists(connection, "gold_satnogs_activity"):
        return {}
    relation = connection.sql(
        """
        SELECT
            MIN(observation_date) AS first_date,
            MAX(observation_date) AS last_date,
            COUNT(*) AS day_count,
            SUM(observation_count) AS total_observations,
            SUM(good_count) AS total_good,
            SUM(completed_count) AS total_completed,
            SUM(scheduled_count) AS total_scheduled,
            ROUND(
                100.0 * SUM(good_count) / NULLIF(SUM(completed_count), 0), 1
            ) AS good_percent_of_completed,
            SUM(distinct_satellites) AS satellite_hears_seen,
            SUM(distinct_stations) AS station_hears_seen
        FROM gold_satnogs_activity
        """
    )

    row = relation.fetchone()
    return dict(zip(relation.columns, row, strict=True))


def debris_summary(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    if not table_exists(connection, "gold_debris_population"):
        return {}
    relation = connection.sql(
        """
        SELECT
            COUNT(*) AS object_count,
            ROUND(MIN(mean_altitude_km), 1) AS min_altitude_km,
            ROUND(MAX(mean_altitude_km), 1) AS max_altitude_km,
            ROUND(MEDIAN(mean_altitude_km), 1) AS median_altitude_km,
            ROUND(MEDIAN(bstar_drag_term), 6) AS median_bstar,
            ROUND(MEDIAN(eccentricity), 6) AS median_eccentricity,
            ROUND(MAX(element_age_hours), 2) AS oldest_element_hours
        FROM gold_debris_population
        """
    )
    row = relation.fetchone()
    return dict(zip(relation.columns, row, strict=True))


def space_weather_summary(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    relation = connection.sql(
        """
        SELECT
            COUNT(*) AS hour_count,
            COUNT(*) FILTER (WHERE disturbance_level = 'quiet') AS quiet_hours,
            COUNT(*) FILTER (WHERE disturbance_level = 'southward_bz') AS southward_hours,
            COUNT(*) FILTER (WHERE disturbance_level = 'fast_wind') AS fast_wind_hours,
            ROUND(MIN(minimum_bz_gsm), 2) AS minimum_bz,
            ROUND(MAX(average_proton_speed), 2) AS maximum_average_speed
        FROM gold_space_weather_hourly
        """
    )
    row = relation.fetchone()
    return dict(zip(relation.columns, row, strict=True))


def population_by_group(connection: duckdb.DuckDBPyConnection) -> list[tuple[str, int]]:
    return [
        (str(row[0]), int(row[1]))
        for row in fetch_rows(
            connection,
            """
            SELECT source_group, COUNT(*) AS object_count
            FROM orbital_snapshot_history h
            WHERE snapshot_at_utc = (
                SELECT MAX(snapshot_at_utc)
                FROM orbital_snapshot_history
                WHERE source_group = h.source_group
            )
            GROUP BY source_group
            ORDER BY source_group
            """,
        )
    ]


def total_tracked_objects(connection: duckdb.DuckDBPyConnection) -> int:
    return int(
        connection.sql(
            "SELECT COUNT(DISTINCT norad_catalog_id) FROM orbital_snapshot_history"
        ).fetchone()[0]
    )


def write_report(
    connection: duckdb.DuckDBPyConnection,
    weather: dict[str, object],
    chart_paths: list[Path],
    snapshot_count: int,
    change_count: int,
) -> Path:
    now = datetime.now(UTC).replace(microsecond=0).isoformat()
    population = population_by_group(connection)
    population_total = sum(count for _, count in population)
    population_text = ", ".join(f"{group}: {count:,}" for group, count in population)
    tracked_total = total_tracked_objects(connection)
    lines = [
        "# StormTrace Research Summary",
        "",
        f"Generated: {now}",
        "",
        "## Data Coverage",
        "",
        f"- Orbital snapshots available: {snapshot_count}",
        f"- Objects in latest snapshot of each group: {population_total:,} ({population_text})",
        f"- Distinct objects ever tracked: {tracked_total:,}",
        f"- Orbit-change measurements computed: {change_count}",
        "",
    ]

    if snapshot_count < 2:
        lines += [
            "## Orbit Change Status",
            "",
            "INSUFFICIENT SNAPSHOTS FOR CHANGE DETECTION.",
            "",
            "At least two orbital snapshots of the same objects are required",
            "to measure mean-motion change. Continue collecting snapshots",
            "every two hours with `python src\\run_pipeline.py`.",
            "",
        ]
    else:
        refresh = fetch_rows(
            connection,
            """
            SELECT
                COUNT(DISTINCT norad_catalog_id) AS compared_objects,
                COUNT(*) AS pair_count,
                COUNT(*) FILTER (WHERE same_element_set) AS same_set_pairs,
                COUNT(*) FILTER (WHERE NOT same_element_set) AS refreshed_pairs
            FROM gold_orbit_change
            """,
        )[0]
        compared_objects, pair_count, same_set_pairs, refreshed_pairs = refresh
        awaiting = max(0, tracked_total - compared_objects)
        lines += [
            "## Orbit Change Status",
            "",
            f"- Objects with two or more snapshots: {compared_objects:,}",
            f"- Element-set pairs compared: {pair_count:,}",
            f"- Objects awaiting a second snapshot: {awaiting:,}",
            f"- Pairs with refreshed element sets: {refreshed_pairs:,}",
            f"- Pairs with republished identical element sets: {same_set_pairs:,}",
            "",
        ]
        if refreshed_pairs == 0:
            lines += [
                "NO NEW ELEMENT SETS BETWEEN SNAPSHOTS.",
                "",
                "The source republished the same orbital element sets, so",
                "measured change is zero by construction. This is expected",
                "when the catalog update cycle has not refreshed these",
                "objects yet. Source update frequency is not the same as",
                "data change frequency. Keep collecting snapshots; the",
                "analysis will measure real changes once new element sets",
                "appear.",
                "",
            ]
        else:
            top = fetch_rows(
                connection,
                """
                SELECT object_name, ROUND(decay_rate_km_per_day, 4),
                       interval_minutes
                FROM gold_orbit_change
                WHERE NOT same_element_set
                ORDER BY decay_rate_km_per_day DESC
                LIMIT 10
                """,
            )
            lines += [
                "## Largest Measured Orbit Changes",
                "",
                "| Object | Decay rate (km/day) | Interval (minutes) |",
                "|---|---:|---:|",
            ]
            for name, rate, interval in top:
                lines.append(f"| {name} | {rate} | {interval} |")
            lines.append("")

    freshness_groups = fetch_rows(
        connection,
        """
        SELECT source_group, object_count, median_age_hours, p90_age_hours,
               stale_count, stale_percent
        FROM gold_freshness_by_group
        ORDER BY source_group
        """,
    )
    if table_exists(connection, "gold_propagation_disagreement"):
        disagreement_stats = fetch_rows(
            connection,
            """
            SELECT
                COUNT(*),
                ROUND(MEDIAN(propagation_span_hours), 2),
                ROUND(MAX(propagation_span_hours), 2),
                ROUND(MEDIAN(total_km), 3),
                ROUND(MAX(total_km), 3),
                ROUND(MEDIAN(along_track_km), 3),
                ROUND(MEDIAN(radial_km), 3),
                ROUND(MEDIAN(cross_track_km), 3)
            FROM gold_propagation_disagreement
            WHERE measurement_quality = 'ok'
            """,
        )
        if disagreement_stats and disagreement_stats[0][0]:
            (
                pair_count,
                median_span,
                max_span,
                median_total,
                max_total,
                median_along,
                median_radial,
                median_cross,
            ) = disagreement_stats[0]
            excluded_pairs = fetch_rows(
                connection,
                """
                SELECT COUNT(*) FROM gold_propagation_disagreement
                WHERE measurement_quality <> 'ok'
                """,
            )[0][0]
            lines += [
                "## SGP4 Propagation Disagreement",
                "",
                "Measured drift between consecutive public element sets: the",
                "earlier element was propagated with SGP4 to the later",
                "element's epoch and compared against the later element's own",
                "position, decomposed in the RIC frame.",
                "",
                "| Metric | Value |",
                "|---|---:|",
                f"| Analysis-grade pairs | {pair_count:,} |",
                f"| Median propagation span | {median_span} h |",
                f"| Maximum propagation span | {max_span} h |",
                f"| Median total disagreement | {median_total} km |",
                f"| Maximum total disagreement | {max_total} km |",
                f"| Median along-track | {median_along} km |",
                f"| Median radial | {median_radial} km |",
                f"| Median cross-track | {median_cross} km |",
                f"| Excluded (outside SGP4 envelope) | {excluded_pairs:,} |",
                "",
                "The later element is not perfect ground truth; this is the",
                "disagreement between successive public estimates. The",
                "along-track component dominating the radial one is the",
                "expected signature of drag-model error accumulating along",
                "the velocity direction.",
                "",
                "Excluded pairs are kept in the Gold table as evidence but left",
                "out of every statistic: SGP4 cannot be trusted past ~72 h, and",
                "a disagreement above 500 km indicates a bad element rather",
                "than measurable drag. Including them once put a 5,079 km",
                "outlier into the reported maximum and the ML target.",
                "",

            ]
            worst = fetch_rows(
                connection,
                """
                SELECT object_name, source_group,
                       ROUND(propagation_span_hours, 1),
                       ROUND(total_km, 2),
                       ROUND(along_track_km, 2)
                FROM gold_propagation_disagreement
                WHERE measurement_quality = 'ok'
                ORDER BY total_km DESC
                LIMIT 10
                """,
            )
            if worst:
                lines += [
                    "### Largest Disagreements",
                    "",
                    "| Object | Group | Span (h) | Total (km) | Along-track (km) |",
                    "|---|---|---:|---:|---:|",
                ]
                for name, group, span, total, along in worst:
                    lines.append(f"| {name} | {group} | {span} | {total} | {along} |")
                lines.append("")

    if table_exists(connection, "gold_ori_validation_bins"):
        validation_bins = fetch_rows(
            connection,
            """
            SELECT reliability_class, pair_count, median_total_km,
                   median_km_per_hour, p90_total_km
            FROM gold_ori_validation_bins
            ORDER BY pair_count
            """,
        )
        validation_stats = dict(
            fetch_rows(
                connection,
                "SELECT metric, value FROM gold_ori_validation_stats",
            )
        )
        if validation_bins:
            lines += [
                "## Orbit Reliability Index Validation",
                "",
                "Each measured disagreement pair is scored with the ORI",
                "components exactly as they stood at the earlier element's",
                "snapshot time (point-in-time correct prediction). Medians",
                "are used because maneuvers produce outliers the index is",
                "not designed to predict.",
                "",
                "| Predicted class | Pairs | Median total (km) | Median rate (km/h) | P90 total (km) |",
                "|---|---:|---:|---:|---:|",
            ]
            for reliability_class, count, median_total, median_rate, p90 in validation_bins:
                rate = median_rate if median_rate is not None else "n/a"
                lines.append(
                    f"| {reliability_class} | {count:,} | {median_total} | {rate} | {p90} |"
                )
            lines += [""]

            score_correlation = validation_stats.get("spearman_score_vs_total_km")
            age_correlation = validation_stats.get("spearman_age_vs_total_km")
            drag_correlation = validation_stats.get(
                "spearman_score_vs_total_km_drag_like"
            )
            drag_pairs = validation_stats.get("drag_like_pairs")
            if score_correlation is not None:
                lines += [
                    "### Correlation Evidence",
                    "",
                    f"- Spearman, predicted score vs measured total km: "
                    f"{score_correlation} (negative is correct)",
                    f"- Spearman, element age at prediction vs measured km: "
                    f"{age_correlation} (positive is correct)",
                ]
                if drag_correlation is not None:
                    lines.append(
                        f"- Spearman, score vs total km, drag-like subset "
                        f"({int(drag_pairs)} along-track-dominant pairs): "
                        f"{drag_correlation}"
                    )
                altitude_corr = validation_stats.get(
                    "spearman_altitude_vs_total_km_drag_like"
                )
                if altitude_corr is not None:
                    lines.append(
                        f"- Spearman, altitude vs total km, drag-like subset: "
                        f"{altitude_corr} (altitude alone is currently the "
                        f"strongest single predictor)"
                    )
                lines += [
                    "",
                    "### What The Validation Shows",
                    "",
                    "- The index ranks reliability in the predicted",
                    "  direction: median error rises monotonically across",
                    "  the moderate, reduced, and low classes.",
                    "- Drag sensitivity (altitude) predicts error more",
                    "  strongly than the composite score, suggesting the",
                    "  freshness weight deserves recalibration with more",
                    "  data.",
                    "- The high class is contaminated by maneuvering",
                    "  spacecraft: high-altitude orbits score high on drag",
                    "  safety, but maneuvers are an unmodeled failure mode",
                    "  that no public-data index can predict.",
                    "- Element age alone shows only a weak signal here,",
                    "  because fresh elements on low-altitude objects still",
                    "  drift faster than stale elements on high objects.",
                    "- The environment factor remains unvalidated until a",
                    "  disturbed space-weather period is captured.",
                    "",
                ]

    if freshness_groups:
        lines += [
            "## Element Freshness (latest snapshot of each group)",
            "",
            "Element age is measured from each element's own epoch to the",
            "snapshot time at which it was captured.",
            "",
            "| Group | Objects | Median age (h) | P90 age (h) | Stale >24h | Stale % |",
            "|---|---:|---:|---:|---:|---:|",
        ]
        for group, count, median, p90, stale, percent in freshness_groups:
            lines.append(f"| {group} | {count:,} | {median} | {p90} | {stale} | {percent} |")
        lines.append("")
        freshness_bands = fetch_rows(
            connection,
            """
            SELECT altitude_band, object_count, median_age_hours,
                   p90_age_hours, stale_percent
            FROM gold_freshness_by_band
            ORDER BY median_age_hours
            """,
        )
        if freshness_bands:
            lines += [
                "| Altitude band | Objects | Median age (h) | P90 age (h) | Stale % |",
                "|---|---:|---:|---:|---:|",
            ]
            for band, count, median, p90, percent in freshness_bands:
                lines.append(f"| {band} | {count:,} | {median} | {p90} | {percent} |")
        lines += [
            "",
            "Fresher elements propagate more reliably. Objects with older",
            "elements carry larger position uncertainty, so element age is a",
            "core input for the planned Orbit Reliability Index. Different",
            "object classes are refreshed at different rates by the public",
            "catalog, which means reliability varies by population.",
            "",
        ]

    reliability_classes = fetch_rows(
        connection,
        """
        SELECT reliability_class, object_count, median_index
        FROM gold_reliability_class_summary
        ORDER BY median_index
        """,
    )
    if reliability_classes:
        environment = fetch_rows(
            connection,
            "SELECT environment_factor, context_through_hour FROM gold_orbit_reliability_index LIMIT 1",
        )
        environment_factor = (
            float(environment[0][0]) if environment else None
        )
        worst = fetch_rows(
            connection,
            """
            SELECT object_name, source_group, orbit_reliability_index,
                   element_age_hours
            FROM gold_orbit_reliability_index
            ORDER BY orbit_reliability_index
            LIMIT 10
            """,
        )
        lines += [
            "## Orbit Reliability Index",
            "",
            "A prototype indicator of how much each object's public orbit",
            "estimate should be trusted right now, on a 0-100 scale:",
            "",
            "```text",
            "freshness_score = 100 * clamp(1 - element_age_hours / 48)",
            "drag_safety    = 100 * clamp((altitude_km - 300) / 500)",
            "base_score     = 0.55 * freshness_score + 0.45 * drag_safety",
            "ORI            = base_score * environment_factor",
            "```",
            "",
            "The environment factor is 1.0 in quiet conditions, 0.9 during",
            "fast solar wind, and 0.8 with sustained southward Bz, based on",
            "the last three hours of observations.",
        ]
        if environment_factor is not None:
            lines += [
                "",
                f"Current environment factor: {environment_factor}",
            ]
        lines += [
            "",
            "| Reliability class | Objects | Median index |",
            "|---|---:|---:|",
        ]
        for reliability_class, count, median in reliability_classes:
            lines.append(f"| {reliability_class} | {count:,} | {median} |")
        lines += [
            "",
            "### Least Reliable Objects",
            "",
            "| Object | Group | ORI | Element age (h) |",
            "|---|---|---:|---:|",
        ]
        for name, group, index, age in worst:
            lines.append(f"| {name} | {group} | {index} | {age} |")
        lines += [
            "",
            "This index is NOT collision probability and NOT a measurement",
            "of true position error. Weights and thresholds are documented",
            "prototype choices. Once enough snapshots exist, ORI will be",
            "validated against measured propagation disagreement between",
            "consecutive element sets.",
            "",
        ]

    lines += [
        "## Space Weather Conditions (last 24 hours)",
        "",
        f"- Hours analyzed: {weather['hour_count']}",
        f"- Quiet hours: {weather['quiet_hours']}",
        f"- Southward-Bz hours: {weather['southward_hours']}",
        f"- Fast-wind hours: {weather['fast_wind_hours']}",
        f"- Minimum Bz observed: {weather['minimum_bz']} nT",
        f"- Maximum hourly average proton speed: {weather['maximum_average_speed']} km/s",
        "",
        "Disturbance thresholds are simple research guides, not official",
        "storm classifications: hourly average Bz below -5 nT or hourly",
        "average proton speed above 500 km/s.",
        "",
        "## The Three-Pillar Research Dataset",
        "",
    ]
    sw_index = sw_index_summary(connection)
    if sw_index:
        lines += [
            "### Daily Solar and Geomagnetic Indices (CelesTrak)",
            "",
            "The classic drivers of thermospheric density: the eight 3-hour",
            "planetary K indices (summed here), their Ap average, and the",
            "observed 10.7 cm solar radio flux. Physical chain: more solar",
            "activity -> hotter, denser upper atmosphere -> more drag on",
            "low orbits.",
            "",
            f"- Days with indices: {sw_index['day_count']:,} "
            f"({sw_index['first_date']} to {sw_index['last_date']})",
            f"- Maximum daily Kp sum: {sw_index['max_kp_sum']}",
            f"- Maximum daily Ap average: {sw_index['max_ap_avg']}",
            f"- F10.7 range: {sw_index['min_f10_7']} - {sw_index['max_f10_7']} sfu",
            "",
            "The most recent day is provisional and is revised between",
            "CelesTrak updates; each revision is kept in history. These are",
            "the explanatory variables the orbit and telemetry pillars will",
            "be joined against.",
            "",
        ]
    debris = debris_summary(connection)
    if debris:
        lines += [
            "### The Debris Population (iridium-33-debris group)",
            "",
            "Debris from a single collision: a tight altitude band, objects",
            "that never maneuver, and a drag signal uncontaminated by",
            "station-keeping. The cleanest natural experiment in the catalog.",
            "",
            f"- Objects tracked: {debris['object_count']:,}",
            f"- Altitude range: {debris['min_altitude_km']} to "
            f"{debris['max_altitude_km']} km "
            f"(median {debris['median_altitude_km']} km)",
            f"- Median bstar: {debris['median_bstar']}",
            f"- Median eccentricity: {debris['median_eccentricity']}",
            f"- Oldest element in the cohort: {debris['oldest_element_hours']} h",
            "",
            "Because this population sits just above the drag-dominated",
            "regime and never maneuvers, its orbit-height trend is the most",
            "direct drag meter StormTrace has.",
            "",
        ]
    satnogs = satnogs_summary(connection)
    if satnogs:
        rate = satnogs.get("good_percent_of_completed")
        rate_text = (
            f"{rate}% of completed passes were good"
            if rate is not None
            else "no pass has completed yet, so the good rate is unknown"
        )
        lines += [
            "### SatNOGS Observation Activity",
            "",
            "Independent, crowd-sourced telemetry evidence: volunteers'",
            "ground stations report when satellites are heard and at what",
            "frequency. Scope, honestly stated: metadata, not decoded frames.",
            "",
            f"- Observation days: {satnogs['day_count']:,} "
            f"({satnogs['first_date']} to {satnogs['last_date']})",
            f"- Distinct observations: {satnogs['total_observations']:,}",
            f"- Completed passes: {satnogs['total_completed']:,} "
            f"({satnogs['total_good']:,} good); {rate_text}",
            f"- Still scheduled (future): {satnogs['total_scheduled']:,}",
            f"- Distinct satellites heard: {satnogs['satellite_hears_seen']:,}",
            f"- Distinct ground stations heard: {satnogs['station_hears_seen']:,}",
            "",
            "Counting is per distinct observation, using each observation's",
            "newest recorded version: the history table keeps a scheduled pass",
            "and its later vetted outcome as separate evidence rows, so a",
            "naive row count would double-count the same pass.",
            "",
            "Coverage is a polite bounded sample per run: the newest listing,",
            "one sweep per terminal status, and a capped number of previously",
            "scheduled passes re-checked once their window closed. Enough to",
            "quantify tracking activity around disturbances, not to",
            "reconstruct full telemetry history.",
            "",
        ]


    lines += [
        "## Charts",
        "",
    ]
    for path in chart_paths:
        lines.append(f"- {path.name}")
    lines += [
        "",
        "## Honest Limitations",
        "",
        "- Mean-motion change is a drag proxy, not a direct density measurement.",
        "- Maneuvers can mimic or hide drag; station-keeping objects must be",
        "  interpreted carefully.",
        "- A later orbital element is not perfect ground truth.",
        "- No causal claim is possible until many snapshots cover both quiet",
        "  and disturbed space-weather periods.",
        "",
    ]

    path = REPORTS_DIR / "research_summary.md"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def change_refresh_count(connection: duckdb.DuckDBPyConnection) -> int:
    return int(
        connection.sql(
            """
            SELECT COUNT(DISTINCT norad_catalog_id)
            FROM gold_orbit_change
            WHERE NOT same_element_set
            """
        ).fetchone()[0]
    )


def main() -> int:
    if not DATABASE.exists():
        print("Run earlier lessons first. The DuckDB database is missing.", file=sys.stderr)
        return 1

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(DATABASE))
    try:
        connection.execute(SQL_PATH.read_text(encoding="utf-8"))
        connection.execute(FRESHNESS_SQL_PATH.read_text(encoding="utf-8"))
        connection.execute(RELIABILITY_SQL_PATH.read_text(encoding="utf-8"))
        connection.execute(PILLARS_SQL_PATH.read_text(encoding="utf-8"))
        GOLD_DIR.mkdir(parents=True, exist_ok=True)
        for table, filename in [
            ("gold_element_freshness", "element_freshness.csv"),
            ("gold_freshness_by_group", "freshness_by_group.csv"),
            ("gold_freshness_by_band", "freshness_by_band.csv"),
            ("gold_orbit_reliability_index", "orbit_reliability_index.csv"),
            ("gold_reliability_class_summary", "reliability_class_summary.csv"),
            ("gold_reliability_group_summary", "reliability_group_summary.csv"),
            ("gold_sw_index_daily", "sw_index_daily.csv"),
            ("gold_satnogs_activity", "satnogs_activity.csv"),
            ("gold_debris_population", "debris_population.csv"),
        ]:
            connection.execute(
                f"COPY {table} TO ? (HEADER, DELIMITER ',')",
                [str(GOLD_DIR / filename)],
            )
        snapshot_count = connection.sql(
            "SELECT COUNT(DISTINCT snapshot_at_utc) FROM orbital_snapshot_history"
        ).fetchone()[0]
        change_count = connection.sql(
            "SELECT COUNT(*) FROM gold_orbit_change"
        ).fetchone()[0]
        weather = space_weather_summary(connection)

        print("StormTrace lesson 8 research analysis")
        print(f"Orbital snapshots: {snapshot_count}")
        population = population_by_group(connection)
        population_total = sum(count for _, count in population)
        tracked_total = total_tracked_objects(connection)
        print(f"Objects in latest snapshot of each group: {population_total:,}")
        for group, count in population:
            print(f"  {group} group: {count:,} objects")
        print(f"Distinct objects ever tracked: {tracked_total:,}")
        print(f"Orbit-change measurements: {change_count}")

        chart_paths: list[Path] = []
        for name, chart in [
            ("space weather timeline", chart_space_weather(connection)),
            ("space-weather index timeline", chart_sw_index_daily(connection)),
            ("orbit altitude distribution", chart_orbit_population(connection)),
            ("orbit decay rates", chart_orbit_change(connection)),
            ("element freshness", chart_element_freshness(connection)),
            ("orbit reliability index", chart_orbit_reliability(connection)),
            ("propagation disagreement", chart_propagation_disagreement(connection)),
            ("ORI validation", chart_ori_validation(connection)),
        ]:
            if chart is not None:
                chart_paths.append(chart)
                print(f"Chart created: {chart.relative_to(ROOT)}")
            else:
                print(f"Chart skipped (not enough data): {name}")

        report_path = write_report(
            connection, weather, chart_paths, snapshot_count, change_count
        )
        refreshed_count = change_refresh_count(connection)
        freshness_groups = fetch_rows(
            connection,
            """
            SELECT source_group, object_count, median_age_hours, stale_percent
            FROM gold_freshness_by_group
            ORDER BY source_group
            """,
        )
        reliability_groups = fetch_rows(
            connection,
            """
            SELECT source_group, object_count, median_index, low_count,
                   reduced_count
            FROM gold_reliability_group_summary
            ORDER BY source_group
            """,
        )
        reliability_classes = fetch_rows(
            connection,
            """
            SELECT reliability_class, object_count
            FROM gold_reliability_class_summary
            ORDER BY object_count DESC
            """,
        )
        disagreement_summary = None
        if table_exists(connection, "gold_propagation_disagreement"):
            disagreement_summary = fetch_rows(
                connection,
                """
                SELECT COUNT(*), ROUND(MEDIAN(total_km), 3), ROUND(MAX(total_km), 3),
                       ROUND(MEDIAN(propagation_span_hours), 2)
                FROM gold_propagation_disagreement
                WHERE measurement_quality = 'ok'
                """,
            )[0]
        validation_stats_console = None
        if table_exists(connection, "gold_ori_validation_stats"):
            validation_stats_console = dict(
                fetch_rows(
                    connection,
                    "SELECT metric, value FROM gold_ori_validation_stats",
                )
            )
        sw_index_console = sw_index_summary(connection)
        debris_console = debris_summary(connection)
        satnogs_console = satnogs_summary(connection)
    except (duckdb.Error, OSError, ValueError) as error:
        print(f"Research analysis error: {error}", file=sys.stderr)
        return 1
    finally:
        connection.close()

    print()
    print("Space weather (last 24 hours):")
    print(f"  Quiet hours: {weather['quiet_hours']}")
    print(f"  Southward-Bz hours: {weather['southward_hours']}")
    print(f"  Fast-wind hours: {weather['fast_wind_hours']}")
    print(f"  Minimum Bz: {weather['minimum_bz']} nT")
    print(f"  Max avg proton speed: {weather['maximum_average_speed']} km/s")
    print()
    if freshness_groups:
        print("Element freshness (latest snapshot of each group):")
        for group, count, median, percent in freshness_groups:
            print(
                f"  {group} group: {count:,} objects, "
                f"median {median} h old, {percent}% stale"
            )
        print()
    if reliability_groups:
        print("Orbit Reliability Index (prototype):")
        class_text = ", ".join(
            f"{label}: {count:,}" for label, count in reliability_classes
        )
        print(f"  Classes: {class_text}")
        for group, count, median, low, reduced in reliability_groups:
            print(
                f"  {group} group: {count:,} objects, median ORI {median}, "
                f"{low:,} low, {reduced:,} reduced"
            )
        print()
    if disagreement_summary and disagreement_summary[0]:
        pair_count, median_total, max_total, median_span = disagreement_summary
        print("SGP4 propagation disagreement (real measurements):")
        print(f"  Analysis-grade pairs: {pair_count:,}")
        print(f"  Median total: {median_total} km, max: {max_total} km")
        print(f"  Median propagation span: {median_span} h")
        print()
    if validation_stats_console:
        print("ORI validation against measured disagreement:")
        print(f"  Validated pairs: {int(validation_stats_console.get('pairs', 0)):,}")
        score_corr = validation_stats_console.get("spearman_score_vs_total_km")
        age_corr = validation_stats_console.get("spearman_age_vs_total_km")
        if score_corr is not None:
            print(f"  Spearman score vs error: {score_corr} (negative is correct)")
        if age_corr is not None:
            print(f"  Spearman element age vs error: {age_corr} (positive is correct)")
        print()
    sw_index = sw_index_console
    if sw_index:
        print("Daily space-weather indices (CelesTrak):")
        print(f"  Days: {sw_index['day_count']:,} "
              f"({sw_index['first_date']} to {sw_index['last_date']})")
        print(f"  Max Kp sum: {sw_index['max_kp_sum']}, "
              f"max Ap {sw_index['max_ap_avg']}, "
              f"F10.7 {sw_index['min_f10_7']}-{sw_index['max_f10_7']} sfu")
        print()
    debris = debris_console
    if debris:
        print("Debris population (iridium-33-debris group):")
        print(f"  Objects: {debris['object_count']:,}, "
              f"altitude {debris['min_altitude_km']}-{debris['max_altitude_km']} km, "
              f"median bstar {debris['median_bstar']}")
        print()
    satnogs = satnogs_console
    if satnogs:
        rate = satnogs.get("good_percent_of_completed")
        rate_text = f"{rate}% good" if rate is not None else "good rate unknown"
        print("SatNOGS observation activity:")
        print(f"  Distinct observations: {satnogs['total_observations']:,}, days "
              f"{satnogs['first_date']} to {satnogs['last_date']}")
        print(f"  Completed: {satnogs['total_completed']:,} "
              f"({satnogs['total_good']:,} good, {rate_text}), "
              f"scheduled: {satnogs['total_scheduled']:,}")
        print(f"  Distinct satellites heard: {satnogs['satellite_hears_seen']:,}, "
              f"stations: {satnogs['station_hears_seen']:,}")
        print()

    if snapshot_count < 2:
        print("Orbit-change detection is ready but needs at least 2 snapshots.")
        print("Keep collecting every 2 hours with: python src\\run_pipeline.py")
    elif refreshed_count == 0:
        print("Orbit change measured: source republished identical element")
        print("sets, so all measured changes are zero. Keep collecting;")
        print("new element sets will produce real decay rates.")
    else:
        print(f"Objects with refreshed element sets: {refreshed_count}")
        print("See orbit_decay_rates.png for measured decay rates.")
    print(f"Report: {report_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
