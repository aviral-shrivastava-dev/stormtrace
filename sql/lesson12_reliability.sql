-- Lesson 12: Orbit Reliability Index (ORI) prototype.
--
-- ORI is an explainable, weighted indicator of how much the public orbit
-- estimate for an object should be trusted right now. It is NOT collision
-- probability and NOT a measurement of true position error.
--
-- Components:
--   freshness_score : 100 at 0 h element age, falling linearly to 0 at 48 h.
--   drag_safety     : 0 at 300 km altitude (strong drag uncertainty growth),
--                     rising linearly to 100 at 800 km (weak drag).
--   base_score      : 0.55 * freshness + 0.45 * drag_safety.
--   environment     : multiplier from the worst disturbance level observed
--                     in the last 3 hours of hourly space weather.
--                     quiet = 1.0, fast_wind = 0.9, southward_bz = 0.8.
--   ORI             : base_score * environment factor, on a 0-100 scale.
--
-- Weights and thresholds are documented prototype choices, not calibrated
-- constants. They will be revisited once enough snapshots exist to compare
-- ORI against measured propagation disagreement.

CREATE OR REPLACE TABLE gold_orbit_reliability_index AS
WITH latest_per_group AS (
    SELECT source_group, MAX(snapshot_at_utc) AS latest_snapshot
    FROM orbital_snapshot_history
    GROUP BY source_group
),
current_objects AS (
    SELECT
        h.object_name,
        h.norad_catalog_id,
        h.source_group,
        h.element_epoch_utc,
        date_diff('minute', h.element_epoch_utc, timezone('UTC', h.snapshot_at_utc)) / 60.0
            AS element_age_hours,
        POWER(
            398600.4418 /
            POWER(h.mean_motion_revolutions_per_day * 2.0 * PI() / 86400.0, 2),
            1.0 / 3.0
        ) - 6378.137 AS mean_altitude_km
    FROM orbital_snapshot_history h
    JOIN latest_per_group l
      ON h.source_group = l.source_group
     AND h.snapshot_at_utc = l.latest_snapshot
    WHERE h.element_epoch_utc IS NOT NULL
      AND h.mean_motion_revolutions_per_day > 0
),
weather_context AS (
    SELECT
        CASE
            WHEN COUNT(*) FILTER (WHERE disturbance_level = 'southward_bz') > 0 THEN 0.8
            WHEN COUNT(*) FILTER (WHERE disturbance_level = 'fast_wind') > 0 THEN 0.9
            ELSE 1.0
        END AS environment_factor,
        MAX(hour_utc) AS context_through_hour,
        COUNT(*) AS hours_considered
    FROM gold_space_weather_hourly
    WHERE hour_utc >= (SELECT MAX(hour_utc) FROM gold_space_weather_hourly) - INTERVAL 3 HOUR
),
scored AS (
    SELECT
        object_name,
        norad_catalog_id,
        source_group,
        element_age_hours,
        mean_altitude_km,
        100.0 * GREATEST(0.0, LEAST(1.0, 1.0 - element_age_hours / 48.0))
            AS freshness_score,
        100.0 * GREATEST(0.0, LEAST(1.0, (mean_altitude_km - 300.0) / 500.0))
            AS drag_safety_score
    FROM current_objects
)
SELECT
    scored.object_name,
    scored.norad_catalog_id,
    scored.source_group,
    ROUND(scored.element_age_hours, 2) AS element_age_hours,
    ROUND(scored.mean_altitude_km, 2) AS mean_altitude_km,
    ROUND(scored.freshness_score, 1) AS freshness_score,
    ROUND(scored.drag_safety_score, 1) AS drag_safety_score,
    weather_context.environment_factor,
    weather_context.context_through_hour,
    ROUND(0.55 * scored.freshness_score + 0.45 * scored.drag_safety_score, 1)
        AS base_score,
    ROUND(
        (0.55 * scored.freshness_score + 0.45 * scored.drag_safety_score)
        * weather_context.environment_factor,
        1
    ) AS orbit_reliability_index,
    CASE
        WHEN (0.55 * scored.freshness_score + 0.45 * scored.drag_safety_score)
             * weather_context.environment_factor >= 80 THEN 'high'
        WHEN (0.55 * scored.freshness_score + 0.45 * scored.drag_safety_score)
             * weather_context.environment_factor >= 60 THEN 'moderate'
        WHEN (0.55 * scored.freshness_score + 0.45 * scored.drag_safety_score)
             * weather_context.environment_factor >= 40 THEN 'reduced'
        ELSE 'low'
    END AS reliability_class
FROM scored
CROSS JOIN weather_context
ORDER BY orbit_reliability_index;

CREATE OR REPLACE TABLE gold_reliability_class_summary AS
SELECT
    reliability_class,
    COUNT(*) AS object_count,
    ROUND(MEDIAN(orbit_reliability_index), 1) AS median_index,
    ROUND(MIN(orbit_reliability_index), 1) AS minimum_index,
    ROUND(MAX(orbit_reliability_index), 1) AS maximum_index
FROM gold_orbit_reliability_index
GROUP BY reliability_class
ORDER BY median_index;

CREATE OR REPLACE TABLE gold_reliability_group_summary AS
SELECT
    source_group,
    COUNT(*) AS object_count,
    ROUND(AVG(orbit_reliability_index), 1) AS average_index,
    ROUND(MEDIAN(orbit_reliability_index), 1) AS median_index,
    SUM(CASE WHEN reliability_class = 'low' THEN 1 ELSE 0 END) AS low_count,
    SUM(CASE WHEN reliability_class = 'reduced' THEN 1 ELSE 0 END) AS reduced_count
FROM gold_orbit_reliability_index
GROUP BY source_group
ORDER BY source_group;
