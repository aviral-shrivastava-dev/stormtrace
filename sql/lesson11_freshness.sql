-- Lesson 11: element freshness analysis.
-- Element age is measured from the element's own epoch to the snapshot
-- time at which we captured it. This is the point-in-time correct view of
-- how fresh the public catalog was for each object when we observed it.

CREATE OR REPLACE TABLE gold_element_freshness AS
WITH latest_per_group AS (
    SELECT source_group, MAX(snapshot_at_utc) AS latest_snapshot
    FROM orbital_snapshot_history
    GROUP BY source_group
),
current_objects AS (
    SELECT h.*
    FROM orbital_snapshot_history h
    JOIN latest_per_group l
      ON h.source_group = l.source_group
     AND h.snapshot_at_utc = l.latest_snapshot
),
aged AS (
    SELECT
        object_name,
        norad_catalog_id,
        source_group,
        element_epoch_utc,
        mean_motion_revolutions_per_day,
        date_diff('minute', element_epoch_utc, timezone('UTC', snapshot_at_utc)) / 60.0
            AS element_age_hours,
        POWER(
            398600.4418 /
            POWER(mean_motion_revolutions_per_day * 2.0 * PI() / 86400.0, 2),
            1.0 / 3.0
        ) - 6378.137 AS mean_altitude_km
    FROM current_objects
    WHERE element_epoch_utc IS NOT NULL
      AND mean_motion_revolutions_per_day > 0
)
SELECT
    object_name,
    norad_catalog_id,
    source_group,
    element_epoch_utc,
    element_age_hours,
    element_age_hours > 24 AS is_stale_over_24_hours,
    mean_altitude_km,
    CASE
        WHEN mean_altitude_km < 300 THEN 'very_low_leo'
        WHEN mean_altitude_km < 600 THEN 'lower_leo'
        WHEN mean_altitude_km < 2000 THEN 'upper_leo'
        ELSE 'above_leo'
    END AS altitude_band
FROM aged;

CREATE OR REPLACE TABLE gold_freshness_by_group AS
SELECT
    source_group,
    COUNT(*) AS object_count,
    ROUND(MEDIAN(element_age_hours), 2) AS median_age_hours,
    ROUND(quantile_cont(element_age_hours, 0.9), 2) AS p90_age_hours,
    ROUND(MAX(element_age_hours), 2) AS maximum_age_hours,
    SUM(CASE WHEN is_stale_over_24_hours THEN 1 ELSE 0 END) AS stale_count,
    ROUND(100.0 * SUM(CASE WHEN is_stale_over_24_hours THEN 1 ELSE 0 END) / COUNT(*), 1) AS stale_percent
FROM gold_element_freshness
GROUP BY source_group
ORDER BY source_group;

CREATE OR REPLACE TABLE gold_freshness_by_band AS
SELECT
    altitude_band,
    COUNT(*) AS object_count,
    ROUND(MEDIAN(element_age_hours), 2) AS median_age_hours,
    ROUND(quantile_cont(element_age_hours, 0.9), 2) AS p90_age_hours,
    SUM(CASE WHEN is_stale_over_24_hours THEN 1 ELSE 0 END) AS stale_count,
    ROUND(100.0 * SUM(CASE WHEN is_stale_over_24_hours THEN 1 ELSE 0 END) / COUNT(*), 1) AS stale_percent
FROM gold_element_freshness
GROUP BY altitude_band
ORDER BY median_age_hours;
