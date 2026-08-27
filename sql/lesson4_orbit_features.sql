-- Lesson 4: derive understandable orbital features from public mean elements.
-- Constants use kilometers, seconds, and Earth's standard gravitational value.
-- Reads every tracked group's Silver file at once. The filename=true option
-- adds the source path, from which the group name is extracted.
CREATE OR REPLACE TABLE gold_satellite_orbit_features AS
WITH typed AS (
    SELECT
        regexp_extract(filename, '([a-z0-9-]+)_satellites_latest\.csv$', 1)
            AS source_group,
        object_name,
        TRY_CAST(norad_catalog_id AS BIGINT) AS norad_catalog_id,
        TRY_CAST(element_epoch_utc AS TIMESTAMP) AS element_epoch_utc,
        TRY_CAST(inclination_degrees AS DOUBLE) AS inclination_degrees,
        TRY_CAST(eccentricity AS DOUBLE) AS eccentricity,
        TRY_CAST(mean_motion_revolutions_per_day AS DOUBLE)
            AS mean_motion_revolutions_per_day,
        TRY_CAST(bstar_drag_term AS DOUBLE) AS bstar_drag_term,
        TRY_CAST(retrieved_at_utc AS TIMESTAMPTZ) AS retrieved_at_utc,
        source_url
    FROM read_csv_auto('data/silver/*_satellites_latest.csv', filename = true)
), calculated AS (
    SELECT
        *,
        1440.0 / mean_motion_revolutions_per_day AS orbital_period_minutes,
        POWER(
            398600.4418 /
            POWER(mean_motion_revolutions_per_day * 2.0 * PI() / 86400.0, 2),
            1.0 / 3.0
        ) AS semi_major_axis_km
    FROM typed
    WHERE mean_motion_revolutions_per_day > 0
      AND eccentricity >= 0
      AND eccentricity < 1
), featured AS (
    SELECT
        *,
        semi_major_axis_km - 6378.137 AS mean_altitude_km,
        semi_major_axis_km * (1.0 - eccentricity) - 6378.137 AS perigee_km,
        semi_major_axis_km * (1.0 + eccentricity) - 6378.137 AS apogee_km,
        date_diff('hour', element_epoch_utc, retrieved_at_utc) AS element_age_hours
    FROM calculated
)
SELECT
    object_name,
    norad_catalog_id,
    source_group,
    element_epoch_utc,
    retrieved_at_utc,
    element_age_hours,
    element_age_hours > 24 AS is_stale_over_24_hours,
    inclination_degrees,
    eccentricity,
    mean_motion_revolutions_per_day,
    bstar_drag_term,
    orbital_period_minutes,
    semi_major_axis_km,
    mean_altitude_km,
    perigee_km,
    apogee_km,
    CASE
        WHEN mean_altitude_km < 300 THEN 'very_low_leo'
        WHEN mean_altitude_km < 600 THEN 'lower_leo'
        WHEN mean_altitude_km < 2000 THEN 'upper_leo'
        ELSE 'above_leo'
    END AS altitude_band,
    source_url,
    CURRENT_TIMESTAMP AS gold_created_at_utc
FROM featured
ORDER BY mean_altitude_km;

CREATE OR REPLACE TABLE gold_orbit_band_summary AS
SELECT
    altitude_band,
    COUNT(*) AS object_count,
    ROUND(AVG(mean_altitude_km), 2) AS average_altitude_km,
    ROUND(MIN(perigee_km), 2) AS minimum_perigee_km,
    ROUND(MAX(apogee_km), 2) AS maximum_apogee_km,
    SUM(CASE WHEN is_stale_over_24_hours THEN 1 ELSE 0 END) AS stale_object_count
FROM gold_satellite_orbit_features
GROUP BY altitude_band
ORDER BY average_altitude_km;
