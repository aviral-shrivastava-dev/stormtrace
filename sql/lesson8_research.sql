-- Lesson 8: research analysis tables.
-- Orbit change detection: compare each object's mean motion between
-- consecutive snapshots. Mean motion INCREASES when the orbit DECAYS,
-- so a positive delta is evidence of drag.

CREATE OR REPLACE TABLE gold_orbit_change AS
WITH ordered AS (
    SELECT
        object_name,
        norad_catalog_id,
        snapshot_at_utc,
        element_epoch_utc,
        mean_motion_revolutions_per_day,
        eccentricity,
        inclination_degrees,
        LAG(snapshot_at_utc) OVER w AS previous_snapshot_at_utc,
        LAG(element_epoch_utc) OVER w AS previous_element_epoch_utc,
        LAG(mean_motion_revolutions_per_day) OVER w AS previous_mean_motion,
        LAG(eccentricity) OVER w AS previous_eccentricity
    FROM orbital_snapshot_history
    WINDOW w AS (PARTITION BY norad_catalog_id ORDER BY snapshot_at_utc)
), changes AS (
    SELECT
        object_name,
        norad_catalog_id,
        timezone('UTC', previous_snapshot_at_utc) AS previous_snapshot_at_utc,
        timezone('UTC', snapshot_at_utc) AS snapshot_at_utc,
        date_diff('minute', previous_snapshot_at_utc, snapshot_at_utc)
            AS interval_minutes,
        previous_element_epoch_utc,
        element_epoch_utc,
        previous_element_epoch_utc = element_epoch_utc
            AS same_element_set,
        previous_mean_motion,
        mean_motion_revolutions_per_day,
        mean_motion_revolutions_per_day - previous_mean_motion
            AS mean_motion_delta,
        previous_eccentricity,
        eccentricity,
        eccentricity - previous_eccentricity AS eccentricity_delta,
        inclination_degrees,
        POWER(398600.4418 / POWER(previous_mean_motion * 2.0 * PI() / 86400.0, 2), 1.0/3.0)
            AS previous_semi_major_axis_km,
        POWER(398600.4418 / POWER(mean_motion_revolutions_per_day * 2.0 * PI() / 86400.0, 2), 1.0/3.0)
            AS current_semi_major_axis_km
    FROM ordered
    WHERE previous_mean_motion IS NOT NULL
      AND previous_mean_motion > 0
      AND mean_motion_revolutions_per_day > 0
)
SELECT
    *,
    current_semi_major_axis_km - previous_semi_major_axis_km
        AS semi_major_axis_change_km,
    CASE
        WHEN interval_minutes IS NULL OR interval_minutes <= 0 THEN NULL
        ELSE -1.0 * (current_semi_major_axis_km - previous_semi_major_axis_km)
             * 1440.0 / interval_minutes
    END AS decay_rate_km_per_day
FROM changes
ORDER BY decay_rate_km_per_day DESC NULLS LAST;

-- Hourly space-weather conditions from the minute-level Gold table.
-- A simple disturbance indicator: hourly average Bz below -5 nT or
-- hourly average proton speed above 500 km/s.
CREATE OR REPLACE TABLE gold_space_weather_hourly AS
WITH hourly AS (
    SELECT
        date_trunc('hour', observation_minute_utc) AS hour_utc,
        AVG(bz_gsm_nanotesla) AS average_bz_gsm,
        MIN(bz_gsm_nanotesla) AS minimum_bz_gsm,
        AVG(proton_speed_km_per_second) AS average_proton_speed,
        AVG(proton_density_per_cubic_cm) AS average_proton_density,
        COUNT(*) AS minute_count,
        COUNT(*) FILTER (WHERE bz_gsm_nanotesla IS NOT NULL)
            AS minutes_with_bz
    FROM gold_space_weather_minute
    GROUP BY 1
)
SELECT
    hour_utc,
    average_bz_gsm,
    minimum_bz_gsm,
    average_proton_speed,
    average_proton_density,
    minute_count,
    minutes_with_bz,
    CASE
        WHEN average_bz_gsm < -5.0 THEN 'southward_bz'
        WHEN average_proton_speed > 500.0 THEN 'fast_wind'
        ELSE 'quiet'
    END AS disturbance_level
FROM hourly
ORDER BY hour_utc;
