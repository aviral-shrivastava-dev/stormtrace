-- Lesson 5: simple historical research summaries.
CREATE OR REPLACE TABLE gold_orbit_snapshot_summary AS
SELECT
    timezone('UTC', snapshot_at_utc) AS snapshot_at_utc,
    COUNT(*) AS object_count,
    COUNT(DISTINCT norad_catalog_id) AS unique_object_count,
    ROUND(AVG(mean_motion_revolutions_per_day), 5) AS average_mean_motion,
    ROUND(AVG(bstar_drag_term), 8) AS average_bstar
FROM orbital_snapshot_history
GROUP BY snapshot_at_utc
ORDER BY snapshot_at_utc;

CREATE OR REPLACE TABLE gold_space_weather_snapshot_summary AS
WITH magnetic AS (
    SELECT
        snapshot_at_utc,
        COUNT(*) AS magnetic_row_count,
        ROUND(AVG(bz_gsm), 3) AS average_bz_gsm,
        ROUND(MIN(bz_gsm), 3) AS minimum_bz_gsm
    FROM noaa_magnetic_history
    GROUP BY snapshot_at_utc
), plasma AS (
    SELECT
        snapshot_at_utc,
        COUNT(*) AS plasma_row_count,
        ROUND(AVG(proton_speed), 2) AS average_proton_speed
    FROM noaa_plasma_history
    GROUP BY snapshot_at_utc
)
SELECT
    timezone('UTC', magnetic.snapshot_at_utc) AS snapshot_at_utc,
    magnetic.magnetic_row_count,
    plasma.plasma_row_count,
    magnetic.average_bz_gsm,
    magnetic.minimum_bz_gsm,
    plasma.average_proton_speed
FROM magnetic
LEFT JOIN plasma USING (snapshot_at_utc)
ORDER BY magnetic.snapshot_at_utc;
