-- Lesson 3 (rebuilt): minute-level Gold space weather from HISTORY.
--
-- This table used to read data/silver/noaa_*_latest.csv, the file each
-- ingestion run overwrites. NOAA's real-time feed is a 24-hour rolling
-- window, so Gold could only ever see one day no matter how long the
-- project collected -- while noaa_magnetic_history accumulated every
-- snapshot ever taken. Every downstream consumer (hourly disturbance
-- levels, the ORI environment factor, the storm comparison) inherited
-- that one-day ceiling, which is why the environment factor was a
-- constant 1.0 across the whole database.
--
-- Reading history instead makes Gold cover the full collection period.
--
-- Deduplication is now required. Overlapping rolling-window fetches store
-- the same observed minute many times (roughly 6x at a 2-hour cadence),
-- so one observation per (minute, spacecraft) is chosen first, then one
-- spacecraft per minute. The winner is the most trustworthy version:
--   1. NOAA's own active-source flag
--   2. the lowest (best) quality code
--   3. the most recent snapshot, which supersedes earlier revisions
CREATE OR REPLACE TABLE gold_space_weather_minute AS
WITH magnetic_ranked AS (
    SELECT
        date_trunc('minute', observed_at_utc) AS observation_minute_utc,
        observed_at_utc AS magnetic_observed_at_utc,
        spacecraft AS magnetic_spacecraft,
        is_active_source AS magnetic_source_is_active,
        bt AS total_field_nanotesla,
        bx_gsm AS bx_gsm_nanotesla,
        by_gsm AS by_gsm_nanotesla,
        bz_gsm AS bz_gsm_nanotesla,
        quality_code AS magnetic_quality_code,
        row_number() OVER (
            PARTITION BY date_trunc('minute', observed_at_utc)
            ORDER BY is_active_source DESC NULLS LAST,
                     quality_code ASC NULLS LAST,
                     snapshot_at_utc DESC,
                     observed_at_utc DESC
        ) AS source_rank
    FROM noaa_magnetic_history
    WHERE observed_at_utc IS NOT NULL
), magnetic AS (
    SELECT * EXCLUDE (source_rank)
    FROM magnetic_ranked
    WHERE source_rank = 1
), plasma_ranked AS (
    SELECT
        date_trunc('minute', observed_at_utc) AS observation_minute_utc,
        observed_at_utc AS plasma_observed_at_utc,
        spacecraft AS plasma_spacecraft,
        is_active_source AS plasma_source_is_active,
        proton_speed AS proton_speed_km_per_second,
        proton_temperature AS proton_temperature_kelvin,
        proton_density AS proton_density_per_cubic_cm,
        quality_code AS plasma_quality_code,
        row_number() OVER (
            PARTITION BY date_trunc('minute', observed_at_utc)
            ORDER BY is_active_source DESC NULLS LAST,
                     quality_code ASC NULLS LAST,
                     snapshot_at_utc DESC,
                     observed_at_utc DESC
        ) AS source_rank
    FROM noaa_plasma_history
    WHERE observed_at_utc IS NOT NULL
), plasma AS (
    SELECT * EXCLUDE (source_rank)
    FROM plasma_ranked
    WHERE source_rank = 1
)
SELECT
    magnetic.observation_minute_utc,
    magnetic.magnetic_observed_at_utc,
    plasma.plasma_observed_at_utc,
    magnetic.magnetic_spacecraft,
    plasma.plasma_spacecraft,
    magnetic.magnetic_source_is_active,
    plasma.plasma_source_is_active,
    magnetic.total_field_nanotesla,
    magnetic.bx_gsm_nanotesla,
    magnetic.by_gsm_nanotesla,
    magnetic.bz_gsm_nanotesla,
    plasma.proton_speed_km_per_second,
    plasma.proton_temperature_kelvin,
    plasma.proton_density_per_cubic_cm,
    magnetic.magnetic_quality_code,
    plasma.plasma_quality_code,
    CURRENT_TIMESTAMP AS gold_created_at_utc
FROM magnetic
LEFT JOIN plasma
    ON magnetic.observation_minute_utc = plasma.observation_minute_utc
ORDER BY magnetic.observation_minute_utc DESC;

