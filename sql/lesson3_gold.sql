-- Lesson 3: create a minute-level Gold space-weather table.
-- Both files contain UTC timestamps, but their seconds do not always match.
CREATE OR REPLACE TABLE gold_space_weather_minute AS
WITH magnetic_raw AS (
    SELECT
        date_trunc('minute', CAST(observed_at_utc AS TIMESTAMP)) AS observation_minute_utc,
        observed_at_utc AS magnetic_observed_at_utc,
        spacecraft AS magnetic_spacecraft,
        is_active_source AS magnetic_source_is_active,
        TRY_CAST(total_field_nanotesla AS DOUBLE) AS total_field_nanotesla,
        TRY_CAST(bx_gsm_nanotesla AS DOUBLE) AS bx_gsm_nanotesla,
        TRY_CAST(by_gsm_nanotesla AS DOUBLE) AS by_gsm_nanotesla,
        TRY_CAST(bz_gsm_nanotesla AS DOUBLE) AS bz_gsm_nanotesla,
        TRY_CAST(quality_code AS INTEGER) AS magnetic_quality_code
    FROM read_csv_auto('data/silver/noaa_magnetic_field_latest.csv')
), magnetic AS (
    SELECT * EXCLUDE (source_rank)
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY observation_minute_utc
                ORDER BY magnetic_source_is_active DESC,
                         magnetic_quality_code ASC,
                         magnetic_observed_at_utc DESC
            ) AS source_rank
        FROM magnetic_raw
    )
    WHERE source_rank = 1
), plasma_raw AS (
    SELECT
        date_trunc('minute', CAST(observed_at_utc AS TIMESTAMP)) AS observation_minute_utc,
        observed_at_utc AS plasma_observed_at_utc,
        spacecraft AS plasma_spacecraft,
        is_active_source AS plasma_source_is_active,
        TRY_CAST(proton_speed_km_per_second AS DOUBLE) AS proton_speed_km_per_second,
        TRY_CAST(proton_temperature_kelvin AS DOUBLE) AS proton_temperature_kelvin,
        TRY_CAST(proton_density_per_cubic_cm AS DOUBLE) AS proton_density_per_cubic_cm,
        TRY_CAST(quality_code AS INTEGER) AS plasma_quality_code
    FROM read_csv_auto('data/silver/noaa_plasma_latest.csv')
), plasma AS (
    SELECT * EXCLUDE (source_rank)
    FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY observation_minute_utc
                ORDER BY plasma_source_is_active DESC,
                         plasma_quality_code ASC,
                         plasma_observed_at_utc DESC
            ) AS source_rank
        FROM plasma_raw
    )
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
