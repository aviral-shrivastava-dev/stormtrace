-- Lesson 23: the three-pillar research dataset.
--
-- StormTrace now tracks three source families beyond orbital elements and
-- solar-wind plasma:
--
--   1. Daily geomagnetic and solar indices (CelesTrak SW-Last5Years): the
--      classic drivers of thermospheric density (Kp, Ap, F10.7, sunspot
--      number). The most recent day is provisional and may be revised, so
--      the history table keeps every version and this Gold table selects
--      the newest snapshot per date.
--   2. Satellite observation activity (SatNOGS network): independent,
--      crowd-sourced evidence that satellites were heard by ground
--      stations -- metadata, not decoded frames.
--   3. A true debris population (iridium-33-debris group): objects from one
--      collision event, a tight ~780 km band, no maneuvers -- the cleanest
--      drag signal in the catalog.
--
-- These are the pillars that, joined against orbital snapshots, turn the
-- project from "drag proxy" into a physical chain:
--
--   solar indices -> atmospheric density -> drag -> orbit degradation
--
--   tracked by the observables: F10.7/Kp (space weather), bstar/altitude
--   change (orbit elements), and SatNOGS pass counts (telemetry activity).

-- Keep the newest observation version per calendar date.
-- NOTE ON UNITS: CelesTrak's SW file stores the three-hourly planetary K
-- indices (and their daily sum) in units of ONE-TENTH of a Kp index, so a
-- file value of 67 means Kp 6.7. Raw Bronze and Silver keep the file's own
-- values as evidence; this Gold table normalizes them to standard Kp
-- units so the indexed sums (max possible 72.0) and the Ap values are on
-- the same familiar scale.
CREATE OR REPLACE TABLE gold_sw_index_daily AS
SELECT
    observation_date,
    ROUND(kp1 / 10.0, 2) AS kp1,
    ROUND(kp2 / 10.0, 2) AS kp2,
    ROUND(kp3 / 10.0, 2) AS kp3,
    ROUND(kp4 / 10.0, 2) AS kp4,
    ROUND(kp5 / 10.0, 2) AS kp5,
    ROUND(kp6 / 10.0, 2) AS kp6,
    ROUND(kp7 / 10.0, 2) AS kp7,
    ROUND(kp8 / 10.0, 2) AS kp8,
    ROUND(kp_sum / 10.0, 2) AS kp_sum,
    ap_avg,
    sunspot_number,
    f10_7_observed,
    f10_7_data_type,
    CAST(last_snapshot_utc AS TIMESTAMP) AS last_retrieved_at_utc
FROM (
    SELECT
        observation_date,
        kp1, kp2, kp3, kp4, kp5, kp6, kp7, kp8,
        kp_sum,
        ap_avg,
        sunspot_number,
        f10_7_observed,
        f10_7_data_type,
        snapshot_at_utc AS last_snapshot_utc,
        ROW_NUMBER() OVER (
            PARTITION BY sw.observation_date
            ORDER BY sw.snapshot_at_utc DESC
        ) AS newest_rank
    FROM space_weather_index_history AS sw
)
WHERE newest_rank = 1
ORDER BY observation_date;

-- Daily SatNOGS observation activity: hears, distinct satellites and
-- stations, and good passes. Combined with orbit features, this answers
-- "was a population still observable and transmitting during a storm?"
CREATE OR REPLACE TABLE gold_satnogs_activity AS
SELECT
    CAST(start_utc AS DATE) AS observation_date,
    COUNT(*) AS observation_count,
    COUNT(DISTINCT norad_catalog_id) AS distinct_satellites,
    COUNT(DISTINCT station_id) AS distinct_stations,
    COUNT(*) FILTER (WHERE status = 'good') AS good_count,
    COUNT(*) FILTER (WHERE status IN ('good', 'future')) AS usable_count
FROM satnogs_observation_history
GROUP BY CAST(start_utc AS DATE)
ORDER BY observation_date;

-- The debris cohort, shaped for direct comparison against the other groups.
CREATE OR REPLACE TABLE gold_debris_population AS
SELECT
    object_name,
    norad_catalog_id,
    ROUND(mean_altitude_km, 1) AS mean_altitude_km,
    ROUND(eccentricity, 6) AS eccentricity,
    bstar_drag_term,
    element_age_hours,
    element_epoch_utc,
    CAST(f.retrieved_at_utc AS TIMESTAMP) AS snapshot_at_utc
FROM gold_satellite_orbit_features AS f
WHERE f.source_group = 'iridium-33-debris'
ORDER BY norad_catalog_id;