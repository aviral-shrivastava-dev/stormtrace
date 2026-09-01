"""Tests for the SQL transformations that produce Gold tables.

Each test builds the minimal source tables in an in-memory DuckDB, runs
the real .sql file, and asserts on the result. Two shipped defects are
pinned:

- `gold_satnogs_activity` counted rows instead of distinct observations,
  inflating daily totals (50 reported where 32 existed) because history
  deliberately keeps several versions of the same observation.
- `gold_space_weather_minute` read the Silver latest-CSV (a 24-hour
  rolling window) instead of the accumulated history, capping Gold at one
  day and pinning the ORI environment factor at a constant 1.0.
"""

from __future__ import annotations

from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "sql"


def run_sql(connection, filename: str) -> None:
    connection.execute((SQL_DIR / filename).read_text(encoding="utf-8"))


def create_satnogs_history(connection) -> None:
    connection.execute(
        """
        CREATE TABLE satnogs_observation_history (
            snapshot_at_utc TIMESTAMPTZ,
            observation_id BIGINT,
            start_utc TIMESTAMP,
            end_utc TIMESTAMP,
            status VARCHAR,
            norad_catalog_id BIGINT,
            satellite_id VARCHAR,
            station_id BIGINT,
            station_lat DOUBLE,
            station_lng DOUBLE,
            observation_frequency_hz DOUBLE,
            transmitter_mode VARCHAR,
            source_file VARCHAR,
            source_sha256 VARCHAR
        )
        """
    )


def insert_observation(
    connection,
    snapshot: str,
    observation_id: int,
    status: str,
    start: str = "2026-08-30 10:00:00",
    norad: int = 100,
    station: int = 7,
) -> None:
    connection.execute(
        "INSERT INTO satnogs_observation_history VALUES "
        "(?, ?, ?, ?, ?, ?, 'SAT-A', ?, 45.0, -111.0, 4.36e8, 'GFSK', 'f.json', 'abc')",
        [snapshot, observation_id, start, start, status, norad, station],
    )


def create_sw_index_history(connection) -> None:
    connection.execute(
        """
        CREATE TABLE space_weather_index_history (
            snapshot_at_utc TIMESTAMPTZ,
            observation_date DATE,
            kp1 DOUBLE, kp2 DOUBLE, kp3 DOUBLE, kp4 DOUBLE,
            kp5 DOUBLE, kp6 DOUBLE, kp7 DOUBLE, kp8 DOUBLE,
            kp_sum DOUBLE,
            ap_avg DOUBLE,
            sunspot_number DOUBLE,
            f10_7_observed DOUBLE,
            f10_7_data_type VARCHAR,
            source_file VARCHAR,
            source_sha256 VARCHAR
        )
        """
    )


def create_orbit_features(connection) -> None:
    connection.execute(
        """
        CREATE TABLE gold_satellite_orbit_features (
            object_name VARCHAR,
            norad_catalog_id BIGINT,
            source_group VARCHAR,
            element_epoch_utc TIMESTAMP,
            retrieved_at_utc TIMESTAMPTZ,
            element_age_hours BIGINT,
            mean_altitude_km DOUBLE,
            eccentricity DOUBLE,
            bstar_drag_term DOUBLE
        )
        """
    )


@pytest.fixture
def pillars_db(duckdb_connection):
    """All source tables lesson23_pillars.sql expects, created empty."""
    create_satnogs_history(duckdb_connection)
    create_sw_index_history(duckdb_connection)
    create_orbit_features(duckdb_connection)
    return duckdb_connection


class TestSatnogsActivity:
    def test_repeated_versions_of_one_observation_count_once(self, pillars_db):
        # The same pass, stored three times as it was re-observed.
        insert_observation(pillars_db, "2026-08-30 12:00:00+00", 1, "future")
        insert_observation(pillars_db, "2026-08-30 14:00:00+00", 1, "unknown")
        insert_observation(pillars_db, "2026-08-30 16:00:00+00", 1, "good")

        run_sql(pillars_db, "lesson23_pillars.sql")
        row = pillars_db.execute(
            "SELECT observation_count, good_count, completed_count, scheduled_count "
            "FROM gold_satnogs_activity"
        ).fetchone()
        # One observation, counted once, using its NEWEST status.
        assert row == (1, 1, 1, 0)

    def test_newest_version_wins_over_an_earlier_good(self, pillars_db):
        insert_observation(pillars_db, "2026-08-30 12:00:00+00", 1, "good")
        insert_observation(pillars_db, "2026-08-30 16:00:00+00", 1, "failed")

        run_sql(pillars_db, "lesson23_pillars.sql")
        assert pillars_db.execute(
            "SELECT good_count, completed_count FROM gold_satnogs_activity"
        ).fetchone() == (0, 1)

    def test_scheduled_passes_are_not_counted_as_outcomes(self, pillars_db):
        for observation_id in (1, 2, 3):
            insert_observation(
                pillars_db, "2026-08-30 12:00:00+00", observation_id, "future"
            )

        run_sql(pillars_db, "lesson23_pillars.sql")
        row = pillars_db.execute(
            "SELECT observation_count, completed_count, scheduled_count, "
            "good_percent_of_completed FROM gold_satnogs_activity"
        ).fetchone()
        assert row[:3] == (3, 0, 3)
        # An unknown success rate must be NULL, never 0: reporting 0 would
        # read as "every pass failed".
        assert row[3] is None

    def test_good_percent_is_computed_over_completed_passes_only(self, pillars_db):
        for observation_id, status in [
            (1, "good"),
            (2, "good"),
            (3, "bad"),
            (4, "failed"),
            (5, "future"),
        ]:
            insert_observation(
                pillars_db, "2026-08-30 12:00:00+00", observation_id, status
            )

        run_sql(pillars_db, "lesson23_pillars.sql")
        # 2 good of 4 completed = 50%, with the scheduled pass excluded
        # from the denominator.
        assert pillars_db.execute(
            "SELECT completed_count, scheduled_count, good_percent_of_completed "
            "FROM gold_satnogs_activity"
        ).fetchone() == (4, 1, 50.0)

    def test_distinct_satellites_and_stations_are_counted(self, pillars_db):
        for observation_id, norad, station in [(1, 100, 7), (2, 100, 8), (3, 200, 7)]:
            insert_observation(
                pillars_db,
                "2026-08-30 12:00:00+00",
                observation_id,
                "good",
                norad=norad,
                station=station,
            )

        run_sql(pillars_db, "lesson23_pillars.sql")
        assert pillars_db.execute(
            "SELECT distinct_satellites, distinct_stations FROM gold_satnogs_activity"
        ).fetchone() == (2, 2)


class TestSwIndexDaily:
    def test_kp_tenths_are_normalized_to_standard_units(self, pillars_db):
        # CelesTrak stores Kp in tenths: a file value of 67 means Kp 6.7.
        pillars_db.execute(
            "INSERT INTO space_weather_index_history VALUES "
            "('2026-08-30 12:00:00+00', DATE '2026-08-29', "
            "67, 30, 30, 30, 30, 30, 30, 30, 277, 15, 120, 155.4, 'OBS', 'f', 'a')"
        )

        run_sql(pillars_db, "lesson23_pillars.sql")
        kp1, kp_sum, ap_avg = pillars_db.execute(
            "SELECT kp1, kp_sum, ap_avg FROM gold_sw_index_daily"
        ).fetchone()
        assert kp1 == pytest.approx(6.7)
        assert kp_sum == pytest.approx(27.7)
        # Ap has its own units and must NOT be divided by ten.
        assert ap_avg == pytest.approx(15.0)

    def test_the_newest_snapshot_per_date_wins(self, pillars_db):
        # The most recent day is provisional and gets revised; history keeps
        # both versions and Gold must select the newer one.
        for snapshot, f107 in [
            ("2026-08-30 12:00:00+00", 150.0),
            ("2026-08-30 18:00:00+00", 158.2),
        ]:
            pillars_db.execute(
                "INSERT INTO space_weather_index_history VALUES "
                f"('{snapshot}', DATE '2026-08-29', "
                f"30, 30, 30, 30, 30, 30, 30, 30, 240, 12, 100, {f107}, 'OBS', 'f', 'a')"
            )

        run_sql(pillars_db, "lesson23_pillars.sql")
        rows = pillars_db.execute(
            "SELECT observation_date, f10_7_observed FROM gold_sw_index_daily"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][1] == pytest.approx(158.2)

    def test_maximum_possible_kp_sum_is_on_the_standard_scale(self, pillars_db):
        # Eight three-hourly Kp values of 9.0 sum to 72.0. In file units
        # that is 720, so a missing division would report an impossible 720.
        pillars_db.execute(
            "INSERT INTO space_weather_index_history VALUES "
            "('2026-08-30 12:00:00+00', DATE '2026-08-29', "
            "90, 90, 90, 90, 90, 90, 90, 90, 720, 400, 200, 200.0, 'OBS', 'f', 'a')"
        )

        run_sql(pillars_db, "lesson23_pillars.sql")
        kp_sum = pillars_db.execute(
            "SELECT kp_sum FROM gold_sw_index_daily"
        ).fetchone()[0]
        assert kp_sum == pytest.approx(72.0)
        assert kp_sum <= 72.0


class TestDebrisPopulation:
    def test_only_the_debris_group_is_selected(self, pillars_db):
        for name, group in [
            ("DEBRIS-1", "iridium-33-debris"),
            ("ISS", "stations"),
            ("CUBE-1", "cubesat"),
        ]:
            pillars_db.execute(
                "INSERT INTO gold_satellite_orbit_features VALUES "
                "(?, 1, ?, '2026-08-29 12:00:00', '2026-08-30 12:00:00+00', "
                "24, 780.0, 0.0004, 0.0001)",
                [name, group],
            )

        run_sql(pillars_db, "lesson23_pillars.sql")
        names = [
            row[0]
            for row in pillars_db.execute(
                "SELECT object_name FROM gold_debris_population"
            ).fetchall()
        ]
        assert names == ["DEBRIS-1"]

    def test_empty_source_yields_an_empty_table_not_an_error(self, pillars_db):
        run_sql(pillars_db, "lesson23_pillars.sql")
        assert pillars_db.execute(
            "SELECT COUNT(*) FROM gold_debris_population"
        ).fetchone()[0] == 0


def create_noaa_history(connection) -> None:
    connection.execute(
        """
        CREATE TABLE noaa_magnetic_history (
            snapshot_at_utc TIMESTAMPTZ,
            observed_at_utc TIMESTAMP,
            spacecraft VARCHAR,
            is_active_source BOOLEAN,
            bt DOUBLE, bx_gsm DOUBLE, by_gsm DOUBLE, bz_gsm DOUBLE,
            quality_code INTEGER,
            source_file VARCHAR,
            source_sha256 VARCHAR
        );
        CREATE TABLE noaa_plasma_history (
            snapshot_at_utc TIMESTAMPTZ,
            observed_at_utc TIMESTAMP,
            spacecraft VARCHAR,
            is_active_source BOOLEAN,
            proton_speed DOUBLE,
            proton_temperature DOUBLE,
            proton_density DOUBLE,
            quality_code INTEGER,
            source_file VARCHAR,
            source_sha256 VARCHAR
        );
        """
    )


def insert_magnetic(
    connection,
    snapshot: str,
    observed: str,
    bz: float,
    spacecraft: str = "DSCOVR",
    active: bool = True,
    quality: int = 0,
) -> None:
    connection.execute(
        "INSERT INTO noaa_magnetic_history VALUES (?, ?, ?, ?, 5.0, 1.0, 1.0, ?, ?, 'f', 'a')",
        [snapshot, observed, spacecraft, active, bz, quality],
    )


def insert_plasma(
    connection, snapshot: str, observed: str, speed: float, active: bool = True
) -> None:
    connection.execute(
        "INSERT INTO noaa_plasma_history VALUES (?, ?, 'DSCOVR', ?, ?, 100000, 5.0, 0, 'f', 'a')",
        [snapshot, observed, active, speed],
    )


@pytest.fixture
def noaa_db(duckdb_connection):
    create_noaa_history(duckdb_connection)
    return duckdb_connection


class TestGoldSpaceWeatherMinute:
    def test_history_is_deduplicated_to_one_row_per_minute(self, noaa_db):
        # The same observed minute arrives in six overlapping rolling-window
        # snapshots. Gold must keep exactly one row for it.
        for hour in range(6):
            insert_magnetic(
                noaa_db, f"2026-08-30 {hour:02d}:00:00+00", "2026-08-29 23:00:00", -6.0
            )

        run_sql(noaa_db, "lesson3_gold.sql")
        assert noaa_db.execute(
            "SELECT COUNT(*) FROM gold_space_weather_minute"
        ).fetchone()[0] == 1

    def test_coverage_spans_the_whole_history_not_just_one_day(self, noaa_db):
        # This is the defect the rewrite fixed: reading the Silver
        # latest-CSV capped Gold at NOAA's 24-hour window regardless of how
        # long the project had been collecting.
        for day in range(1, 8):
            insert_magnetic(
                noaa_db,
                f"2026-08-{day + 1:02d} 00:00:00+00",
                f"2026-08-{day:02d} 12:00:00",
                -4.0,
            )

        run_sql(noaa_db, "lesson3_gold.sql")
        first, last, count = noaa_db.execute(
            "SELECT MIN(observation_minute_utc), MAX(observation_minute_utc), COUNT(*) "
            "FROM gold_space_weather_minute"
        ).fetchone()
        assert count == 7
        assert (last - first).days == 6

    def test_the_active_source_wins_over_an_inactive_one(self, noaa_db):
        insert_magnetic(
            noaa_db, "2026-08-30 00:00:00+00", "2026-08-29 23:00:00", -1.0,
            spacecraft="ACE", active=False,
        )
        insert_magnetic(
            noaa_db, "2026-08-30 00:00:00+00", "2026-08-29 23:00:00", -9.0,
            spacecraft="DSCOVR", active=True,
        )

        run_sql(noaa_db, "lesson3_gold.sql")
        spacecraft, bz = noaa_db.execute(
            "SELECT magnetic_spacecraft, bz_gsm_nanotesla FROM gold_space_weather_minute"
        ).fetchone()
        assert spacecraft == "DSCOVR"
        assert bz == pytest.approx(-9.0)

    def test_the_best_quality_code_wins_among_active_sources(self, noaa_db):
        insert_magnetic(
            noaa_db, "2026-08-30 00:00:00+00", "2026-08-29 23:00:00", -1.0, quality=5
        )
        insert_magnetic(
            noaa_db, "2026-08-30 00:00:00+00", "2026-08-29 23:00:00", -7.0, quality=0
        )

        run_sql(noaa_db, "lesson3_gold.sql")
        assert noaa_db.execute(
            "SELECT bz_gsm_nanotesla FROM gold_space_weather_minute"
        ).fetchone()[0] == pytest.approx(-7.0)

    def test_a_later_snapshot_supersedes_an_earlier_revision(self, noaa_db):
        insert_magnetic(noaa_db, "2026-08-30 00:00:00+00", "2026-08-29 23:00:00", -2.0)
        insert_magnetic(noaa_db, "2026-08-30 06:00:00+00", "2026-08-29 23:00:00", -8.0)

        run_sql(noaa_db, "lesson3_gold.sql")
        assert noaa_db.execute(
            "SELECT bz_gsm_nanotesla FROM gold_space_weather_minute"
        ).fetchone()[0] == pytest.approx(-8.0)

    def test_plasma_joins_on_the_minute(self, noaa_db):
        # Seconds differ between the two feeds, so the join is by minute.
        insert_magnetic(noaa_db, "2026-08-30 00:00:00+00", "2026-08-29 23:00:12", -5.0)
        insert_plasma(noaa_db, "2026-08-30 00:00:00+00", "2026-08-29 23:00:47", 640.0)

        run_sql(noaa_db, "lesson3_gold.sql")
        speed = noaa_db.execute(
            "SELECT proton_speed_km_per_second FROM gold_space_weather_minute"
        ).fetchone()[0]
        assert speed == pytest.approx(640.0)

    def test_magnetic_rows_survive_without_a_plasma_match(self, noaa_db):
        insert_magnetic(noaa_db, "2026-08-30 00:00:00+00", "2026-08-29 23:00:00", -5.0)

        run_sql(noaa_db, "lesson3_gold.sql")
        count, speed = noaa_db.execute(
            "SELECT COUNT(*), MAX(proton_speed_km_per_second) "
            "FROM gold_space_weather_minute"
        ).fetchone()
        assert count == 1
        assert speed is None


def create_orbital_history(connection) -> None:
    connection.execute(
        """
        CREATE TABLE orbital_snapshot_history (
            snapshot_at_utc TIMESTAMPTZ,
            object_name VARCHAR,
            norad_catalog_id BIGINT,
            element_epoch_utc TIMESTAMP,
            inclination_degrees DOUBLE,
            eccentricity DOUBLE,
            mean_motion_revolutions_per_day DOUBLE,
            bstar_drag_term DOUBLE,
            source_group VARCHAR
        )
        """
    )


def build_hourly_weather(connection, bz: float, speed: float) -> None:
    """Three minutes of one hour, then the minute and hourly Gold tables."""
    for minute in range(3):
        insert_magnetic(
            connection, "2026-08-30 06:00:00+00", f"2026-08-30 05:{minute:02d}:00", bz
        )
        insert_plasma(
            connection, "2026-08-30 06:00:00+00", f"2026-08-30 05:{minute:02d}:00", speed
        )
    run_sql(connection, "lesson3_gold.sql")
    run_sql(connection, "lesson8_research.sql")


class TestHourlyDisturbance:
    """The disturbance level drives the ORI environment factor."""

    def level(self, connection, bz: float, speed: float) -> str:
        create_noaa_history(connection)
        # lesson8_research.sql also builds the orbit-change table, so the
        # orbital history table must exist even when it is empty.
        create_orbital_history(connection)
        build_hourly_weather(connection, bz, speed)
        return connection.execute(
            "SELECT disturbance_level FROM gold_space_weather_hourly"
        ).fetchone()[0]


    def test_quiet_conditions(self, duckdb_connection):
        assert self.level(duckdb_connection, bz=-1.0, speed=380.0) == "quiet"

    def test_strongly_southward_bz_is_flagged(self, duckdb_connection):
        assert self.level(duckdb_connection, bz=-8.0, speed=380.0) == "southward_bz"

    def test_fast_solar_wind_is_flagged(self, duckdb_connection):
        assert self.level(duckdb_connection, bz=-1.0, speed=650.0) == "fast_wind"

    def test_southward_bz_takes_priority_over_fast_wind(self, duckdb_connection):
        # Both conditions hold; the stronger geomagnetic driver wins.
        assert self.level(duckdb_connection, bz=-8.0, speed=650.0) == "southward_bz"

    def test_just_inside_the_thresholds_stays_quiet(self, duckdb_connection):
        assert self.level(duckdb_connection, bz=-4.9, speed=499.0) == "quiet"


class TestOrbitReliabilityIndex:
    def setup_sources(self, connection, bz: float, speed: float) -> None:
        create_noaa_history(connection)
        create_orbital_history(connection)
        build_hourly_weather(connection, bz, speed)

    def add_object(self, connection, norad: int, epoch: str, mean_motion: float) -> None:
        connection.execute(
            "INSERT INTO orbital_snapshot_history VALUES "
            "('2026-08-30 06:00:00+00', ?, ?, ?, 51.6, 0.0004, ?, 0.0001, 'stations')",
            [f"SAT-{norad}", norad, epoch, mean_motion],
        )

    def score(self, connection) -> list[tuple]:
        run_sql(connection, "lesson11_freshness.sql")
        run_sql(connection, "lesson12_reliability.sql")
        return connection.execute(
            "SELECT norad_catalog_id, base_score, environment_factor, "
            "orbit_reliability_index FROM gold_orbit_reliability_index"
        ).fetchall()

    def test_fresh_high_orbit_scores_higher_than_stale_low_orbit(
        self, duckdb_connection
    ):
        self.setup_sources(duckdb_connection, bz=-1.0, speed=380.0)
        # 12.5 rev/day is roughly 1,100 km; 16.2 is roughly 300 km.
        self.add_object(duckdb_connection, 1, "2026-08-30 05:30:00", 12.5)
        self.add_object(duckdb_connection, 2, "2026-08-28 06:00:00", 16.2)

        scores = {row[0]: row[3] for row in self.score(duckdb_connection)}
        assert scores[1] > scores[2]

    def test_environment_factor_reflects_a_disturbance(self, duckdb_connection):
        # The factor was pinned at a constant 1.0 while Gold could only see
        # one quiet day. It must move when the hourly data is disturbed.
        self.setup_sources(duckdb_connection, bz=-9.0, speed=380.0)
        self.add_object(duckdb_connection, 1, "2026-08-30 05:30:00", 15.0)

        assert float(self.score(duckdb_connection)[0][2]) == pytest.approx(0.8)

    def test_fast_wind_gives_the_intermediate_factor(self, duckdb_connection):
        self.setup_sources(duckdb_connection, bz=-1.0, speed=700.0)
        self.add_object(duckdb_connection, 1, "2026-08-30 05:30:00", 15.0)

        assert float(self.score(duckdb_connection)[0][2]) == pytest.approx(0.9)

    def test_quiet_conditions_leave_the_score_unmultiplied(self, duckdb_connection):
        self.setup_sources(duckdb_connection, bz=-1.0, speed=380.0)
        self.add_object(duckdb_connection, 1, "2026-08-30 05:30:00", 15.0)

        _, base, factor, index = self.score(duckdb_connection)[0]
        assert float(factor) == pytest.approx(1.0)
        assert index == pytest.approx(base)

    def test_a_disturbance_lowers_the_index_below_the_base_score(
        self, duckdb_connection
    ):
        self.setup_sources(duckdb_connection, bz=-9.0, speed=380.0)
        self.add_object(duckdb_connection, 1, "2026-08-30 05:30:00", 15.0)

        _, base, _, index = self.score(duckdb_connection)[0]
        assert index < base

    def test_scores_stay_within_the_documented_zero_to_hundred_scale(
        self, duckdb_connection
    ):
        self.setup_sources(duckdb_connection, bz=-9.0, speed=700.0)
        for norad, epoch, mean_motion in [
            (1, "2026-08-30 05:59:00", 11.0),  # very fresh, high orbit
            (2, "2026-08-20 00:00:00", 16.4),  # very stale, very low orbit
        ]:
            self.add_object(duckdb_connection, norad, epoch, mean_motion)

        indices = [row[3] for row in self.score(duckdb_connection)]
        assert all(0.0 <= value <= 100.0 for value in indices)





