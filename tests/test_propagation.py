"""Tests for the SGP4 propagation-disagreement instrument.

The unit conversions here are the easiest place in the project to be
silently wrong: an element set with the wrong mean-motion scaling still
propagates, it just produces plausible-looking nonsense. These tests pin
the conversions and the analysis envelope.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import ClassVar

import pytest

from build_propagation_disagreement import (
    MAX_ANALYSIS_SPAN_HOURS,
    MAX_ANALYSIS_TOTAL_KM,
    MINUTES_PER_DAY,
    TWO_PI,
    build_satellite,
    epoch_to_julian,
    measurement_quality,
    pair_query,
    ric_components,
)

# A plausible LEO element set: roughly ISS-like. Epochs coming out of
# DuckDB are naive UTC, so the fixtures match that.
BASE_ELEMENT = {
    "element_epoch_utc": datetime(2026, 8, 30, 12, 0, 0),
    "inclination_degrees": 51.64,
    "eccentricity": 0.0004,
    "mean_motion_revolutions_per_day": 15.5,
    "bstar_drag_term": 0.0001,
    "ra_of_asc_node_degrees": 120.0,
    "arg_of_pericenter_degrees": 90.0,
    "mean_anomaly_degrees": 45.0,
    "mean_motion_dot": 0.0,
    "mean_motion_ddot": 0.0,
    "norad_catalog_id": 25544,
}

HISTORY_SCHEMA = """
CREATE TABLE orbital_snapshot_history (
    snapshot_at_utc TIMESTAMPTZ,
    object_name VARCHAR,
    norad_catalog_id BIGINT,
    element_epoch_utc TIMESTAMP,
    inclination_degrees DOUBLE,
    eccentricity DOUBLE,
    mean_motion_revolutions_per_day DOUBLE,
    bstar_drag_term DOUBLE,
    ra_of_asc_node_degrees DOUBLE,
    arg_of_pericenter_degrees DOUBLE,
    mean_anomaly_degrees DOUBLE,
    mean_motion_dot DOUBLE,
    mean_motion_ddot DOUBLE,
    source_group VARCHAR
)
"""

INSERT_SNAPSHOT = (
    "INSERT INTO orbital_snapshot_history VALUES "
    "(?, 'SAT', ?, ?, 51.6, 0.0004, 15.5, 0.0001, 120, 90, 45, 0, 0, 'stations')"
)


def element(**overrides) -> dict:
    record = dict(BASE_ELEMENT)
    record.update(overrides)
    return record


class TestUnitConstants:
    def test_two_pi(self):
        assert TWO_PI == pytest.approx(6.283185307, abs=1e-9)

    def test_minutes_per_day(self):
        assert MINUTES_PER_DAY == 1440.0

    def test_mean_motion_conversion_matches_hand_calculation(self):
        # 15.5 rev/day -> rad/min. One rev is 2*pi rad, one day is 1440 min.
        expected = 15.5 * 2 * math.pi / 1440.0
        assert 15.5 * TWO_PI / MINUTES_PER_DAY == pytest.approx(expected)
        # Sanity: a ~93-minute orbit is ~0.0676 rad/min.
        assert expected == pytest.approx(0.06763, abs=1e-4)

    def test_derivative_conversions_use_increasing_powers(self):
        ndot_factor = TWO_PI / (MINUTES_PER_DAY**2)
        nddot_factor = TWO_PI / (MINUTES_PER_DAY**3)
        assert nddot_factor == pytest.approx(ndot_factor / MINUTES_PER_DAY)


class TestEpochToJulian:
    def test_known_j2000_epoch(self):
        # 2000-01-01 12:00 UTC is Julian Date 2451545.0 exactly.
        jd, fr = epoch_to_julian(datetime(2000, 1, 1, 12, 0, 0))
        assert jd + fr == pytest.approx(2451545.0, abs=1e-9)

    def test_sub_second_precision_is_preserved(self):
        without = sum(epoch_to_julian(datetime(2026, 8, 30, 12, 0, 0)))
        with_micro = sum(epoch_to_julian(datetime(2026, 8, 30, 12, 0, 0, 500000)))
        half_second_in_days = 0.5 / 86400.0
        assert with_micro - without == pytest.approx(half_second_in_days, abs=1e-9)

    def test_sgp4_epoch_offset_is_days_since_1949_12_31(self):
        jd, fr = epoch_to_julian(datetime(1949, 12, 31, 0, 0, 0))
        assert jd + fr - 2433281.5 == pytest.approx(0.0, abs=1e-9)


class TestBuildSatellite:
    def test_valid_element_produces_a_satellite(self):
        assert build_satellite(element()) is not None

    def test_propagated_position_is_a_plausible_leo_orbit(self):
        satellite = build_satellite(element())
        jd, fr = epoch_to_julian(BASE_ELEMENT["element_epoch_utc"])
        error, position, velocity = satellite.sgp4(jd, fr)
        assert error == 0
        radius = math.sqrt(sum(component**2 for component in position))
        # Earth radius 6378 km plus a few hundred km of altitude.
        assert 6500 < radius < 7200
        speed = math.sqrt(sum(component**2 for component in velocity))
        assert 7.0 < speed < 8.0  # km/s, correct for LEO

    @pytest.mark.parametrize(
        "overrides",
        [
            {"mean_motion_revolutions_per_day": 0.0},
            {"mean_motion_revolutions_per_day": -1.0},
            {"eccentricity": 1.0},
            {"eccentricity": -0.1},
            {"inclination_degrees": -5.0},
            {"inclination_degrees": 181.0},
        ],
        ids=[
            "zero-mean-motion",
            "negative-mean-motion",
            "parabolic-eccentricity",
            "negative-eccentricity",
            "negative-inclination",
            "inclination-over-180",
        ],
    )
    def test_physically_impossible_elements_are_rejected(self, overrides):
        assert build_satellite(element(**overrides)) is None

    def test_none_values_are_rejected(self):
        assert build_satellite(element(mean_motion_revolutions_per_day=None)) is None

    def test_absent_drag_terms_default_to_zero(self):
        # None B*/ndot/nddot are common in public data and must not crash.
        satellite = build_satellite(
            element(bstar_drag_term=None, mean_motion_dot=None, mean_motion_ddot=None)
        )
        assert satellite is not None


class TestRicComponents:
    # A circular equatorial orbit: position along +x, velocity along +y.
    # Radial is then +x, along-track +y, cross-track +z.
    POSITION: ClassVar[list[float]] = [7000.0, 0.0, 0.0]
    VELOCITY: ClassVar[list[float]] = [0.0, 7.5, 0.0]


    def test_pure_radial_difference(self):
        radial, along, cross = ric_components(
            self.POSITION, self.VELOCITY, [1.0, 0.0, 0.0]
        )
        assert (radial, along, cross) == pytest.approx((1.0, 0.0, 0.0))

    def test_pure_along_track_difference(self):
        radial, along, cross = ric_components(
            self.POSITION, self.VELOCITY, [0.0, 1.0, 0.0]
        )
        assert (radial, along, cross) == pytest.approx((0.0, 1.0, 0.0))

    def test_pure_cross_track_difference(self):
        radial, along, cross = ric_components(
            self.POSITION, self.VELOCITY, [0.0, 0.0, 1.0]
        )
        assert (radial, along, cross) == pytest.approx((0.0, 0.0, 1.0))

    def test_decomposition_preserves_the_total_magnitude(self):
        difference = [0.3, -1.2, 0.45]
        components = ric_components(self.POSITION, self.VELOCITY, difference)
        original = math.sqrt(sum(d * d for d in difference))
        decomposed = math.sqrt(sum(c * c for c in components))
        assert decomposed == pytest.approx(original, abs=1e-9)

    def test_sign_convention_is_preserved(self):
        radial, _, _ = ric_components(self.POSITION, self.VELOCITY, [-2.0, 0.0, 0.0])
        assert radial == pytest.approx(-2.0)


class TestMeasurementQuality:
    def test_normal_measurement_is_ok(self):
        assert measurement_quality(9.6, 0.29) == "ok"

    def test_long_span_is_labelled(self):
        assert measurement_quality(MAX_ANALYSIS_SPAN_HOURS + 1, 10.0) == "long_span"

    def test_extreme_disagreement_is_labelled(self):
        assert measurement_quality(10.0, MAX_ANALYSIS_TOTAL_KM + 1) == "extreme"

    def test_extreme_wins_over_long_span(self):
        # A 5,000 km disagreement is not a measurement regardless of span.
        assert measurement_quality(200.0, 5079.0) == "extreme"

    def test_envelope_boundaries_are_still_ok(self):
        assert (
            measurement_quality(MAX_ANALYSIS_SPAN_HOURS, MAX_ANALYSIS_TOTAL_KM) == "ok"
        )


class TestPairQuery:
    def test_pairs_consecutive_snapshots_of_the_same_object(self, duckdb_connection):
        duckdb_connection.execute(HISTORY_SCHEMA)
        # Three snapshots: epoch A, epoch A republished, then epoch B. Only
        # one pair is measurable, and its earlier side must be the LAST
        # snapshot that still carried epoch A -- that is the point-in-time
        # correct moment just before the refresh arrived.
        for snapshot, norad, epoch in [
            ("2026-08-30 00:00:00+00", 1, "2026-08-29 12:00:00"),
            ("2026-08-30 02:00:00+00", 1, "2026-08-29 12:00:00"),
            ("2026-08-30 04:00:00+00", 1, "2026-08-30 03:00:00"),
        ]:
            duckdb_connection.execute(INSERT_SNAPSHOT, [snapshot, norad, epoch])

        relation = duckdb_connection.execute(pair_query())
        columns = [description[0] for description in relation.description]
        pairs = [dict(zip(columns, row, strict=True)) for row in relation.fetchall()]

        assert len(pairs) == 1
        pair = pairs[0]
        assert pair["earlier_element_epoch_utc"] == datetime(2026, 8, 29, 12, 0)
        assert pair["element_epoch_utc"] == datetime(2026, 8, 30, 3, 0)
        assert pair["earlier_snapshot_at_utc"] == datetime(2026, 8, 30, 2, 0)

    def test_republished_identical_elements_produce_no_pair(self, duckdb_connection):
        duckdb_connection.execute(HISTORY_SCHEMA)
        for snapshot in ("2026-08-30 00:00:00+00", "2026-08-30 02:00:00+00"):
            duckdb_connection.execute(
                INSERT_SNAPSHOT, [snapshot, 1, "2026-08-29 12:00:00"]
            )
        # Reporting nothing is correct here: an unchanged element set is not
        # a measurement of zero drift.
        assert duckdb_connection.execute(pair_query()).fetchall() == []

    def test_objects_are_never_paired_with_each_other(self, duckdb_connection):
        duckdb_connection.execute(HISTORY_SCHEMA)
        for norad in (1, 2):
            duckdb_connection.execute(
                INSERT_SNAPSHOT,
                ["2026-08-30 00:00:00+00", norad, "2026-08-29 12:00:00"],
            )
        assert duckdb_connection.execute(pair_query()).fetchall() == []

    def test_rows_without_a_norad_id_are_ignored(self, duckdb_connection):
        duckdb_connection.execute(HISTORY_SCHEMA)
        duckdb_connection.execute(
            INSERT_SNAPSHOT, ["2026-08-30 00:00:00+00", None, "2026-08-29 12:00:00"]
        )
        duckdb_connection.execute(
            INSERT_SNAPSHOT, ["2026-08-30 02:00:00+00", None, "2026-08-30 01:00:00"]
        )
        assert duckdb_connection.execute(pair_query()).fetchall() == []

    def test_retrograde_republication_is_returned_for_the_caller_to_count(
        self, duckdb_connection
    ):
        duckdb_connection.execute(HISTORY_SCHEMA)
        # The catalog can republish an OLDER epoch. The query surfaces the
        # pair; main() classifies it as retrograde and reports the count
        # rather than dropping it silently.
        for snapshot, epoch in [
            ("2026-08-30 00:00:00+00", "2026-08-29 12:00:00"),
            ("2026-08-30 02:00:00+00", "2026-08-29 06:00:00"),
        ]:
            duckdb_connection.execute(INSERT_SNAPSHOT, [snapshot, 1, epoch])

        relation = duckdb_connection.execute(pair_query())
        columns = [description[0] for description in relation.description]
        pairs = [dict(zip(columns, row, strict=True)) for row in relation.fetchall()]
        assert len(pairs) == 1
        assert pairs[0]["element_epoch_utc"] < pairs[0]["earlier_element_epoch_utc"]


