"""Tests for the data-quality gate.

The operator lookup is tested because the previous implementation treated
any operator that was not exactly "less_than_or_equal" as a greater-than,
so a typo silently inverted the check it was meant to enforce.
"""

from __future__ import annotations

import pytest

import check_quality
from check_quality import CHECKS, OPERATORS, evaluate, is_pass


class TestIsPass:
    @pytest.mark.parametrize(
        ("operator", "value", "threshold", "expected"),
        [
            ("greater_than", 1.0, 0.0, True),
            ("greater_than", 0.0, 0.0, False),
            ("greater_than_or_equal", 0.0, 0.0, True),
            ("less_than", 0.0, 1.0, True),
            ("less_than", 1.0, 1.0, False),
            ("less_than_or_equal", 0.0, 0.0, True),
            ("less_than_or_equal", 1.0, 0.0, False),
            ("equals", 5.0, 5.0, True),
            ("equals", 4.0, 5.0, False),
        ],
    )
    def test_each_operator(self, operator, value, threshold, expected):
        assert is_pass(value, operator, threshold) is expected

    def test_unknown_operator_raises_instead_of_guessing(self):
        # The old code fell through to `value > threshold`, so a typo like
        # "less_then_or_equal" inverted the check without any warning.
        with pytest.raises(ValueError, match="Unknown check operator"):
            is_pass(0.0, "less_then_or_equal", 0.0)

    def test_the_error_message_lists_the_valid_operators(self):
        with pytest.raises(ValueError) as info:
            is_pass(0.0, "nonsense", 0.0)
        assert "less_than_or_equal" in str(info.value)


class TestCheckDefinitions:
    def test_every_check_uses_a_known_operator(self):
        assert [c.name for c in CHECKS if c.operator not in OPERATORS] == []

    def test_every_check_uses_a_known_severity(self):
        assert all(check.severity in ("error", "warning") for check in CHECKS)

    def test_check_names_are_unique(self):
        names = [check.name for check in CHECKS]
        assert len(names) == len(set(names))

    def test_every_check_has_a_human_readable_description(self):
        assert all(check.description.strip() for check in CHECKS)

    def test_structural_integrity_checks_are_errors_not_warnings(self):
        # These protect the Bronze immutability guarantee; a violation must
        # stop the pipeline rather than merely be noted.
        blocking = {
            "modified_bronze_paths",
            "orphan_history_rows",
            "registered_files_missing_rows",
        }
        severities = {c.name: c.severity for c in CHECKS if c.name in blocking}
        assert severities == dict.fromkeys(blocking, "error")


class TestGeneratedSql:
    def test_orphan_sql_covers_every_history_table(self):
        sql = check_quality.orphan_rows_sql()
        for table in check_quality.HISTORY_TABLES:
            assert table in sql

    def test_unreferenced_sql_covers_every_history_table(self):
        sql = check_quality.unreferenced_files_sql()
        for table in check_quality.HISTORY_TABLES:
            assert table in sql

    def test_a_new_history_table_is_picked_up_by_both_queries(self, monkeypatch):
        # Adding a source should not require editing two long SQL strings.
        monkeypatch.setattr(check_quality, "HISTORY_TABLES", ["alpha", "beta"])
        assert "beta" in check_quality.orphan_rows_sql()
        assert "beta" in check_quality.unreferenced_files_sql()


WAREHOUSE_SCHEMA = """
CREATE TABLE bronze_file_registry (
    file_path VARCHAR, source VARCHAR, snapshot_at_utc TIMESTAMPTZ,
    sha256 VARCHAR, loaded_at_utc TIMESTAMPTZ, row_count INTEGER
);
CREATE TABLE orbital_snapshot_history (
    snapshot_at_utc TIMESTAMPTZ, norad_catalog_id BIGINT,
    inclination_degrees DOUBLE, eccentricity DOUBLE,
    mean_motion_revolutions_per_day DOUBLE, source_file VARCHAR
);
CREATE TABLE noaa_magnetic_history (
    bz_gsm DOUBLE, is_active_source BOOLEAN, source_file VARCHAR
);
CREATE TABLE noaa_plasma_history (
    proton_speed DOUBLE, is_active_source BOOLEAN, source_file VARCHAR
);
CREATE TABLE space_weather_index_history (source_file VARCHAR);
CREATE TABLE satnogs_observation_history (source_file VARCHAR);
"""

CHECKED_AT = "2026-08-30T12:00:00+00:00"


@pytest.fixture
def warehouse(duckdb_connection, monkeypatch):
    """An empty warehouse with the filesystem check stubbed out."""
    monkeypatch.setattr(check_quality, "unloadable_files", lambda: [])
    duckdb_connection.execute(WAREHOUSE_SCHEMA)
    return duckdb_connection


def results_by_name(connection) -> dict[str, dict]:
    return {row["check_name"]: row for row in evaluate(connection, CHECKED_AT)}


class TestEvaluate:
    def test_an_empty_warehouse_fails_the_has_rows_check(self, warehouse):
        results = results_by_name(warehouse)
        assert results["orbital_history_has_rows"]["status"] == "fail"

    def test_a_valid_row_passes_the_physical_plausibility_checks(self, warehouse):
        warehouse.execute(
            "INSERT INTO orbital_snapshot_history VALUES "
            "('2026-08-30 00:00:00+00', 1, 51.6, 0.0004, 15.5, 'f.csv')"
        )
        warehouse.execute(
            "INSERT INTO bronze_file_registry VALUES "
            "('f.csv', 'celestrak', '2026-08-30 00:00:00+00', 'abc', "
            "'2026-08-30 00:00:00+00', 1)"
        )
        results = results_by_name(warehouse)
        for name in (
            "orbital_history_has_rows",
            "invalid_mean_motion_rows",
            "invalid_eccentricity_rows",
            "invalid_inclination_rows",
            "orphan_history_rows",
            "registered_files_missing_rows",
        ):
            assert results[name]["status"] == "pass", name

    @pytest.mark.parametrize(
        ("check_name", "inclination", "eccentricity", "mean_motion"),
        [
            ("invalid_eccentricity_rows", 51.6, 1.5, 15.5),
            ("invalid_inclination_rows", 200.0, 0.0004, 15.5),
            ("invalid_mean_motion_rows", 51.6, 0.0004, 25.0),
        ],
    )
    def test_physically_impossible_values_are_caught(
        self, warehouse, check_name, inclination, eccentricity, mean_motion
    ):
        warehouse.execute(
            "INSERT INTO orbital_snapshot_history VALUES "
            "('2026-08-30 00:00:00+00', 1, ?, ?, ?, 'f.csv')",
            [inclination, eccentricity, mean_motion],
        )
        assert results_by_name(warehouse)[check_name]["status"] == "fail"

    def test_history_rows_without_a_registered_file_are_orphans(self, warehouse):
        warehouse.execute(
            "INSERT INTO orbital_snapshot_history VALUES "
            "('2026-08-30 00:00:00+00', 1, 51.6, 0.0004, 15.5, 'unregistered.csv')"
        )
        assert results_by_name(warehouse)["orphan_history_rows"]["status"] == "fail"

    def test_a_bronze_path_with_two_checksums_is_a_failure(self, warehouse):
        # Bronze is immutable: one path must never have two contents.
        for digest in ("aaa", "bbb"):
            warehouse.execute(
                "INSERT INTO bronze_file_registry VALUES "
                "('f.csv', 'celestrak', '2026-08-30 00:00:00+00', ?, "
                "'2026-08-30 00:00:00+00', 1)",
                [digest],
            )
        assert results_by_name(warehouse)["modified_bronze_paths"]["status"] == "fail"

    def test_unloadable_bronze_files_are_reported_as_a_warning(
        self, duckdb_connection, monkeypatch
    ):
        # A file matching no loader pattern is invisible to SQL, so this
        # check is measured on the filesystem instead.
        monkeypatch.setattr(
            check_quality, "unloadable_files", lambda: ["bronze/celestrak/active.json"]
        )
        duckdb_connection.execute(WAREHOUSE_SCHEMA)
        row = results_by_name(duckdb_connection)["unloadable_bronze_files"]
        assert row["observed_value"] == 1
        # A warning, so collection is not blocked, but it is now visible.
        assert row["status"] == "warn"

    def test_every_result_carries_the_reporting_fields(self, warehouse):
        results = evaluate(warehouse, CHECKED_AT)
        assert len(results) == len(CHECKS)
        for row in results:
            assert set(row) == {
                "checked_at_utc",
                "check_name",
                "status",
                "severity",
                "observed_value",
                "operator",
                "threshold",
                "description",
            }

