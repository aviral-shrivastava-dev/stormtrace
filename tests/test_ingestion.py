"""Tests for the collectors' pure logic. No test makes a network request.

The SatNOGS pending-resolution logic is the important part: without it the
collector only ever saw scheduled ('future') passes, so the "good pass"
metric was structurally incapable of being anything but zero.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

import ingest_satnogs
from ingest_satnogs import (
    NON_TERMINAL_STATUSES,
    RESOLVE_LIMIT,
    TERMINAL_STATUSES,
    observation_row,
    parse_utc,
)

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)


def observation(
    observation_id: int,
    status: str,
    end: str = "2026-09-01T10:00:00Z",
    start: str = "2026-09-01T09:50:00Z",
) -> dict:
    return {
        "id": observation_id,
        "start": start,
        "end": end,
        "status": status,
        "norad_cat_id": 25544,
        "sat_id": "ABCD-1234",
        "ground_station": 42,
        "station_lat": 45.0,
        "station_lng": -111.0,
        "observation_frequency": 436000000,
        "transmitter_mode": "GFSK",
    }


def write_snapshot(directory, timestamp: str, records: list[dict]) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / f"observations_{timestamp}.json").write_text(
        json.dumps(records), encoding="utf-8"
    )


class TestStatusVocabulary:
    def test_terminal_and_non_terminal_statuses_do_not_overlap(self):
        assert set(TERMINAL_STATUSES).isdisjoint(NON_TERMINAL_STATUSES)

    def test_good_is_terminal(self):
        assert "good" in TERMINAL_STATUSES

    def test_future_is_not_terminal(self):
        # A scheduled pass has no outcome yet; treating it as final is what
        # froze the metric at zero good passes.
        assert "future" in NON_TERMINAL_STATUSES


class TestParseUtc:
    def test_zulu_suffix_is_understood(self):
        assert parse_utc("2026-09-01T10:00:00Z") == datetime(
            2026, 9, 1, 10, 0, tzinfo=UTC
        )

    def test_offset_form_is_understood(self):
        assert parse_utc("2026-09-01T10:00:00+00:00") == datetime(
            2026, 9, 1, 10, 0, tzinfo=UTC
        )

    @pytest.mark.parametrize("value", [None, "", "not-a-date", 12345])
    def test_unusable_values_return_none_instead_of_raising(self, value):
        assert parse_utc(value) is None


class TestObservationRow:
    def test_api_fields_map_to_silver_column_names(self):
        row = observation_row(observation(1, "good"))
        assert row["observation_id"] == 1
        assert row["norad_catalog_id"] == 25544
        assert row["station_id"] == 42
        assert row["observation_frequency_hz"] == 436000000

    def test_missing_fields_become_none_rather_than_raising(self):
        row = observation_row({"id": 5})
        assert row["observation_id"] == 5
        assert row["status"] is None
        assert row["norad_catalog_id"] is None


@pytest.fixture
def bronze(tmp_path, monkeypatch):
    directory = tmp_path / "satnogs"
    monkeypatch.setattr(ingest_satnogs, "BRONZE_DIR", directory)
    return directory


class TestPendingObservationIds:
    def test_a_finished_pass_with_a_provisional_status_is_pending(self, bronze):
        write_snapshot(
            bronze,
            "20260901T000000Z",
            [observation(1, "future", end="2026-09-01T10:00:00Z")],
        )
        assert ingest_satnogs.pending_observation_ids(NOW) == [1]

    def test_a_pass_still_in_the_future_is_not_queried(self, bronze):
        write_snapshot(
            bronze,
            "20260901T000000Z",
            [observation(1, "future", end="2026-09-01T23:00:00Z")],
        )
        # Asking before the pass has happened cannot resolve anything.
        assert ingest_satnogs.pending_observation_ids(NOW) == []

    def test_an_already_resolved_observation_is_not_queried_again(self, bronze):
        write_snapshot(bronze, "20260901T000000Z", [observation(1, "future")])
        write_snapshot(bronze, "20260901T060000Z", [observation(1, "good")])
        # The newest stored status wins, so this id is finished.
        assert ingest_satnogs.pending_observation_ids(NOW) == []

    def test_a_regression_to_provisional_is_queried_again(self, bronze):
        write_snapshot(bronze, "20260901T000000Z", [observation(1, "good")])
        write_snapshot(bronze, "20260901T060000Z", [observation(1, "unknown")])
        assert ingest_satnogs.pending_observation_ids(NOW) == [1]

    def test_the_number_of_requests_stays_bounded(self, bronze):
        write_snapshot(
            bronze,
            "20260901T000000Z",
            [observation(i, "future") for i in range(RESOLVE_LIMIT * 5)],
        )
        # Provider respect: a large backlog must not become a request storm.
        assert len(ingest_satnogs.pending_observation_ids(NOW)) == RESOLVE_LIMIT

    def test_the_oldest_passes_are_resolved_first(self, bronze):
        records = [
            observation(
                index,
                "future",
                end=(NOW - timedelta(hours=index + 1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
            for index in range(3)
        ]
        write_snapshot(bronze, "20260901T000000Z", records)
        # Index 2 ended earliest, so it is resolved first.
        assert ingest_satnogs.pending_observation_ids(NOW) == [2, 1, 0]

    def test_a_corrupt_snapshot_does_not_stop_collection(self, bronze):
        write_snapshot(bronze, "20260901T000000Z", [observation(1, "future")])
        (bronze / "observations_20260901T030000Z.json").write_text(
            "{not json", encoding="utf-8"
        )
        assert ingest_satnogs.pending_observation_ids(NOW) == [1]

    def test_an_empty_bronze_directory_yields_nothing(self, bronze):
        assert ingest_satnogs.pending_observation_ids(NOW) == []

    def test_records_without_an_integer_id_are_ignored(self, bronze):
        write_snapshot(
            bronze,
            "20260901T000000Z",
            [{"status": "future", "end": "2026-09-01T10:00:00Z"}],
        )
        assert ingest_satnogs.pending_observation_ids(NOW) == []


class TestCollect:
    """collect() is exercised with fetch_json stubbed: no network access."""

    @pytest.fixture(autouse=True)
    def no_sleeping(self, monkeypatch):
        monkeypatch.setattr(ingest_satnogs.time, "sleep", lambda seconds: None)

    def test_terminal_statuses_are_swept_and_merged(self, bronze, monkeypatch):
        def fake_fetch(url: str) -> list[dict]:
            for index, status in enumerate(TERMINAL_STATUSES, start=10):
                if f"status={status}" in url:
                    return [observation(index, status)]
            return [observation(1, "future")]

        monkeypatch.setattr(ingest_satnogs, "fetch_json", fake_fetch)

        observations, counts = ingest_satnogs.collect(NOW)
        statuses = {record["status"] for record in observations}
        # The whole point of the rewrite: real outcomes now reach Bronze.
        assert statuses == {"future", *TERMINAL_STATUSES}
        assert counts["terminal"] == len(TERMINAL_STATUSES)
        assert counts["listing"] == 1

    def test_a_failed_optional_request_does_not_abort_collection(
        self, bronze, monkeypatch
    ):
        def fake_fetch(url: str) -> list[dict]:
            if "status=" in url:
                raise TimeoutError("provider slow")
            return [observation(1, "future")]

        monkeypatch.setattr(ingest_satnogs, "fetch_json", fake_fetch)

        observations, counts = ingest_satnogs.collect(NOW)
        assert len(observations) == 1
        assert counts["request_errors"] == len(TERMINAL_STATUSES)

    def test_the_newest_version_of_an_observation_wins(self, bronze, monkeypatch):
        def fake_fetch(url: str) -> list[dict]:
            if "status=good" in url:
                return [observation(1, "good")]
            if "status=" in url:
                return []
            return [observation(1, "future")]

        monkeypatch.setattr(ingest_satnogs, "fetch_json", fake_fetch)

        observations, _ = ingest_satnogs.collect(NOW)
        assert len(observations) == 1
        assert observations[0]["status"] == "good"

    def test_observations_are_returned_in_id_order(self, bronze, monkeypatch):
        def fake_fetch(url: str) -> list[dict]:
            if "status=good" in url:
                return [observation(3, "good"), observation(1, "good")]
            if "status=" in url:
                return []
            return [observation(2, "future")]

        monkeypatch.setattr(ingest_satnogs, "fetch_json", fake_fetch)

        observations, _ = ingest_satnogs.collect(NOW)
        assert [record["id"] for record in observations] == [1, 2, 3]

    def test_pending_ids_are_resolved_by_id(self, bronze, monkeypatch):
        write_snapshot(bronze, "20260901T000000Z", [observation(7, "future")])
        requested: list[str] = []

        def fake_fetch(url: str) -> list[dict]:
            requested.append(url)
            if "id=7" in url:
                return [observation(7, "good")]
            if "status=" in url:
                return []
            return [observation(1, "future")]

        monkeypatch.setattr(ingest_satnogs, "fetch_json", fake_fetch)

        observations, counts = ingest_satnogs.collect(NOW)
        assert any("id=7" in url for url in requested)
        assert counts["resolved"] == 1
        resolved = {record["id"]: record["status"] for record in observations}
        assert resolved[7] == "good"

    def test_a_listing_failure_is_fatal(self, bronze, monkeypatch):
        # The first request is the collector's reason to exist; if it fails
        # the step should fail loudly rather than write an empty snapshot.
        def fake_fetch(url: str) -> list[dict]:
            raise TimeoutError("unreachable")

        monkeypatch.setattr(ingest_satnogs, "fetch_json", fake_fetch)

        with pytest.raises(TimeoutError):
            ingest_satnogs.collect(NOW)


