"""Tests for the orchestrator: locking, timeouts, and freshness gating.

Two real defects are pinned here:

- `subprocess.TimeoutExpired` was never caught, so a slow step crashed the
  orchestrator with a traceback and logged nothing, while the docstring
  claimed timeouts were handled.
- `acquire_lock()` used check-then-write, leaving a race window where the
  hourly scheduled task and a manual run could both believe they held the
  DuckDB write lock.
"""

from __future__ import annotations

import json
import subprocess
from datetime import UTC, datetime, timedelta

import pytest

import run_pipeline


@pytest.fixture
def isolated_logs(tmp_path, monkeypatch):
    """Point the orchestrator's log and lock paths at a temp directory."""
    log_dir = tmp_path / "logs"
    monkeypatch.setattr(run_pipeline, "LOG_DIR", log_dir)
    monkeypatch.setattr(run_pipeline, "LOG_PATH", log_dir / "pipeline_runs.jsonl")
    monkeypatch.setattr(run_pipeline, "LOCK_PATH", log_dir / "pipeline.lock")
    return log_dir


def read_events(log_dir) -> list[dict]:
    path = log_dir / "pipeline_runs.jsonl"
    if not path.exists():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


class TestLocking:
    def test_first_acquisition_succeeds_and_creates_the_lock(self, isolated_logs):
        assert run_pipeline.acquire_lock() is True
        assert run_pipeline.LOCK_PATH.exists()

    def test_second_acquisition_is_refused(self, isolated_logs):
        assert run_pipeline.acquire_lock() is True
        assert run_pipeline.acquire_lock() is False

    def test_release_allows_reacquisition(self, isolated_logs):
        run_pipeline.acquire_lock()
        run_pipeline.release_lock()
        assert not run_pipeline.LOCK_PATH.exists()
        assert run_pipeline.acquire_lock() is True

    def test_release_is_safe_when_no_lock_exists(self, isolated_logs):
        run_pipeline.release_lock()  # must not raise

    def test_stale_lock_is_taken_over(self, isolated_logs):
        run_pipeline.acquire_lock()
        stale = datetime.now(UTC) - timedelta(
            minutes=run_pipeline.STALE_LOCK_MINUTES + 1
        )
        timestamp = stale.timestamp()
        import os

        os.utime(run_pipeline.LOCK_PATH, (timestamp, timestamp))
        assert run_pipeline.acquire_lock() is True

    def test_a_lock_just_under_the_stale_window_is_respected(self, isolated_logs):
        run_pipeline.acquire_lock()
        recent = datetime.now(UTC) - timedelta(
            minutes=run_pipeline.STALE_LOCK_MINUTES - 1
        )
        timestamp = recent.timestamp()
        import os

        os.utime(run_pipeline.LOCK_PATH, (timestamp, timestamp))
        assert run_pipeline.acquire_lock() is False

    def test_refresh_lock_advances_the_modification_time(self, isolated_logs):
        run_pipeline.acquire_lock()
        old = datetime.now(UTC) - timedelta(minutes=5)
        timestamp = old.timestamp()
        import os

        os.utime(run_pipeline.LOCK_PATH, (timestamp, timestamp))
        before = run_pipeline.LOCK_PATH.stat().st_mtime
        run_pipeline.refresh_lock()
        assert run_pipeline.LOCK_PATH.stat().st_mtime > before

    def test_refresh_lock_does_not_recreate_a_released_lock(self, isolated_logs):
        run_pipeline.acquire_lock()
        run_pipeline.release_lock()
        run_pipeline.refresh_lock()
        assert not run_pipeline.LOCK_PATH.exists()

    def test_stale_window_exceeds_one_step_timeout(self):
        # A healthy run must never have its lock stolen while a single step
        # is still legitimately working.
        assert (
            run_pipeline.STALE_LOCK_MINUTES * 60 > run_pipeline.STEP_TIMEOUT_SECONDS
        )


class TestDecodeStream:
    def test_none_becomes_empty_string(self):
        assert run_pipeline.decode_stream(None) == ""

    def test_bytes_are_decoded_and_stripped(self):
        assert run_pipeline.decode_stream(b"  hello\n") == "hello"

    def test_text_is_stripped(self):
        assert run_pipeline.decode_stream("  hello\n") == "hello"

    def test_undecodable_bytes_do_not_raise(self):
        assert run_pipeline.decode_stream(b"\xff\xfe ok") != ""


class TestRunStep:
    def test_timeout_is_logged_as_a_step_failure_not_a_crash(
        self, isolated_logs, monkeypatch
    ):
        def fake_run(command, **kwargs):
            raise subprocess.TimeoutExpired(
                cmd=command, timeout=1, output=b"partial work", stderr=b"slow"
            )

        monkeypatch.setattr(run_pipeline.subprocess, "run", fake_run)

        # The old code let TimeoutExpired escape: no log entry, traceback,
        # and no controlled shutdown. It must now return False instead.
        assert run_pipeline.run_step("run-1", "slow_step", "slow.py", timeout=1) is False

        events = read_events(isolated_logs)
        assert len(events) == 1
        event = events[0]
        assert event["status"] == "timeout"
        assert event["return_code"] is None
        assert "timed out after 1 seconds" in event["stderr"]
        assert event["stdout"] == "partial work"

    def test_successful_step_is_logged_and_returns_true(
        self, isolated_logs, monkeypatch
    ):
        monkeypatch.setattr(
            run_pipeline.subprocess,
            "run",
            lambda command, **kwargs: subprocess.CompletedProcess(
                command, 0, stdout="done\n", stderr=""
            ),
        )
        assert run_pipeline.run_step("run-1", "ok_step", "ok.py") is True
        event = read_events(isolated_logs)[0]
        assert event["status"] == "success"
        assert event["return_code"] == 0

    def test_failing_step_returns_false(self, isolated_logs, monkeypatch):
        monkeypatch.setattr(
            run_pipeline.subprocess,
            "run",
            lambda command, **kwargs: subprocess.CompletedProcess(
                command, 1, stdout="", stderr="boom"
            ),
        )
        assert run_pipeline.run_step("run-1", "bad_step", "bad.py") is False
        assert read_events(isolated_logs)[0]["status"] == "failed"

    def test_tolerated_exit_code_is_a_skip_and_does_not_stop_the_run(
        self, isolated_logs, monkeypatch
    ):
        monkeypatch.setattr(
            run_pipeline.subprocess,
            "run",
            lambda command, **kwargs: subprocess.CompletedProcess(
                command, 2, stdout="", stderr="MinIO not running"
            ),
        )
        result = run_pipeline.run_step(
            "run-1", "sync_minio", "upload_to_minio.py", skipped_exit_codes=(2,)
        )
        assert result is True
        assert read_events(isolated_logs)[0]["status"] == "skipped"

    def test_untolerated_exit_code_is_still_a_failure(
        self, isolated_logs, monkeypatch
    ):
        monkeypatch.setattr(
            run_pipeline.subprocess,
            "run",
            lambda command, **kwargs: subprocess.CompletedProcess(
                command, 3, stdout="", stderr="real error"
            ),
        )
        result = run_pipeline.run_step(
            "run-1", "sync_minio", "upload_to_minio.py", skipped_exit_codes=(2,)
        )
        assert result is False

    def test_unstartable_process_is_logged_rather_than_raising(
        self, isolated_logs, monkeypatch
    ):
        def fake_run(command, **kwargs):
            raise OSError("interpreter missing")

        monkeypatch.setattr(run_pipeline.subprocess, "run", fake_run)
        assert run_pipeline.run_step("run-1", "gone", "gone.py") is False
        event = read_events(isolated_logs)[0]
        assert event["status"] == "failed"
        assert "Could not start the step process" in event["stderr"]

    def test_every_event_carries_the_run_id(self, isolated_logs, monkeypatch):
        monkeypatch.setattr(
            run_pipeline.subprocess,
            "run",
            lambda command, **kwargs: subprocess.CompletedProcess(
                command, 0, stdout="", stderr=""
            ),
        )
        run_pipeline.run_step("run-abc", "one", "one.py")
        run_pipeline.run_step("run-abc", "two", "two.py")
        assert [event["run_id"] for event in read_events(isolated_logs)] == [
            "run-abc",
            "run-abc",
        ]


class TestIsDue:
    """Rate limiting is a promise to the data providers, so it is tested."""

    def make_snapshot(self, tmp_path, monkeypatch, age: timedelta):
        monkeypatch.setattr(run_pipeline, "ROOT", tmp_path)
        directory = tmp_path / "data" / "bronze" / "celestrak"
        directory.mkdir(parents=True)
        path = directory / "stations_20260830T120000Z.csv"
        path.write_text("x", encoding="utf-8")
        moment = (datetime.now(UTC) - age).timestamp()
        import os

        os.utime(path, (moment, moment))
        return path

    def test_no_previous_snapshot_is_due(self, tmp_path, monkeypatch):
        monkeypatch.setattr(run_pipeline, "ROOT", tmp_path)
        due, reason = run_pipeline.is_due(
            "data/bronze/celestrak/stations_*.csv", datetime.now(UTC)
        )
        assert due is True
        assert "no previous snapshot" in reason

    def test_recent_snapshot_is_not_due(self, tmp_path, monkeypatch):
        self.make_snapshot(tmp_path, monkeypatch, timedelta(minutes=30))
        due, reason = run_pipeline.is_due(
            "data/bronze/celestrak/stations_*.csv", datetime.now(UTC)
        )
        assert due is False
        assert "wait about" in reason

    def test_snapshot_older_than_the_interval_is_due(self, tmp_path, monkeypatch):
        self.make_snapshot(tmp_path, monkeypatch, timedelta(hours=3))
        due, _ = run_pipeline.is_due(
            "data/bronze/celestrak/stations_*.csv", datetime.now(UTC)
        )
        assert due is True

    def test_exactly_at_the_interval_is_due(self, tmp_path, monkeypatch):
        self.make_snapshot(
            tmp_path, monkeypatch, run_pipeline.MIN_COLLECTION_INTERVAL
        )
        due, _ = run_pipeline.is_due(
            "data/bronze/celestrak/stations_*.csv", datetime.now(UTC)
        )
        assert due is True

    def test_space_weather_uses_its_own_longer_interval(self, tmp_path, monkeypatch):
        # CelesTrak publishes the SW file every three hours, so a snapshot
        # two hours old must NOT be re-fetched even though the default
        # interval would allow it.
        self.make_snapshot(tmp_path, monkeypatch, timedelta(hours=2, minutes=30))
        due, _ = run_pipeline.is_due(
            "data/bronze/celestrak/stations_*.csv",
            datetime.now(UTC),
            run_pipeline.SPACE_WEATHER_INTERVAL,
        )
        assert due is False

    def test_the_newest_snapshot_decides(self, tmp_path, monkeypatch):
        monkeypatch.setattr(run_pipeline, "ROOT", tmp_path)
        directory = tmp_path / "data" / "bronze" / "celestrak"
        directory.mkdir(parents=True)
        import os

        for name, age in [
            ("stations_20260829T120000Z.csv", timedelta(days=2)),
            ("stations_20260830T120000Z.csv", timedelta(minutes=10)),
        ]:
            path = directory / name
            path.write_text("x", encoding="utf-8")
            moment = (datetime.now(UTC) - age).timestamp()
            os.utime(path, (moment, moment))

        due, _ = run_pipeline.is_due(
            "data/bronze/celestrak/stations_*.csv", datetime.now(UTC)
        )
        assert due is False


class TestIntervals:
    def test_minimum_interval_respects_the_documented_two_hours(self):
        assert run_pipeline.MIN_COLLECTION_INTERVAL == timedelta(hours=2)

    def test_space_weather_interval_is_at_least_the_minimum(self):
        assert (
            run_pipeline.SPACE_WEATHER_INTERVAL
            >= run_pipeline.MIN_COLLECTION_INTERVAL
        )


