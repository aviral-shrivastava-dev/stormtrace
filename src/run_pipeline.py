"""Run one policy-aware StormTrace collection cycle."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from ingest_celestrak import DEFAULT_GROUPS as CELESTRAK_GROUPS

ROOT = Path(__file__).resolve().parents[1]
LOG_DIR = ROOT / "data" / "logs"
LOG_PATH = LOG_DIR / "pipeline_runs.jsonl"
LOCK_PATH = LOG_DIR / "pipeline.lock"

# Per-step timeout, and the stale-lock window derived from it.
#
# The lock is refreshed after every step, so its modification time tracks
# progress rather than the run's start. A lock is only stale once nothing
# has happened for longer than one step could possibly take, plus margin.
# Deriving the window instead of hardcoding 30 minutes keeps a long but
# healthy run from having its lock stolen mid-flight.
STEP_TIMEOUT_SECONDS = 300
STALE_LOCK_MINUTES = int(STEP_TIMEOUT_SECONDS / 60 * 2) + 5

MIN_COLLECTION_INTERVAL = timedelta(hours=2)
# CelesTrak's space-weather index file updates every three hours, which is
# its own rate-limit interval; every other source follows the two-hour
# minimum used by the orbital catalog.
SPACE_WEATHER_INTERVAL = timedelta(hours=3)
NOAA_PATTERN = "data/bronze/noaa/magnetic_field_*.json"
SPACE_WEATHER_PATTERN = "data/bronze/spaceweather/sw_*.csv"
SATNOGS_PATTERN = "data/bronze/satnogs/observations_*.json"


def decode_stream(stream: bytes | str | None) -> str:
    """Normalize captured subprocess output to a stripped string.

    subprocess.run(text=True) yields str, but TimeoutExpired can carry
    bytes depending on how far the child got, so both are handled.
    """
    if stream is None:
        return ""
    if isinstance(stream, bytes):
        return stream.decode("utf-8", errors="replace").strip()
    return stream.strip()


def acquire_lock() -> bool:
    """Prevent two pipelines from writing DuckDB at the same time.

    Creation is atomic: O_CREAT | O_EXCL fails if the file already exists,
    so two runs starting at the same instant cannot both believe they won.
    A previous check-then-write version had a race window between
    exists() and write_text() wide enough for the hourly scheduler to
    overlap a manual run.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    for attempt in (1, 2):
        try:
            descriptor = os.open(
                LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY
            )
        except FileExistsError:
            if attempt == 2:
                return False
            modified = datetime.fromtimestamp(LOCK_PATH.stat().st_mtime, UTC)
            if datetime.now(UTC) - modified < timedelta(minutes=STALE_LOCK_MINUTES):
                return False
            # The holder died without releasing. Remove the stale lock and
            # retry the atomic create exactly once; if another run wins the
            # retry, this run stands down.
            LOCK_PATH.unlink(missing_ok=True)
            continue
        with os.fdopen(descriptor, "w", encoding="utf-8") as file:
            file.write(datetime.now(UTC).isoformat())
        return True
    return False


def refresh_lock() -> None:
    """Mark the lock as still alive after each step completes."""
    if LOCK_PATH.exists():
        LOCK_PATH.write_text(datetime.now(UTC).isoformat(), encoding="utf-8")


def release_lock() -> None:
    LOCK_PATH.unlink(missing_ok=True)



def latest_snapshot(pattern: str) -> Path | None:
    files = list(ROOT.glob(pattern))
    return max(files, key=lambda path: path.stat().st_mtime) if files else None


def snapshot_age(path: Path | None, now: datetime) -> timedelta | None:
    if path is None:
        return None
    modified = datetime.fromtimestamp(path.stat().st_mtime, UTC)
    return now - modified


def is_due(
    pattern: str,
    now: datetime,
    interval: timedelta = MIN_COLLECTION_INTERVAL,
) -> tuple[bool, str]:
    latest = latest_snapshot(pattern)
    age = snapshot_age(latest, now)
    if latest is None or age is None:
        return True, "no previous snapshot"
    if age >= interval:
        return True, f"latest snapshot is {age.total_seconds() / 3600:.2f} hours old"
    wait = interval - age
    minutes = max(1, int(wait.total_seconds() / 60) + 1)
    relative = latest.relative_to(ROOT)
    return False, f"{relative} is recent; wait about {minutes} more minutes"


def append_log(event: dict[str, object]) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as file:
        file.write(json.dumps(event, ensure_ascii=True) + "\n")


def run_step(
    run_id: str,
    name: str,
    script: str,
    extra_args: list[str] | None = None,
    timeout: int = STEP_TIMEOUT_SECONDS,
    skipped_exit_codes: tuple[int, ...] = (),
) -> bool:
    """Run one pipeline script as a subprocess.

    The default timeout is generous because a machine that just woke from
    sleep runs everything slower: cold caches, antivirus scanning, and a
    growing backlog of Bronze files to checksum. A step killed by timeout
    leaves no partial data: the history loader commits each file
    atomically, and every other step rebuilds its outputs from scratch.

    A timeout is a step FAILURE, not an orchestrator crash: it is logged
    with return_code None and a 'timeout' status marker in stderr, the
    lock is released by main(), and the run stops in the same controlled
    way as any other failed step. The lock is refreshed while waiting so a
    long but healthy run is never mistaken for a stale lock.

    Exit codes listed in skipped_exit_codes are logged as a skip rather
    than a failure (used for optional steps like the MinIO sync, which
    reports 2 when the lakehouse is simply not running).
    """
    command = [sys.executable, str(ROOT / "src" / script), *(extra_args or [])]
    started_at = datetime.now(UTC)
    started = time.monotonic()
    print(f"[{name}] Starting...")

    timed_out = False
    try:
        result = subprocess.run(
            command,
            cwd=ROOT,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return_code: int | None = result.returncode
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()
    except subprocess.TimeoutExpired as expired:
        timed_out = True
        return_code = None
        stdout = decode_stream(expired.stdout)
        stderr = "\n".join(
            part
            for part in (
                f"Step timed out after {timeout} seconds and was terminated.",
                decode_stream(expired.stderr),
            )
            if part
        )
    except OSError as error:
        # The interpreter or script path could not be executed at all.
        timed_out = False
        return_code = None
        stdout = ""
        stderr = f"Could not start the step process: {error}"

    duration = round(time.monotonic() - started, 3)
    if return_code == 0:
        status = "success"
    elif return_code is not None and return_code in skipped_exit_codes:
        status = "skipped"
    elif timed_out:
        status = "timeout"
    else:
        status = "failed"

    append_log(
        {
            "run_id": run_id,
            "step": name,
            "status": status,
            "started_at_utc": started_at.isoformat(),
            "duration_seconds": duration,
            "return_code": return_code,
            "command": command,
            "stdout": stdout,
            "stderr": stderr,
        }
    )
    if stdout:
        print(stdout)
    if stderr:
        print(stderr, file=sys.stderr)
    print(f"[{name}] {status} in {duration:.2f} seconds")
    return status in ("success", "skipped")



def log_skip(run_id: str, name: str, reason: str) -> None:
    print(f"[{name}] Skipped: {reason}")
    append_log(
        {
            "run_id": run_id,
            "step": name,
            "status": "skipped",
            "started_at_utc": datetime.now(UTC).isoformat(),
            "duration_seconds": 0,
            "return_code": None,
            "command": None,
            "stdout": "",
            "stderr": reason,
        }
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="show what is due without running any scripts",
    )
    return parser.parse_args()


def run_pipeline(args: argparse.Namespace) -> int:
    now = datetime.now(UTC)
    run_id = now.strftime("%Y%m%dT%H%M%S.%fZ")

    print(f"StormTrace pipeline run: {run_id}")
    due_groups: list[str] = []
    for group in CELESTRAK_GROUPS:
        due, reason = is_due(f"data/bronze/celestrak/{group}_*.csv", now)
        print(f"  celestrak/{group}: {'due' if due else 'not due'} ({reason})")
        if due:
            due_groups.append(group)
    noaa_due, noaa_reason = is_due(NOAA_PATTERN, now)
    print(f"  ingest_noaa: {'due' if noaa_due else 'not due'} ({noaa_reason})")
    sw_due, sw_reason = is_due(SPACE_WEATHER_PATTERN, now, SPACE_WEATHER_INTERVAL)
    print(f"  ingest_space_weather: {'due' if sw_due else 'not due'} ({sw_reason})")
    satnogs_due, satnogs_reason = is_due(SATNOGS_PATTERN, now)
    print(f"  ingest_satnogs: {'due' if satnogs_due else 'not due'} ({satnogs_reason})")

    if args.dry_run:
        print("Dry run only: no network requests or database changes were made.")
        return 0

    # Ingestion steps, each gated by its own freshness check. Running them
    # from one table (instead of four near-identical if/else blocks) keeps
    # the rate-limit contract in one place.
    ingestion_steps: list[tuple[str, str, list[str] | None, bool, str]] = [
        (
            "ingest_celestrak",
            "ingest_celestrak.py",
            ["--groups", ",".join(due_groups)] if due_groups else None,
            bool(due_groups),
            f"all tracked CelesTrak groups ({', '.join(CELESTRAK_GROUPS)}) are fresh",
        ),
        ("ingest_noaa", "ingest_noaa.py", None, noaa_due, noaa_reason),
        (
            "ingest_space_weather",
            "ingest_space_weather.py",
            None,
            sw_due,
            sw_reason,
        ),
        ("ingest_satnogs", "ingest_satnogs.py", None, satnogs_due, satnogs_reason),
    ]

    for name, script, extra_args, due, skip_reason in ingestion_steps:
        if not due:
            log_skip(run_id, name, skip_reason)
            continue
        if not run_step(run_id, name, script, extra_args):
            print(f"Pipeline stopped because {name} failed.", file=sys.stderr)
            return 1
        refresh_lock()

    for name, script, skipped_codes in [
        ("load_history", "load_history.py", ()),
        ("check_quality", "check_quality.py", ()),
        ("summarize_history", "summarize_history.py", ()),
        ("build_gold", "build_gold.py", ()),
        ("build_orbit_features", "build_orbit_features.py", ()),
        ("build_propagation_disagreement", "build_propagation_disagreement.py", ()),
        ("validate_ori", "validate_ori.py", ()),
        ("analyze_research", "analyze_research.py", ()),
        # Optional: retrains the disagreement model when enough measured
        # pairs exist; exits 2 (tolerated as a skip) when the dataset is
        # still too small to train honestly.
        ("train_model", "train_model.py", (2,)),
        # Optional: mirrors all zones into the MinIO lakehouse when it is
        # running; exits 2 (tolerated as a skip) when it is not.
        ("sync_minio", "upload_to_minio.py", (2,)),
        # Optional: replays this run's events onto the Redpanda stream when
        # the broker is running; exits 2 (tolerated as a skip) when not.
        ("publish_events", "publish_events.py", (2,)),
    ]:
        if not run_step(run_id, name, script, skipped_exit_codes=skipped_codes):
            print(f"Pipeline stopped because {name} failed.", file=sys.stderr)
            return 1
        refresh_lock()


    append_log(
        {
            "run_id": run_id,
            "step": "pipeline",
            "status": "success",
            "started_at_utc": now.isoformat(),
            "duration_seconds": round((datetime.now(UTC) - now).total_seconds(), 3),
            "return_code": 0,
            "command": None,
            "stdout": "all required steps completed",
            "stderr": "",
        }
    )
    print(f"Pipeline succeeded. Log: {LOG_PATH.relative_to(ROOT)}")
    return 0


def main() -> int:
    args = parse_args()
    if not acquire_lock():
        print(
            "Another StormTrace pipeline run is already in progress. "
            "The DuckDB database allows only one writer at a time.",
            file=sys.stderr,
        )
        return 1
    try:
        return run_pipeline(args)
    finally:
        release_lock()


if __name__ == "__main__":
    raise SystemExit(main())
