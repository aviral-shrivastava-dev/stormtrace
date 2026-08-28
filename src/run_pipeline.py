"""Run one policy-aware StormTrace collection cycle."""

from __future__ import annotations

import argparse
import json
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
STALE_LOCK_MINUTES = 30
MIN_COLLECTION_INTERVAL = timedelta(hours=2)
NOAA_PATTERN = "data/bronze/noaa/magnetic_field_*.json"


def acquire_lock() -> bool:
    """Prevent two pipelines from writing DuckDB at the same time."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if LOCK_PATH.exists():
        modified = datetime.fromtimestamp(LOCK_PATH.stat().st_mtime, UTC)
        if datetime.now(UTC) - modified < timedelta(minutes=STALE_LOCK_MINUTES):
            return False
        LOCK_PATH.unlink()
    LOCK_PATH.write_text(datetime.now(UTC).isoformat(), encoding="utf-8")
    return True


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


def is_due(pattern: str, now: datetime) -> tuple[bool, str]:
    latest = latest_snapshot(pattern)
    age = snapshot_age(latest, now)
    if latest is None or age is None:
        return True, "no previous snapshot"
    if age >= MIN_COLLECTION_INTERVAL:
        return True, f"latest snapshot is {age.total_seconds() / 3600:.2f} hours old"
    wait = MIN_COLLECTION_INTERVAL - age
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
    timeout: int = 300,
) -> bool:
    """Run one pipeline script as a subprocess.

    The default timeout is generous because a machine that just woke from
    sleep runs everything slower: cold caches, antivirus scanning, and a
    growing backlog of Bronze files to checksum. A step killed by timeout
    leaves no partial data: the history loader commits each file
    atomically, and every other step rebuilds its outputs from scratch.
    """
    command = [sys.executable, str(ROOT / "src" / script), *(extra_args or [])]
    started_at = datetime.now(UTC)
    started = time.monotonic()
    print(f"[{name}] Starting...")
    result = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    duration = round(time.monotonic() - started, 3)
    status = "success" if result.returncode == 0 else "failed"
    event = {
        "run_id": run_id,
        "step": name,
        "status": status,
        "started_at_utc": started_at.isoformat(),
        "duration_seconds": duration,
        "return_code": result.returncode,
        "command": command,
        "stdout": result.stdout.strip(),
        "stderr": result.stderr.strip(),
    }
    append_log(event)
    if result.stdout.strip():
        print(result.stdout.strip())
    if result.stderr.strip():
        print(result.stderr.strip(), file=sys.stderr)
    print(f"[{name}] {status} in {duration:.2f} seconds")
    return result.returncode == 0


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

    if args.dry_run:
        print("Dry run only: no network requests or database changes were made.")
        return 0

    if due_groups:
        if not run_step(
            run_id,
            "ingest_celestrak",
            "ingest_celestrak.py",
            ["--groups", ",".join(due_groups)],
            timeout=300,
        ):
            print("Pipeline stopped because ingestion failed.", file=sys.stderr)
            return 1
    else:
        log_skip(
            run_id,
            "ingest_celestrak",
            f"all tracked CelesTrak groups ({', '.join(CELESTRAK_GROUPS)}) are fresh",
        )

    if noaa_due:
        if not run_step(
            run_id,
            "ingest_noaa",
            "ingest_noaa.py",
            timeout=300,
        ):
            print("Pipeline stopped because ingestion failed.", file=sys.stderr)
            return 1
    else:
        log_skip(run_id, "ingest_noaa", noaa_reason)

    for name, script in [
        ("load_history", "load_history.py"),
        ("check_quality", "check_quality.py"),
        ("summarize_history", "summarize_history.py"),
        ("build_gold", "build_gold.py"),
        ("build_orbit_features", "build_orbit_features.py"),
        ("build_propagation_disagreement", "build_propagation_disagreement.py"),
        ("analyze_research", "analyze_research.py"),
    ]:
        if not run_step(run_id, name, script):
            print(f"Pipeline stopped because {name} failed.", file=sys.stderr)
            return 1

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
