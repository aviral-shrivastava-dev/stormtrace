"""Download SatNOGS observations into Bronze and Silver.

SatNOGS is a global network of volunteer ground stations that track
amateur and research satellites. Its network API exposes observation
metadata: which satellite (NORAD id) was heard, when, by which station,
at what frequency, and whether the pass was good. This is the project's
telemetry pillar: independent, crowd-sourced evidence that specific
satellites were transmitting and receivable at specific times.

    python src/ingest_satnogs.py

Scope, honestly stated: this ingests observation METADATA, not decoded
telemetry frames. Frames are base64-encoded in per-satellite formats;
decoding requires a decoder per satellite and is out of scope.

Why this collector makes several requests
-----------------------------------------
The default listing returns the newest observations, which are almost
always still SCHEDULED: status 'future'. An earlier version sampled only
that page and never revisited it, so every row ever stored had status
'future' and the "good pass" metric was structurally stuck at zero -- a
pillar that could not measure what it claimed. Three additions fix that:

1. Terminal-status sweeps (`?status=good|bad|failed`) collect passes that
   have already been vetted. These carry the real outcome signal.
2. The default listing is still sampled, so upcoming coverage is visible.
3. Pending resolution: observations already in Bronze whose pass window
   has closed but whose status is still non-terminal are re-fetched by id
   (`?id=N`), capped per run, oldest first. This is what turns a
   previously stored 'future' row into its final outcome.

Provider respect: requests are bounded (at most
1 + len(TERMINAL_STATUSES) + RESOLVE_LIMIT per run) and spaced by
REQUEST_SPACING_SECONDS. The pipeline additionally gates this collector
to once every two hours.

Versioned service lesson: an earlier version paginated with ?page=N, but
the SatNOGS network API changed and now rejects the page parameter
(HTTP 400) while still accepting the plain ?format=json list. The
collector treats the service's changing surface as a soft constraint,
never a pipeline failure. This is routine maintenance on any public API.
"""

from __future__ import annotations

import csv
import json
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = ROOT / "data" / "bronze" / "satnogs"
SILVER_DIR = ROOT / "data" / "silver"

BASE_URL = "https://network.satnogs.org/api/observations/"
SOURCE_URL = f"{BASE_URL}?format=json"
USER_AGENT = "StormTrace student project/0.2"
REQUEST_TIMEOUT = 30

# Statuses that will never change again. Anything else ('future',
# 'unknown') is provisional and worth re-checking once the pass is over.
TERMINAL_STATUSES = ("good", "bad", "failed")
NON_TERMINAL_STATUSES = ("future", "unknown")

# At most this many pending observations are resolved per run, so the
# request count stays bounded no matter how large the backlog grows.
RESOLVE_LIMIT = 10
REQUEST_SPACING_SECONDS = 1.0

SILVER_FIELDS = [
    "observation_id",
    "start_utc",
    "end_utc",
    "status",
    "norad_catalog_id",
    "satellite_id",
    "station_id",
    "station_lat",
    "station_lng",
    "observation_frequency_hz",
    "transmitter_mode",
]


def fetch_json(url: str) -> list[dict]:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=REQUEST_TIMEOUT) as response:
        payload = json.loads(response.read())
    if isinstance(payload, dict):
        # A single-object response (the ?id= form can return either).
        return [payload]
    if not isinstance(payload, list):
        raise ValueError("SatNOGS returned an unexpected payload structure.")
    return payload



def observation_row(observation: dict) -> dict[str, object]:
    return {
        "observation_id": observation.get("id"),
        "start_utc": observation.get("start"),
        "end_utc": observation.get("end"),
        "status": observation.get("status"),
        "norad_catalog_id": observation.get("norad_cat_id"),
        "satellite_id": observation.get("sat_id"),
        "station_id": observation.get("ground_station"),
        "station_lat": observation.get("station_lat"),
        "station_lng": observation.get("station_lng"),
        "observation_frequency_hz": observation.get("observation_frequency"),
        "transmitter_mode": observation.get("transmitter_mode"),
    }


def parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def pending_observation_ids(now: datetime) -> list[int]:
    """Ids in Bronze whose pass has ended but whose status is provisional.

    Reading Bronze (rather than DuckDB) keeps ingestion independent of the
    warehouse: the loader has not run yet at this point in the pipeline.
    The newest stored status for each id wins, so an id resolved on an
    earlier run is not queried again.
    """
    latest_status: dict[int, tuple[str, object]] = {}
    for path in sorted(BRONZE_DIR.glob("observations_*.json")):
        try:
            records = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            # A single unreadable snapshot must not stop collection.
            continue
        if not isinstance(records, list):
            continue
        for record in records:
            if not isinstance(record, dict):
                continue
            observation_id = record.get("id")
            if not isinstance(observation_id, int):
                continue
            latest_status[observation_id] = (
                str(record.get("status")),
                record.get("end"),
            )

    pending: list[tuple[datetime, int]] = []
    for observation_id, (status, end_value) in latest_status.items():
        if status not in NON_TERMINAL_STATUSES:
            continue
        end = parse_utc(end_value)
        if end is None or end >= now:
            # The pass has not finished yet; asking now cannot help.
            continue
        pending.append((end, observation_id))

    pending.sort()
    return [observation_id for _, observation_id in pending[:RESOLVE_LIMIT]]


def collect(now: datetime) -> tuple[list[dict], dict[str, int]]:
    """Gather observations from every source, newest status winning."""
    by_id: dict[int, dict] = {}
    counts = {"listing": 0, "terminal": 0, "resolved": 0, "request_errors": 0}

    def absorb(records: list[dict]) -> int:
        added = 0
        for record in records:
            if not isinstance(record, dict):
                continue
            observation_id = record.get("id")
            if not isinstance(observation_id, int):
                continue
            by_id[observation_id] = record
            added += 1
        return added

    def optional_request(url: str) -> list[dict] | None:
        """Fetch one URL, treating a per-request failure as non-fatal."""
        try:
            return fetch_json(url)
        except (HTTPError, URLError, TimeoutError, ValueError,
                json.JSONDecodeError) as error:
            print(f"  request failed ({url}): {error}", file=sys.stderr)
            counts["request_errors"] += 1
            return None

    print("  fetching the latest listing...")
    counts["listing"] = absorb(fetch_json(SOURCE_URL))

    for status in TERMINAL_STATUSES:
        time.sleep(REQUEST_SPACING_SECONDS)
        print(f"  fetching completed passes with status '{status}'...")
        records = optional_request(f"{BASE_URL}?format=json&status={status}")
        if records:
            counts["terminal"] += absorb(records)

    pending = pending_observation_ids(now)
    if pending:
        print(
            f"  resolving {len(pending)} pending observation(s) whose pass "
            "window has closed..."
        )
    for observation_id in pending:
        time.sleep(REQUEST_SPACING_SECONDS)
        records = optional_request(f"{BASE_URL}?format=json&id={observation_id}")
        if records:
            counts["resolved"] += absorb(records)

    return [by_id[key] for key in sorted(by_id)], counts


def main() -> int:
    retrieved_at = datetime.now(UTC).replace(microsecond=0)
    print("StormTrace SatNOGS observation ingestion")

    try:
        observations, counts = collect(retrieved_at)
        if not observations:
            print("SatNOGS returned no observations.", file=sys.stderr)
            return 1

        rows = [observation_row(observation) for observation in observations]

        BRONZE_DIR.mkdir(parents=True, exist_ok=True)
        timestamp = retrieved_at.strftime("%Y%m%dT%H%M%SZ")
        bronze_path = BRONZE_DIR / f"observations_{timestamp}.json"
        bronze_path.write_text(
            json.dumps(observations, indent=1), encoding="utf-8"
        )

        SILVER_DIR.mkdir(parents=True, exist_ok=True)
        silver_path = SILVER_DIR / "satnogs_observations_latest.csv"
        with silver_path.open("w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(
                file, fieldnames=[*SILVER_FIELDS, "retrieved_at_utc", "source_url"]
            )
            writer.writeheader()
            for row in rows:
                output = dict(row)
                output["retrieved_at_utc"] = retrieved_at.isoformat()
                output["source_url"] = SOURCE_URL
                writer.writerow(output)
    except HTTPError as error:
        print(f"SatNOGS HTTP error: {error}", file=sys.stderr)
        return 1
    except (URLError, TimeoutError) as error:
        print(f"Network error: {error}", file=sys.stderr)
        return 1
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Pipeline error: {error}", file=sys.stderr)
        return 1

    statuses: dict[str, int] = {}
    for row in rows:
        status = str(row.get("status"))
        statuses[status] = statuses.get(status, 0) + 1
    distinct_satellites = len(
        {row["norad_catalog_id"] for row in rows if row["norad_catalog_id"]}
    )
    terminal = sum(
        count for status, count in statuses.items() if status in TERMINAL_STATUSES
    )

    print()
    print(f"Observations sampled: {len(rows):,} ({distinct_satellites} satellites)")
    print("Statuses: " + ", ".join(f"{k}={v}" for k, v in sorted(statuses.items())))
    print(f"Completed passes with a final outcome: {terminal:,}")
    print(
        f"Sources: {counts['listing']} from the listing, "
        f"{counts['terminal']} from status sweeps, "
        f"{counts['resolved']} resolved from pending"
    )
    if counts["request_errors"]:
        print(
            f"Optional requests that failed: {counts['request_errors']} "
            "(collection continued with what was available)"
        )
    print(f"Bronze: {bronze_path.relative_to(ROOT)}")
    print(f"Silver: {silver_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

