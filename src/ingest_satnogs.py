"""Download recent SatNOGS observations into Bronze and Silver.

SatNOGS is a global network of volunteer ground stations that track
amateur and research satellites. Its network API exposes observation
metadata: which satellite (NORAD id) was heard, when, by which station,
at what frequency, and whether the pass was good. This is the project's
telemetry pillar: independent, crowd-sourced evidence that specific
satellites were transmitting and receivable at specific times.

    python src/ingest_satnogs.py

Scope, honestly stated: this ingests observation METADATA, not decoded
telemetry frames. Frames are base64-encoded in per-satellite formats;
decoding requires a decoder per satellite and is out of scope. Coverage
is the latest API page (25 observations) each run - a rolling sample
sufficient to quantify tracking activity, not to reconstruct full
telemetry history.

Versioned service lesson: an earlier version paginated with ?page=N, but
the SatNOGS network API changed and now rejects the page parameter
(HTTP 400) while still accepting the plain ?format=json list. The
collector therefore samples whatever the first page returns and treats
the service's changing surface as a soft constraint, never a pipeline
failure. This is routine maintenance on any public API.
"""

from __future__ import annotations

import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = ROOT / "data" / "bronze" / "satnogs"
SILVER_DIR = ROOT / "data" / "silver"

SOURCE_URL = "https://network.satnogs.org/api/observations/?format=json"

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


def fetch_observations() -> list[dict]:
    request = Request(SOURCE_URL, headers={"User-Agent": "StormTrace student project/0.2"})
    with urlopen(request, timeout=30) as response:
        payload = json.loads(response.read())
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


def main() -> int:
    retrieved_at = datetime.now(UTC).replace(microsecond=0)
    print("StormTrace SatNOGS observation ingestion")
    observations: list[dict] = []
    try:
        observations.extend(fetch_observations())
        if not observations:
            print("SatNOGS returned no observations.", file=sys.stderr)
            return 1

        rows = [observation_row(observation) for observation in observations]
        seen: set[int] = set()
        deduplicated = []
        for row in rows:
            observation_id = row["observation_id"]
            if observation_id in seen:
                continue
            seen.add(observation_id)
            deduplicated.append(row)
        rows = deduplicated

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
                row = dict(row)
                row["retrieved_at_utc"] = retrieved_at.isoformat()
                row["source_url"] = SOURCE_URL
                writer.writerow(row)
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
    print(f"Observations sampled: {len(rows):,} "
          f"({distinct_satellites} satellites)")
    print("Statuses: " + ", ".join(f"{k}={v}" for k, v in sorted(statuses.items())))
    print(f"Bronze: {bronze_path.relative_to(ROOT)}")
    print(f"Silver: {silver_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
