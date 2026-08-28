"""Download the CelesTrak space-weather index file into Bronze and Silver.

The SW file carries the daily geomagnetic and solar indices that drive
thermospheric density: the eight 3-hour planetary K indices (KP1..KP8),
their sum, the Ap average, and the observed 10.7 cm solar flux (F10.7).
These are the classic drivers of satellite drag -- the physical chain the
project studies.

    python src/ingest_space_weather.py

CelesTrak's usage policy: space-weather data updates every 3 hours, so
the pipeline gates this collector at 3 hours. Rows without KP values are
forecast rows (the file extends years ahead with predicted F10.7 only)
and are excluded from Silver; the last observed day is provisional and
may be revised -- the history loader tracks revisions via content hash.
"""

from __future__ import annotations

import csv
import io
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = ROOT / "data" / "bronze" / "spaceweather"
SILVER_DIR = ROOT / "data" / "silver"

SOURCE_URL = "https://celestrak.org/SpaceData/SW-Last5Years.csv"

SILVER_COLUMNS = {
    "DATE": "observation_date",
    "KP1": "kp1",
    "KP2": "kp2",
    "KP3": "kp3",
    "KP4": "kp4",
    "KP5": "kp5",
    "KP6": "kp6",
    "KP7": "kp7",
    "KP8": "kp8",
    "KP_SUM": "kp_sum",
    "AP_AVG": "ap_avg",
    "ISN": "sunspot_number",
    "F10.7_OBS": "f10_7_observed",
    "F10.7_DATA_TYPE": "f10_7_data_type",
}


def download_csv(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "StormTrace student project/0.2"})
    with urlopen(request, timeout=60) as response:
        return response.read()


def parse_observed_rows(raw: bytes) -> list[dict[str, str]]:
    """Return only observed days (KP1 present); forecast rows lack Kp."""
    reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig")))
    required = {"DATE", "KP1", "KP8", "F10.7_OBS"}
    missing = required - set(reader.fieldnames or [])
    if missing:
        raise ValueError(
            f"Space-weather file missing required columns: {sorted(missing)}"
        )
    return [row for row in reader if row.get("KP1", "").strip() != ""]


def save_bronze(raw: bytes, retrieved_at: datetime) -> Path:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = retrieved_at.strftime("%Y%m%dT%H%M%SZ")
    path = BRONZE_DIR / f"sw_{timestamp}.csv"
    path.write_bytes(raw)
    return path


def save_silver(rows: list[dict[str, str]], retrieved_at: datetime) -> Path:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    path = SILVER_DIR / "sw_indices_latest.csv"
    fieldnames = [*SILVER_COLUMNS.values(), "retrieved_at_utc", "source_url"]
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            output = {
                target: row.get(source, "").strip()
                for source, target in SILVER_COLUMNS.items()
            }
            output["retrieved_at_utc"] = retrieved_at.isoformat()
            output["source_url"] = SOURCE_URL
            writer.writerow(output)
    return path


def main() -> int:
    retrieved_at = datetime.now(UTC).replace(microsecond=0)
    print("StormTrace space-weather index ingestion")
    try:
        raw = download_csv(SOURCE_URL)
        rows = parse_observed_rows(raw)
        bronze_path = save_bronze(raw, retrieved_at)
        silver_path = save_silver(rows, retrieved_at)
    except HTTPError as error:
        if error.code == 403:
            print(
                "CelesTrak refused this request (HTTP 403). Wait before "
                "retrying; do not retry in a loop.",
                file=sys.stderr,
            )
        else:
            print(f"CelesTrak HTTP error: {error}", file=sys.stderr)
        return 1
    except (URLError, TimeoutError) as error:
        print(f"Network error: {error}", file=sys.stderr)
        return 1
    except (OSError, UnicodeDecodeError, ValueError) as error:
        print(f"Pipeline error: {error}", file=sys.stderr)
        return 1

    latest = rows[-1]
    print(f"Observed days: {len(rows):,}")
    print(f"Latest day:    {latest['DATE']}")
    print(f"  KP_SUM={latest['KP_SUM']}  AP_AVG={latest['AP_AVG']}  "
          f"F10.7={latest['F10.7_OBS']} ({latest.get('F10.7_DATA_TYPE', '')})")
    print(f"Bronze: {bronze_path.relative_to(ROOT)}")
    print(f"Silver: {silver_path.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
