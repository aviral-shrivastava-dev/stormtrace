"""Download satellite orbit records into Bronze and Silver files."""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


# Tracked CelesTrak GP groups. The small stations group covers crewed
# vehicles; the cubesat group adds a large drag-sensitive LEO population
# for population-level research. The science group adds several hundred
# research satellites across many orbital regimes. Add more groups here
# as the project grows.
DEFAULT_GROUPS = ["stations", "cubesat", "science"]

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "celestrak"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"

REQUIRED_FIELDS = {
    "OBJECT_NAME",
    "NORAD_CAT_ID",
    "EPOCH",
    "INCLINATION",
    "ECCENTRICITY",
    "MEAN_MOTION",
}

SILVER_COLUMNS = {
    "OBJECT_NAME": "object_name",
    "NORAD_CAT_ID": "norad_catalog_id",
    "EPOCH": "element_epoch_utc",
    "INCLINATION": "inclination_degrees",
    "ECCENTRICITY": "eccentricity",
    "MEAN_MOTION": "mean_motion_revolutions_per_day",
    "BSTAR": "bstar_drag_term",
}


def source_url(group: str) -> str:
    return f"https://celestrak.org/NORAD/elements/gp.php?GROUP={group}&FORMAT=csv"


def download_csv(url: str) -> bytes:
    """Return the raw response so Bronze preserves exactly what was received."""
    request = Request(
        url,
        headers={"User-Agent": "StormTrace student project/0.2"},
    )
    with urlopen(request, timeout=60) as response:
        return response.read()


def parse_and_validate(raw_data: bytes) -> list[dict[str, object]]:
    text = raw_data.decode("utf-8-sig")
    records = list(csv.DictReader(text.splitlines()))
    if not records:
        raise ValueError("CelesTrak returned no satellite records.")

    missing = REQUIRED_FIELDS - records[0].keys()
    if missing:
        fields = ", ".join(sorted(missing))
        raise ValueError(f"The response is missing required fields: {fields}")

    return records


def save_bronze(group: str, raw_data: bytes, retrieved_at: datetime) -> Path:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = retrieved_at.strftime("%Y%m%dT%H%M%SZ")
    path = BRONZE_DIR / f"{group}_{timestamp}.csv"
    path.write_bytes(raw_data)
    return path


def save_silver(
    group: str,
    records: list[dict[str, object]],
    retrieved_at: datetime,
) -> Path:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    path = SILVER_DIR / f"{group}_satellites_latest.csv"
    fieldnames = [*SILVER_COLUMNS.values(), "retrieved_at_utc", "source_url"]

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            row = {
                output_name: record.get(source_name)
                for source_name, output_name in SILVER_COLUMNS.items()
            }
            row["retrieved_at_utc"] = retrieved_at.isoformat()
            row["source_url"] = source_url(group)
            writer.writerow(row)

    return path


def ingest_group(group: str, retrieved_at: datetime) -> tuple[int, Path, Path]:
    raw_data = download_csv(source_url(group))
    records = parse_and_validate(raw_data)
    bronze_path = save_bronze(group, raw_data, retrieved_at)
    silver_path = save_silver(group, records, retrieved_at)
    return len(records), bronze_path, silver_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--groups",
        default=",".join(DEFAULT_GROUPS),
        help="comma-separated CelesTrak groups to download (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    groups = [group.strip() for group in args.groups.split(",") if group.strip()]
    if not groups:
        print("No valid groups requested.", file=sys.stderr)
        return 1

    retrieved_at = datetime.now(UTC).replace(microsecond=0)
    print("StormTrace orbital ingestion")
    results: list[tuple[str, int, Path, Path]] = []

    try:
        for group in groups:
            print(f"Downloading CelesTrak '{group}' group...")
            results.append((group, *ingest_group(group, retrieved_at)))
    except HTTPError as error:
        if error.code == 403:
            print(
                "CelesTrak refused this request (HTTP 403). Wait at least "
                "two hours before trying again; do not retry in a loop.",
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

    print()
    for group, count, bronze_path, silver_path in results:
        print(f"{group}: {count:,} records")
        print(f"  Bronze: {bronze_path.relative_to(PROJECT_ROOT)}")
        print(f"  Silver: {silver_path.relative_to(PROJECT_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
