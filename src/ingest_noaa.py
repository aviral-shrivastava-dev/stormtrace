"""Download NOAA real-time solar-wind data into Bronze and Silver files."""

from __future__ import annotations

import csv
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze" / "noaa"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"

SOURCES = {
    "magnetic_field": {
        "url": "https://services.swpc.noaa.gov/json/rtsw/rtsw_mag_1m.json",
        "required": {"time_tag", "source", "active", "bt", "bz_gsm"},
        "columns": {
            "time_tag": "observed_at_utc",
            "source": "spacecraft",
            "active": "is_active_source",
            "bt": "total_field_nanotesla",
            "bx_gsm": "bx_gsm_nanotesla",
            "by_gsm": "by_gsm_nanotesla",
            "bz_gsm": "bz_gsm_nanotesla",
            "overall_quality": "quality_code",
        },
    },
    "plasma": {
        "url": "https://services.swpc.noaa.gov/json/rtsw/rtsw_wind_1m.json",
        "required": {
            "time_tag",
            "source",
            "active",
            "proton_speed",
            "proton_density",
        },
        "columns": {
            "time_tag": "observed_at_utc",
            "source": "spacecraft",
            "active": "is_active_source",
            "proton_speed": "proton_speed_km_per_second",
            "proton_temperature": "proton_temperature_kelvin",
            "proton_density": "proton_density_per_cubic_cm",
            "overall_quality": "quality_code",
        },
    },
}


def download(url: str) -> bytes:
    request = Request(
        url,
        headers={"User-Agent": "StormTrace student project/0.2"},
    )
    with urlopen(request, timeout=60) as response:
        return response.read()


def parse_and_validate(
    raw_data: bytes, required_fields: set[str]
) -> list[dict[str, object]]:
    records = json.loads(raw_data)
    if not isinstance(records, list) or not records:
        raise ValueError("NOAA returned no observations.")
    if not isinstance(records[0], dict):
        raise ValueError("NOAA returned an unexpected JSON structure.")

    missing = required_fields - records[0].keys()
    if missing:
        fields = ", ".join(sorted(missing))
        raise ValueError(f"The NOAA response is missing required fields: {fields}")

    return records


def save_bronze(name: str, raw_data: bytes, retrieved_at: datetime) -> Path:
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = retrieved_at.strftime("%Y%m%dT%H%M%SZ")
    path = BRONZE_DIR / f"{name}_{timestamp}.json"
    path.write_bytes(raw_data)
    return path


def save_silver(
    name: str,
    records: list[dict[str, object]],
    columns: dict[str, str],
    source_url: str,
    retrieved_at: datetime,
) -> Path:
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    path = SILVER_DIR / f"noaa_{name}_latest.csv"
    fieldnames = [*columns.values(), "retrieved_at_utc", "source_url"]

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            row = {
                output_name: record.get(source_name)
                for source_name, output_name in columns.items()
            }
            row["retrieved_at_utc"] = retrieved_at.isoformat()
            row["source_url"] = source_url
            writer.writerow(row)

    return path


def main() -> int:
    retrieved_at = datetime.now(UTC).replace(microsecond=0)
    results: list[tuple[str, int, Path, Path]] = []
    print("StormTrace lesson 2")

    try:
        for name, config in SOURCES.items():
            url = str(config["url"])
            print(f"Downloading NOAA {name.replace('_', ' ')} data...")
            raw_data = download(url)
            records = parse_and_validate(raw_data, config["required"])
            bronze_path = save_bronze(name, raw_data, retrieved_at)
            silver_path = save_silver(
                name,
                records,
                config["columns"],
                url,
                retrieved_at,
            )
            results.append((name, len(records), bronze_path, silver_path))
    except HTTPError as error:
        print(f"NOAA HTTP error: {error}", file=sys.stderr)
        return 1
    except (URLError, TimeoutError) as error:
        print(f"Network error: {error}", file=sys.stderr)
        return 1
    except (json.JSONDecodeError, OSError, UnicodeDecodeError, ValueError) as error:
        print(f"Pipeline error: {error}", file=sys.stderr)
        return 1

    print()
    for name, count, bronze_path, silver_path in results:
        print(f"{name.replace('_', ' ').title()}: {count:,} observations")
        print(f"  Bronze: {bronze_path.relative_to(PROJECT_ROOT)}")
        print(f"  Silver: {silver_path.relative_to(PROJECT_ROOT)}")
    print("Next lesson: use SQL to inspect and join observations by UTC time.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
