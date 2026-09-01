"""Query the MinIO lakehouse directly with DuckDB, without local downloads.

Run after uploading:

    docker compose up -d
    python src/upload_to_minio.py
    python src/query_minio.py

DuckDB's httpfs extension speaks the S3 API, so read_csv_auto can scan
objects inside MinIO as if they were local files. This is the essential
lakehouse trick: compute travels to the storage, storage does not pile
up on the laptop.

The first run downloads the httpfs extension from DuckDB's official
repository; subsequent runs use the cached copy.

Credentials come from the environment (STORMTRACE_S3_*), with the local
docker-compose defaults as a fallback. Those defaults are
development-only.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]

# The httpfs extension wants a bare host:port, so any scheme is stripped.
ENDPOINT = (
    os.environ.get("STORMTRACE_S3_ENDPOINT", "http://127.0.0.1:9000")
    .removeprefix("https://")
    .removeprefix("http://")
)
ACCESS_KEY = os.environ.get("STORMTRACE_S3_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.environ.get("STORMTRACE_S3_SECRET_KEY", "minioadmin")
BUCKET = os.environ.get("STORMTRACE_S3_BUCKET", "stormtrace")
USE_SSL = os.environ.get("STORMTRACE_S3_ENDPOINT", "").startswith("https://")


def configure(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute("INSTALL httpfs")
    connection.execute("LOAD httpfs")
    # Parameterized SET is not supported for these settings, so values are
    # passed through DuckDB's own escaping of single quotes.
    for setting, value in [
        ("s3_endpoint", ENDPOINT),
        ("s3_access_key_id", ACCESS_KEY),
        ("s3_secret_access_key", SECRET_KEY),
        ("s3_url_style", "path"),
        ("s3_region", "us-east-1"),
    ]:
        connection.execute(f"SET {setting} = '{value.replace(chr(39), chr(39) * 2)}'")
    connection.execute(f"SET s3_use_ssl = {'true' if USE_SSL else 'false'}")



def main() -> int:
    connection = duckdb.connect()
    try:
        configure(connection)

        bronze_prefix = f"s3://{BUCKET}/bronze/celestrak"
        print("StormTrace lakehouse query demo (DuckDB + httpfs + MinIO)")
        print()

        # 1. Count orbital snapshots straight from object storage.
        orbital = connection.execute(
            f"""
            SELECT COUNT(*) AS rows,
                   COUNT(DISTINCT NORAD_CAT_ID) AS objects
            FROM read_csv_auto('{bronze_prefix}/*.csv')
            """
        ).fetchone()
        print(f"Bronze orbital snapshots: {orbital[0]:,} rows, "
              f"{orbital[1]:,} distinct objects")

        # 2. Objects per CelesTrak group, still entirely inside MinIO.
        groups = connection.execute(
            f"""
            SELECT
                regexp_extract(filename, '([a-z0-9-]+)_\\d{{8}}T\\d{{6}}Z\\.csv$', 1)
                    AS source_group,
                COUNT(DISTINCT NORAD_CAT_ID) AS objects
            FROM read_csv_auto('{bronze_prefix}/*.csv', filename = true)
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()
        for group, count in groups:
            print(f"  {group}: {count:,} objects")

        # 3. A real research query over the lakehouse: median altitude of the
        #    cubesat population, computed from mean motion, in SQL, in the
        #    object store, with nothing copied to the laptop.
        altitude = connection.execute(
            f"""
            SELECT ROUND(MEDIAN(
                POWER(398600.4418 /
                    POWER(TRY_CAST(MEAN_MOTION AS DOUBLE) * 2.0 * PI() / 86400.0, 2),
                    1.0/3.0
                ) - 6378.137
            ), 1) AS median_altitude_km
            FROM read_csv_auto('{bronze_prefix}/cubesat_*.csv')
            WHERE TRY_CAST(MEAN_MOTION AS DOUBLE) > 0
            """
        ).fetchone()[0]
        print(f"Cubesat median altitude (computed in the lakehouse): "
              f"{altitude} km")

        # 4. Silver zone: the space-weather table built by the pipeline.
        weather = connection.execute(
            f"""
            SELECT COUNT(*) AS rows
            FROM read_csv_auto('s3://{BUCKET}/silver/noaa_magnetic_field_latest.csv')
            """
        ).fetchone()[0]
        print(f"Silver magnetic-field rows (latest): {weather:,}")

        print()
        print("Every number above was read directly from MinIO. No data")
        print("was downloaded to a local file. This is the lakehouse")
        print("pattern: one storage layer, SQL everywhere.")
    except duckdb.Error as error:
        message = str(error)
        if "Connection" in message or "refused" in message or "extension" in message.lower():
            print(
                "Could not reach MinIO or load httpfs. Run "
                "`docker compose up -d` and `python src\\upload_to_minio.py` "
                "first.",
                file=sys.stderr,
            )
            return 1
        print(f"Lakehouse query error: {message}", file=sys.stderr)
        return 1
    finally:
        connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
