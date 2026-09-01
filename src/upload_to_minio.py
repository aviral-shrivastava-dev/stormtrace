"""Sync StormTrace data zones into the MinIO lakehouse (idempotent).

Run MinIO first:

    docker compose up -d

Then:

    python src/upload_to_minio.py

Each local file under the tracked zones is uploaded with its SHA-256 digest
stored as object metadata (x-amz-meta-sha256). On re-run, an object whose
stored digest already matches is skipped, so the sync is idempotent and
cheap. Bronze snapshots are immutable, so their digests never change;
regenerated silver/gold/report artifacts get re-uploaded when they change.

The lakehouse layout mirrors the local zones inside one bucket:

    s3://stormtrace/bronze/...
    s3://stormtrace/silver/...
    s3://stormtrace/gold/...
    s3://stormtrace/reports/...
    s3://stormtrace/quality/...

Git keeps the code; the object store keeps the data.

Credentials come from the environment, with the local docker-compose
defaults as a fallback so a fresh clone works without setup:

    STORMTRACE_S3_ENDPOINT   default http://127.0.0.1:9000
    STORMTRACE_S3_BUCKET     default stormtrace
    STORMTRACE_S3_ACCESS_KEY default minioadmin
    STORMTRACE_S3_SECRET_KEY default minioadmin

Those defaults are development-only. Anything reachable beyond localhost
must set real credentials in the environment.
"""

from __future__ import annotations

import hashlib
import os
import sys
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

ENDPOINT = os.environ.get("STORMTRACE_S3_ENDPOINT", "http://127.0.0.1:9000")
BUCKET = os.environ.get("STORMTRACE_S3_BUCKET", "stormtrace")
ACCESS_KEY = os.environ.get("STORMTRACE_S3_ACCESS_KEY", "minioadmin")
SECRET_KEY = os.environ.get("STORMTRACE_S3_SECRET_KEY", "minioadmin")

SYNCED_ZONES = ["bronze", "silver", "gold", "reports", "quality"]
CHUNK_SIZE = 1024 * 1024



def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        while chunk := file.read(CHUNK_SIZE):
            digest.update(chunk)
    return digest.hexdigest()


def collect_files() -> list[Path]:
    files: list[Path] = []
    for zone in SYNCED_ZONES:
        zone_dir = DATA_DIR / zone
        if zone_dir.exists():
            files.extend(
                path for path in zone_dir.rglob("*") if path.is_file()
            )
    return sorted(files)


def main() -> int:
    files = collect_files()
    if not files:
        print("Nothing to upload: no zone files found under data\\.", file=sys.stderr)
        return 1

    try:
        client = boto3.client(
            "s3",
            endpoint_url=ENDPOINT,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        client.head_bucket(Bucket=BUCKET)
    except EndpointConnectionError:
        print(
            "Could not reach MinIO. Start it with: docker compose up -d",
            file=sys.stderr,
        )
        return 2
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket", "403"):
            print(
                f"Bucket '{BUCKET}' is missing or inaccessible. "
                "Run: docker compose up -d",
                file=sys.stderr,
            )
            return 2
        print(f"MinIO error: {error}", file=sys.stderr)
        return 1

    uploaded = 0
    skipped = 0
    total_bytes = 0
    for path in files:
        key = path.relative_to(DATA_DIR).as_posix()
        digest = file_digest(path)

        try:
            head = client.head_object(Bucket=BUCKET, Key=key)
            stored = head.get("Metadata", {}).get("sha256", "")
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") != "404":
                raise
            stored = ""

        if stored == digest:
            skipped += 1
            continue

        client.upload_file(
            str(path),
            BUCKET,
            key,
            ExtraArgs={"Metadata": {"sha256": digest}},
        )
        uploaded += 1
        total_bytes += path.stat().st_size

    print("StormTrace MinIO lakehouse sync")
    print(f"Files scanned: {len(files):,}")
    print(f"Uploaded:      {uploaded:,} ({total_bytes / (1024 * 1024):.1f} MB)")
    print(f"Unchanged:     {skipped:,}")
    print(f"Endpoint:      {ENDPOINT}  bucket: {BUCKET}")
    print("Run again to verify idempotency: uploads should be 0.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
