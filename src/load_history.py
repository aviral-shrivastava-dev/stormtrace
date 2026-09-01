"""Load immutable Bronze snapshots into DuckDB history tables."""

from __future__ import annotations

import csv
import hashlib
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

try:
    import duckdb
except ImportError as error:
    print("DuckDB is not installed. Run: python -m pip install duckdb")
    raise SystemExit(1) from error



ROOT = Path(__file__).resolve().parents[1]
DATABASE = ROOT / "data" / "stormtrace.duckdb"
BRONZE = ROOT / "data" / "bronze"

CELESTRAK_FILENAME = re.compile(r"^(?P<group>[a-z0-9-]+)_\d{8}T\d{6}Z\.csv$")
CHUNK_SIZE = 500

ORBITAL_COLUMNS = [
    "snapshot_at_utc",
    "object_name",
    "norad_catalog_id",
    "element_epoch_utc",
    "inclination_degrees",
    "eccentricity",
    "mean_motion_revolutions_per_day",
    "bstar_drag_term",
    "ra_of_asc_node_degrees",
    "arg_of_pericenter_degrees",
    "mean_anomaly_degrees",
    "mean_motion_dot",
    "mean_motion_ddot",
    "source_group",
    "source_file",
    "source_sha256",
]
MAGNETIC_COLUMNS = [
    "snapshot_at_utc",
    "observed_at_utc",
    "spacecraft",
    "is_active_source",
    "bt",
    "bx_gsm",
    "by_gsm",
    "bz_gsm",
    "quality_code",
    "source_file",
    "source_sha256",
]
PLASMA_COLUMNS = [
    "snapshot_at_utc",
    "observed_at_utc",
    "spacecraft",
    "is_active_source",
    "proton_speed",
    "proton_temperature",
    "proton_density",
    "quality_code",
    "source_file",
    "source_sha256",
]


def snapshot_time(path: Path) -> datetime:
    match = re.search(r"(\d{8}T\d{6}Z)", path.name)
    if not match:
        raise ValueError(f"Cannot find UTC timestamp in filename: {path.name}")
    return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=UTC)


def checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def setup(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS bronze_file_registry (
            file_path VARCHAR PRIMARY KEY,
            source VARCHAR NOT NULL,
            snapshot_at_utc TIMESTAMPTZ NOT NULL,
            sha256 VARCHAR NOT NULL,
            loaded_at_utc TIMESTAMPTZ NOT NULL,
            row_count INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS orbital_snapshot_history (
            snapshot_at_utc TIMESTAMPTZ,
            object_name VARCHAR,
            norad_catalog_id BIGINT,
            element_epoch_utc TIMESTAMP,
            inclination_degrees DOUBLE,
            eccentricity DOUBLE,
            mean_motion_revolutions_per_day DOUBLE,
            bstar_drag_term DOUBLE,
            ra_of_asc_node_degrees DOUBLE,
            arg_of_pericenter_degrees DOUBLE,
            mean_anomaly_degrees DOUBLE,
            mean_motion_dot DOUBLE,
            mean_motion_ddot DOUBLE,
            source_group VARCHAR,
            source_file VARCHAR,
            source_sha256 VARCHAR
        );
        CREATE TABLE IF NOT EXISTS noaa_magnetic_history (
            snapshot_at_utc TIMESTAMPTZ,
            observed_at_utc TIMESTAMP,
            spacecraft VARCHAR,
            is_active_source BOOLEAN,
            bt DOUBLE,
            bx_gsm DOUBLE,
            by_gsm DOUBLE,
            bz_gsm DOUBLE,
            quality_code INTEGER,
            source_file VARCHAR,
            source_sha256 VARCHAR
        );
        CREATE TABLE IF NOT EXISTS noaa_plasma_history (
            snapshot_at_utc TIMESTAMPTZ,
            observed_at_utc TIMESTAMP,
            spacecraft VARCHAR,
            is_active_source BOOLEAN,
            proton_speed DOUBLE,
            proton_temperature DOUBLE,
            proton_density DOUBLE,
            quality_code INTEGER,
            source_file VARCHAR,
            source_sha256 VARCHAR
        );
        CREATE TABLE IF NOT EXISTS space_weather_index_history (
            snapshot_at_utc TIMESTAMPTZ,
            observation_date DATE,
            kp1 DOUBLE, kp2 DOUBLE, kp3 DOUBLE, kp4 DOUBLE,
            kp5 DOUBLE, kp6 DOUBLE, kp7 DOUBLE, kp8 DOUBLE,
            kp_sum DOUBLE,
            ap_avg DOUBLE,
            sunspot_number DOUBLE,
            f10_7_observed DOUBLE,
            f10_7_data_type VARCHAR,
            source_file VARCHAR,
            source_sha256 VARCHAR
        );
        CREATE TABLE IF NOT EXISTS satnogs_observation_history (
            snapshot_at_utc TIMESTAMPTZ,
            observation_id BIGINT,
            start_utc TIMESTAMP,
            end_utc TIMESTAMP,
            status VARCHAR,
            norad_catalog_id BIGINT,
            satellite_id VARCHAR,
            station_id BIGINT,
            station_lat DOUBLE,
            station_lng DOUBLE,
            observation_frequency_hz DOUBLE,
            transmitter_mode VARCHAR,
            source_file VARCHAR,
            source_sha256 VARCHAR
        );
        """
    )
    # Migration for databases created before the source_group column existed.
    columns = {
        row[0]
        for row in connection.execute(
            "DESCRIBE orbital_snapshot_history"
        ).fetchall()
    }
    if "source_group" not in columns:
        connection.execute(
            "ALTER TABLE orbital_snapshot_history ADD COLUMN source_group VARCHAR"
        )
        connection.execute(
            "UPDATE orbital_snapshot_history "
            "SET source_group = 'stations' WHERE source_group IS NULL"
        )
    # Migration for databases created before the full element set was stored.
    # The extra fields (RAAN, argument of perigee, mean anomaly, and the
    # mean-motion derivatives) are required for SGP4 propagation. Existing
    # rows lack them, so the orbital history is rebuilt from Bronze files,
    # which remain on disk as the source of truth. No network access occurs.
    element_columns = [
        "ra_of_asc_node_degrees",
        "arg_of_pericenter_degrees",
        "mean_anomaly_degrees",
        "mean_motion_dot",
        "mean_motion_ddot",
    ]
    missing_element_columns = [
        column for column in element_columns if column not in columns
    ]
    if missing_element_columns:
        for column in missing_element_columns:
            connection.execute(
                f"ALTER TABLE orbital_snapshot_history ADD COLUMN {column} DOUBLE"
            )
        connection.execute("DELETE FROM orbital_snapshot_history")
        connection.execute(
            "DELETE FROM bronze_file_registry WHERE source = 'celestrak'"
        )
    # Migration for the SatNOGS satellite id: the network API changed this
    # field from an integer to a string UUID, so the column widened to
    # VARCHAR. The table was never populated as BIGINT (the new loader
    # appeared with the new API), so an in-place type change is safe.
    satnogs_columns = {
        row[0]: row[1]
        for row in connection.execute(
            "DESCRIBE satnogs_observation_history"
        ).fetchall()
    }
    satnogs_id_type = satnogs_columns.get("satellite_id", "").upper()
    if satnogs_id_type.startswith("BIGINT"):
        connection.execute(
            "ALTER TABLE satnogs_observation_history "
            "ALTER COLUMN satellite_id TYPE VARCHAR"
        )


def insert_rows(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    columns: list[str],
    rows: list[tuple],
) -> None:
    """Insert rows in chunked multi-value statements.

    One INSERT per row is extremely slow in DuckDB because every statement
    is its own transaction. Batching hundreds of rows per statement is far
    faster and keeps the loader well inside the pipeline step timeout as
    the tracked population grows.
    """
    if not rows:
        return
    column_list = ", ".join(columns)
    placeholders = "(" + ", ".join("?" for _ in columns) + ")"
    for start in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[start : start + CHUNK_SIZE]
        values_sql = ", ".join([placeholders] * len(chunk))
        parameters = [value for row in chunk for value in row]
        connection.execute(
            f"INSERT INTO {table} ({column_list}) VALUES {values_sql}",
            parameters,
        )


def number(value: object) -> float | None:
    if value in (None, "", "null"):
        return None
    return float(value)


def integer(value: object) -> int | None:
    if value in (None, "", "null"):
        return None
    return int(value)


def boolean(value: object) -> bool | None:
    if value in (None, "", "null"):
        return None
    return bool(value)


def load_file(connection: duckdb.DuckDBPyConnection, path: Path) -> int:
    relative = str(path.relative_to(ROOT)).replace("\\", "/")
    digest = checksum(path)
    registered = connection.execute(
        "SELECT sha256 FROM bronze_file_registry WHERE file_path = ?",
        [relative],
    ).fetchone()
    if registered:
        if registered[0] != digest:
            raise ValueError(f"Immutable Bronze file was modified: {relative}")
        return 0

    # Atomicity: a file's rows and its registry entry are committed together
    # in one transaction. If the process dies mid-file (timeout, crash,
    # power loss), the uncommitted transaction is discarded when the database
    # reopens, so a partially loaded file can never leave orphan rows that a
    # later run would duplicate. Each chunked INSERT is otherwise its own
    # auto-committed transaction, which is exactly how a partial load once
    # slipped through.
    connection.execute("BEGIN TRANSACTION")
    try:
        row_count = _load_file_rows(connection, path, relative, digest)
        connection.execute("COMMIT")
    except BaseException:
        try:
            connection.execute("ROLLBACK")
        except duckdb.Error:
            pass  # transaction already aborted by the failing statement
        raise
    return row_count


def _load_file_rows(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    relative: str,
    digest: str,
) -> int:
    source = path.parent.name
    captured_at = snapshot_time(path)
    rows: list[tuple] = []
    table = ""

    if source == "celestrak":
        table = "orbital_snapshot_history"
        match = CELESTRAK_FILENAME.match(path.name)
        if not match:
            raise ValueError(f"Unrecognized CelesTrak Bronze filename: {relative}")
        group = match.group("group")
        # Cross-group deduplication: two groups downloaded in the same run
        # share one snapshot timestamp, and some objects can appear in more
        # than one group. Keep only the first copy of each object per
        # snapshot so the uniqueness quality check stays valid.
        existing_ids = {
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT norad_catalog_id "
                "FROM orbital_snapshot_history "
                "WHERE snapshot_at_utc = ? AND norad_catalog_id IS NOT NULL",
                [captured_at],
            ).fetchall()
        }
        with path.open(encoding="utf-8-sig", newline="") as file:
            for item in csv.DictReader(file):
                norad = integer(item.get("NORAD_CAT_ID"))
                if norad is None or norad in existing_ids:
                    continue
                existing_ids.add(norad)
                rows.append(
                    (
                        captured_at,
                        item.get("OBJECT_NAME"),
                        norad,
                        item.get("EPOCH"),
                        number(item.get("INCLINATION")),
                        number(item.get("ECCENTRICITY")),
                        number(item.get("MEAN_MOTION")),
                        number(item.get("BSTAR")),
                        number(item.get("RA_OF_ASC_NODE")),
                        number(item.get("ARG_OF_PERICENTER")),
                        number(item.get("MEAN_ANOMALY")),
                        number(item.get("MEAN_MOTION_DOT")),
                        number(item.get("MEAN_MOTION_DDOT")),
                        group,
                        relative,
                        digest,
                    )
                )
        insert_rows(connection, table, ORBITAL_COLUMNS, rows)
    elif source == "spaceweather":
        # CelesTrak space-weather indices. Rows are daily and mostly
        # static, but the most recent day is provisional and revised
        # between updates. Content-hash dedupe: a row is inserted only
        # when (date, values) has never been seen, so revisions append
        # new versions while unchanged history is skipped. Gold tables
        # select the latest snapshot per date.
        table = "space_weather_index_history"
        existing = {
            (row[0], row[1])
            for row in connection.execute(
                "SELECT CAST(observation_date AS VARCHAR), "
                "md5(concat_ws('|', kp1, kp2, kp3, kp4, kp5, kp6, kp7, kp8, "
                "kp_sum, ap_avg, sunspot_number, f10_7_observed, "
                "f10_7_data_type)) "
                "FROM space_weather_index_history"
            ).fetchall()
        }
        with path.open(encoding="utf-8-sig", newline="") as file:
            for item in csv.DictReader(file):
                if not (item.get("KP1") or "").strip():
                    continue  # forecast rows carry no indices
                observation_date = (item.get("DATE") or "").strip()
                if not observation_date:
                    continue
                values = (
                    number(item.get("KP1")), number(item.get("KP2")),
                    number(item.get("KP3")), number(item.get("KP4")),
                    number(item.get("KP5")), number(item.get("KP6")),
                    number(item.get("KP7")), number(item.get("KP8")),
                    number(item.get("KP_SUM")), number(item.get("AP_AVG")),
                    number(item.get("ISN")), number(item.get("F10.7_OBS")),
                    (item.get("F10.7_DATA_TYPE") or "").strip() or None,
                )
                row_digest = hashlib.md5(
                    "|".join("" if value is None else str(value) for value in values)
                    .encode("utf-8")
                ).hexdigest()
                if (observation_date, row_digest) in existing:
                    continue
                existing.add((observation_date, row_digest))
                rows.append(
                    (captured_at, observation_date, *values, relative, digest)
                )
        insert_rows(
            connection,
            table,
            [
                "snapshot_at_utc", "observation_date",
                "kp1", "kp2", "kp3", "kp4", "kp5", "kp6", "kp7", "kp8",
                "kp_sum", "ap_avg", "sunspot_number",
                "f10_7_observed", "f10_7_data_type",
                "source_file", "source_sha256",
            ],
            rows,
        )
    elif source == "satnogs":
        # SatNOGS observation metadata. Observations are keyed by id and
        # change status over their life (future -> good/bad/...), so the
        # same content-hash dedupe applies: new (id, content) combos
        # append; unchanged observations are skipped.
        table = "satnogs_observation_history"
        records = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(records, list):
            raise ValueError(f"Unsupported SatNOGS Bronze payload: {relative}")
        existing = {
            (row[0], row[1])
            for row in connection.execute(
                "SELECT observation_id, "
                "md5(concat_ws('|', start_utc, end_utc, status, "
                "norad_catalog_id, satellite_id, station_id, station_lat, "
                "station_lng, observation_frequency_hz, transmitter_mode)) "
                "FROM satnogs_observation_history"
            ).fetchall()
        }
        for item in records:
            observation_id = integer(item.get("id"))
            if observation_id is None:
                continue
            values = (
                item.get("start"),
                item.get("end"),
                item.get("status"),
                integer(item.get("norad_cat_id")),
                item.get("sat_id"),
                integer(item.get("ground_station")),
                number(item.get("station_lat")),
                number(item.get("station_lng")),
                number(item.get("observation_frequency")),
                item.get("transmitter_mode"),
            )
            row_digest = hashlib.md5(
                "|".join("" if value is None else str(value) for value in values)
                .encode("utf-8")
            ).hexdigest()
            if (observation_id, row_digest) in existing:
                continue
            existing.add((observation_id, row_digest))
            rows.append(
                (
                    captured_at,
                    observation_id,
                    *values,
                    relative,
                    digest,
                )
            )
        insert_rows(
            connection,
            table,
            [
                "snapshot_at_utc", "observation_id", "start_utc", "end_utc",
                "status", "norad_catalog_id", "satellite_id", "station_id",
                "station_lat", "station_lng", "observation_frequency_hz",
                "transmitter_mode", "source_file", "source_sha256",
            ],
            rows,
        )
    else:
        records = json.loads(path.read_text(encoding="utf-8"))
        if source != "noaa" or not isinstance(records, list):
            raise ValueError(f"Unsupported Bronze source: {relative}")
        if path.name.startswith("magnetic_field_"):
            table = "noaa_magnetic_history"
            for item in records:
                rows.append((
                    captured_at, item.get("time_tag"), item.get("source"),
                    boolean(item.get("active")), number(item.get("bt")),
                    number(item.get("bx_gsm")), number(item.get("by_gsm")),
                    number(item.get("bz_gsm")), integer(item.get("overall_quality")),
                    relative, digest,
                ))
            insert_rows(connection, table, MAGNETIC_COLUMNS, rows)
        elif path.name.startswith("plasma_"):
            table = "noaa_plasma_history"
            for item in records:
                rows.append((
                    captured_at, item.get("time_tag"), item.get("source"),
                    boolean(item.get("active")), number(item.get("proton_speed")),
                    number(item.get("proton_temperature")), number(item.get("proton_density")),
                    integer(item.get("overall_quality")), relative, digest,
                ))
            insert_rows(connection, table, PLASMA_COLUMNS, rows)
        else:
            raise ValueError(f"Unknown NOAA Bronze filename: {relative}")

    connection.execute(
        "INSERT INTO bronze_file_registry VALUES (?, ?, ?, ?, ?, ?)",
        [relative, source, captured_at, digest, datetime.now(UTC), len(rows)],
    )
    print(f"Loaded {len(rows):,} rows from {relative} into {table}")
    return len(rows)


LOADER_PATTERNS = [
    "celestrak/*.csv",
    "noaa/magnetic_field_*.json",
    "noaa/plasma_*.json",
    "spaceweather/sw_*.csv",
    "satnogs/observations_*.json",
]


def loadable_files() -> list[Path]:
    """Every Bronze file this loader knows how to read, in load order.

    The curated stations group is loaded before larger groups so that an
    object appearing in several groups keeps its stations copy.
    """
    celestrak_files = sorted(BRONZE.glob("celestrak/*.csv"))
    celestrak_files.sort(
        key=lambda path: (path.name.split("_", 1)[0] != "stations", path.name)
    )
    files = list(celestrak_files)
    for pattern in LOADER_PATTERNS[1:]:
        files += sorted(BRONZE.glob(pattern))
    return files


def unloadable_files() -> list[Path]:
    """Bronze files that match no loader pattern.

    Such a file is silently ignored forever: unregistered, unloaded, and
    invisible to the quality gate. One 6.9 MB `celestrak/active_*.json`
    snapshot sat in Bronze for exactly that reason. The quality gate now
    reports these so the invariant "every Bronze file is accounted for"
    can actually be checked.
    """
    if not BRONZE.exists():
        return []
    known = {path.resolve() for path in loadable_files()}
    return sorted(
        path
        for path in BRONZE.rglob("*")
        if path.is_file() and path.resolve() not in known
    )


def main() -> int:
    files = loadable_files()
    if not files:
        print("No Bronze snapshots found. Run Lessons 1 and 2 first.", file=sys.stderr)
        return 1

    connection = duckdb.connect(str(DATABASE))
    try:
        setup(connection)
        loaded_rows = sum(load_file(connection, path) for path in files)
    except (OSError, ValueError, json.JSONDecodeError, duckdb.Error) as error:
        print(f"History loader error: {error}", file=sys.stderr)
        return 1
    finally:
        connection.close()

    print(f"Snapshots checked: {len(files)}")
    print(f"New rows loaded: {loaded_rows:,}")
    ignored = unloadable_files()
    if ignored:
        print(
            f"Warning: {len(ignored)} Bronze file(s) match no loader pattern "
            "and were NOT loaded:",
            file=sys.stderr,
        )
        for path in ignored[:5]:
            print(f"  {path.relative_to(ROOT)}", file=sys.stderr)
        if len(ignored) > 5:
            print(f"  ... and {len(ignored) - 5} more", file=sys.stderr)
    print("Run this command again to see idempotency: it should load 0 new rows.")
    return 0



if __name__ == "__main__":
    raise SystemExit(main())
