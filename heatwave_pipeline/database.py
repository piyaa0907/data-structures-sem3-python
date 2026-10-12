"""SQLite persistence: schema, connections, optimizations, and maintenance."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Optional, Union

from . import config

PathLike = Union[str, Path]

SCHEMA = """
CREATE TABLE IF NOT EXISTS observations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source          TEXT    NOT NULL CHECK (source IN ('IMD', 'AWS')),
    station_id      TEXT    NOT NULL DEFAULT '',
    region          TEXT    NOT NULL DEFAULT '',
    latitude        REAL    NOT NULL CHECK (latitude BETWEEN -90 AND 90),
    longitude       REAL    NOT NULL CHECK (longitude BETWEEN -180 AND 180),
    observed_at     TEXT    NOT NULL,
    season          TEXT    NOT NULL,
    measurement     TEXT    NOT NULL,
    temperature_c   REAL    NOT NULL,
    humidity        REAL CHECK (humidity IS NULL OR humidity BETWEEN 0 AND 100),
    ingested_at     TEXT    NOT NULL,
    UNIQUE (source, station_id, latitude, longitude, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_obs_observed_at   ON observations (observed_at);
CREATE INDEX IF NOT EXISTS idx_obs_region_time   ON observations (region, observed_at);
CREATE INDEX IF NOT EXISTS idx_obs_season        ON observations (season, region);
CREATE INDEX IF NOT EXISTS idx_obs_station_time  ON observations (station_id, observed_at);

CREATE TABLE IF NOT EXISTS ingestion_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at       TEXT    NOT NULL,
    source       TEXT    NOT NULL,
    input_name   TEXT    NOT NULL,
    file_hash    TEXT    NOT NULL DEFAULT '',
    records_read INTEGER NOT NULL DEFAULT 0,
    inserted     INTEGER NOT NULL DEFAULT 0,
    duplicates   INTEGER NOT NULL DEFAULT 0,
    rejected     INTEGER NOT NULL DEFAULT 0,
    status       TEXT    NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED')),
    message      TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_ingest_hash ON ingestion_log (file_hash);

CREATE TABLE IF NOT EXISTS rejections (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    log_id             INTEGER NOT NULL REFERENCES ingestion_log (id) ON DELETE CASCADE,
    file_or_stream_id  TEXT    NOT NULL DEFAULT '',
    row_number         INTEGER,
    field_name         TEXT    NOT NULL DEFAULT '',
    rejected_value     TEXT    NOT NULL DEFAULT '',
    reason_code        TEXT    NOT NULL,
    timestamp          TEXT    NOT NULL DEFAULT '',
    raw_data           TEXT    NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_rej_reason ON rejections (reason_code);
CREATE INDEX IF NOT EXISTS idx_rej_log    ON rejections (log_id);
CREATE INDEX IF NOT EXISTS idx_rej_field  ON rejections (field_name);
"""

# Backward compatibility view & trigger for legacy queries expecting rejected_records
VIEW_REJECTED_RECORDS = """
CREATE VIEW IF NOT EXISTS rejected_records AS
SELECT id, log_id, row_number AS line_no, reason_code AS reason, raw_data
FROM rejections;
"""

TRIGGER_REJECTED_RECORDS_INSERT = """
CREATE TRIGGER IF NOT EXISTS trg_rejected_records_insert
INSTEAD OF INSERT ON rejected_records
BEGIN
    INSERT INTO rejections (log_id, file_or_stream_id, row_number, field_name, rejected_value, reason_code, timestamp, raw_data)
    VALUES (NEW.log_id, '', NEW.line_no, '', '', NEW.reason, '', NEW.raw_data);
END;
"""

# ON CONFLICT ... DO NOTHING silences duplicate unique keys without swallowing
# check constraint violations or type errors.
INSERT_OBSERVATION_SQL = """
INSERT INTO observations
    (source, station_id, region, latitude, longitude, observed_at, season,
     measurement, temperature_c, humidity, ingested_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (source, station_id, latitude, longitude, observed_at) DO NOTHING
"""

INSERT_REJECTION_SQL = """
INSERT INTO rejections
    (log_id, file_or_stream_id, row_number, field_name, rejected_value, reason_code, timestamp, raw_data)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""


def connect(db_path: Optional[PathLike] = None) -> sqlite3.Connection:
    """Open an optimized SQLite connection with WAL mode and memory caching.

    Pragmas configured:
    - journal_mode = WAL: enables concurrent reads while Person 2 writes.
    - synchronous = NORMAL: safe for WAL while eliminating redundant disk flushes.
    - cache_size = -64000: allocates 64 MB of page cache in memory.
    - foreign_keys = ON: ensures referential integrity for cascaded deletes.
    """
    path = Path(db_path) if db_path else config.DEFAULT_DB_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, timeout=30.0)
    conn.row_factory = sqlite3.Row

    # Concurrency and performance pragmas
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA cache_size = -64000")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create tables, indexes, views, and migrate older schemas safely."""
    # 1. Check if ingestion_log already exists and lacks file_hash column
    has_ingest = conn.execute(
        "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='ingestion_log'"
    ).fetchone()[0] > 0
    if has_ingest:
        cols = [col["name"] for col in conn.execute("PRAGMA table_info(ingestion_log)").fetchall()]
        if "file_hash" not in cols:
            conn.execute("ALTER TABLE ingestion_log ADD COLUMN file_hash TEXT NOT NULL DEFAULT ''")

    # 2. Handle migration from old 'rejected_records' table to 'rejections' table + VIEW
    table_type = conn.execute(
        "SELECT type FROM sqlite_master WHERE name = 'rejected_records'"
    ).fetchone()

    if table_type is not None and table_type["type"] == "table":
        # Create rejections table first if needed
        conn.execute("""
        CREATE TABLE IF NOT EXISTS rejections (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            log_id             INTEGER NOT NULL REFERENCES ingestion_log (id) ON DELETE CASCADE,
            file_or_stream_id  TEXT    NOT NULL DEFAULT '',
            row_number         INTEGER,
            field_name         TEXT    NOT NULL DEFAULT '',
            rejected_value     TEXT    NOT NULL DEFAULT '',
            reason_code        TEXT    NOT NULL,
            timestamp          TEXT    NOT NULL DEFAULT '',
            raw_data           TEXT    NOT NULL DEFAULT ''
        );
        """)
        # Migrate existing rows into rejections table
        conn.execute(
            """INSERT INTO rejections (log_id, row_number, reason_code, raw_data)
               SELECT log_id, line_no, reason, raw_data FROM rejected_records"""
        )
        conn.execute("DROP TABLE rejected_records")

    # 3. Execute base schema
    conn.executescript(SCHEMA)
    conn.executescript(VIEW_REJECTED_RECORDS)
    conn.executescript(TRIGGER_REJECTED_RECORDS_INSERT)
    conn.commit()


@contextmanager
def open_database(db_path: Optional[PathLike] = None) -> Iterator[sqlite3.Connection]:
    """Context manager: connect, create schema, yield, commit, close."""
    conn = connect(db_path)
    try:
        init_db(conn)
        yield conn
        conn.commit()
    finally:
        conn.close()


def get_db_stats(conn: sqlite3.Connection, db_path: Optional[PathLike] = None) -> dict[str, Any]:
    """Gather diagnostic metrics, table row counts, file sizes, and rejection statistics."""
    path = Path(db_path) if db_path else config.DEFAULT_DB_PATH
    file_size_bytes = path.stat().st_size if path.exists() else 0
    wal_path = Path(f"{path}-wal")
    wal_size_bytes = wal_path.stat().st_size if wal_path.exists() else 0

    def val(query: str, default: Any = 0) -> Any:
        row = conn.execute(query).fetchone()
        return row[0] if row and row[0] is not None else default

    obs_total = val("SELECT COUNT(*) FROM observations")
    by_source = {
        r["source"]: r["cnt"]
        for r in conn.execute("SELECT source, COUNT(*) AS cnt FROM observations GROUP BY source")
    }

    date_span = conn.execute(
        "SELECT MIN(observed_at) AS first_obs, MAX(observed_at) AS last_obs FROM observations"
    ).fetchone()
    unique_days = val("SELECT COUNT(DISTINCT SUBSTR(observed_at, 1, 10)) FROM observations")
    unique_stations = val("SELECT COUNT(DISTINCT station_id) FROM observations WHERE station_id <> ''")
    unique_regions = val("SELECT COUNT(DISTINCT region) FROM observations WHERE region <> ''")

    runs = conn.execute(
        """SELECT COUNT(*) AS total_runs,
                  COALESCE(SUM(records_read), 0) AS total_read,
                  COALESCE(SUM(inserted), 0)     AS total_inserted,
                  COALESCE(SUM(duplicates), 0)   AS total_duplicates,
                  COALESCE(SUM(rejected), 0)     AS total_rejected
             FROM ingestion_log"""
    ).fetchone()

    rejections_total = val("SELECT COUNT(*) FROM rejections")
    rejections_by_reason = {
        r["reason_code"]: r["cnt"]
        for r in conn.execute(
            "SELECT reason_code, COUNT(*) AS cnt FROM rejections GROUP BY reason_code ORDER BY cnt DESC"
        )
    }
    rejections_by_field = {
        r["field_name"]: r["cnt"]
        for r in conn.execute(
            "SELECT field_name, COUNT(*) AS cnt FROM rejections WHERE field_name <> '' GROUP BY field_name ORDER BY cnt DESC"
        )
    }

    page_size = val("PRAGMA page_size")
    page_count = val("PRAGMA page_count")
    journal_mode = val("PRAGMA journal_mode", default="unknown")
    synchronous = val("PRAGMA synchronous", default="unknown")
    cache_size = val("PRAGMA cache_size", default=0)

    return {
        "db_path": str(path),
        "file_size_bytes": file_size_bytes,
        "wal_size_bytes": wal_size_bytes,
        "page_size": page_size,
        "page_count": page_count,
        "journal_mode": str(journal_mode).upper(),
        "synchronous": synchronous,
        "cache_size_pages": cache_size,
        "observations_total": obs_total,
        "observations_by_source": by_source,
        "date_span": {
            "first": date_span["first_obs"] if date_span else None,
            "last": date_span["last_obs"] if date_span else None,
        },
        "unique_days": unique_days,
        "unique_stations": unique_stations,
        "unique_regions": unique_regions,
        "ingestion_runs": runs["total_runs"],
        "records_read": runs["total_read"],
        "total_inserted": runs["total_inserted"],
        "total_duplicates": runs["total_duplicates"],
        "total_rejected": runs["total_rejected"],
        "rejections_count": rejections_total,
        "rejections_by_reason": rejections_by_reason,
        "rejections_by_field": rejections_by_field,
    }


def vacuum_db(conn: sqlite3.Connection) -> None:
    """Defragment the SQLite database file and release unused pages."""
    conn.execute("VACUUM")
