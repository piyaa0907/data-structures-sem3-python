"""Ingestion orchestration: IMD files and AWS message streams -> SQLite.

Flow for every input (IMD file or AWS stream):

    raw row --> validate & QC (step & flatline) --> clean --> dedupe (in-memory set)
            --> batch insert (SQLite UNIQUE check) --> notify Person 1 listeners --> commit

Rejected rows are stored with field-level metadata in ``rejections``.
Historical files compute a SHA-256 checksum to skip unaltered duplicate files.
If a run fails part-way, changes are rolled back to keep the database consistent.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional

from . import config
from .cleaning import clean_row
from .database import INSERT_OBSERVATION_SQL, INSERT_REJECTION_SQL
from .loaders import canonical_row, compute_file_sha256, iter_imd_rows
from .models import (
    MEASUREMENT_DAILY_MAX,
    MEASUREMENT_INSTANT,
    SOURCE_AWS,
    SOURCE_IMD,
    Observation,
    RawRow,
)
from .repository import notify_observation_listeners
from .validation import StationQCValidator, validate_row_detailed

log = logging.getLogger(__name__)


@dataclass
class IngestionResult:
    """Outcome of one ingestion run. Mirrors the ``ingestion_log`` row."""

    log_id: int
    source: str
    input_name: str
    file_hash: str = ""
    records_read: int = 0
    inserted: int = 0
    duplicates: int = 0
    rejected: int = 0
    status: str = "RUNNING"
    message: str = ""


def now_ist() -> datetime:
    """Current time as a naive IST datetime (matches stored timestamps)."""
    return datetime.now(config.IST).replace(tzinfo=None)


def _open_log(
    conn: sqlite3.Connection,
    source: str,
    input_name: str,
    file_hash: str,
    now: datetime,
) -> int:
    cursor = conn.execute(
        """INSERT INTO ingestion_log
               (run_at, source, input_name, file_hash, records_read, inserted, duplicates, rejected, status, message)
           VALUES (?, ?, ?, ?, 0, 0, 0, 0, 'RUNNING', '')""",
        (now.isoformat(timespec="seconds"), source, input_name, file_hash),
    )
    conn.commit()  # Log entry survives rollback of data
    return cursor.lastrowid


def _close_log(conn: sqlite3.Connection, result: IngestionResult) -> None:
    conn.execute(
        """UPDATE ingestion_log
              SET status = ?, records_read = ?, inserted = ?, duplicates = ?,
                  rejected = ?, message = ?
            WHERE id = ?""",
        (
            result.status,
            result.records_read,
            result.inserted,
            result.duplicates,
            result.rejected,
            result.message,
            result.log_id,
        ),
    )


def _insert_batch(conn: sqlite3.Connection, batch: list[Observation], ingested_at: str) -> int:
    """Insert a batch and return how many rows were actually stored."""
    params = [
        (
            o.source, o.station_id, o.region, o.latitude, o.longitude, o.observed_at,
            o.season, o.measurement, o.temperature_c, o.humidity, ingested_at,
        )
        for o in batch
    ]
    before = conn.total_changes
    conn.executemany(INSERT_OBSERVATION_SQL, params)
    return conn.total_changes - before


def _flush(conn: sqlite3.Connection, pending: list[Observation], result: IngestionResult, ingested_at: str) -> None:
    if not pending:
        return
    batch = list(pending)
    stored = _insert_batch(conn, batch, ingested_at)
    result.inserted += stored
    result.duplicates += len(batch) - stored
    pending.clear()

    # Incrementally notify Person 1 sinks of newly inserted clean observations
    if stored == len(batch):
        for obs in batch:
            notify_observation_listeners(obs)
    elif stored > 0:
        # Some were duplicates from earlier runs; notify only those matching current ingested_at
        for obs in batch:
            row = conn.execute(
                """SELECT 1 FROM observations
                    WHERE source = ? AND station_id = ? AND latitude = ?
                      AND longitude = ? AND observed_at = ? AND ingested_at = ?""",
                (obs.source, obs.station_id, obs.latitude, obs.longitude, obs.observed_at, ingested_at),
            ).fetchone()
            if row:
                notify_observation_listeners(obs)


def _ingest_rows(
    conn: sqlite3.Connection,
    rows: Iterable[RawRow],
    *,
    source: str,
    measurement: str,
    input_name: str,
    file_hash: str,
    now: datetime,
) -> IngestionResult:
    log_id = _open_log(conn, source, input_name, file_hash, now)
    result = IngestionResult(log_id=log_id, source=source, input_name=input_name, file_hash=file_hash)
    ingested_at = now.isoformat(timespec="seconds")

    seen: set[tuple] = set()           # Deduplicate identical keys within this run
    pending: list[Observation] = []    # Cleaned rows waiting for batch insert
    rejects: list[tuple[int, str, int, str, str, str, str, str]] = []
    qc = StationQCValidator()

    try:
        for line_no, row, raw in rows:
            result.records_read += 1

            reason, detail, field_name, rejected_val = validate_row_detailed(row, now, qc)
            if reason is not None:
                result.rejected += 1
                ts_str = str(row.get("timestamp") or "") if isinstance(row, dict) else ""
                val_str = str(rejected_val if rejected_val is not None else "")
                rejects.append((log_id, input_name, line_no, field_name, val_str, reason, ts_str, raw))
                log.debug("%s line %s rejected (%s): %s", input_name, line_no, reason, detail)
                continue

            observation = clean_row(row, source, measurement)
            key = observation.dedupe_key()
            if key in seen:
                result.duplicates += 1
                continue
            seen.add(key)
            pending.append(observation)

            if len(pending) >= config.BATCH_SIZE:
                _flush(conn, pending, result, ingested_at)

        _flush(conn, pending, result, ingested_at)

        if rejects:
            conn.executemany(INSERT_REJECTION_SQL, rejects)

        result.status = "SUCCESS"
        result.message = f"{result.rejected} rejected, {result.duplicates} duplicates skipped"
        _close_log(conn, result)
        conn.commit()

    except Exception as exc:
        conn.rollback()
        result.status = "FAILED"
        result.inserted = 0
        result.message = f"rolled back: {exc}"
        log.error("%s failed: %s", input_name, exc)
        _close_log(conn, result)
        conn.commit()
        return result

    log.info("%s: %s inserted, %s", input_name, result.inserted, result.message)
    return result


def ingest_imd_file(
    conn: sqlite3.Connection,
    path,
    now: Optional[datetime] = None,
    force: bool = False,
) -> IngestionResult:
    """Load an IMD historical file (.csv, .json, or .grd) into observations.

    Automatically checks the file's SHA-256 hash. If an unaltered file was already
    successfully ingested, re-ingestion is skipped unless ``force=True``.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"input file not found: {path}")

    moment = now or now_ist()
    file_hash = compute_file_sha256(path)

    if not force:
        # Check if an identical file was already successfully ingested
        prev = conn.execute(
            """SELECT id, run_at, records_read, inserted, duplicates, rejected
                 FROM ingestion_log
                WHERE file_hash = ? AND status = 'SUCCESS'
                ORDER BY id DESC LIMIT 1""",
            (file_hash,),
        ).fetchone()

        if prev:
            msg = f"Skipped: file '{path.name}' already ingested in run #{prev['id']} (SHA-256: {file_hash[:12]}...)"
            log.info(msg)
            # Log the skip action
            cursor = conn.execute(
                """INSERT INTO ingestion_log
                       (run_at, source, input_name, file_hash, records_read, inserted, duplicates, rejected, status, message)
                   VALUES (?, ?, ?, ?, 0, 0, 0, 0, 'SUCCESS', ?)""",
                (moment.isoformat(timespec="seconds"), SOURCE_IMD, path.name, file_hash, msg),
            )
            conn.commit()
            return IngestionResult(
                log_id=cursor.lastrowid,
                source=SOURCE_IMD,
                input_name=path.name,
                file_hash=file_hash,
                records_read=0,
                inserted=0,
                duplicates=0,
                rejected=0,
                status="SUCCESS",
                message=msg,
            )

    return _ingest_rows(
        conn,
        iter_imd_rows(path),
        source=SOURCE_IMD,
        measurement=MEASUREMENT_DAILY_MAX,
        input_name=path.name,
        file_hash=file_hash,
        now=moment,
    )


def ingest_aws_readings(
    conn: sqlite3.Connection,
    readings: Iterable[dict],
    *,
    input_name: str = "aws-stream",
    now: Optional[datetime] = None,
) -> IngestionResult:
    """Load a stream of AWS reading dictionaries with temporal sorting and QC.

    Readings are sorted by (station_id, timestamp) to properly resolve network
    jitter or out-of-order delivery before stateful meteorological QC validation.
    """
    moment = now or now_ist()

    # Sort incoming readings to restore chronological sequence per station
    reading_list = list(readings)
    reading_list.sort(key=lambda r: (str(r.get("station_id") or ""), str(r.get("timestamp") or "")))

    rows = (
        (seq, canonical_row(reading), json.dumps(reading, default=str))
        for seq, reading in enumerate(reading_list, start=1)
    )
    return _ingest_rows(
        conn,
        rows,
        source=SOURCE_AWS,
        measurement=MEASUREMENT_INSTANT,
        input_name=input_name,
        file_hash="",
        now=moment,
    )
