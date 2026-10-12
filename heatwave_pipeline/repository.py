"""Read-side API and real-time event sink handoff for Person 1.

Provides:
- Streaming queries for cleaned observations with compound indexing.
- Typed sinks and validation contracts for Person 1's Spatial QuadTree and Temporal B-Tree.
- Event listener hooks (``register_observation_listener``) allowing Person 1's
  in-memory indexes to receive newly ingested AWS observations incrementally
  without performing a full database reload.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import date, datetime
from typing import Any, Callable, Iterator, Optional, Protocol, Union, runtime_checkable

from .cleaning import clean_region
from .models import Observation
from .validation import parse_timestamp

log = logging.getLogger(__name__)

Moment = Union[str, datetime]

_COLUMNS = (
    "source", "station_id", "region", "latitude", "longitude", "observed_at",
    "season", "measurement", "temperature_c", "humidity",
)


@runtime_checkable
class RecordSink(Protocol):
    """Contract for Person 1's index sinks: must implement ``insert(record)``."""

    def insert(self, record: Observation) -> None:
        """Insert one cleaned meteorological observation into the index."""
        ...


class FunctionalSink:
    """Adapts a simple callable ``fn(Observation)`` to the ``RecordSink`` protocol."""

    def __init__(self, callback: Callable[[Observation], None]) -> None:
        if not callable(callback):
            raise TypeError("FunctionalSink requires a callable")
        self._callback = callback

    def insert(self, record: Observation) -> None:
        self._callback(record)


class SpatialQuadTreeAdapter:
    """Adapter allowing Person 1's QuadTree to directly ingest Observation records."""

    def __init__(self, quadtree: Any, region_lookup: Optional[dict[str, int]] = None) -> None:
        if not hasattr(quadtree, "insert"):
            raise TypeError("quadtree must implement an insert method")
        self.quadtree = quadtree
        self.region_lookup = region_lookup or {}
        self._next_id = len(self.region_lookup) + 1

    def insert(self, record: Observation) -> None:
        key = record.station_id or f"{record.latitude:.2f},{record.longitude:.2f}"
        reg_id = self.region_lookup.setdefault(key, self._next_id)
        if reg_id == self._next_id:
            self._next_id += 1

        try:
            # Look up p1_core Region if available
            from p1_core.spatial import Region
            region_obj = Region(
                id=reg_id,
                name=record.station_id or record.region,
                lat=record.latitude,
                lng=record.longitude,
                temp_anomaly=0.0,
            )
            self.quadtree.insert(region_obj)
        except ImportError:
            # Fallback if p1_core is not present
            self.quadtree.insert(record)


class TemporalBTreeAdapter:
    """Adapter allowing Person 1's TemporalIndex to directly ingest Observation records."""

    def __init__(self, temporal_index: Any, region_lookup: Optional[dict[str, int]] = None) -> None:
        if not hasattr(temporal_index, "insert"):
            raise TypeError("temporal_index must implement an insert method")
        self.temporal_index = temporal_index
        self.region_lookup = region_lookup or {}
        self._next_id = len(self.region_lookup) + 1

    def insert(self, record: Observation) -> None:
        key = record.station_id or f"{record.latitude:.2f},{record.longitude:.2f}"
        reg_id = self.region_lookup.setdefault(key, self._next_id)
        if reg_id == self._next_id:
            self._next_id += 1

        # Convert observed_at date to day number
        d = date.fromisoformat(record.observed_at[:10])
        from p1_core.temporal import days_from_civil
        day_num = days_from_civil(d.year, d.month, d.day)
        self.temporal_index.insert(reg_id, day_num, record.temperature_c)


# -----------------------------------------------------------------------------
# Incremental Real-time Observation Event Listener Hook
# -----------------------------------------------------------------------------
ObservationListener = Callable[[Observation], None]
_OBSERVATION_LISTENERS: list[ObservationListener] = []
_LISTENERS_LOCK = threading.Lock()


def register_observation_listener(listener: ObservationListener) -> None:
    """Register a callback to receive cleaned Observation records incrementally upon ingestion.

    Enables Person 1's spatial and temporal indexes to be updated on live AWS ticks
    without performing an expensive full-table query or database reload.
    """
    if not callable(listener):
        raise TypeError(f"listener must be callable, got {type(listener).__name__}")
    with _LISTENERS_LOCK:
        if listener not in _OBSERVATION_LISTENERS:
            _OBSERVATION_LISTENERS.append(listener)


def unregister_observation_listener(listener: ObservationListener) -> None:
    """Remove a previously registered observation callback listener."""
    with _LISTENERS_LOCK:
        if listener in _OBSERVATION_LISTENERS:
            _OBSERVATION_LISTENERS.remove(listener)


def clear_observation_listeners() -> None:
    """Clear all registered observation listeners."""
    with _LISTENERS_LOCK:
        _OBSERVATION_LISTENERS.clear()


def notify_observation_listeners(observation: Observation) -> None:
    """Dispatch a newly inserted cleaned observation to all registered Person 1 sinks."""
    with _LISTENERS_LOCK:
        listeners = list(_OBSERVATION_LISTENERS)
    for listener in listeners:
        try:
            listener(observation)
        except Exception as exc:
            log.warning("Observation listener %r failed: %s", listener, exc)


# -----------------------------------------------------------------------------
# Database Queries & Single-pass Index Initialization
# -----------------------------------------------------------------------------
def _to_iso(value: Optional[Moment]) -> Optional[str]:
    if value is None or value == "":
        return None
    return parse_timestamp(value).isoformat(timespec="seconds")


def _build_filters(
    *,
    source: Optional[str],
    region: Optional[str],
    season: Optional[str],
    station_id: Optional[str],
    start: Optional[Moment],
    end: Optional[Moment],
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if source:
        clauses.append("source = ?")
        params.append(source.strip().upper())
    if region:
        clauses.append("region = ?")
        params.append(clean_region(region))
    if season:
        clauses.append("season = ?")
        params.append(season.strip().upper())
    if station_id:
        clauses.append("station_id = ?")
        params.append(station_id.strip())

    start_iso = _to_iso(start)
    if start_iso:
        clauses.append("observed_at >= ?")
        params.append(start_iso)
    end_iso = _to_iso(end)
    if end_iso:
        clauses.append("observed_at <= ?")
        params.append(end_iso)

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


def iter_observations(
    conn: sqlite3.Connection,
    *,
    source: Optional[str] = None,
    region: Optional[str] = None,
    season: Optional[str] = None,
    station_id: Optional[str] = None,
    start: Optional[Moment] = None,
    end: Optional[Moment] = None,
    limit: Optional[int] = None,
) -> Iterator[Observation]:
    """Stream cleaned observations in chronological order, optionally filtered."""
    where, params = _build_filters(
        source=source, region=region, season=season, station_id=station_id, start=start, end=end
    )
    sql = f"SELECT {', '.join(_COLUMNS)} FROM observations{where} ORDER BY observed_at, id"
    if limit:
        sql += " LIMIT ?"
        params.append(int(limit))

    for row in conn.execute(sql, params):
        yield Observation(**dict(row))


def count_observations(conn: sqlite3.Connection, **filters: Any) -> int:
    """Count cleaned observations matching given filters."""
    where, params = _build_filters(
        source=filters.get("source"),
        region=filters.get("region"),
        season=filters.get("season"),
        station_id=filters.get("station_id"),
        start=filters.get("start"),
        end=filters.get("end"),
    )
    return conn.execute(f"SELECT COUNT(*) FROM observations{where}", params).fetchone()[0]


def list_regions(conn: sqlite3.Connection) -> list[str]:
    """Return distinct non-empty region labels present in the database."""
    rows = conn.execute(
        "SELECT DISTINCT region FROM observations WHERE region <> '' ORDER BY region"
    )
    return [row[0] for row in rows]


def load_into_index(sink: RecordSink, conn: sqlite3.Connection, **filters: Any) -> int:
    """Feed every matching record to one index sink. Returns the number of records fed."""
    if not hasattr(sink, "insert") or not callable(getattr(sink, "insert")):
        raise TypeError(f"sink must provide a callable 'insert' method, got {type(sink).__name__}")
    count = 0
    for record in iter_observations(conn, **filters):
        sink.insert(record)
        count += 1
    return count


def initialise_indexes(
    conn: sqlite3.Connection,
    *,
    spatial: Optional[RecordSink] = None,
    temporal: Optional[RecordSink] = None,
    source: Optional[str] = None,
    region: Optional[str] = None,
    season: Optional[str] = None,
    station_id: Optional[str] = None,
    start: Optional[Moment] = None,
    end: Optional[Moment] = None,
) -> dict[str, int]:
    """Populate Person 1's spatial and temporal indexes in a single pass over the data.

    Validates that each sink implements a callable ``insert`` method.
    Returns counts of records dispatched to each index: ``{"spatial": N, "temporal": N}``.
    """
    sinks: dict[str, RecordSink] = {}

    if spatial is not None:
        if not hasattr(spatial, "insert") or not callable(getattr(spatial, "insert")):
            raise TypeError(f"spatial sink must provide a callable 'insert' method, got {type(spatial).__name__}")
        sinks["spatial"] = spatial

    if temporal is not None:
        if not hasattr(temporal, "insert") or not callable(getattr(temporal, "insert")):
            raise TypeError(f"temporal sink must provide a callable 'insert' method, got {type(temporal).__name__}")
        sinks["temporal"] = temporal

    counts = {name: 0 for name in sinks}

    for record in iter_observations(
        conn,
        source=source,
        region=region,
        season=season,
        station_id=station_id,
        start=start,
        end=end,
    ):
        for name, sink in sinks.items():
            sink.insert(record)
            counts[name] += 1

    return counts
