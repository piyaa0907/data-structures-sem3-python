"""In-memory engine: Person 1's structures, loaded from the SQLite database.

The database is the source of truth. The engine replays it into:

* a quad-tree of locations (``Region`` objects) for radius searches,
* a temporal index (one B-tree per location and season) for history and statistics,
* an alert priority queue, filled by Person 1's ``ingest_reading`` in date order.

Conventions:

* A location is one physical point: an AWS station or an IMD grid point.
* Temperatures are daily maxima. AWS readings are reduced to one value per day,
  which matches IMD's DAILY_MAX measurement.
* "Today" for forecasting is the latest date in the database, not the wall-clock
  date, so forecasts stay meaningful for historical data.

The engine is read-only after it is built. ``EngineCache`` rebuilds it when an
ingestion run starts or finishes.
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

from heatwave_pipeline import config
from heatwave_pipeline.database import connect
from heatwave_pipeline.repository import iter_observations
from p1_core.alerts import AlertQueue
from p1_core.analytics import forecast_region, ingest_reading
from p1_core.spatial import QuadTree, Region, haversine_km
from p1_core.temporal import TemporalIndex, civil_from_days, days_from_civil

log = logging.getLogger("heatwave_api.engine")


@dataclass(frozen=True)
class Location:
    id: int
    source: str
    station_id: str
    region: str
    latitude: float
    longitude: float
    name: str
    days_of_history: int


@dataclass
class Engine:
    locations: dict[int, Location]
    regions: dict[int, Region]
    spatial: QuadTree
    temporal: TemporalIndex
    alerts: AlertQueue
    as_of_day: Optional[int]        # day number of the latest reading; None if no data
    alerts_raised: int
    points_replayed: int


def day_of(observed_at: str) -> int:
    """'2025-05-20T00:00:00' -> day number used by Person 1's temporal index."""
    d = date.fromisoformat(observed_at[:10])
    return days_from_civil(d.year, d.month, d.day)


def iso_day(day: Optional[int]) -> Optional[str]:
    if day is None:
        return None
    return date(*civil_from_days(day)).isoformat()


def build_engine(db_path) -> Engine:
    """Read every observation and replay it into Person 1's structures."""
    # Pass 1: reduce every location's readings to one maximum per day.
    daily: dict[tuple, dict[int, float]] = {}
    labels: dict[tuple, str] = {}
    conn = connect(db_path)
    try:
        for obs in iter_observations(conn):
            key = (obs.source, obs.station_id, obs.latitude, obs.longitude)
            labels.setdefault(key, obs.region)
            per_day = daily.setdefault(key, {})
            day = day_of(obs.observed_at)
            if day not in per_day or obs.temperature_c > per_day[day]:
                per_day[day] = obs.temperature_c
    finally:
        conn.close()

    # Pass 2: create one Region per location, in sorted key order so ids are stable.
    keys = sorted(daily)
    locations: dict[int, Location] = {}
    regions: dict[int, Region] = {}
    spatial = QuadTree(*config.INDIA_LAT_RANGE, *config.INDIA_LON_RANGE)

    schedule: list[tuple[int, int, float]] = []   # (day, location_id, temp)
    for loc_id, key in enumerate(keys, start=1):
        source, station, lat, lng = key
        label = labels[key]
        name = station or f"{label or 'IMD grid'} ({lat:.2f}, {lng:.2f})"
        locations[loc_id] = Location(loc_id, source, station, label, lat, lng, name, len(daily[key]))
        region = Region(loc_id, name, lat, lng)
        regions[loc_id] = region
        if not spatial.insert(region):
            log.warning("location %s (%s, %s) is outside the map and is not searchable", name, lat, lng)
        schedule.extend((day, loc_id, temp) for day, temp in daily[key].items())

    # Pass 3: replay in date order through the same ingestion rule the design describes.
    schedule.sort()
    temporal = TemporalIndex()
    alerts = AlertQueue()
    raised = 0
    for day, loc_id, temp in schedule:
        if ingest_reading(temporal, alerts, regions[loc_id], day, temp):
            raised += 1

    as_of = schedule[-1][0] if schedule else None
    log.info(
        "engine built: %d locations, %d daily points, %d alerts, as of %s",
        len(locations), len(schedule), raised, iso_day(as_of),
    )
    return Engine(
        locations=locations,
        regions=regions,
        spatial=spatial,
        temporal=temporal,
        alerts=alerts,
        as_of_day=as_of,
        alerts_raised=raised,
        points_replayed=len(schedule),
    )


class EngineCache:
    """Holds one Engine and rebuilds it when the database file changes."""

    def __init__(self, db_path) -> None:
        self.db_path = Path(db_path)
        self._lock = threading.Lock()
        self._engine: Optional[Engine] = None
        self._stamp: Optional[tuple[int, int]] = None

    def _current_stamp(self) -> tuple:
        """Changes whenever an ingestion run starts or finishes.

        Exact, unlike file modification times, which can be too coarse to notice
        two writes made in quick succession.
        """
        conn = connect(self.db_path)
        try:
            return tuple(conn.execute(
                "SELECT COALESCE(MAX(id), 0), SUM(status <> 'RUNNING'), COUNT(*) FROM ingestion_log"
            ).fetchone())
        finally:
            conn.close()

    def get(self) -> Engine:
        with self._lock:
            stamp = self._current_stamp()
            if self._engine is None or stamp != self._stamp:
                self._engine = build_engine(self.db_path)
                self._stamp = stamp
            return self._engine


# ----------------------------------------------------------------- queries

def _forecast_item(engine: Engine, region: Region, lat: float, lng: float, days_ahead: int) -> dict:
    loc = engine.locations[region.id]
    f = forecast_region(engine.temporal, region, engine.as_of_day, days_ahead)
    item = {
        "location_id": loc.id,
        "name": loc.name,
        "source": loc.source,
        "station_id": loc.station_id,
        "region": loc.region,
        "latitude": loc.latitude,
        "longitude": loc.longitude,
        "distance_km": round(haversine_km(lat, lng, loc.latitude, loc.longitude), 2),
        "current_anomaly_c": round(region.temp_anomaly, 2),
        "days_ahead": days_ahead,
        "target_date": iso_day(engine.as_of_day + days_ahead),
        "status": "ok" if f is not None else "insufficient_data",
        "forecast": None,
    }
    if f is not None:
        item["forecast"] = {
            "predicted_c": round(f.predicted, 2),
            "confidence": round(f.confidence, 3),
            "severity": f.severity,
            "slope_c_per_day": round(f.slope, 3),
            "mean_c": round(f.mean, 2),
        }
    return item


def _rank(item: dict) -> tuple:
    """Forecasts first (most severe, then hottest), then insufficient data; nearest breaks ties."""
    f = item["forecast"]
    if f is None:
        return (1, 0, 0.0, item["distance_km"])
    return (0, -f["severity"], -f["predicted_c"], item["distance_km"])


def forecasts_near(engine: Engine, lat: float, lng: float, radius_km: float,
                   days_ahead: int, limit: int) -> list[dict]:
    if engine.as_of_day is None:
        return []
    hits = engine.spatial.search(lat, lng, radius_km)          # quad-tree radius query
    items = [_forecast_item(engine, r, lat, lng, days_ahead) for r in hits]
    items.sort(key=_rank)
    return items[:limit]


def forecast_for_location(engine: Engine, location_id: int, days_ahead: int) -> Optional[dict]:
    loc = engine.locations.get(location_id)
    if loc is None or engine.as_of_day is None:
        return None
    return _forecast_item(engine, engine.regions[location_id], loc.latitude, loc.longitude, days_ahead)


def list_locations(engine: Engine) -> list[dict]:
    out = []
    for loc in engine.locations.values():
        region = engine.regions[loc.id]
        out.append({
            "id": loc.id,
            "name": loc.name,
            "source": loc.source,
            "station_id": loc.station_id,
            "region": loc.region,
            "latitude": loc.latitude,
            "longitude": loc.longitude,
            "current_anomaly_c": round(region.temp_anomaly, 2),
            "days_of_history": loc.days_of_history,
        })
    return out


def top_alerts(engine: Engine, limit: int, min_severity: int = 1) -> list[dict]:
    """Most urgent alerts first. Works on a copy, so the live queue is not drained."""
    view = AlertQueue()
    view.data = list(engine.alerts.data)       # a heap is valid as a plain list
    items: list[dict] = []
    while len(items) < limit:
        top = view.peek()
        if top is None or top.severity < min_severity:
            break                               # severity never increases down the queue
        alert = view.pop()
        loc = engine.locations[alert.region_id]
        items.append({
            "rank": len(items) + 1,
            "location_id": loc.id,
            "name": loc.name,
            "severity": alert.severity,
            "temperature_c": round(alert.temp, 2),
            "anomaly_c": round(alert.anomaly, 2),
            "date": iso_day(alert.day),
        })
    return items
