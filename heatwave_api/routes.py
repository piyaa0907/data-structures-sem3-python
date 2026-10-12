"""Endpoints and request dispatch for the heatwave API.

``dispatch(ctx, method, target)`` returns ``(status, payload)`` and never touches
sockets, so every endpoint can be tested directly.

Data endpoints read the database through ``heatwave_pipeline``. Forecast, alert
and location endpoints read the in-memory engine built from the same database.
"""

from __future__ import annotations

import dataclasses
import logging
import math
import sqlite3
from typing import Any, Callable, Mapping, Optional
from urllib.parse import parse_qs, urlsplit

from heatwave_pipeline import __version__, config
from heatwave_pipeline.database import connect
from heatwave_pipeline.quality_report import build_quality_report
from heatwave_pipeline.repository import count_observations, iter_observations, list_regions

from . import engine as eng

log = logging.getLogger("heatwave_api")

DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
MAX_FORECAST_DAYS = 7
DEFAULT_RADIUS_KM = 200.0
MAX_RADIUS_KM = 1000.0
VALID_SOURCES = ("IMD", "AWS")
VALID_SEASONS = tuple(sorted(set(config.SEASON_BY_MONTH.values())))

Query = Mapping[str, list[str]]
Payload = dict[str, Any]
Handler = Callable[["ApiContext", Query], Payload]


class ApiContext:
    """What handlers need: the database path and the engine cache."""

    def __init__(self, db_path, cache: "eng.EngineCache") -> None:
        self.db_path = db_path
        self.cache = cache

    def connect(self) -> sqlite3.Connection:
        return connect(self.db_path)

    def engine(self) -> "eng.Engine":
        return self.cache.get()


class ApiError(Exception):
    """Raised by a handler to return a specific HTTP status."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status
        self.message = message


# ------------------------------------------------------------- parameters

def _single(query: Query, name: str) -> Optional[str]:
    values = query.get(name)
    if not values:
        return None
    text = values[-1].strip()
    return text or None


def _int_param(query: Query, name: str, default: int, minimum: int, maximum: int) -> int:
    raw = _single(query, name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        raise ValueError(f"'{name}' must be an integer") from None
    if not minimum <= value <= maximum:
        raise ValueError(f"'{name}' must be between {minimum} and {maximum}")
    return value


def _float_param(query: Query, name: str, default: Optional[float],
                 minimum: float, maximum: float) -> float:
    raw = _single(query, name)
    if raw is None:
        if default is None:
            raise ValueError(f"'{name}' is required")
        return default
    try:
        value = float(raw)
    except ValueError:
        raise ValueError(f"'{name}' must be a number") from None
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise ValueError(f"'{name}' must be between {minimum} and {maximum}")
    return value


def _choice(query: Query, name: str, choices: tuple[str, ...]) -> Optional[str]:
    value = _single(query, name)
    if value is None:
        return None
    value = value.upper()
    if value not in choices:
        raise ValueError(f"'{name}' must be one of: {', '.join(choices)}")
    return value


def _with_db(ctx: ApiContext, fn: Callable[[sqlite3.Connection], Any]) -> Any:
    conn = ctx.connect()
    try:
        return fn(conn)
    finally:
        conn.close()


# --------------------------------------------------------- data endpoints

def _health(ctx: ApiContext, query: Query) -> Payload:
    def check(conn):
        conn.execute("SELECT 1").fetchone()
    _with_db(ctx, check)
    return {"status": "ok", "service": "heatwave-api", "version": __version__}


def _regions(ctx: ApiContext, query: Query) -> Payload:
    regions = _with_db(ctx, list_regions)
    return {"count": len(regions), "regions": regions}


def _observations(ctx: ApiContext, query: Query) -> Payload:
    """Cleaned observations, oldest first. Filters: source, region, season, station, start, end, limit."""
    limit = _int_param(query, "limit", DEFAULT_LIMIT, 1, MAX_LIMIT)
    filters = {
        "source": _choice(query, "source", VALID_SOURCES),
        "region": _single(query, "region"),
        "season": _choice(query, "season", VALID_SEASONS),
        "station_id": _single(query, "station"),
        "start": _single(query, "start"),
        "end": _single(query, "end"),
    }

    def read(conn):
        total = count_observations(conn, **filters)
        items = [dataclasses.asdict(o) for o in iter_observations(conn, limit=limit, **filters)]
        return total, items

    total, items = _with_db(ctx, read)
    return {"total": total, "returned": len(items), "limit": limit, "items": items}


def _quality(ctx: ApiContext, query: Query) -> Payload:
    return _with_db(ctx, build_quality_report)


# ----------------------------------------------------- forecast and alerts

def _forecasts(ctx: ApiContext, query: Query) -> Payload:
    """Forecasts for one location (location_id=) or for locations near a point (lat, lng, radius_km)."""
    days = _int_param(query, "days", 1, 1, MAX_FORECAST_DAYS)
    engine = ctx.engine()
    as_of = eng.iso_day(engine.as_of_day)

    if _single(query, "location_id") is not None:
        location_id = _int_param(query, "location_id", 0, 1, 10**9)
        item = eng.forecast_for_location(engine, location_id, days)
        if item is None:
            raise ApiError(404, f"no location with id {location_id}")
        return {"as_of": as_of, "days_ahead": days, "count": 1, "items": [item]}

    lat = _float_param(query, "lat", None, -90.0, 90.0)
    lng = _float_param(query, "lng", None, -180.0, 180.0)
    radius = _float_param(query, "radius_km", DEFAULT_RADIUS_KM, 1.0, MAX_RADIUS_KM)
    limit = _int_param(query, "limit", 20, 1, 200)
    items = eng.forecasts_near(engine, lat, lng, radius, days, limit)
    return {"as_of": as_of, "days_ahead": days, "count": len(items), "items": items}


def _alerts(ctx: ApiContext, query: Query) -> Payload:
    """Pending alerts, most urgent first. Optional min_severity (1-5) and limit."""
    limit = _int_param(query, "limit", 20, 1, 200)
    min_severity = _int_param(query, "min_severity", 1, 1, 5)
    engine = ctx.engine()
    items = eng.top_alerts(engine, limit, min_severity)
    return {
        "pending": len(engine.alerts),
        "returned": len(items),
        "min_severity": min_severity,
        "items": items,
    }


def _locations(ctx: ApiContext, query: Query) -> Payload:
    items = eng.list_locations(ctx.engine())
    return {"count": len(items), "items": items}


ROUTES: dict[str, Handler] = {
    "/api/health": _health,
    "/api/regions": _regions,
    "/api/observations": _observations,
    "/api/quality": _quality,
    "/api/locations": _locations,
    "/api/forecasts": _forecasts,
    "/api/alerts": _alerts,
}


# --------------------------------------------------------------- dispatch

def dispatch(ctx: ApiContext, method: str, target: str) -> tuple[int, Payload]:
    """Route one request. Returns ``(http_status, json_payload)``."""
    parts = urlsplit(target)
    path = parts.path.rstrip("/") or "/"

    handler = ROUTES.get(path)
    if handler is None:
        return 404, {"error": f"no route for '{path}'", "routes": sorted(ROUTES)}
    if method.upper() != "GET":
        return 405, {"error": f"method {method.upper()} not allowed; use GET"}

    query = parse_qs(parts.query, keep_blank_values=False)
    try:
        return 200, handler(ctx, query)
    except ApiError as exc:
        return exc.status, {"error": exc.message}
    except ValueError as exc:
        return 400, {"error": str(exc)}
    except sqlite3.Error as exc:
        log.error("database error on %s: %s", path, exc)
        return 500, {"error": "database error; see server log"}
    except Exception:  # keep the server alive; the traceback goes to the log
        log.exception("unexpected error on %s", path)
        return 500, {"error": "internal error; see server log"}
