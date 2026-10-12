"""Normalisation of validated rows into ``Observation`` records."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Mapping

from . import config
from .models import Observation
from .validation import is_missing, parse_number, parse_timestamp


def season_for(moment: datetime) -> str:
    """Return the IMD climatic season label for a datetime."""
    return config.SEASON_BY_MONTH[moment.month]


def clean_region(value: Any) -> str:
    """Trim, collapse internal whitespace and upper-case a region label."""
    if is_missing(value):
        return ""
    return " ".join(str(value).split()).upper()


def clean_station(value: Any) -> str:
    """Sanitize and trim a weather station identifier."""
    if is_missing(value):
        return ""
    return str(value).strip()


def region_for_coordinates(lat: float, lon: float) -> str:
    """Map geographical coordinates to one of IMD's 7 meteorological regions.

    Used when region metadata is absent (e.g. raw gridded binary or fixed-width grids).
    """
    if lat >= 24.0 and lon <= 80.0:
        return "NORTHWEST INDIA"
    if 20.0 <= lat < 28.0 and 74.0 <= lon <= 84.0:
        return "CENTRAL INDIA"
    if 15.0 <= lat < 22.0 and 72.0 <= lon <= 77.0:
        return "WEST CENTRAL INDIA"
    if lat < 16.0:
        return "PENINSULAR INDIA"
    if 20.0 <= lat < 28.0 and 84.0 < lon <= 89.0:
        return "EAST INDIA"
    if lon > 89.0:
        return "NORTHEAST INDIA"
    if lat >= 28.0 and lon > 80.0:
        return "NORTH INDIA"
    if lat >= 16.0 and lon <= 80.0:
        return "CENTRAL INDIA"
    return "PENINSULAR INDIA"


def clean_row(row: Mapping[str, Any], source: str, measurement: str) -> Observation:
    """Turn an already-validated canonical row into an ``Observation``.

    Coordinates are rounded to ``COORD_DECIMALS`` so that the same physical
    point always produces the same deduplication key. Missing region labels
    are automatically inferred from coordinates.
    """
    moment = parse_timestamp(row["timestamp"]).replace(microsecond=0)
    humidity = None if is_missing(row.get("humidity")) else round(parse_number(row["humidity"]), 1)

    lat = round(parse_number(row["latitude"]), config.COORD_DECIMALS)
    lon = round(parse_number(row["longitude"]), config.COORD_DECIMALS)

    raw_region = clean_region(row.get("region"))
    region = raw_region if raw_region else region_for_coordinates(lat, lon)

    return Observation(
        source=source,
        latitude=lat,
        longitude=lon,
        observed_at=moment.isoformat(timespec="seconds"),
        temperature_c=round(parse_number(row["temperature_c"]), config.TEMP_DECIMALS),
        measurement=measurement,
        season=season_for(moment),
        station_id=clean_station(row.get("station_id")),
        region=region,
        humidity=humidity,
    )
