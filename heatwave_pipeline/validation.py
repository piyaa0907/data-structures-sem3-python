"""Row-level validation and meteorological quality control (QC) rules.

Validates IMD and AWS readings against physical ranges, coverage bounds,
and time windows, and applies automated meteorological QC tests:
1. Rate-of-Change (Step Check): flags physically implausible temperature jumps
   exceeding 10°C within 1 hour for the same station.
2. Sensor Persistence (Flatline Check): flags stuck sensors where the temperature
   remains identical across 6 or more consecutive readings within a sensor sampling window.
"""

from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Optional

from . import config
from .loaders import REQUIRED_FIELDS

REASON_MALFORMED = "malformed_row"
REASON_MISSING = "missing_value"
REASON_BAD_TIMESTAMP = "invalid_timestamp"
REASON_NOT_NUMERIC = "non_numeric_value"
REASON_SENTINEL = "missing_data_code"
REASON_FUTURE = "future_timestamp"
REASON_OUTSIDE_INDIA = "outside_india_coverage"
REASON_TEMP_RANGE = "temperature_out_of_range"
REASON_HUMIDITY_RANGE = "humidity_out_of_range"
REASON_RATE_OF_CHANGE = "rate_of_change_exceeded"
REASON_SENSOR_PERSISTENCE = "sensor_persistence_flatline"


class StationQCValidator:
    """Stateful Meteorological Quality Control (QC) validator.

    Maintains recent observation history per station to enforce:
    - Step Check: temperature change must not exceed threshold within 1 hour.
    - Flatline Check: temperature must not remain identical across 6+ consecutive readings.
    """

    def __init__(
        self,
        rate_of_change_threshold: float = config.RATE_OF_CHANGE_THRESHOLD_C,
        rate_of_change_window_hours: float = config.RATE_OF_CHANGE_WINDOW_HOURS,
        flatline_limit: int = config.FLATLINE_PERSISTENCE_COUNT,
        flatline_max_gap_hours: float = 6.0,
    ) -> None:
        self.rate_of_change_threshold = rate_of_change_threshold
        self.rate_of_change_window_seconds = rate_of_change_window_hours * 3600.0
        self.flatline_limit = flatline_limit
        self.flatline_max_gap_seconds = flatline_max_gap_hours * 3600.0
        # station_id -> {"last_time": datetime, "last_temp": float, "flatline_temp": float, "consecutive_count": int}
        self._history: dict[str, dict[str, Any]] = {}

    def reset(self) -> None:
        """Clear all historical station tracking state."""
        self._history.clear()

    def check_reading(
        self,
        station_key: str,
        timestamp: datetime,
        temp_c: float,
    ) -> tuple[Optional[str], str, str, Any]:
        """Perform step and flatline checks for a candidate reading.

        Returns (reason_code, detail, field_name, rejected_value) if failed,
        or (None, "", "", None) if the reading passes QC.
        """
        if not station_key:
            return None, "", "", None

        state = self._history.get(station_key)
        if state is None:
            # First reading for this station
            self._history[station_key] = {
                "last_time": timestamp,
                "last_temp": temp_c,
                "flatline_temp": temp_c,
                "consecutive_count": 1,
            }
            return None, "", "", None

        last_time = state["last_time"]
        last_temp = state["last_temp"]
        delta_sec = (timestamp - last_time).total_seconds()

        # 1. Flatline / Sensor Persistence Check
        # Detect stuck sensor if temperature remains identical across consecutive sensor reports
        is_identical = abs(temp_c - state["flatline_temp"]) < 1e-3
        # Flatline applies to consecutive high-frequency sensor readings (within flatline_max_gap_seconds)
        is_consecutive_stream = 0.0 <= delta_sec <= self.flatline_max_gap_seconds

        if is_identical and is_consecutive_stream:
            consecutive = state["consecutive_count"] + 1
            if consecutive >= self.flatline_limit:
                state["consecutive_count"] = consecutive
                # Faulty reading does not update last_time/last_temp
                return (
                    REASON_SENSOR_PERSISTENCE,
                    f"sensor flatline: temperature remained identical ({temp_c:.2f} C) across {consecutive} consecutive readings",
                    "temperature_c",
                    temp_c,
                )
            state["consecutive_count"] = consecutive
        else:
            # Temperature changed or sampling interval exceeds sensor stream gap: reset flatline tracker
            state["flatline_temp"] = temp_c
            state["consecutive_count"] = 1

        # 2. Rate-of-Change (Step Check)
        if 0.0 <= delta_sec <= self.rate_of_change_window_seconds:
            temp_delta = abs(temp_c - last_temp)
            if temp_delta > self.rate_of_change_threshold:
                # Faulty reading: do not advance valid baseline
                return (
                    REASON_RATE_OF_CHANGE,
                    f"rate-of-change exceeded: temperature changed by {temp_delta:.2f} C within {delta_sec / 60.0:.1f} min (> {self.rate_of_change_threshold:.1f} C/hr limit)",
                    "temperature_c",
                    temp_c,
                )

        # Passed QC checks: record as latest valid baseline
        state["last_time"] = timestamp
        state["last_temp"] = temp_c
        return None, "", "", None


def is_missing(value: Any) -> bool:
    """True for None or for text that providers use to mean 'no value'."""
    if value is None:
        return True
    return isinstance(value, str) and value.strip().upper() in config.MISSING_TEXT


def parse_number(value: Any) -> float:
    """Convert to a finite float. Raises ValueError or TypeError on bad input."""
    if isinstance(value, bool):
        raise ValueError("booleans are not numeric readings")
    number = float(value.strip() if isinstance(value, str) else value)
    if not math.isfinite(number):
        raise ValueError(f"{value!r} is not a finite number")
    return number


def parse_timestamp(value: Any) -> datetime:
    """Parse an ISO 8601 date or datetime into a naive IST datetime.

    Accepts "2025-05-20", "2025-05-20 10:30", "2025-05-20T10:30:00" and
    timezone-aware strings (which are converted to IST).
    Raises ValueError for impossible dates such as 2025-02-31.
    """
    if isinstance(value, datetime):
        moment = value
    else:
        moment = datetime.fromisoformat(str(value).strip())
    if moment.tzinfo is not None:
        moment = moment.astimezone(config.IST).replace(tzinfo=None)
    return moment


def _within(value: float, bounds: tuple[float, float]) -> bool:
    return bounds[0] <= value <= bounds[1]


def validate_row_detailed(
    row: Optional[dict],
    now: datetime,
    qc_validator: Optional[StationQCValidator] = None,
) -> tuple[Optional[str], str, str, Any]:
    """Validate a single row with metadata for audit logging.

    Returns: (reason_code, detail_message, field_name, rejected_value)
    """
    if row is None:
        return REASON_MALFORMED, "row is not a JSON object", "", None

    for field in REQUIRED_FIELDS:
        if is_missing(row.get(field)):
            return REASON_MISSING, f"required field '{field}' is empty", field, row.get(field)

    try:
        timestamp = parse_timestamp(row["timestamp"])
    except (TypeError, ValueError):
        return REASON_BAD_TIMESTAMP, f"cannot read timestamp {row['timestamp']!r}", "timestamp", row.get("timestamp")

    try:
        latitude = parse_number(row["latitude"])
    except (TypeError, ValueError):
        return REASON_NOT_NUMERIC, "latitude must be numeric", "latitude", row.get("latitude")

    try:
        longitude = parse_number(row["longitude"])
    except (TypeError, ValueError):
        return REASON_NOT_NUMERIC, "longitude must be numeric", "longitude", row.get("longitude")

    try:
        temperature = parse_number(row["temperature_c"])
    except (TypeError, ValueError):
        return REASON_NOT_NUMERIC, "temperature must be numeric", "temperature_c", row.get("temperature_c")

    for name, value in (
        ("temperature_c", temperature),
        ("latitude", latitude),
        ("longitude", longitude),
    ):
        if value in config.MISSING_SENTINELS:
            return REASON_SENTINEL, f"{name} holds missing-data code {value:g}", name, value

    if timestamp > now:
        return REASON_FUTURE, f"timestamp {timestamp:%Y-%m-%d %H:%M} is later than the current time", "timestamp", row.get("timestamp")

    if not (_within(latitude, config.INDIA_LAT_RANGE) and _within(longitude, config.INDIA_LON_RANGE)):
        return REASON_OUTSIDE_INDIA, f"({latitude}, {longitude}) is outside the India coverage box", "latitude/longitude", f"({latitude}, {longitude})"

    if not _within(temperature, config.TEMP_RANGE_C):
        return REASON_TEMP_RANGE, f"temperature {temperature} C is outside {config.TEMP_RANGE_C}", "temperature_c", temperature

    if not is_missing(row.get("humidity")):
        try:
            humidity = parse_number(row["humidity"])
        except (TypeError, ValueError):
            return REASON_NOT_NUMERIC, "humidity must be numeric when present", "humidity", row.get("humidity")
        if not _within(humidity, config.HUMIDITY_RANGE):
            return REASON_HUMIDITY_RANGE, f"humidity {humidity} is outside 0-100", "humidity", humidity

    # Stateful Meteorological QC (Rate-of-Change & Flatline Check)
    if qc_validator is not None:
        station_key = str(row.get("station_id") or "").strip()
        if not station_key:
            station_key = f"{round(latitude, config.COORD_DECIMALS)},{round(longitude, config.COORD_DECIMALS)}"
        qc_reason, qc_detail, qc_field, qc_val = qc_validator.check_reading(
            station_key, timestamp, temperature
        )
        if qc_reason is not None:
            return qc_reason, qc_detail, qc_field, qc_val

    return None, "", "", None


def validate_row(
    row: Optional[dict],
    now: datetime,
    qc_validator: Optional[StationQCValidator] = None,
) -> tuple[Optional[str], str]:
    """Return ``(reason, detail)`` for a rejected row, or ``(None, "")`` if valid."""
    reason, detail, _, _ = validate_row_detailed(row, now, qc_validator)
    return reason, detail
