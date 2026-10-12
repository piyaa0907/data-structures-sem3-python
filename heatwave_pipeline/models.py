"""Shared record types passed between the pipeline and Person 1's modules."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

SOURCE_IMD = "IMD"
SOURCE_AWS = "AWS"

MEASUREMENT_DAILY_MAX = "DAILY_MAX"   # IMD gridded daily maximum temperature
MEASUREMENT_INSTANT = "INSTANT"       # AWS point reading

# A raw input row: (line number or sequence number, canonical dict or None, raw text)
RawRow = tuple


@dataclass(frozen=True, slots=True)
class Observation:
    """One cleaned meteorological observation, exactly as stored in SQLite."""

    source: str
    latitude: float
    longitude: float
    observed_at: str          # ISO 8601, e.g. "2025-05-20T00:00:00"
    temperature_c: float
    measurement: str
    season: str
    station_id: str = ""
    region: str = ""
    humidity: Optional[float] = None

    def dedupe_key(self) -> tuple:
        """Identity of a physical observation. Matches the SQLite UNIQUE constraint."""
        return (self.source, self.station_id, self.latitude, self.longitude, self.observed_at)


@dataclass(frozen=True, slots=True)
class RejectionRecord:
    """Audit log entry for a rejected reading with detailed metadata."""

    file_or_stream_id: str
    row_number: Optional[int]
    field_name: str
    rejected_value: str
    reason_code: str
    timestamp: str
    raw_data: str
    log_id: int = 0
