"""Person 2 module of the Heatwave Intelligence System: data pipeline and SQLite storage.

Strictly standard library implementation (Python 3.10+):
- IMD Historical Ingestion (CSV, JSON, GRD fixed-width and binary) with SHA-256 deduplication
- Automated Weather Station (AWS) real-time streaming simulation with network anomalies
- Meteorological Quality Control (QC): Step check (rate-of-change) and flatline persistence check
- High-concurrency SQLite storage configured with WAL mode and 64MB memory caching
- Person 1 index initialization and incremental real-time observation event hooks
"""

from .models import Observation, RejectionRecord
from .repository import (
    RecordSink,
    SpatialQuadTreeAdapter,
    TemporalBTreeAdapter,
    clear_observation_listeners,
    initialise_indexes,
    iter_observations,
    register_observation_listener,
    unregister_observation_listener,
)

__version__ = "2.0.0"

__all__ = [
    "Observation",
    "RejectionRecord",
    "RecordSink",
    "SpatialQuadTreeAdapter",
    "TemporalBTreeAdapter",
    "register_observation_listener",
    "unregister_observation_listener",
    "clear_observation_listeners",
    "initialise_indexes",
    "iter_observations",
]
