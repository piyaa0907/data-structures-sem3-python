"""Central configuration for the heatwave data pipeline.

Every threshold used by validation and QC rules lives here so it can be
tuned without touching the core logic.
"""

from datetime import timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "heatwave.db"
SAMPLE_IMD_FILE = PROJECT_ROOT / "data" / "sample_imd_2025_05.csv"
SAMPLE_IMD_JSON_FILE = PROJECT_ROOT / "data" / "sample_imd_2025_05.json"
SAMPLE_IMD_GRD_FILE = PROJECT_ROOT / "data" / "sample_imd_2025_05_20.grd"
LOG_DIR = PROJECT_ROOT / "logs"

# All stored timestamps are local Indian Standard Time, naive (no tz suffix).
IST = timezone(timedelta(hours=5, minutes=30))

# Coverage box for the Indian subcontinent (degrees). Points outside are rejected.
INDIA_LAT_RANGE = (6.5, 37.6)
INDIA_LON_RANGE = (68.0, 97.5)

# Physically plausible surface air temperature for India (degrees Celsius).
TEMP_RANGE_C = (-10.0, 60.0)
HUMIDITY_RANGE = (0.0, 100.0)

# Meteorological Quality Control (QC) Thresholds
RATE_OF_CHANGE_THRESHOLD_C = 10.0      # Reject if temperature changes > 10°C within 1 hour
RATE_OF_CHANGE_WINDOW_HOURS = 1.0      # Time window for rate-of-change check (hours)
FLATLINE_PERSISTENCE_COUNT = 6         # Reject if identical temp persists across 6+ readings

# Real-time AWS Network Anomaly Defaults
DEFAULT_PACKET_LOSS_RATE = 0.0         # 0.0 to 1.0
DEFAULT_OUT_OF_ORDER_RATE = 0.0        # 0.0 to 1.0
DEFAULT_RETRANSMISSION_RATE = 0.0      # 0.0 to 1.0

# IMD Gridded Data Defaults (1° x 1° grid specifications)
IMD_GRID_LAT_RANGE = (6.5, 37.5)
IMD_GRID_LON_RANGE = (66.5, 97.5)
IMD_GRID_STEP = 1.0
IMD_GRID_NODATA = -999.0

# Numeric codes and text values that providers use to mean "no data".
MISSING_SENTINELS = frozenset({-999.0, -9999.0})
MISSING_TEXT = frozenset({"", "NA", "N/A", "NAN", "NULL", "-"})

COORD_DECIMALS = 4
TEMP_DECIMALS = 2

# Rows are written to SQLite in batches of this size.
BATCH_SIZE = 500

# Four climatic seasons used by IMD (month -> season label).
SEASON_BY_MONTH = {
    1: "WINTER", 2: "WINTER",
    3: "PRE_MONSOON", 4: "PRE_MONSOON", 5: "PRE_MONSOON",
    6: "MONSOON", 7: "MONSOON", 8: "MONSOON", 9: "MONSOON",
    10: "POST_MONSOON", 11: "POST_MONSOON", 12: "POST_MONSOON",
}
