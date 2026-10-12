# Heatwave Data Pipeline (KJS-CES-01)
## Person 2: Data Pipeline & Database Layer

A clean, beginner-friendly Python data pipeline for ingesting, validating, cleaning, and storing meteorological observations in SQLite. Built strictly using the Python standard library (no external packages like pandas or numpy required).

---

## 1. Quick Start

Run these commands from the project root:

```bash
# 1. Initialize the SQLite database (enables WAL mode & indexes)
python main.py init

# 2. Ingest historical IMD data (CSV, JSON, or GRD)
python main.py ingest-imd data/sample_imd_2025_05.csv
python main.py ingest-imd data/sample_imd_2025_05.json
python main.py ingest-imd data/sample_imd_2025_05_20.grd

# Note: Re-running an unaltered file is automatically skipped via SHA-256 hash.
# To force re-ingestion: python main.py ingest-imd data/sample_imd_2025_05.csv --force

# 3. Ingest simulated real-time AWS readings (batch mode)
python main.py simulate-aws --steps 24 --seed 42

# 4. Or run live continuous AWS streaming (stop anytime with Ctrl+C)
python main.py simulate-aws --live --tick-seconds 2.0

# 5. Check database statistics & row counts
python main.py db-stats

# 6. Defragment / optimize database file
python main.py db-vacuum

# 7. Query observations
python main.py query --region "NORTHWEST INDIA" --limit 10

# 8. Run unit tests
python -m unittest discover -s tests -v
```

---

## 2. Project Layout

```
heatwave_data_pipeline/
├── main.py                     # Command-line entry point
├── README.md                    # This documentation file
├── data/
│   ├── heatwave.db              # SQLite database (auto-created)
│   ├── sample_imd_2025_05.csv   # Sample IMD historical CSV
│   ├── sample_imd_2025_05.json  # Sample IMD historical JSON
│   └── sample_imd_2025_05_20.grd# Sample IMD gridded file
├── heatwave_pipeline/
│   ├── config.py                # QC thresholds, boundaries, paths
│   ├── models.py                # Observation & RejectionRecord dataclasses
│   ├── database.py              # SQLite connection, WAL mode, schema
│   ├── loaders.py               # Streaming CSV, JSON, GRD parsers & SHA-256
│   ├── validation.py            # Quality control (rate-of-change, flatline)
│   ├── cleaning.py              # Normalization & region coordinate mapping
│   ├── ingestion.py             # Ingestion orchestrator & deduplication
│   ├── aws_simulator.py         # IoT weather station simulator & live mode
│   ├── repository.py            # Query engine & Person 1 index adapters
│   ├── quality_report.py        # Generates text/JSON audit reports
│   └── cli.py                   # CLI commands implementation
└── tests/
    └── test_pipeline.py         # Comprehensive unit tests
```

---

## 3. What Person 2 Does

| Feature | Description |
|---|---|
| **File Formats Supported** | CSV, JSON, and IMD `.grd` (fixed-width/binary) streamed row-by-row. |
| **SHA-256 Deduplication** | Computes file checksums to skip re-ingesting duplicate historical files. |
| **Meteorological QC (Step Check)** | Rejects readings if temperature jumps $> 10^\circ\text{C}$ in $< 1\text{ hour}$ for the same station. |
| **Meteorological QC (Flatline Check)** | Detects stuck sensors when temperature stays identical for $6+$ consecutive readings. |
| **SQLite WAL Concurrency** | Configures `PRAGMA journal_mode = WAL` so Person 1 & 3 can read while Person 2 writes. |
| **AWS Streaming Simulation** | Simulates live weather stations with network packet loss, out-of-order jitter, and duplicates. |
| **Person 1 Integration** | Provides typed sinks for QuadTree/B-Tree and real-time event listener hooks. |

---

## 4. Database Schema Summary

The database uses SQLite with 3 main tables:

1. **`observations`**: Cleaned weather readings.
   - Columns: `id`, `source` (IMD/AWS), `station_id`, `region`, `latitude`, `longitude`, `observed_at`, `season`, `measurement`, `temperature_c`, `humidity`, `ingested_at`.
   - Unique on: `(source, station_id, latitude, longitude, observed_at)`.
   - Indexed on `observed_at`, `(region, observed_at)`, and `(season, region)`.

2. **`ingestion_log`**: Audit history of every ingestion run.
   - Tracks `file_hash` (SHA-256), `records_read`, `inserted`, `duplicates`, `rejected`, and `status`.

3. **`rejections`**: Detailed audit trail for bad data.
   - Stores `file_or_stream_id`, `row_number`, `field_name`, `rejected_value`, `reason_code`, and `timestamp`.

---

## 5. Person 1 Integration Guide

Person 1 can connect custom spatial and temporal indexes in two simple ways:

### A. Populate Indexes from Existing Data (One Pass)
```python
from heatwave_pipeline.database import connect
from heatwave_pipeline.repository import initialise_indexes, SpatialQuadTreeAdapter, TemporalBTreeAdapter
from p1_core.spatial import QuadTree
from p1_core.temporal import TemporalIndex

conn = connect()

# Instantiate Person 1's structures
quadtree = QuadTree(min_lat=6.5, max_lat=37.6, min_lng=68.0, max_lng=97.5)
temporal = TemporalIndex()

# Stream records into both indexes in a single scan
counts = initialise_indexes(
    conn,
    spatial=SpatialQuadTreeAdapter(quadtree),
    temporal=TemporalBTreeAdapter(temporal),
)
print(f"Loaded {counts['spatial']} spatial and {counts['temporal']} temporal points.")
conn.close()
```

### B. Receive Live AWS Updates Incrementally
```python
from heatwave_pipeline.repository import register_observation_listener

def on_live_reading(record):
    print(f"New clean reading: {record.station_id} at {record.observed_at}: {record.temperature_c}°C")
    # Update QuadTree / B-Tree incrementally here

register_observation_listener(on_live_reading)
```

---

## 6. Common CLI Commands

| Command | Purpose |
|---|---|
| `python main.py init` | Create database and apply WAL pragmas |
| `python main.py ingest-imd <file>` | Ingest CSV, JSON, or GRD file |
| `python main.py simulate-aws --steps 24` | Simulate 24 hours of AWS telemetry |
| `python main.py simulate-aws --live` | Stream live AWS data continuously |
| `python main.py db-stats` | Show total rows, sizes, and rejection reasons |
| `python main.py db-vacuum` | Defragment SQLite database file |
| `python main.py report` | Print data quality summary |
| `python main.py query --limit 20` | View recent cleaned observations |
| `python -m unittest discover -s tests -v` | Run all 73 automated tests |
