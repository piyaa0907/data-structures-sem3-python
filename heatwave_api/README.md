# Heatwave API

Read-only HTTP endpoints for the heatwave system. Standard library only.

* **Data endpoints** (`/api/observations`, `/api/regions`, `/api/quality`) read the SQLite
  database through the Person 2 repository layer.
* **Forecast, alert and location endpoints** (`/api/forecasts`, `/api/alerts`,
  `/api/locations`) read an in-memory engine. The engine replays the database into
  Person 1's quad-tree, B-tree and alert queue.

## Run

From the project root, after `python main.py init` and some ingestion:

```bash
python -m heatwave_api.server --port 8000
# optional: --db PATH   --host 0.0.0.0 (only on a trusted network)
```

The server builds the engine at startup, so a broken database fails immediately.

## Endpoints

All endpoints are `GET` and return JSON. Other methods return `405`. Errors look like
`{"error": "..."}`.

| Endpoint | Purpose |
|---|---|
| `/api/health` | liveness and database check |
| `/api/regions` | distinct region labels in the database |
| `/api/observations` | cleaned records, oldest first |
| `/api/quality` | data-quality report (same content as `main.py report`) |
| `/api/locations` | every location with its current anomaly and days of history |
| `/api/forecasts` | forecasts for one location, or for locations near a point |
| `/api/alerts` | pending alerts, most urgent first |

### `/api/observations`

Query: `source` (IMD, AWS), `region`, `season`, `station`, `start`, `end` (inclusive),
`limit` (1–1000, default 100).

```json
{ "total": 7, "returned": 1, "limit": 1, "items": [ { "source": "IMD", "observed_at": "2025-05-20T00:00:00", "temperature_c": 44.2, ... } ] }
```

### `/api/forecasts`

Either `location_id=<id>`, or `lat=<deg>&lng=<deg>` with optional `radius_km`
(1–1000, default 200).

Also: `days` (1–7, default 1) and `limit` (1–200, default 20, near-point mode only).

```json
{
  "as_of": "2026-10-12",
  "days_ahead": 2,
  "count": 1,
  "items": [{
    "location_id": 2, "name": "AWS-DEL-01", "distance_km": 0.44,
    "current_anomaly_c": 0.64, "target_date": "2026-10-14", "status": "ok",
    "forecast": { "predicted_c": 44.7, "confidence": 0.062, "severity": 3,
                  "slope_c_per_day": 0.064, "mean_c": 44.42 }
  }]
}
```

`status` is `insufficient_data` when a location has fewer than 3 readings in the last
7 days. In that case `forecast` is `null`. Items are ranked by severity, then predicted
temperature, then distance.

### `/api/alerts`

Query: `limit` (1–200, default 20), `min_severity` (1–5, default 1).

Reading alerts does not remove them. The queue is copied for each request.

```json
{ "pending": 1, "returned": 1, "min_severity": 1,
  "items": [{ "rank": 1, "location_id": 1, "name": "AWS-DEL-01", "severity": 5,
              "temperature_c": 46.0, "anomaly_c": 11.9, "date": "2025-05-21" }] }
```

### `/api/locations`

```json
{ "count": 10, "items": [{ "id": 1, "name": "AWS-CHE-01", "source": "AWS",
  "station_id": "AWS-CHE-01", "region": "PENINSULAR INDIA", "latitude": 13.0827,
  "longitude": 80.2707, "current_anomaly_c": 0.0, "days_of_history": 11 }] }
```

## How the engine maps the data

Person 1's structures work on *locations*. Each location is one physical point: an AWS
station, or an IMD grid point identified by source, station and coordinates.

| Database | Engine |
|---|---|
| each distinct point | one `Region` in the quad-tree, with a stable integer id |
| hourly AWS readings | reduced to the **daily maximum**, matching IMD's `DAILY_MAX` |
| all readings in date order | replayed through `ingest_reading()`, which fills the B-trees, sets each region's anomaly, and raises alerts |

**"Today" is the latest date in the database**, not the wall clock. This keeps
forecasts meaningful when the data is historical, such as the May 2025 sample.
The response gives it as `as_of`.

**Rebuilds.** The engine is rebuilt when an ingestion run starts or finishes. The check
reads the `ingestion_log` table, which is exact, so a change is never missed because
of coarse file timestamps. The rebuild replays the whole database, which is fine at
this scale.

## Tests

```bash
python -m unittest discover -s tests -v
```

The API tests cover every endpoint, the 400/404/405 paths, a 21-day synthetic heatwave
(severity 5 alert on the spike day, forecast using that day as "today"), engine
rebuilds, and a live HTTP round trip. They also run Person 1's own suite.

## Limits

* No authentication. Bind to `127.0.0.1` unless the network is trusted.
* CORS is open (`Access-Control-Allow-Origin: *`) so the dashboard can call the API from
  another port. The API is read-only, but this is not for public deployment.
* The engine lives in memory. Memory grows with the number of locations and days.
* Person 1's forecast is a damped linear trend and its severity thresholds are marked
  "check against IMD" in their code. Both are Person 1's to confirm.
* Simulated AWS data gives low forecast confidence because it is random noise around a
  daily cycle.
