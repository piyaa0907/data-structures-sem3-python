"""Tests for the heatwave API: endpoints, engine replay, and the HTTP server.

Run from the project root:

    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from heatwave_api.engine import EngineCache, build_engine
from heatwave_api.routes import ApiContext, dispatch
from heatwave_api.server import make_server
from heatwave_pipeline import config
from heatwave_pipeline.database import connect, init_db
from heatwave_pipeline.ingestion import ingest_imd_file

NOW = datetime(2026, 10, 12, 12, 0, 0)
CSV_HEADER = "timestamp,latitude,longitude,region,max_temp_c"


class DbTestCase(unittest.TestCase):
    """Each test gets a fresh database and an API context bound to it."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.db = self.dir / "api.db"
        conn = connect(self.db)
        init_db(conn)
        conn.close()
        self.ctx = ApiContext(self.db, EngineCache(self.db))

    def tearDown(self):
        self._tmp.cleanup()

    def load(self, path):
        conn = connect(self.db)
        try:
            return ingest_imd_file(conn, path, now=NOW)
        finally:
            conn.close()

    def write_csv(self, name, rows):
        path = self.dir / name
        path.write_text(CSV_HEADER + "\n" + "\n".join(rows) + "\n", encoding="utf-8")
        return path

    def call(self, target, method="GET"):
        return dispatch(self.ctx, method, target)


class SampleDataTests(DbTestCase):
    """Endpoints against the bundled sample file (7 valid, 2 days at most per location)."""

    def setUp(self):
        super().setUp()
        result = self.load(config.SAMPLE_IMD_FILE)
        self.assertEqual(result.inserted, 7)

    def test_health(self):
        status, body = self.call("/api/health")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")

    def test_regions(self):
        status, body = self.call("/api/regions")
        self.assertEqual(status, 200)
        self.assertEqual(body["count"], 4)

    def test_observations_default_returns_everything(self):
        status, body = self.call("/api/observations")
        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 7)
        self.assertEqual(body["returned"], 7)
        self.assertEqual(body["limit"], 100)
        self.assertIn("observed_at", body["items"][0])

    def test_observations_region_filter_and_limit(self):
        status, body = self.call("/api/observations?region=NORTHWEST+INDIA&limit=1")
        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 2)
        self.assertEqual(body["returned"], 1)
        self.assertEqual(body["items"][0]["observed_at"], "2025-05-20T00:00:00")

    def test_observations_season_filter_is_case_insensitive(self):
        status, body = self.call("/api/observations?season=pre_monsoon")
        self.assertEqual(status, 200)
        self.assertEqual(body["total"], 7)

    def test_bad_query_values_return_400(self):
        for target in (
            "/api/observations?limit=0",
            "/api/observations?limit=abc",
            "/api/observations?limit=5000",
            "/api/observations?start=2025-02-31",
            "/api/observations?source=SATELLITE",
            "/api/observations?season=SUMMER",
            "/api/forecasts?lat=28.6",                    # lng missing
            "/api/forecasts?lat=28.6&lng=77.2&days=9",
            "/api/forecasts?lat=28.6&lng=77.2&radius_km=0",
            "/api/alerts?min_severity=9",
        ):
            with self.subTest(target=target):
                status, body = self.call(target)
                self.assertEqual(status, 400, body)
                self.assertIn("error", body)

    def test_quality_report(self):
        status, body = self.call("/api/quality")
        self.assertEqual(status, 200)
        self.assertEqual(body["total_observations"], 7)
        self.assertEqual(body["missing_humidity"], 1)

    def test_locations_are_the_distinct_points(self):
        status, body = self.call("/api/locations")
        self.assertEqual(status, 200)
        # NW, WC, Central (21.15, 79.09), Peninsular, Central (26.85, 80.95)
        self.assertEqual(body["count"], 5)
        self.assertTrue(all(item["days_of_history"] >= 1 for item in body["items"]))

    def test_forecast_for_short_history_reports_insufficient_data(self):
        status, body = self.call("/api/forecasts?lat=28.61&lng=77.21&radius_km=50&days=1")
        self.assertEqual(status, 200)
        self.assertGreaterEqual(body["count"], 1)
        item = body["items"][0]
        self.assertEqual(item["status"], "insufficient_data")
        self.assertIsNone(item["forecast"])
        self.assertEqual(item["distance_km"], 0.0)

    def test_forecast_for_unknown_location_is_404(self):
        status, body = self.call("/api/forecasts?location_id=999")
        self.assertEqual(status, 404)

    def test_no_alerts_from_two_days_of_history(self):
        status, body = self.call("/api/alerts")
        self.assertEqual(status, 200)
        self.assertEqual(body["pending"], 0)
        self.assertEqual(body["items"], [])


class HeatwaveEngineTests(DbTestCase):
    """A 21-day history at one point, with a sudden spike on the last day."""

    LOCATION = (28.61, 77.21)

    def setUp(self):
        super().setUp()
        start = date(2025, 5, 1)
        rows = []
        for i in range(21):
            day = start + timedelta(days=i)
            temp = 46.0 if i == 20 else 34.0 + 0.1 * (i % 3)     # calm, then a spike
            rows.append(f"{day.isoformat()},{self.LOCATION[0]},{self.LOCATION[1]},NORTHWEST INDIA,{temp}")
        self.load(self.write_csv("heatwave.csv", rows))
        self.last_day = date(2025, 5, 21).isoformat()

    def test_spike_raises_a_severe_alert_on_the_last_day(self):
        status, body = self.call("/api/alerts?limit=5")
        self.assertEqual(status, 200)
        self.assertEqual(body["pending"], 1)
        top = body["items"][0]
        self.assertEqual(top["severity"], 5)
        self.assertEqual(top["date"], self.last_day)
        self.assertEqual(top["temperature_c"], 46.0)
        self.assertGreater(top["anomaly_c"], 6.5)

    def test_alert_filter_by_minimum_severity(self):
        # the only alert is severity 5, so every threshold up to 5 keeps it
        self.assertEqual(self.call("/api/alerts?min_severity=5")[1]["returned"], 1)
        self.assertEqual(self.call("/api/alerts?min_severity=1")[1]["returned"], 1)

    def test_alert_filter_drops_alerts_below_threshold(self):
        # a milder spike at a second location: 38.5 C is 4.5 C above normal but under 40 C -> severity 2
        rows = [f"2025-06-{d:02d},9.9,77.0,SOUTH TEST,{34.0 if d < 25 else 38.5}" for d in range(1, 26)]
        self.load(self.write_csv("mild.csv", rows))
        self.assertEqual(self.call("/api/alerts?min_severity=5")[1]["returned"], 1)
        all_alerts = self.call("/api/alerts?limit=10")[1]["items"]
        self.assertTrue(any(a["severity"] < 5 for a in all_alerts))
        self.assertTrue(all(a["severity"] >= 5 for a in self.call("/api/alerts?min_severity=5")[1]["items"]))

    def test_reading_the_alerts_does_not_drain_them(self):
        first = self.call("/api/alerts")[1]
        second = self.call("/api/alerts")[1]
        self.assertEqual(first, second)
        self.assertEqual(second["pending"], 1)

    def test_forecast_uses_the_last_day_as_today(self):
        status, body = self.call(
            f"/api/forecasts?location_id=1&days=2"
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["as_of"], self.last_day)
        item = body["items"][0]
        self.assertEqual(item["status"], "ok")
        self.assertEqual(item["target_date"], (date(2025, 5, 21) + timedelta(days=2)).isoformat())
        self.assertIn(item["forecast"]["severity"], range(1, 6))
        self.assertGreater(item["current_anomaly_c"], 6.0)

    def test_forecast_near_point_ranks_by_severity(self):
        status, body = self.call("/api/forecasts?lat=28.61&lng=77.21&radius_km=100&days=1")
        self.assertEqual(status, 200)
        self.assertEqual(body["items"][0]["location_id"], 1)

    def test_locations_show_history_length(self):
        status, body = self.call("/api/locations")
        self.assertEqual(body["count"], 1)
        self.assertEqual(body["items"][0]["days_of_history"], 21)


class EngineCacheTests(DbTestCase):
    def test_engine_is_rebuilt_after_an_ingestion_run(self):
        self.load(config.SAMPLE_IMD_FILE)
        first = self.ctx.engine()
        self.assertEqual(len(first.locations), 5)

        extra = self.write_csv("extra.csv", ["2025-05-25,15.5,74.5,GOA,33.0"])
        self.load(extra)
        second = self.ctx.engine()

        self.assertIsNot(first, second)
        self.assertEqual(len(second.locations), 6)

    def test_engine_is_reused_when_nothing_changed(self):
        self.load(config.SAMPLE_IMD_FILE)
        self.assertIs(self.ctx.engine(), self.ctx.engine())

    def test_build_engine_on_empty_database(self):
        engine = build_engine(self.db)
        self.assertEqual(engine.locations, {})
        self.assertIsNone(engine.as_of_day)
        self.assertEqual(self.call("/api/forecasts?lat=28&lng=77")[1]["count"], 0)


class RoutingTests(DbTestCase):
    def test_unknown_route_is_404(self):
        status, body = self.call("/api/nope")
        self.assertEqual(status, 404)
        self.assertIn("/api/forecasts", body["routes"])

    def test_root_is_404(self):
        self.assertEqual(self.call("/")[0], 404)

    def test_non_get_is_405(self):
        for method in ("POST", "PUT", "DELETE"):
            with self.subTest(method=method):
                self.assertEqual(self.call("/api/health", method=method)[0], 405)

    def test_trailing_slash_is_accepted(self):
        self.assertEqual(self.call("/api/health/")[0], 200)


class HttpServerTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db = Path(self._tmp.name) / "http.db"
        conn = connect(self.db)
        init_db(conn)
        ingest_imd_file(conn, config.SAMPLE_IMD_FILE, now=NOW)
        conn.close()

    def tearDown(self):
        self._tmp.cleanup()

    def test_missing_database_is_refused(self):
        missing = Path(self._tmp.name) / "does_not_exist.db"
        with self.assertRaises(FileNotFoundError):
            make_server(missing, port=0)

    def test_round_trip_over_http(self):
        server = make_server(self.db, host="127.0.0.1", port=0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_address[1]
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/regions") as resp:
                self.assertEqual(resp.status, 200)
                self.assertEqual(resp.headers["Access-Control-Allow-Origin"], "*")
                body = json.loads(resp.read())
            self.assertEqual(body["count"], 4)

            request = urllib.request.Request(f"http://127.0.0.1:{port}/api/health", method="POST")
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(request)
            self.assertEqual(ctx.exception.code, 405)
            self.assertEqual(ctx.exception.headers["Allow"], "GET")
        finally:
            server.shutdown()
            server.server_close()


class Person1ChecksTests(unittest.TestCase):
    """Runs Person 1's own test script (unchanged apart from package-relative imports)."""

    def test_person1_suite_passes(self):
        import subprocess

        result = subprocess.run(
            [sys.executable, "-m", "p1_core.test_p1"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=300,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
