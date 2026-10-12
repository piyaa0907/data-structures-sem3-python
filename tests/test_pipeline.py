"""Tests for the Person 2 data pipeline and database layer.

Covers:
- Single-row boundary and validity rules.
- Stateful Meteorological QC: Step Check (Rate of change >10°C/hr) and Flatline Check (6+ identical readings).
- Streaming CSV, JSON, and IMD Gridded (.grd) loaders.
- SHA-256 duplicate file detection and automatic skipping.
- Enhanced rejections table schema metadata verification.
- AWS Telemetry simulation, network anomalies (loss, jitter, retransmission), and temporal sorting.
- SQLite WAL mode concurrency during active writes.
- Person 1 typed index initialization and incremental real-time observation listener handoff.
- Database diagnostics (db-stats) and vacuuming.

Run from the project root:
    python -m unittest discover -s tests -v
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from heatwave_pipeline import config
from heatwave_pipeline import validation as v
from heatwave_pipeline.aws_simulator import (
    DEFAULT_STATIONS,
    ReadingQueue,
    apply_network_anomalies,
    generate_readings,
    run_aws_simulation,
)
from heatwave_pipeline.cleaning import clean_row, region_for_coordinates, season_for
from heatwave_pipeline.database import connect, get_db_stats, init_db, vacuum_db
from heatwave_pipeline.ingestion import ingest_aws_readings, ingest_imd_file
from heatwave_pipeline.loaders import compute_file_sha256, iter_csv_rows, iter_grd_rows, iter_json_rows
from heatwave_pipeline.quality_report import build_quality_report, render_text
from heatwave_pipeline.repository import (
    clear_observation_listeners,
    initialise_indexes,
    iter_observations,
    list_regions,
    register_observation_listener,
    unregister_observation_listener,
)

# Fixed "current time" so the future-date rule is deterministic
NOW = datetime(2026, 10, 12, 12, 0, 0)


def base_row(**overrides):
    """A valid canonical row, with optional field overrides."""
    row = {
        "timestamp": "2025-05-20",
        "latitude": "28.61",
        "longitude": "77.21",
        "region": "NORTHWEST INDIA",
        "temperature_c": "44.2",
        "humidity": "22",
        "station_id": None,
    }
    row.update(overrides)
    return row


class ValidationTests(unittest.TestCase):
    def test_valid_row_passes(self):
        self.assertEqual(v.validate_row(base_row(), NOW), (None, ""))

    def test_missing_temperature_rejected(self):
        reason, _ = v.validate_row(base_row(temperature_c=""), NOW)
        self.assertEqual(reason, v.REASON_MISSING)

    def test_sentinel_value_rejected(self):
        reason, _ = v.validate_row(base_row(temperature_c="-999"), NOW)
        self.assertEqual(reason, v.REASON_SENTINEL)

    def test_temperature_out_of_range_rejected(self):
        reason, _ = v.validate_row(base_row(temperature_c="75"), NOW)
        self.assertEqual(reason, v.REASON_TEMP_RANGE)

    def test_point_outside_india_rejected(self):
        reason, _ = v.validate_row(base_row(latitude="51.5", longitude="-0.12"), NOW)
        self.assertEqual(reason, v.REASON_OUTSIDE_INDIA)

    def test_future_timestamp_rejected(self):
        reason, _ = v.validate_row(base_row(timestamp="2026-12-01"), NOW)
        self.assertEqual(reason, v.REASON_FUTURE)

    def test_impossible_date_rejected(self):
        reason, _ = v.validate_row(base_row(timestamp="2025-02-31"), NOW)
        self.assertEqual(reason, v.REASON_BAD_TIMESTAMP)

    def test_non_numeric_rejected(self):
        reason, _ = v.validate_row(base_row(temperature_c="hot"), NOW)
        self.assertEqual(reason, v.REASON_NOT_NUMERIC)

    def test_humidity_out_of_range_rejected(self):
        reason, _ = v.validate_row(base_row(humidity="120"), NOW)
        self.assertEqual(reason, v.REASON_HUMIDITY_RANGE)

    def test_missing_humidity_is_allowed(self):
        self.assertEqual(v.validate_row(base_row(humidity=""), NOW), (None, ""))

    def test_non_object_row_is_malformed(self):
        reason, _ = v.validate_row(None, NOW)
        self.assertEqual(reason, v.REASON_MALFORMED)

    def test_timezone_aware_timestamp_converted_to_ist(self):
        moment = v.parse_timestamp("2025-05-20T10:00:00+00:00")
        self.assertEqual(moment, datetime(2025, 5, 20, 15, 30))

    # Meteorological QC Tests
    def test_rate_of_change_qc_rejected(self):
        qc = v.StationQCValidator()
        r1 = base_row(timestamp="2025-05-20T10:00:00", station_id="AWS-DEL-01", temperature_c="30.0")
        r2 = base_row(timestamp="2025-05-20T10:30:00", station_id="AWS-DEL-01", temperature_c="45.0")  # +15°C in 30min (>10°C/hr)

        res1, _ = v.validate_row(r1, NOW, qc)
        self.assertIsNone(res1)

        res2, detail = v.validate_row(r2, NOW, qc)
        self.assertEqual(res2, v.REASON_RATE_OF_CHANGE)
        self.assertIn("rate-of-change exceeded", detail)

    def test_rate_of_change_qc_allowed_after_window(self):
        qc = v.StationQCValidator()
        r1 = base_row(timestamp="2025-05-20T10:00:00", station_id="AWS-DEL-01", temperature_c="30.0")
        r2 = base_row(timestamp="2025-05-20T11:30:00", station_id="AWS-DEL-01", temperature_c="45.0")  # 90min later (>1 hour window)

        self.assertIsNone(v.validate_row(r1, NOW, qc)[0])
        self.assertIsNone(v.validate_row(r2, NOW, qc)[0])

    def test_sensor_persistence_flatline_qc_rejected(self):
        qc = v.StationQCValidator()
        station = "AWS-NAG-01"
        temp = "38.5"

        # Readings 1 to 5 must pass
        for i in range(1, 6):
            row = base_row(timestamp=f"2025-05-20T10:{i:02d}:00", station_id=station, temperature_c=temp)
            reason, _ = v.validate_row(row, NOW, qc)
            self.assertIsNone(reason, f"Reading {i} should pass")

        # 6th consecutive identical reading must be rejected as flatline
        row6 = base_row(timestamp="2025-05-20T10:06:00", station_id=station, temperature_c=temp)
        reason6, detail6 = v.validate_row(row6, NOW, qc)
        self.assertEqual(reason6, v.REASON_SENSOR_PERSISTENCE)
        self.assertIn("sensor flatline", detail6)

        # 7th consecutive identical reading must also be rejected
        row7 = base_row(timestamp="2025-05-20T10:07:00", station_id=station, temperature_c=temp)
        self.assertEqual(v.validate_row(row7, NOW, qc)[0], v.REASON_SENSOR_PERSISTENCE)

        # Reading with changed temperature resets flatline tracker and passes
        row8 = base_row(timestamp="2025-05-20T10:08:00", station_id=station, temperature_c="39.0")
        self.assertIsNone(v.validate_row(row8, NOW, qc)[0])


class CleaningTests(unittest.TestCase):
    def test_clean_row_normalises_fields(self):
        row = base_row(
            timestamp="2025-05-20",
            latitude="28.61391234",
            temperature_c="44.25",
            humidity="",
            region="  north   west india ",
        )
        obs = clean_row(row, "IMD", "DAILY_MAX")
        self.assertEqual(obs.observed_at, "2025-05-20T00:00:00")
        self.assertEqual(obs.latitude, 28.6139)
        self.assertEqual(obs.temperature_c, 44.25)
        self.assertIsNone(obs.humidity)
        self.assertEqual(obs.region, "NORTH WEST INDIA")
        self.assertEqual(obs.season, "PRE_MONSOON")

    def test_season_mapping_covers_four_seasons(self):
        self.assertEqual(season_for(datetime(2025, 1, 5)), "WINTER")
        self.assertEqual(season_for(datetime(2025, 4, 5)), "PRE_MONSOON")
        self.assertEqual(season_for(datetime(2025, 7, 5)), "MONSOON")
        self.assertEqual(season_for(datetime(2025, 10, 5)), "POST_MONSOON")

    def test_region_for_coordinates_mapping(self):
        self.assertEqual(region_for_coordinates(28.61, 77.21), "NORTHWEST INDIA")
        self.assertEqual(region_for_coordinates(19.07, 72.87), "WEST CENTRAL INDIA")
        self.assertEqual(region_for_coordinates(21.14, 79.08), "CENTRAL INDIA")
        self.assertEqual(region_for_coordinates(13.08, 80.27), "PENINSULAR INDIA")


class LoaderTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)

    def tearDown(self):
        self._tmp.cleanup()

    def test_csv_header_aliases_are_mapped(self):
        path = self.dir / "aliases.csv"
        path.write_text(
            "date,lat,lon,max_temp,region\n2025-05-20,28.6,77.2,44.0,NORTHWEST INDIA\n",
            encoding="utf-8",
        )
        (line_no, row, _raw), = list(iter_csv_rows(path))
        self.assertEqual(line_no, 2)
        self.assertEqual(row["timestamp"], "2025-05-20")
        self.assertEqual(row["temperature_c"], "44.0")

    def test_csv_missing_required_column_raises(self):
        path = self.dir / "bad_header.csv"
        path.write_text("date,lat,lon\n2025-05-20,28.6,77.2\n", encoding="utf-8")
        with self.assertRaises(ValueError):
            list(iter_csv_rows(path))

    def test_json_records_wrapper_and_non_object_rows(self):
        path = self.dir / "data.json"
        payload = {"records": [{"timestamp": "2025-05-20", "lat": 28.6, "lon": 77.2, "max_temp_c": 44}, 5]}
        path.write_text(json.dumps(payload), encoding="utf-8")
        rows = list(iter_json_rows(path))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][1]["temperature_c"], 44)
        self.assertIsNone(rows[1][1])

    def test_grd_file_parsing(self):
        path = self.dir / "sample_2025_05_20.grd"
        path.write_text(
            "ncols 2\nnrows 2\nxllcorner 70.0\nyllcorner 20.0\ncellsize 1.0\nnodata_value -999.0\n"
            "42.5 43.1\n41.0 41.8\n",
            encoding="utf-8",
        )
        rows = list(iter_grd_rows(path))
        self.assertEqual(len(rows), 4)
        first_row = rows[0][1]
        self.assertEqual(first_row["timestamp"], "2025-05-20")
        self.assertEqual(first_row["latitude"], 21.0)
        self.assertEqual(first_row["longitude"], 70.0)
        self.assertEqual(first_row["temperature_c"], 42.5)

    def test_sha256_checksum_computation(self):
        path = self.dir / "test_file.txt"
        path.write_text("heatwave test payload", encoding="utf-8")
        h1 = compute_file_sha256(path)
        h2 = compute_file_sha256(path)
        self.assertEqual(h1, h2)
        self.assertEqual(len(h1), 64)


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.conn = connect(Path(self._tmp.name) / "test.db")
        init_db(self.conn)

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()

    def count(self, table):
        return self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

    def test_sample_file_counts(self):
        result = ingest_imd_file(self.conn, config.SAMPLE_IMD_FILE, now=NOW)
        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual(result.records_read, 14)
        self.assertEqual(result.inserted, 7)
        self.assertEqual(result.duplicates, 1)   # repeated NORTHWEST row in the same file
        self.assertEqual(result.rejected, 6)
        self.assertEqual(self.count("observations"), 7)

    def test_rejection_reasons_are_stored(self):
        ingest_imd_file(self.conn, config.SAMPLE_IMD_FILE, now=NOW)
        reasons = {
            r["reason"]: r["n"]
            for r in self.conn.execute(
                "SELECT reason, COUNT(*) AS n FROM rejected_records GROUP BY reason"
            )
        }
        self.assertEqual(
            reasons,
            {
                v.REASON_SENTINEL: 1,
                v.REASON_MISSING: 1,
                v.REASON_OUTSIDE_INDIA: 1,
                v.REASON_BAD_TIMESTAMP: 1,
                v.REASON_NOT_NUMERIC: 1,
                v.REASON_FUTURE: 1,
            },
        )

    def test_enhanced_rejections_table_fields(self):
        ingest_imd_file(self.conn, config.SAMPLE_IMD_FILE, now=NOW)
        rej_row = self.conn.execute(
            """SELECT file_or_stream_id, row_number, field_name, rejected_value, reason_code, timestamp
                 FROM rejections WHERE reason_code = ?""",
            (v.REASON_SENTINEL,),
        ).fetchone()

        self.assertIsNotNone(rej_row)
        self.assertEqual(rej_row["file_or_stream_id"], Path(config.SAMPLE_IMD_FILE).name)
        self.assertEqual(rej_row["row_number"], 9)
        self.assertEqual(rej_row["field_name"], "temperature_c")
        self.assertEqual(rej_row["rejected_value"], "-999.0")
        self.assertEqual(rej_row["reason_code"], v.REASON_SENTINEL)
        self.assertEqual(rej_row["timestamp"], "2025-05-21")

    def test_sha256_duplicate_file_skips_reingestion(self):
        res1 = ingest_imd_file(self.conn, config.SAMPLE_IMD_FILE, now=NOW)
        self.assertEqual(res1.status, "SUCCESS")
        self.assertEqual(res1.inserted, 7)

        # Second ingestion of the unaltered file without force=True must automatically skip
        res2 = ingest_imd_file(self.conn, config.SAMPLE_IMD_FILE, now=NOW)
        self.assertEqual(res2.status, "SUCCESS")
        self.assertEqual(res2.inserted, 0)
        self.assertIn("Skipped: file", res2.message)
        self.assertEqual(self.count("observations"), 7)

        # Force re-ingestion re-evaluates and flags duplicate rows
        res3 = ingest_imd_file(self.conn, config.SAMPLE_IMD_FILE, now=NOW, force=True)
        self.assertEqual(res3.status, "SUCCESS")
        self.assertEqual(res3.inserted, 0)
        self.assertEqual(res3.duplicates, 8)

    def test_failed_run_is_rolled_back_and_logged(self):
        path = Path(self._tmp.name) / "no_temperature.csv"
        path.write_text(
            "timestamp,latitude,longitude,region\n2025-05-20,28.6,77.2,NORTHWEST INDIA\n",
            encoding="utf-8",
        )
        result = ingest_imd_file(self.conn, path, now=NOW)
        self.assertEqual(result.status, "FAILED")
        self.assertEqual(self.count("observations"), 0)
        status = self.conn.execute("SELECT status FROM ingestion_log WHERE id = ?", (result.log_id,)).fetchone()[0]
        self.assertEqual(status, "FAILED")

    def test_aws_counts_always_add_up(self):
        readings = list(
            generate_readings(start=datetime(2026, 10, 11), steps=12, fault_rate=0.3, seed=7)
        )
        result = ingest_aws_readings(self.conn, readings, now=NOW)
        self.assertEqual(result.records_read, len(readings))
        self.assertEqual(
            result.inserted + result.duplicates + result.rejected, result.records_read
        )

    def test_retransmitted_message_is_a_duplicate(self):
        reading = next(generate_readings(start=datetime(2026, 10, 11), steps=1, fault_rate=0.0))
        result = ingest_aws_readings(self.conn, [reading, dict(reading)], now=NOW)
        self.assertEqual(result.inserted, 1)
        self.assertEqual(result.duplicates, 1)


class AWSSimulatorTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.conn = connect(Path(self._tmp.name) / "aws.db")
        init_db(self.conn)

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()

    def test_same_seed_gives_same_readings(self):
        start = datetime(2026, 10, 11)
        first = list(generate_readings(start=start, steps=5, seed=3, fault_rate=0.2))
        second = list(generate_readings(start=start, steps=5, seed=3, fault_rate=0.2))
        self.assertEqual(first, second)

    def test_queue_is_first_in_first_out(self):
        queue = ReadingQueue()
        for item in (1, 2, 3):
            queue.push(item)
        self.assertEqual(len(queue), 3)
        self.assertEqual(list(queue.drain()), [1, 2, 3])
        self.assertEqual(len(queue), 0)

    def test_clean_simulation_inserts_every_reading(self):
        result = run_aws_simulation(self.conn, steps=6, seed=1, fault_rate=0.0, now=NOW)
        expected = 6 * len(DEFAULT_STATIONS)
        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual(result.inserted, expected)
        self.assertEqual(result.rejected, 0)

    def test_network_anomalies_packet_loss_and_duplicates(self):
        import random
        msgs = [{"timestamp": f"2026-10-12T10:{i:02d}:00", "val": i} for i in range(10)]
        # 100% loss
        dropped = apply_network_anomalies(msgs, packet_loss_rate=1.0, out_of_order_rate=0.0, retransmission_rate=0.0, rng=random.Random(1))
        self.assertEqual(len(dropped), 0)

        # 100% duplicate retransmission
        doubled = apply_network_anomalies(msgs, packet_loss_rate=0.0, out_of_order_rate=0.0, retransmission_rate=1.0, rng=random.Random(1))
        self.assertEqual(len(doubled), 20)

    def test_out_of_order_stream_is_sorted_and_ingested(self):
        # Provide out-of-order readings; ingestion sorts them chronologically
        station = "AWS-DEL-01"
        r_late = base_row(timestamp="2025-05-20T11:00:00", station_id=station, temperature_c="36.0")
        r_early = base_row(timestamp="2025-05-20T10:00:00", station_id=station, temperature_c="35.0")

        # Send r_late before r_early
        result = ingest_aws_readings(self.conn, [r_late, r_early], now=NOW)
        self.assertEqual(result.inserted, 2)
        rows = list(iter_observations(self.conn, station_id=station))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].observed_at, "2025-05-20T10:00:00")
        self.assertEqual(rows[1].observed_at, "2025-05-20T11:00:00")


class RepositoryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.conn = connect(Path(self._tmp.name) / "repo.db")
        init_db(self.conn)
        ingest_imd_file(self.conn, config.SAMPLE_IMD_FILE, now=NOW)

    def tearDown(self):
        clear_observation_listeners()
        self.conn.close()
        self._tmp.cleanup()

    def test_filter_by_region_is_ordered_by_time(self):
        rows = list(iter_observations(self.conn, region="northwest india"))
        self.assertEqual(
            [r.observed_at for r in rows],
            ["2025-05-20T00:00:00", "2025-05-21T00:00:00"],
        )

    def test_time_window_is_inclusive(self):
        rows = list(iter_observations(self.conn, start="2025-05-21", end="2025-05-21"))
        self.assertEqual(len(rows), 2)

    def test_list_regions(self):
        self.assertEqual(
            list_regions(self.conn),
            ["CENTRAL INDIA", "NORTHWEST INDIA", "PENINSULAR INDIA", "WEST CENTRAL INDIA"],
        )

    def test_initialise_indexes_feeds_each_index_once(self):
        class Recorder:
            def __init__(self):
                self.items = []

            def insert(self, record):
                self.items.append(record)

        spatial, temporal = Recorder(), Recorder()
        counts = initialise_indexes(self.conn, spatial=spatial, temporal=temporal)
        self.assertEqual(counts, {"spatial": 7, "temporal": 7})
        self.assertEqual(spatial.items[0].observed_at, "2025-05-20T00:00:00")
        self.assertEqual(len(temporal.items), 7)

    def test_initialise_indexes_validates_sink_protocol(self):
        with self.assertRaises(TypeError):
            initialise_indexes(self.conn, spatial="not_a_sink")

    def test_incremental_observation_listener_handoff(self):
        received = []

        def listener(record):
            received.append(record)

        register_observation_listener(listener)
        reading = base_row(timestamp="2025-05-20T12:00:00", station_id="AWS-DEL-LIVE", temperature_c="37.5")
        ingest_aws_readings(self.conn, [reading], now=NOW)

        self.assertEqual(len(received), 1)
        self.assertEqual(received[0].station_id, "AWS-DEL-LIVE")
        self.assertEqual(received[0].temperature_c, 37.5)

        # Unregister and check no further calls
        unregister_observation_listener(listener)
        reading2 = base_row(timestamp="2025-05-20T13:00:00", station_id="AWS-DEL-LIVE", temperature_c="38.0")
        ingest_aws_readings(self.conn, [reading2], now=NOW)
        self.assertEqual(len(received), 1)


class QualityReportTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.conn = connect(Path(self._tmp.name) / "report.db")
        init_db(self.conn)
        ingest_imd_file(self.conn, config.SAMPLE_IMD_FILE, now=NOW)

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()

    def test_totals_and_yield(self):
        report = build_quality_report(self.conn)
        self.assertEqual(report["total_observations"], 7)
        self.assertEqual(report["by_source"], {"IMD": 7})
        self.assertEqual(report["ingestion_totals"]["inserted"], 7)
        self.assertEqual(report["ingestion_totals"]["duplicates"], 1)
        self.assertEqual(report["ingestion_totals"]["rejected"], 6)
        self.assertEqual(report["clean_yield_pct"], 50.0)
        self.assertEqual(report["missing_humidity"], 1)
        self.assertEqual(sum(report["rejections_by_reason"].values()), 6)

    def test_text_render_contains_key_sections(self):
        text = render_text(build_quality_report(self.conn))
        self.assertIn("Stored observations", text)
        self.assertIn("Rejections by reason", text)
        self.assertIn("NORTHWEST INDIA", text)


class DatabaseConcurrencyAndMaintenanceTests(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmp.name) / "wal_test.db"
        self.conn = connect(self.db_path)
        init_db(self.conn)

    def tearDown(self):
        self.conn.close()
        self._tmp.cleanup()

    def test_wal_mode_concurrency_during_writes(self):
        """Verify Person 1/3 can read concurrently while Person 2 ingests under WAL mode."""
        stop_signal = threading.Event()
        read_counts = []
        read_errors = []

        def reader_worker():
            reader_conn = connect(self.db_path)
            try:
                while not stop_signal.is_set():
                    # Concurrent read during active writes
                    count = reader_conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
                    read_counts.append(count)
                    time.sleep(0.01)
            except Exception as e:
                read_errors.append(e)
            finally:
                reader_conn.close()

        # Start concurrent reader thread
        reader_thread = threading.Thread(target=reader_worker)
        reader_thread.start()

        # Writer thread performing batches
        try:
            for step in range(10):
                readings = [
                    base_row(
                        timestamp=f"2025-05-20T{step:02d}:{min_step:02d}:00",
                        station_id=f"AWS-CONCUR-{min_step}",
                        temperature_c=str(30.0 + step),
                    )
                    for min_step in range(5)
                ]
                ingest_aws_readings(self.conn, readings, now=NOW)
                time.sleep(0.02)
        finally:
            stop_signal.set()
            reader_thread.join()

        self.assertEqual(len(read_errors), 0, f"Concurrent reads produced errors: {read_errors}")
        self.assertGreater(len(read_counts), 0)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0], 50)

    def test_db_stats_and_vacuum(self):
        ingest_imd_file(self.conn, config.SAMPLE_IMD_FILE, now=NOW)
        stats = get_db_stats(self.conn, self.db_path)
        self.assertEqual(stats["journal_mode"], "WAL")
        self.assertEqual(stats["observations_total"], 7)
        self.assertGreater(stats["file_size_bytes"], 0)
        self.assertEqual(stats["rejections_count"], 6)

        # Test defragmentation vacuum
        vacuum_db(self.conn)
        post_stats = get_db_stats(self.conn, self.db_path)
        self.assertEqual(post_stats["observations_total"], 7)


if __name__ == "__main__":
    unittest.main()
