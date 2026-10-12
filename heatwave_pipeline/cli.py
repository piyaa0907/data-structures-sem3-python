"""Command-line interface. Run from project root with ``python main.py --help``."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import sys
from pathlib import Path
from typing import Optional, Sequence

from . import config
from .aws_simulator import run_aws_simulation, run_live_aws_simulation
from .database import get_db_stats, open_database, vacuum_db
from .ingestion import IngestionResult, ingest_imd_file
from .quality_report import build_quality_report, render_text, write_json_report
from .repository import iter_observations

log = logging.getLogger("heatwave_pipeline.cli")


def _setup_logging(verbose: bool) -> None:
    config.LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(config.LOG_DIR / "pipeline.log", encoding="utf-8"),
        ],
    )


def _print_result(result: IngestionResult) -> None:
    print(f"[{result.status}] {result.input_name}")
    print(f"  records read : {result.records_read}")
    print(f"  inserted     : {result.inserted}")
    print(f"  duplicates   : {result.duplicates}")
    print(f"  rejected     : {result.rejected}")
    if result.message:
        print(f"  note         : {result.message}")


def cmd_init(args) -> int:
    with open_database(args.db):
        pass
    print(f"Database initialized and optimized (WAL mode enabled): {args.db}")
    return 0


def cmd_ingest_imd(args) -> int:
    with open_database(args.db) as conn:
        result = ingest_imd_file(conn, args.file, force=args.force)
    _print_result(result)
    return 0 if result.status in ("SUCCESS", "SKIPPED") else 1


def cmd_simulate_aws(args) -> int:
    with open_database(args.db) as conn:
        if args.live:
            run_live_aws_simulation(
                conn,
                cadence_seconds=args.tick_seconds,
                fault_rate=args.fault_rate,
                packet_loss_rate=args.packet_loss,
                out_of_order_rate=args.out_of_order,
                retransmission_rate=args.retransmit,
                max_ticks=args.max_ticks,
                seed=args.seed,
            )
            return 0
        else:
            result = run_aws_simulation(
                conn,
                steps=args.steps,
                interval_minutes=args.interval,
                seed=args.seed,
                fault_rate=args.fault_rate,
                packet_loss_rate=args.packet_loss,
                out_of_order_rate=args.out_of_order,
                retransmission_rate=args.retransmit,
            )
            _print_result(result)
            return 0 if result.status == "SUCCESS" else 1


def cmd_report(args) -> int:
    with open_database(args.db) as conn:
        report = build_quality_report(conn)
    print(render_text(report))
    if args.json_out:
        path = write_json_report(report, args.json_out)
        print(f"\nJSON report written to {path}")
    return 0


def cmd_query(args) -> int:
    with open_database(args.db) as conn:
        records = list(
            iter_observations(
                conn,
                source=args.source,
                region=args.region,
                season=args.season,
                station_id=args.station,
                start=args.start,
                end=args.end,
                limit=args.limit or None,
            )
        )
    print(
        f"{'observed_at':<19} {'src':<4} {'station':<12} {'region':<20} "
        f"{'lat':>8} {'lon':>9} {'temp_c':>7} {'hum':>6}"
    )
    for r in records:
        humidity = "-" if r.humidity is None else f"{r.humidity:.1f}"
        print(
            f"{r.observed_at:<19} {r.source:<4} {r.station_id or '-':<12} {r.region or '-':<20} "
            f"{r.latitude:>8.4f} {r.longitude:>9.4f} {r.temperature_c:>7.2f} {humidity:>6}"
        )
    print(f"{len(records)} record(s)")
    return 0


def cmd_db_stats(args) -> int:
    with open_database(args.db) as conn:
        stats = get_db_stats(conn, args.db)

    print("==================================================")
    print("      HEATWAVE DATABASE DIAGNOSTICS & METRICS     ")
    print("==================================================")
    print(f"Database File      : {stats['db_path']}")
    print(f"File Size          : {stats['file_size_bytes']:,} bytes ({stats['file_size_bytes'] / (1024*1024):.2f} MB)")
    if stats["wal_size_bytes"] > 0:
        print(f"WAL File Size      : {stats['wal_size_bytes']:,} bytes ({stats['wal_size_bytes'] / 1024:.1f} KB)")
    print(f"Journal Mode       : {stats['journal_mode']}")
    print(f"Synchronous Mode   : {stats['synchronous']}")
    print(f"Cache Size (pages) : {stats['cache_size_pages']} (~64 MB)")
    print(f"Page Count / Size  : {stats['page_count']} pages @ {stats['page_size']} bytes")
    print("--------------------------------------------------")
    print(f"Total Observations : {stats['observations_total']:,}")
    for src, cnt in stats["observations_by_source"].items():
        print(f"  - {src:<16} : {cnt:,}")
    print(f"Date Span          : {stats['date_span']['first'] or 'N/A'} -> {stats['date_span']['last'] or 'N/A'}")
    print(f"Distinct Days      : {stats['unique_days']}")
    print(f"Distinct Stations  : {stats['unique_stations']}")
    print(f"Distinct Regions   : {stats['unique_regions']}")
    print("--------------------------------------------------")
    print(f"Ingestion Runs     : {stats['ingestion_runs']}")
    print(f"Records Processed  : {stats['records_read']:,}")
    print(f"Successfully Stored: {stats['total_inserted']:,}")
    print(f"Duplicates Skipped : {stats['total_duplicates']:,}")
    print(f"Total Rejections   : {stats['rejections_count']:,}")
    if stats["rejections_by_reason"]:
        print("Rejections by Reason Code:")
        for r_code, cnt in stats["rejections_by_reason"].items():
            print(f"  - {r_code:<28} : {cnt}")
    if stats["rejections_by_field"]:
        print("Rejections by Field:")
        for fld, cnt in stats["rejections_by_field"].items():
            print(f"  - {fld:<28} : {cnt}")
    print("==================================================")
    return 0


def cmd_db_vacuum(args) -> int:
    path = Path(args.db)
    before_size = path.stat().st_size if path.exists() else 0
    with open_database(args.db) as conn:
        print(f"Vacuuming database '{args.db}'...")
        vacuum_db(conn)
    after_size = path.stat().st_size if path.exists() else 0
    saved = before_size - after_size
    print(f"Vacuum complete.")
    print(f"Size before : {before_size:,} bytes ({before_size / 1024:.1f} KB)")
    print(f"Size after  : {after_size:,} bytes ({after_size / 1024:.1f} KB)")
    print(f"Reclaimed   : {saved:,} bytes ({saved / 1024:.1f} KB)")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="heatwave-data",
        description="Data pipeline for the Heatwave Intelligence System (Person 2).",
    )
    parser.add_argument("--db", default=str(config.DEFAULT_DB_PATH), help="SQLite database path")
    parser.add_argument("-v", "--verbose", action="store_true", help="show per-row debug messages")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init", help="create the database, tables, and WAL configuration")

    p = sub.add_parser("ingest-imd", help="load an IMD historical file (.csv, .json, or .grd)")
    p.add_argument("file", help="path to the IMD file")
    p.add_argument("--force", action="store_true", help="re-ingest even if SHA-256 matches an existing log")

    p = sub.add_parser("simulate-aws", help="simulate and ingest real-time AWS telemetry")
    p.add_argument("--steps", type=int, default=24, help="number of time steps for batch mode (default 24)")
    p.add_argument("--interval", type=int, default=60, help="minutes between steps (default 60)")
    p.add_argument("--seed", type=int, default=42, help="random seed for reproducibility")
    p.add_argument("--fault-rate", type=float, default=0.02, help="share of corrupted messages (0-1)")
    p.add_argument("--live", action="store_true", help="run continuously in real-time streaming mode")
    p.add_argument("--tick-seconds", type=float, default=2.0, help="cadence between streaming ticks in seconds (default 2.0)")
    p.add_argument("--max-ticks", type=int, default=None, help="maximum streaming ticks before stopping (default infinite)")
    p.add_argument("--packet-loss", type=float, default=0.0, help="network anomaly: packet loss rate (0.0 to 1.0)")
    p.add_argument("--out-of-order", type=float, default=0.0, help="network anomaly: out-of-order delivery rate (0.0 to 1.0)")
    p.add_argument("--retransmit", type=float, default=0.0, help="network anomaly: retransmission duplicate rate (0.0 to 1.0)")

    p = sub.add_parser("report", help="print the data-quality report")
    p.add_argument("--json-out", help="also write the report as JSON to this path")

    p = sub.add_parser("query", help="list cleaned records")
    p.add_argument("--source", choices=["IMD", "AWS", "imd", "aws"])
    p.add_argument("--region")
    p.add_argument("--season", choices=sorted(set(config.SEASON_BY_MONTH.values())))
    p.add_argument("--station")
    p.add_argument("--start", help="inclusive lower bound, e.g. 2025-05-01 or 2025-05-01T06:00")
    p.add_argument("--end", help="inclusive upper bound")
    p.add_argument("--limit", type=int, default=20, help="max rows to print (0 = all)")

    sub.add_parser("db-stats", help="summary of row counts, file sizes, date spans, and rejection metrics")
    sub.add_parser("db-vacuum", help="defragments and optimizes the database file")

    return parser


HANDLERS = {
    "init": cmd_init,
    "ingest-imd": cmd_ingest_imd,
    "simulate-aws": cmd_simulate_aws,
    "report": cmd_report,
    "query": cmd_query,
    "db-stats": cmd_db_stats,
    "db-vacuum": cmd_db_vacuum,
}


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    _setup_logging(args.verbose)
    try:
        return HANDLERS[args.command](args)
    except (OSError, ValueError, sqlite3.Error) as exc:
        log.error("%s", exc)
        print(f"error: {exc}", file=sys.stderr)
        return 1
