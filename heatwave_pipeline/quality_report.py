"""Data-quality reporting: summaries over stored observations and ingestion logs."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .ingestion import now_ist


def build_quality_report(conn: sqlite3.Connection) -> dict[str, Any]:
    """Collect coverage, counts, rejection reasons and recent runs into one dict."""

    def rows(sql: str, params: tuple = ()) -> list[dict]:
        return [dict(r) for r in conn.execute(sql, params)]

    total = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    by_source = {
        r["source"]: r["n"]
        for r in rows("SELECT source, COUNT(*) AS n FROM observations GROUP BY source ORDER BY source")
    }
    by_region = rows(
        """SELECT region,
                  COUNT(*)                     AS records,
                  ROUND(MIN(temperature_c), 2) AS min_temp_c,
                  ROUND(MAX(temperature_c), 2) AS max_temp_c,
                  ROUND(AVG(temperature_c), 2) AS avg_temp_c
             FROM observations
            GROUP BY region
            ORDER BY region"""
    )
    by_season = {
        r["season"]: r["n"]
        for r in rows("SELECT season, COUNT(*) AS n FROM observations GROUP BY season ORDER BY season")
    }
    span = rows("SELECT MIN(observed_at) AS first, MAX(observed_at) AS last FROM observations")[0]
    missing_humidity = conn.execute(
        "SELECT COUNT(*) FROM observations WHERE humidity IS NULL"
    ).fetchone()[0]

    totals = rows(
        """SELECT COALESCE(SUM(records_read), 0) AS records_read,
                  COALESCE(SUM(inserted), 0)     AS inserted,
                  COALESCE(SUM(duplicates), 0)   AS duplicates,
                  COALESCE(SUM(rejected), 0)     AS rejected
             FROM ingestion_log
            WHERE status = 'SUCCESS'"""
    )[0]
    reasons = {
        r["reason"]: r["n"]
        for r in rows(
            "SELECT reason, COUNT(*) AS n FROM rejected_records GROUP BY reason ORDER BY n DESC, reason"
        )
    }
    rejections_by_field = {
        r["field_name"]: r["n"]
        for r in rows(
            "SELECT field_name, COUNT(*) AS n FROM rejections WHERE field_name <> '' GROUP BY field_name ORDER BY n DESC, field_name"
        )
    }
    recent_runs = rows(
        """SELECT id, run_at, source, input_name, status,
                  records_read, inserted, duplicates, rejected
             FROM ingestion_log
            ORDER BY id DESC
            LIMIT 10"""
    )

    read = totals["records_read"]
    clean_yield = round(100.0 * totals["inserted"] / read, 1) if read else 0.0

    return {
        "generated_at": now_ist().isoformat(timespec="seconds"),
        "total_observations": total,
        "by_source": by_source,
        "by_region": by_region,
        "by_season": by_season,
        "coverage": {"first": span["first"], "last": span["last"]},
        "missing_humidity": missing_humidity,
        "ingestion_totals": totals,
        "clean_yield_pct": clean_yield,
        "rejections_by_reason": reasons,
        "rejections_by_field": rejections_by_field,
        "recent_runs": recent_runs,
    }


def render_text(report: dict[str, Any]) -> str:
    """Human-readable version of the report."""
    totals = report["ingestion_totals"]
    lines = [
        "Heatwave data-quality report",
        f"Generated          : {report['generated_at']}",
        "",
        f"Stored observations: {report['total_observations']}",
    ]
    for source, count in report["by_source"].items():
        lines.append(f"  {source:<6}: {count}")
    lines.append(f"Coverage           : {report['coverage']['first']} -> {report['coverage']['last']}")
    lines.append(f"Missing humidity   : {report['missing_humidity']}")

    lines += [
        "",
        "Ingestion totals (successful runs)",
        f"  read {totals['records_read']}, inserted {totals['inserted']}, "
        f"duplicates {totals['duplicates']}, rejected {totals['rejected']}",
        f"  clean-record yield : {report['clean_yield_pct']}%",
        "",
        "Rejections by reason",
    ]
    if report["rejections_by_reason"]:
        for reason, count in report["rejections_by_reason"].items():
            lines.append(f"  {reason:<28} {count}")
    else:
        lines.append("  none")

    if report.get("rejections_by_field"):
        lines += ["", "Rejections by field"]
        for fld, count in report["rejections_by_field"].items():
            lines.append(f"  {fld:<28} {count}")

    lines += ["", "By region"]
    for row in report["by_region"]:
        label = row["region"] or "(none)"
        lines.append(
            f"  {label:<22} records={row['records']:<5} "
            f"min={row['min_temp_c']} max={row['max_temp_c']} avg={row['avg_temp_c']}"
        )

    lines += ["", "By season"]
    for season, count in report["by_season"].items():
        lines.append(f"  {season:<14} {count}")

    lines += ["", "Recent ingestion runs"]
    if report["recent_runs"]:
        for run in report["recent_runs"]:
            lines.append(
                f"  #{run['id']} {run['run_at']} {run['source']} {run['input_name']} -> {run['status']} "
                f"(inserted {run['inserted']}, duplicates {run['duplicates']}, rejected {run['rejected']})"
            )
    else:
        lines.append("  none")
    return "\n".join(lines)


def write_json_report(report: dict[str, Any], path) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    return path
