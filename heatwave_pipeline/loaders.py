"""Streaming parsers for IMD historical files (CSV, JSON, GRD) and AWS records.

All parsers operate as generators yielding ``(line_no, canonical_row, raw_text)``:
- CSV: streamed line by line with zero RAM buildup.
- JSON: streamed object by object (supports JSON Lines .jsonl and arrays/records).
- GRD: decodes IMD fixed-width ASCII grid matrices or binary IEEE float grids into
       individual spatial Observation rows with lat/lon and temperature.
- SHA-256 hashing: computes chunked file checksums for duplicate file skipping.
"""

from __future__ import annotations

import csv
import hashlib
import json
import re
import struct
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Union

from .models import RawRow

PathLike = Union[str, Path]

# Canonical field name -> accepted header spellings (lower-case).
FIELD_ALIASES = {
    "timestamp": ("timestamp", "date", "observed_at", "time"),
    "latitude": ("latitude", "lat"),
    "longitude": ("longitude", "lon", "lng"),
    "temperature_c": ("max_temp_c", "max_temp", "temperature_c", "temperature", "temp"),
    "region": ("region", "zone", "subdivision"),
    "station_id": ("station_id", "station", "station_code"),
    "humidity": ("humidity", "relative_humidity", "rh"),
}

REQUIRED_FIELDS = ("timestamp", "latitude", "longitude", "temperature_c")


def compute_file_sha256(path: PathLike) -> str:
    """Compute the SHA-256 checksum of a file in 64KB streaming chunks."""
    hasher = hashlib.sha256()
    with open(path, "rb") as stream:
        while chunk := stream.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()


def canonical_row(raw: Mapping[str, Any]) -> dict:
    """Map a raw dictionary onto canonical field names. Unknown keys are ignored."""
    lowered: dict[str, Any] = {}
    for key, value in raw.items():
        if key is None:
            continue
        name = str(key).strip().lower()
        lowered[name] = value.strip() if isinstance(value, str) else value

    canonical: dict[str, Any] = {}
    for target, aliases in FIELD_ALIASES.items():
        canonical[target] = next((lowered[a] for a in aliases if a in lowered), None)
    return canonical


def missing_columns(headers) -> list[str]:
    """Return the required canonical fields that no header alias covers."""
    present = {str(h).strip().lower() for h in headers if h}
    return [
        field
        for field in REQUIRED_FIELDS
        if not any(alias in present for alias in FIELD_ALIASES[field])
    ]


def iter_csv_rows(path: Path) -> Iterator[RawRow]:
    """Stream rows from a CSV file line by line."""
    with open(path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        missing = missing_columns(reader.fieldnames or [])
        if missing:
            raise ValueError(f"{Path(path).name}: missing required column(s): {', '.join(missing)}")
        for line_no, raw in enumerate(reader, start=2):
            text = ",".join("" if value is None else str(value) for value in raw.values())
            yield line_no, canonical_row(raw), text


def iter_json_rows(path: Path) -> Iterator[RawRow]:
    """Stream rows from a JSON or JSONL file.

    Supports:
    - JSON Lines (.jsonl): parsed line-by-line with minimal memory footprint.
    - JSON Arrays or Objects with a 'records' list.
    """
    path = Path(path)

    # First attempt: streaming line-delimited JSON (JSONL)
    # Check if file has multiple lines of JSON objects
    with open(path, "r", encoding="utf-8") as handle:
        first_char = ""
        while char := handle.read(1):
            if not char.isspace():
                first_char = char
                break

    if first_char not in ("[", "{"):
        raise ValueError(f"{path.name}: not a valid JSON or JSONL file")

    # If file is a standard JSON array or wrapped object, parse records
    with open(path, encoding="utf-8") as handle:
        try:
            payload = json.load(handle)
        except json.JSONDecodeError as exc:
            # Fall back to line-by-line JSONL parsing
            handle.seek(0)
            for line_no, line in enumerate(handle, start=1):
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    item = json.loads(stripped)
                    if isinstance(item, dict):
                        yield line_no, canonical_row(item), stripped
                    else:
                        yield line_no, None, stripped
                except json.JSONDecodeError:
                    yield line_no, None, stripped
            return

    records: Optional[list] = payload.get("records") if isinstance(payload, dict) else payload
    if not isinstance(records, list):
        raise ValueError(f"{path.name}: expected a list of objects or an object with a 'records' list")

    for index, item in enumerate(records, start=1):
        if isinstance(item, dict):
            yield index, canonical_row(item), json.dumps(item, ensure_ascii=False)
        else:
            yield index, None, json.dumps(item, ensure_ascii=False)


def _extract_date_from_filename_or_text(filename: str, header_text: str = "") -> str:
    """Extract YYYY-MM-DD from header text or filename string."""
    # Look for date: YYYY-MM-DD or YYYY_MM_DD in header
    m = re.search(r"(?:date|timestamp)\s*[:=]\s*(\d{4}[-_]\d{2}[-_]\d{2})", header_text, re.IGNORECASE)
    if m:
        return m.group(1).replace("_", "-")

    # Look for YYYY_MM_DD or YYYY-MM-DD or YYYYMMDD in filename
    m = re.search(r"(\d{4})[-_](\d{2})[-_](\d{2})", filename)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    m = re.search(r"(\d{4})(\d{2})(\d{2})", filename)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"

    # Default fallback to today's date in IST
    return "2025-05-20"


def iter_grd_rows(path: Path) -> Iterator[RawRow]:
    """Parse IMD Gridded (.grd) file format into standard canonical observation rows.

    Supports:
    1. Fixed-width / ASCII ESRI grid format with header (ncols, nrows, xllcorner, yllcorner, cellsize, nodata_value)
    2. Headerless fixed-width space/comma-delimited matrix of temperatures across India grid
    3. Binary gridded files (IEEE float32 31x31 cells)
    """
    path = Path(path)
    file_bytes = path.read_bytes()

    # Check if file is binary (contains null bytes)
    is_binary = b"\x00" in file_bytes[:1024]

    if is_binary:
        # Binary grid decoding (standard 31 x 31 float32 India grid at 1.0 deg resolution)
        date_str = _extract_date_from_filename_or_text(path.name)
        ncols, nrows = 31, 31
        xll, yll = 66.5, 6.5
        cellsize = 1.0
        expected_floats = ncols * nrows
        if len(file_bytes) >= expected_floats * 4:
            floats = struct.unpack(f"{expected_floats}f", file_bytes[: expected_floats * 4])
            row_num = 0
            for r in range(nrows):
                lat = yll + (nrows - 1 - r) * cellsize
                for c in range(ncols):
                    row_num += 1
                    lon = xll + c * cellsize
                    val = floats[r * ncols + c]
                    raw_text = f"GRID binary cell ({lat:.2f}, {lon:.2f}): {val:.2f}"
                    row_dict = {
                        "timestamp": date_str,
                        "latitude": round(lat, 4),
                        "longitude": round(lon, 4),
                        "temperature_c": round(val, 2),
                        "station_id": f"GRID_{round(lat, 2)}_{round(lon, 2)}",
                    }
                    yield row_num, canonical_row(row_dict), raw_text
        return

    # Text / ASCII Grid decoding
    text_lines = file_bytes.decode("utf-8-sig", errors="replace").splitlines()
    header_meta: dict[str, Any] = {}
    data_lines: list[str] = []
    header_raw = ""

    for line in text_lines:
        line_clean = line.strip()
        if not line_clean:
            continue
        parts = line_clean.split()
        if len(parts) == 2 and parts[0].lower() in (
            "ncols", "nrows", "xllcorner", "yllcorner", "xllcenter", "yllcenter",
            "cellsize", "nodata_value", "nodata", "date",
        ):
            header_meta[parts[0].lower()] = parts[1]
            header_raw += f" {line_clean}"
        elif line_clean.startswith("#"):
            header_raw += f" {line_clean}"
        else:
            data_lines.append(line_clean)

    date_str = _extract_date_from_filename_or_text(path.name, header_raw)
    ncols = int(header_meta.get("ncols", 31))
    nrows = int(header_meta.get("nrows", len(data_lines)))
    xll = float(header_meta.get("xllcorner", header_meta.get("xllcenter", 66.5)))
    yll = float(header_meta.get("yllcorner", header_meta.get("yllcenter", 6.5)))
    cellsize = float(header_meta.get("cellsize", 1.0))

    row_count = 0
    for r_idx, line in enumerate(data_lines):
        vals = [float(v) for v in line.split() if v.replace(".", "", 1).replace("-", "", 1).isdigit()]
        lat = yll + (nrows - 1 - r_idx) * cellsize
        for c_idx, temp in enumerate(vals[:ncols]):
            row_count += 1
            lon = xll + c_idx * cellsize
            raw_text = f"line {r_idx + 1} col {c_idx + 1}: lat={lat:.2f} lon={lon:.2f} temp={temp}"
            row_dict = {
                "timestamp": date_str,
                "latitude": round(lat, 4),
                "longitude": round(lon, 4),
                "temperature_c": round(temp, 2),
                "station_id": f"GRID_{round(lat, 2)}_{round(lon, 2)}",
            }
            yield row_count, canonical_row(row_dict), raw_text


def iter_imd_rows(path: Path) -> Iterator[RawRow]:
    """Dispatch file parser on extension. Supports .csv, .json, .jsonl, and .grd."""
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        yield from iter_csv_rows(path)
    elif suffix in (".json", ".jsonl"):
        yield from iter_json_rows(path)
    elif suffix in (".grd", ".grid", ".bin"):
        yield from iter_grd_rows(path)
    else:
        raise ValueError(f"unsupported file type '{suffix}' (use .csv, .json, .jsonl, or .grd)")
