"""Simulated real-time Automated Weather Station (AWS) telemetry feed.

Simulates IoT gateways and weather station clusters across India:
- Emits realistic diurnal temperature cycles with Gaussian meteorological noise.
- Injects sensor faults: missing values, sentinels (-999), out-of-range spikes,
  bad timestamps, rate-of-change jumps (>10°C), and sensor flatlines (stuck sensors).
- Injects network anomalies: configurable packet loss, out-of-order delivery (jitter),
  and retransmission duplicates to stress-test pipeline deduplication and sorting.
- Supports both batch simulation and live continuous streaming (``--live`` mode).
"""

from __future__ import annotations

import logging
import math
import random
import time
from collections import deque
from datetime import datetime, timedelta
from typing import Callable, Iterator, Optional

from . import config
from .ingestion import IngestionResult, ingest_aws_readings, now_ist

log = logging.getLogger(__name__)

# Representative weather stations across Indian climate regions
DEFAULT_STATIONS: tuple[dict, ...] = (
    {"station_id": "AWS-DEL-01", "region": "NORTHWEST INDIA", "latitude": 28.6139, "longitude": 77.2090, "mean_temp_c": 36.0, "amplitude_c": 8.0},
    {"station_id": "AWS-JAI-01", "region": "NORTHWEST INDIA", "latitude": 26.9124, "longitude": 75.7873, "mean_temp_c": 35.0, "amplitude_c": 9.0},
    {"station_id": "AWS-NAG-01", "region": "CENTRAL INDIA", "latitude": 21.1458, "longitude": 79.0882, "mean_temp_c": 34.0, "amplitude_c": 8.5},
    {"station_id": "AWS-PUN-01", "region": "WEST CENTRAL INDIA", "latitude": 18.5204, "longitude": 73.8567, "mean_temp_c": 31.0, "amplitude_c": 6.0},
    {"station_id": "AWS-CHE-01", "region": "PENINSULAR INDIA", "latitude": 13.0827, "longitude": 80.2707, "mean_temp_c": 32.0, "amplitude_c": 4.5},
)

FAULT_KINDS = (
    "sentinel",
    "out_of_range",
    "missing",
    "bad_timestamp",
    "retransmit",
    "step_change",
    "flatline",
)


class ReadingQueue:
    """FIFO queue backed by deque, modeling an IoT gateway telemetry buffer."""

    def __init__(self) -> None:
        self._items: deque[dict] = deque()

    def push(self, message: dict) -> None:
        self._items.append(message)

    def pop(self) -> dict:
        return self._items.popleft()

    def drain(self) -> Iterator[dict]:
        """Yield queued messages in arrival order until empty."""
        while self._items:
            yield self._items.popleft()

    def __len__(self) -> int:
        return len(self._items)


def expected_temperature(station: dict, moment: datetime, rng: random.Random) -> float:
    """Calculate expected diurnal temperature peaking at 15:00 IST plus noise."""
    hour = moment.hour + moment.minute / 60.0
    diurnal = station["amplitude_c"] * math.cos(2.0 * math.pi * (hour - 15.0) / 24.0)
    return station["mean_temp_c"] + diurnal + rng.gauss(0.0, 0.5)


def _inject_fault(reading: dict, kind: str) -> dict:
    """Mutate a reading into a known failure condition for QC testing."""
    faulty = dict(reading)
    if kind == "sentinel":
        faulty["temperature_c"] = -999.0
    elif kind == "out_of_range":
        faulty["temperature_c"] = 95.0
    elif kind == "missing":
        faulty["temperature_c"] = None
    elif kind == "bad_timestamp":
        faulty["timestamp"] = "2026-13-45 10:00:00"
    elif kind == "step_change":
        # Sudden 15°C jump violating the 10°C/hr rate-of-change check
        curr = faulty.get("temperature_c") or 35.0
        faulty["temperature_c"] = round(curr + 15.0, 2)
    elif kind == "flatline":
        # Fixed temperature (37.50°C) testing the 6+ consecutive flatline check
        faulty["temperature_c"] = 37.50
    return faulty


def apply_network_anomalies(
    readings: list[dict],
    *,
    packet_loss_rate: float,
    out_of_order_rate: float,
    retransmission_rate: float,
    rng: random.Random,
) -> list[dict]:
    """Model realistic wireless network anomalies on an outgoing telemetry batch.

    1. Packet Loss: drops packets (e.g. weak radio signal).
    2. Retransmissions: duplicates packets (e.g. TCP retry / ACK timeout).
    3. Out-of-order Delivery: swaps or delays packets in transmission order (network jitter).
    """
    output: list[dict] = []
    jitter_buffer: list[dict] = []

    for msg in readings:
        # 1. Packet Loss Check
        if rng.random() < packet_loss_rate:
            continue  # Message dropped on the wire

        # 2. Retransmission Duplicate Check
        if rng.random() < retransmission_rate:
            output.append(dict(msg))  # Emits first copy immediately

        # 3. Out-of-Order Check (Jitter)
        if rng.random() < out_of_order_rate:
            jitter_buffer.append(dict(msg))
        else:
            output.append(dict(msg))
            if jitter_buffer and rng.random() < 0.5:
                output.append(jitter_buffer.pop(0))

    # Flush any remaining delayed jitter messages to the wire
    while jitter_buffer:
        output.append(jitter_buffer.pop(0))

    return output


def generate_readings(
    *,
    start: datetime,
    steps: int,
    stations: tuple[dict, ...] = DEFAULT_STATIONS,
    interval_minutes: int = 60,
    seed: int = 42,
    fault_rate: float = 0.02,
    packet_loss_rate: float = 0.0,
    out_of_order_rate: float = 0.0,
    retransmission_rate: float = 0.0,
) -> Iterator[dict]:
    """Generate simulated readings for multiple stations across time steps."""
    if not (0.0 <= fault_rate <= 1.0):
        raise ValueError("fault_rate must be between 0 and 1")
    if not (0.0 <= packet_loss_rate <= 1.0):
        raise ValueError("packet_loss_rate must be between 0 and 1")
    if not (0.0 <= out_of_order_rate <= 1.0):
        raise ValueError("out_of_order_rate must be between 0 and 1")
    if not (0.0 <= retransmission_rate <= 1.0):
        raise ValueError("retransmission_rate must be between 0 and 1")

    rng = random.Random(seed)
    previous: Optional[dict] = None

    for step in range(steps):
        moment = start + timedelta(minutes=step * interval_minutes)
        step_batch: list[dict] = []

        for station in stations:
            reading = {
                "timestamp": moment.isoformat(timespec="seconds"),
                "station_id": station["station_id"],
                "region": station["region"],
                "latitude": station["latitude"],
                "longitude": station["longitude"],
                "temperature_c": round(expected_temperature(station, moment, rng), 2),
                "humidity": round(rng.uniform(20.0, 90.0), 1),
            }

            kind = rng.choice(FAULT_KINDS) if rng.random() < fault_rate else None
            if kind == "retransmit" and previous is not None:
                step_batch.append(dict(previous))
            elif kind in (None, "retransmit"):
                step_batch.append(reading)
            else:
                step_batch.append(_inject_fault(reading, kind))

            previous = reading

        # Apply network anomalies if configured
        if packet_loss_rate > 0 or out_of_order_rate > 0 or retransmission_rate > 0:
            step_batch = apply_network_anomalies(
                step_batch,
                packet_loss_rate=packet_loss_rate,
                out_of_order_rate=out_of_order_rate,
                retransmission_rate=retransmission_rate,
                rng=rng,
            )

        yield from step_batch


def run_aws_simulation(
    conn,
    *,
    steps: int = 24,
    interval_minutes: int = 60,
    seed: int = 42,
    fault_rate: float = 0.02,
    packet_loss_rate: float = 0.0,
    out_of_order_rate: float = 0.0,
    retransmission_rate: float = 0.0,
    stations: tuple[dict, ...] = DEFAULT_STATIONS,
    now: Optional[datetime] = None,
) -> IngestionResult:
    """Generate and ingest a batch of simulated AWS readings ending just before ``now``."""
    moment = now or now_ist()
    span = timedelta(minutes=steps * interval_minutes)
    start = moment.replace(minute=0, second=0, microsecond=0) - span

    queue = ReadingQueue()
    for reading in generate_readings(
        start=start,
        steps=steps,
        stations=stations,
        interval_minutes=interval_minutes,
        seed=seed,
        fault_rate=fault_rate,
        packet_loss_rate=packet_loss_rate,
        out_of_order_rate=out_of_order_rate,
        retransmission_rate=retransmission_rate,
    ):
        queue.push(reading)

    log.info("Simulated %d AWS telemetry packets from %d stations", len(queue), len(stations))

    return ingest_aws_readings(
        conn,
        queue.drain(),
        input_name=f"simulated-aws-seed{seed}",
        now=moment,
    )


def run_live_aws_simulation(
    conn,
    *,
    cadence_seconds: float = 2.0,
    interval_minutes: int = 15,
    fault_rate: float = 0.03,
    packet_loss_rate: float = 0.02,
    out_of_order_rate: float = 0.02,
    retransmission_rate: float = 0.02,
    max_ticks: Optional[int] = None,
    seed: int = 42,
    stations: tuple[dict, ...] = DEFAULT_STATIONS,
    on_tick: Optional[Callable[[int, IngestionResult, list[dict]], None]] = None,
) -> None:
    """Continuously stream simulated telemetry readings into SQLite in real time.

    Runs until interrupted by KeyboardInterrupt (Ctrl+C) or until ``max_ticks`` is reached.
    """
    rng = random.Random(seed)
    current_time = now_ist()
    tick = 0

    print(f"--- Live AWS Simulation Started (Cadence: {cadence_seconds}s, Stations: {len(stations)}) ---")
    print("Press Ctrl+C at any time to safely stop the simulation.\n")

    try:
        while max_ticks is None or tick < max_ticks:
            tick += 1
            tick_time = current_time + timedelta(minutes=tick * interval_minutes)
            raw_readings: list[dict] = []

            for st in stations:
                temp = expected_temperature(st, tick_time, rng)
                reading = {
                    "timestamp": tick_time.isoformat(timespec="seconds"),
                    "station_id": st["station_id"],
                    "region": st["region"],
                    "latitude": st["latitude"],
                    "longitude": st["longitude"],
                    "temperature_c": round(temp, 2),
                    "humidity": round(rng.uniform(20.0, 85.0), 1),
                }

                if rng.random() < fault_rate:
                    kind = rng.choice(FAULT_KINDS)
                    reading = _inject_fault(reading, kind)

                raw_readings.append(reading)

            # Apply network anomalies
            delivered = apply_network_anomalies(
                raw_readings,
                packet_loss_rate=packet_loss_rate,
                out_of_order_rate=out_of_order_rate,
                retransmission_rate=retransmission_rate,
                rng=rng,
            )

            # Ingest live batch
            result = ingest_aws_readings(
                conn,
                delivered,
                input_name=f"live-aws-tick-{tick}",
                now=tick_time,
            )

            summary = (
                f"[Tick {tick:>3}] {tick_time.strftime('%Y-%m-%d %H:%M:%S')} | "
                f"Generated: {len(raw_readings)} | Delivered: {len(delivered)} | "
                f"Inserted: {result.inserted} | Duplicates: {result.duplicates} | Rejected: {result.rejected}"
            )
            print(summary)

            if on_tick is not None:
                on_tick(tick, result, delivered)

            if cadence_seconds > 0:
                time.sleep(cadence_seconds)

    except KeyboardInterrupt:
        print(f"\n--- Live AWS Simulation Stopped by User after {tick} ticks ---")
