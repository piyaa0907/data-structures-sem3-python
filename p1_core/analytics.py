"""
analytics.py - severity classification, simple forecasting, ingestion

No machine learning: a least-squares line through the last few days,
extrapolated with a damped trend. It is cheap, explainable, and uses
the B-tree range query exactly as the design report describes.
"""
from dataclasses import dataclass

from .alerts import Alert


@dataclass
class Forecast:
    predicted: float        # deg C
    confidence: float       # 0..1
    severity: int           # 1..5
    slope: float            # deg C per day over the input window
    mean: float             # mean of the input window


# ------------------------------------------------------------------ severity
def classify_severity(temp, anomaly):
    """1 (normal) .. 5 (severe heatwave). anomaly = deg C above normal.

    Based on IMD's heatwave criteria (plains): heatwave if Tmax >= 40 C and
    4.5 C above normal; severe if 6.5 C above normal; or absolute 45 / 47 C.
    Please double-check the exact numbers against IMD before submitting."""
    if temp >= 47.0 or (temp >= 40.0 and anomaly >= 6.5):
        return 5                                    # severe heatwave
    if temp >= 45.0 or (temp >= 40.0 and anomaly >= 4.5):
        return 4                                    # heatwave
    if temp >= 40.0:
        return 3                                    # very hot
    if temp >= 37.0:
        return 2                                    # watch
    return 1                                        # normal


# ------------------------------------------------------------------ forecast
def forecast_temp(temps, days_ahead):
    """Forecast `days_ahead` days past the last value of `temps` (oldest first).
    Needs at least 3 values; returns a Forecast, or None otherwise."""
    n = len(temps)
    if n < 3 or days_ahead < 1:
        return None

    # 1. Least-squares line  y = a + b*x  through (0,t0), (1,t1), ...
    sx = sum(range(n))
    sy = sum(temps)
    sxx = sum(i * i for i in range(n))
    sxy = sum(i * t for i, t in enumerate(temps))
    b = (n * sxy - sx * sy) / (n * sxx - sx * sx)       # slope: deg C per day
    a = (sy - b * sx) / n                               # intercept
    mean = sy / n

    # 2. R^2: how much of the day-to-day variation the line explains (0..1).
    ss_res = sum((t - (a + b * i)) ** 2 for i, t in enumerate(temps))
    ss_tot = sum((t - mean) ** 2 for t in temps)
    r2 = 1.0 if ss_tot < 1e-12 else 1.0 - ss_res / ss_tot   # flat series = perfectly predictable
    r2 = min(1.0, max(0.0, r2))

    # 3. Damped trend: each future day adds only 0.8x the previous day's
    #    change, so a steep slope cannot run away to absurd temperatures.
    #    Extra rise = b * (phi + phi^2 + ... + phi^d).
    phi = 0.8
    damp = phi * (1.0 - phi ** days_ahead) / (1.0 - phi)
    predicted = a + b * (n - 1) + b * damp

    return Forecast(
        predicted=predicted,
        confidence=r2 / (1.0 + 0.1 * days_ahead),       # worse fit or longer horizon = less sure
        severity=classify_severity(predicted, predicted - mean),
        slope=b,
        mean=mean,
    )


def forecast_region(ti, region, today, days_ahead):
    """Forecast a region from the last 7 days stored in the temporal index."""
    readings = ti.range(region.id, today - 6, today)    # B-tree range query
    f = forecast_temp([r.temp for r in readings], days_ahead)
    if f is None:
        return None
    base = ti.stats(region.id, today - 30, today)       # re-grade severity vs 30-day normal
    if base is not None:
        f.severity = classify_severity(f.predicted, f.predicted - base.mean)
    return f


# ----------------------------------------------------------------- ingestion
def ingest_reading(ti, aq, region, day, temp):
    """The report's insertTemperatureReading():
      1. look at the previous 30 days  (B-tree range query)
      2. store the new reading         (B-tree insert)
      3. if temp > mean + 2*sigma AND severity >= 2 -> push an Alert
    The baseline is computed BEFORE inserting so the new reading cannot
    pull its own average up. Returns True if an alert was raised."""
    base = ti.stats(region.id, day - 30, day - 1)
    have_baseline = base is not None and base.count >= 10

    ti.insert(region.id, day, temp)

    if not have_baseline:                               # not enough history yet
        region.temp_anomaly = 0.0
        return False

    sigma = max(base.stddev, 0.5)                       # floor: avoid false alarms on flat data
    anomaly = temp - base.mean
    region.temp_anomaly = anomaly       # quad-tree holds this same object, so it sees the update

    if anomaly > 2.0 * sigma:
        severity = classify_severity(temp, anomaly)
        if severity >= 2:
            aq.push(Alert(region.id, severity, temp, anomaly, day))
            return True
    return False
