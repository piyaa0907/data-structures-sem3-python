"""
test_p1.py - tests for every Person 1 module, plus an optional benchmark.
    python test_p1.py            run the tests
    python test_p1.py --bench    also run timing comparisons (for the report)
"""
import math
import random
import sys
import time

from .alerts import Alert, AlertQueue
from .analytics import classify_severity, forecast_region, forecast_temp, ingest_reading
from .spatial import QT_NO_FILTER, QuadTree, Region, haversine_km
from .temporal import (BTree, Season, TemporalIndex, civil_from_days,
                      days_from_civil, season_of)

total = 0
failed = 0


def check(cond, label=""):
    global total, failed
    total += 1
    if not cond:
        failed += 1
        print(f"  FAIL: {label}")


def test_calendar():
    print("[calendar]")
    check(days_from_civil(1970, 1, 1) == 0, "epoch")
    check(days_from_civil(2000, 3, 1) == 11017, "2000-03-01")
    check(all(days_from_civil(*civil_from_days(z)) == z for z in range(-1000, 30000)), "round trip")
    check(season_of(days_from_civil(2024, 6, 1)) == Season.MONSOON, "june")
    check(season_of(days_from_civil(2024, 4, 15)) == Season.PRE_MONSOON, "april")
    check(season_of(days_from_civil(2024, 1, 10)) == Season.WINTER, "january")
    check(season_of(days_from_civil(2024, 11, 3)) == Season.POST_MONSOON, "november")


def test_btree():
    print("[b-tree]")
    n = 20000
    keys = [i * 3 for i in range(n)]                         # 0, 3, 6, ...
    random.shuffle(keys)
    t = BTree()
    check(all(t.insert(k, k * 0.5) for k in keys), "all inserts new")
    check(t.size == n, "size")
    print(f"  {n} keys, height {t.height()}")
    check(all(t.search(k) == k * 0.5 for k in keys), "every key found")
    check(t.search(1) is None and t.search(-5) is None, "missing keys")
    check(t.insert(300, 99.0) is False and t.search(300) == 99.0 and t.size == n, "duplicate updates")
    full = t.range(-10 ** 9, 10 ** 9)                        # full in-order walk
    check(len(full) == n and all(full[i - 1].day < full[i].day for i in range(1, n)), "sorted walk")
    r = t.range(1000, 1299)                                  # multiples of 3 in [1000, 1299]
    check(len(r) == 100 and r[0].day == 1002 and r[-1].day == 1299, "range query")


def test_quadtree():
    print("[quad-tree]")
    n, q = 500, 300
    regs = [Region(i, f"R{i}", random.uniform(8, 37), random.uniform(68, 97),
                   random.uniform(0, 8)) for i in range(n)]
    tree = QuadTree(6.0, 38.0, 66.0, 98.0)
    check(all(tree.insert(r) for r in regs), "inserts")
    check(not tree.insert(Region(999, "Nowhere", 60.0, 10.0)), "outside the map rejected")

    match = valid = order = True
    visited = 0
    for _ in range(q):
        lat, lng = random.uniform(8, 37), random.uniform(68, 97)
        rad, thr = random.uniform(50, 800), random.uniform(0, 4)
        brute = sum(1 for r in regs if r.temp_anomaly >= thr
                    and haversine_km(lat, lng, r.lat, r.lng) <= rad)
        got = tree.search(lat, lng, rad, thr)
        visited += tree.last_nodes_visited
        match &= len(got) == brute                           # same count as brute force
        valid &= all(r.temp_anomaly >= thr and haversine_km(lat, lng, r.lat, r.lng) <= rad for r in got)
        order &= all(got[i - 1].temp_anomaly >= got[i].temp_anomaly for i in range(1, len(got)))
    check(match, "matches brute force")
    check(valid, "every hit qualifies")
    check(order, "hottest first")
    print(f"  height {tree.height()}, avg nodes visited per query: {visited / q:.1f}")

    dup_tree = QuadTree(0, 10, 0, 10)                        # identical points must not recurse forever
    for i in range(20):
        dup_tree.insert(Region(i, "d", 5, 5))
    check(len(dup_tree.search(5, 5, 1.0)) == 20, "duplicate points")


def test_alerts():
    print("[alert queue]")
    q = AlertQueue()
    for a in [Alert(1, 3, 40.1, 4.0, 100), Alert(2, 5, 47.5, 8.0, 105), Alert(3, 2, 37.5, 3.0, 90),
              Alert(4, 5, 46.0, 7.0, 101), Alert(5, 4, 45.2, 5.0, 99), Alert(6, 3, 41.0, 4.5, 100)]:
        q.push(a)
    check(len(q) == 6, "size")
    # sev5 (older day first), sev4, sev3 (same day: hotter first), sev2
    check([q.pop().region_id for _ in range(6)] == [4, 2, 5, 6, 1, 3], "priority order")
    check(len(q) == 0 and q.pop() is None and q.peek() is None, "empty queue")

    for i in range(5000):                                    # stress: heap order holds
        q.push(Alert(i, random.randint(1, 5), 30.0, 0.0, random.randint(0, 999)))
    sev = [q.pop().severity for _ in range(5000)]
    check(all(sev[i - 1] >= sev[i] for i in range(1, 5000)), "severity never increases")


def test_forecast():
    print("[forecast + severity]")
    f = forecast_temp([32, 33, 34, 35, 36, 37, 38], 1)       # exactly +1 C per day
    check(abs(f.slope - 1.0) < 1e-9, "slope")
    check(abs(f.predicted - 38.8) < 1e-9, "damped prediction (38 + 0.8)")
    check(abs(f.confidence - 1 / 1.1) < 1e-9, "confidence")
    f = forecast_temp([35, 35, 35, 35], 3)
    check(abs(f.predicted - 35.0) < 1e-9 and abs(f.slope) < 1e-9, "flat series")
    f = forecast_temp([30, 38, 29, 39, 31, 37, 30], 1)
    check(f.confidence < 0.3, "noisy series -> low confidence")
    check(forecast_temp([32, 33], 1) is None, "too little data")

    check([classify_severity(t, a) for t, a in
           [(30, 0), (38, 2), (41, 2), (41, 5), (46, 3), (43, 7), (47.5, 0)]] == [1, 2, 3, 4, 4, 5, 5],
          "severity levels")


def test_pipeline():
    print("[ingest pipeline]")
    ti, aq = TemporalIndex(), AlertQueue()
    reg = Region(7, "Nagpur", 21.15, 79.09)
    start = days_from_civil(2024, 5, 1)                      # 1 May .. 9 Jun straddles two seasons

    results = [ingest_reading(ti, aq, reg, start + i, 36.0 + math.sin(i)) for i in range(40)]
    check(not any(results[:10]) and not any(results) and len(aq) == 0, "calm baseline raises nothing")

    st = ti.stats(7, start, start + 39)                      # spans pre-monsoon + monsoon trees
    check(st is not None and st.count == 40, "stats merge both season trees")
    rng = ti.range(7, start, start + 39)
    check(len(rng) == 40 and rng[0].day == start and rng[-1].day == start + 39, "range merge sorted")

    check(ingest_reading(ti, aq, reg, start + 40, 44.5) is True, "heat spike raises an alert")
    check(len(aq) == 1 and reg.temp_anomaly > 6.0, "anomaly recorded on the region")
    a = aq.peek()
    check(a.region_id == 7 and a.severity >= 4, "alert content")

    f = forecast_region(ti, reg, start + 40, 2)
    check(f is not None and 0.0 <= f.confidence <= 1.0 and 1 <= f.severity <= 5, "forecast_region")

    # the quad-tree holds the same Region object, so it sees the new anomaly with no re-insert
    tree = QuadTree(6, 38, 66, 98)
    tree.insert(reg)
    check(tree.search(21.15, 79.09, 10, min_anomaly=6.0) == [reg], "quad-tree sees updated anomaly")


# ----------------------------------------------------------------- benchmark
def bench():
    print("\n[benchmark]  (pure Python, so absolute numbers are bigger than C; compare the ratios)")
    print("Spatial: 100 radius queries (200 km), quad-tree vs linear scan")
    print(f"{'regions':>10} {'quadtree ms':>12} {'linear ms':>12} {'avg nodes':>10}")
    for n in (1000, 10000, 50000):
        regs = [Region(i, "", random.uniform(8, 37), random.uniform(68, 97), random.uniform(0, 8))
                for i in range(n)]
        tree = QuadTree(6, 38, 66, 98)
        for r in regs:
            tree.insert(r)
        qs = [(random.uniform(8, 37), random.uniform(68, 97)) for _ in range(100)]
        t0 = time.perf_counter()
        vis = 0
        for lat, lng in qs:
            tree.search(lat, lng, 200)
            vis += tree.last_nodes_visited
        t1 = time.perf_counter()
        for lat, lng in qs:
            [r for r in regs if haversine_km(lat, lng, r.lat, r.lng) <= 200]
        t2 = time.perf_counter()
        print(f"{n:>10} {(t1 - t0) * 1000:>12.1f} {(t2 - t1) * 1000:>12.1f} {vis / 100:>10.1f}")

    print("\nTemporal: 300 point lookups, B-tree vs a plain loop over a list")
    print(f"{'readings':>10} {'b-tree ms':>12} {'linear ms':>12}")
    for n in (2000, 20000, 100000):
        arr = list(range(n))
        tree = BTree()
        for k in arr:
            tree.insert(k, 30.0)
        qs = [random.randrange(n) for _ in range(300)]
        t0 = time.perf_counter()
        for k in qs:
            tree.search(k)
        t1 = time.perf_counter()
        for k in qs:
            for x in arr:
                if x == k:
                    break
        t2 = time.perf_counter()
        print(f"{n:>10} {(t1 - t0) * 1000:>12.2f} {(t2 - t1) * 1000:>12.2f}")


if __name__ == "__main__":
    random.seed(42)
    test_calendar()
    test_btree()
    test_quadtree()
    test_alerts()
    test_forecast()
    test_pipeline()
    print(f"\n{total} checks, {failed} failed")
    if "--bench" in sys.argv:
        bench()
    sys.exit(1 if failed else 0)
