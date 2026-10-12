"""
temporal.py - B-tree temporal index

A B-tree is a balanced search tree where every node holds several keys
(here up to 5, because BT_T = 3). Keys inside a node are sorted, and
children[i] holds everything between keys[i-1] and keys[i].
Insert, search and range queries are all O(log n).

Key   = day number (days since 1970-01-01)
Value = temperature in deg C

Insertion follows the standard CLRS "split on the way down" method:
if a node is full we split it BEFORE descending into it, so we never
have to walk back up the tree.
"""
import math
from bisect import bisect_left
from dataclasses import dataclass
from datetime import date
from enum import IntEnum
from typing import NamedTuple, Optional

BT_T = 3
BT_MAX_KEYS = 2 * BT_T - 1

_EPOCH_ORDINAL = date(1970, 1, 1).toordinal()


# ------------------------------------------------------------------ calendar
def days_from_civil(year, month, day):
    """Calendar date -> day number (days since 1970-01-01)."""
    return date(year, month, day).toordinal() - _EPOCH_ORDINAL


def civil_from_days(days):
    """Day number -> (year, month, day)."""
    d = date.fromordinal(days + _EPOCH_ORDINAL)
    return d.year, d.month, d.day


class Season(IntEnum):             # IMD seasons
    WINTER = 0                     # Jan-Feb
    PRE_MONSOON = 1                # Mar-May (heatwave season)
    MONSOON = 2                    # Jun-Sep
    POST_MONSOON = 3               # Oct-Dec


def season_of(day):
    month = civil_from_days(day)[1]
    if month <= 2:
        return Season.WINTER
    if month <= 5:
        return Season.PRE_MONSOON
    if month <= 9:
        return Season.MONSOON
    return Season.POST_MONSOON


class Reading(NamedTuple):         # one daily observation
    day: int
    temp: float


@dataclass
class RangeStats:
    count: int
    mean: float
    stddev: float
    min: float
    max: float


# -------------------------------------------------------------------- B-tree
class _BNode:
    __slots__ = ("keys", "vals", "children", "leaf")

    def __init__(self, leaf):
        self.keys = []             # sorted keys (at most BT_MAX_KEYS)
        self.vals = []             # vals[i] belongs to keys[i]
        self.children = []         # len(keys) + 1 children, or empty if leaf
        self.leaf = leaf


class BTree:
    def __init__(self):
        self.root = None
        self.size = 0

    # ---------------------------------------------------------------- search
    def _find(self, key):
        """Return (node, index) holding `key`, or None."""
        x = self.root
        while x is not None:
            i = bisect_left(x.keys, key)                     # first key >= target
            if i < len(x.keys) and x.keys[i] == key:
                return x, i
            if x.leaf:
                return None
            x = x.children[i]                                # go to the right child
        return None

    def search(self, key, default=None):
        hit = self._find(key)
        return default if hit is None else hit[0].vals[hit[1]]

    def __contains__(self, key):
        return self._find(key) is not None

    def __len__(self):
        return self.size

    # ---------------------------------------------------------------- insert
    def _split_child(self, parent, i):
        """Split the full child parent.children[i] in two; the middle key moves up."""
        y = parent.children[i]                               # full node: 2T-1 keys
        z = _BNode(y.leaf)                                   # new right sibling
        z.keys, z.vals = y.keys[BT_T:], y.vals[BT_T:]        # upper T-1 keys -> z
        mid_key, mid_val = y.keys[BT_T - 1], y.vals[BT_T - 1]
        if not y.leaf:
            z.children = y.children[BT_T:]
            y.children = y.children[:BT_T]
        y.keys, y.vals = y.keys[:BT_T - 1], y.vals[:BT_T - 1]   # y keeps the lower T-1 keys
        parent.keys.insert(i, mid_key)                       # median key moves up
        parent.vals.insert(i, mid_val)
        parent.children.insert(i + 1, z)

    def _insert_nonfull(self, x, key, val):
        """Insert into a subtree whose root x is guaranteed NOT to be full."""
        while not x.leaf:
            i = bisect_left(x.keys, key)                     # child to descend into
            if len(x.children[i].keys) == BT_MAX_KEYS:       # full? split first
                self._split_child(x, i)
                if key > x.keys[i]:                          # median moved up: pick side
                    i += 1
            x = x.children[i]
        i = bisect_left(x.keys, key)
        x.keys.insert(i, key)
        x.vals.insert(i, val)

    def insert(self, key, val):
        """Returns True if a new key was added, False if an existing key was updated."""
        hit = self._find(key)
        if hit is not None:                                  # same day again: overwrite
            node, i = hit
            node.vals[i] = val
            return False
        if self.root is None:
            self.root = _BNode(True)
        if len(self.root.keys) == BT_MAX_KEYS:               # root full: tree grows in height
            s = _BNode(False)
            s.children.append(self.root)
            self._split_child(s, 0)
            self.root = s
        self._insert_nonfull(self.root, key, val)
        self.size += 1
        return True

    # ----------------------------------------------------------------- range
    def walk(self, lo, hi):
        """Yield (key, value) in increasing key order for lo <= key <= hi."""
        if self.root is not None and lo <= hi:
            yield from self._walk(self.root, lo, hi)

    def _walk(self, x, lo, hi):
        for i, k in enumerate(x.keys):
            # children[i] only holds keys < k; skip it if k < lo
            if not x.leaf and k >= lo:
                yield from self._walk(x.children[i], lo, hi)
            if k > hi:                                       # everything after is too big
                return
            if k >= lo:
                yield k, x.vals[i]
        if not x.leaf:
            yield from self._walk(x.children[-1], lo, hi)

    def range(self, lo, hi):
        return [Reading(k, v) for k, v in self.walk(lo, hi)]

    def height(self):
        h, x = 0, self.root
        while x is not None:
            h += 1
            x = None if x.leaf else x.children[0]
        return h


# --------------------------------------------- per-region, per-season index
class TemporalIndex:
    """One B-tree per (region, season), as in the design report."""

    def __init__(self):
        self._trees = {}                                     # (region_id, Season) -> BTree

    def insert(self, region_id, day, temp):
        tree = self._trees.setdefault((region_id, season_of(day)), BTree())
        return tree.insert(day, temp)

    def _trees_for(self, region_id):
        return [self._trees[(region_id, s)] for s in Season if (region_id, s) in self._trees]

    def range(self, region_id, lo, hi):
        """Readings with lo <= day <= hi, sorted by day.
        A date range can straddle two seasons (e.g. 20 May - 10 Jun), so we ask
        every season tree for this region and merge. Each query is O(log n)."""
        out = []
        for tree in self._trees_for(region_id):
            out.extend(Reading(k, v) for k, v in tree.walk(lo, hi))
        out.sort(key=lambda r: r.day)
        return out

    def stats(self, region_id, lo, hi):
        """Mean / std deviation / min / max over a date range, or None if no data.
        Uses Welford's method: numerically stable, one pass."""
        n, mean, m2 = 0, 0.0, 0.0
        vmin = vmax = 0.0
        for tree in self._trees_for(region_id):
            for _, v in tree.walk(lo, hi):
                n += 1
                delta = v - mean
                mean += delta / n
                m2 += delta * (v - mean)
                vmin = v if n == 1 else min(vmin, v)
                vmax = v if n == 1 else max(vmax, v)
        if n == 0:
            return None
        return RangeStats(n, mean, math.sqrt(m2 / n), vmin, vmax)   # population std deviation
