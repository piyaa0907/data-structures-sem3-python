"""
spatial.py - quad-tree spatial index

Idea: the map is a rectangle (lat/lng box). If a rectangle holds more than
QT_CAPACITY regions we cut it into 4 equal quadrants and push the regions
down into them. A radius query then skips every rectangle that cannot touch
the search circle, so we only look at a tiny part of the map.
"""
import math
from dataclasses import dataclass

QT_CAPACITY = 4          # a leaf splits when it holds more than this
QT_MAX_DEPTH = 12        # stops infinite splitting on duplicate points
QT_NO_FILTER = -1.0e9    # pass as min_anomaly to get every region

EARTH_RADIUS_KM = 6371.0
KM_PER_DEG_LAT = 111.195  # 2 * pi * R / 360


@dataclass
class Region:
    id: int
    name: str
    lat: float
    lng: float
    temp_anomaly: float = 0.0   # deg C above recent normal; updated by ingest_reading()


def haversine_km(lat1, lng1, lat2, lng2):
    """Great-circle distance between two points on a sphere, in km."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dphi = p2 - p1
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlam / 2) ** 2
    a = min(1.0, max(0.0, a))
    return 2.0 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


class _Node:
    __slots__ = ("min_lat", "max_lat", "min_lng", "max_lng", "depth", "items", "children")

    def __init__(self, depth, min_lat, max_lat, min_lng, max_lng):
        self.min_lat, self.max_lat = min_lat, max_lat      # the rectangle this node covers
        self.min_lng, self.max_lng = min_lng, max_lng
        self.depth = depth
        self.items = []          # leaf only: the regions stored here
        self.children = None     # internal only: [SW, SE, NW, NE]


def _quadrant(n, lat, lng):
    """Which of the 4 quadrants holds this point? Points exactly on a dividing
    line go to the north / east side, consistently."""
    mid_lat = (n.min_lat + n.max_lat) / 2.0
    mid_lng = (n.min_lng + n.max_lng) / 2.0
    return (2 if lat >= mid_lat else 0) + (1 if lng >= mid_lng else 0)   # 0=SW 1=SE 2=NW 3=NE


class QuadTree:
    def __init__(self, min_lat, max_lat, min_lng, max_lng):
        """Empty tree covering a lat/lng box (e.g. India: 6..38 N, 66..98 E)."""
        self.root = _Node(0, min_lat, max_lat, min_lng, max_lng)
        self.last_nodes_visited = 0     # filled in by search(); handy for the report

    # ---------------------------------------------------------------- insert
    def insert(self, region):
        """Insert a Region (the tree keeps the object itself, not a copy, so
        later changes to region.temp_anomaly are seen automatically).
        Returns True, or False if the region is outside the map."""
        r = self.root
        if not (r.min_lat <= region.lat <= r.max_lat and r.min_lng <= region.lng <= r.max_lng):
            return False
        self._insert(r, region)
        return True

    def _insert(self, n, region):
        while n.children is not None:                        # internal: go down
            n = n.children[_quadrant(n, region.lat, region.lng)]
        n.items.append(region)                               # leaf: store it
        if len(n.items) > QT_CAPACITY and n.depth < QT_MAX_DEPTH:
            self._split(n)                                   # too crowded: split

    def _split(self, n):
        """Turn a full leaf into an internal node with 4 children."""
        mid_lat = (n.min_lat + n.max_lat) / 2.0
        mid_lng = (n.min_lng + n.max_lng) / 2.0
        d = n.depth + 1
        n.children = [
            _Node(d, n.min_lat, mid_lat, n.min_lng, mid_lng),   # SW
            _Node(d, n.min_lat, mid_lat, mid_lng, n.max_lng),   # SE
            _Node(d, mid_lat, n.max_lat, n.min_lng, mid_lng),   # NW
            _Node(d, mid_lat, n.max_lat, mid_lng, n.max_lng),   # NE
        ]
        old, n.items = n.items, []          # take the regions out of this node ...
        for region in old:                  # ... and re-insert them (they go down)
            self._insert(n, region)

    # ---------------------------------------------------------------- search
    def search(self, lat, lng, radius_km, min_anomaly=QT_NO_FILTER, max_out=None):
        """Regions within radius_km of (lat, lng) whose temp_anomaly >= min_anomaly,
        hottest first (at most max_out of them)."""
        # Bounding box of the circle in degrees. Longitude degrees get shorter
        # away from the equator, so use the cosine at the highest latitude the
        # circle reaches (smallest cosine = widest box = never misses a region).
        # Longitude wrap-around at +-180 is ignored: fine for India.
        dlat = radius_km / KM_PER_DEG_LAT
        worst_lat = min(89.0, max(abs(lat - dlat), abs(lat + dlat)))
        dlng = radius_km / (KM_PER_DEG_LAT * math.cos(math.radians(worst_lat)))
        box_min_lat, box_max_lat = lat - dlat, lat + dlat
        box_min_lng, box_max_lng = lng - dlng, lng + dlng

        hits = []
        visited = 0
        stack = [self.root]
        while stack:
            n = stack.pop()
            visited += 1
            # Prune: does this rectangle overlap the circle's bounding box at all?
            if (n.max_lat < box_min_lat or n.min_lat > box_max_lat or
                    n.max_lng < box_min_lng or n.min_lng > box_max_lng):
                continue
            if n.children is None:                           # leaf: test each region exactly
                for r in n.items:
                    if r.temp_anomaly >= min_anomaly and \
                            haversine_km(lat, lng, r.lat, r.lng) <= radius_km:
                        hits.append(r)
            else:
                stack.extend(n.children)

        self.last_nodes_visited = visited
        hits.sort(key=lambda r: r.temp_anomaly, reverse=True)    # hottest first
        return hits if max_out is None else hits[:max_out]

    # ------------------------------------------------------------------ misc
    def height(self):
        def h(n):
            return 1 if n.children is None else 1 + max(h(c) for c in n.children)
        return h(self.root)
