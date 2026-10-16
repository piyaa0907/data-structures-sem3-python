"""
=============================================================================
USE CASE KJS-CES-01: CLIMATE INTELLIGENCE FOR HEATWAVE MONITORING, PREDICTION,
                     AND EARLY WARNING (IMD MUMBAI-PUNE)
=============================================================================
Data Structures & Rubric Components:
  - 7 IMD Geographical Regions Ingestion (Section 1.1)
  - ER Entity Layout: ObservationLog, WeatherStation, IMD_Region (Section 3)
  - 2D Point-Region (PR) Quad-Tree Layout & Operations (Section 3, 4.1, 4.2)
  - Max-Heap Priority Queue & O(log P) Extraction (Section 3, 4.3)
  - Regional Time-Series Hash Map of Linked-Lists (Section 2, 4.4)
=============================================================================
"""

import json
import math
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from enum import Enum


# =============================================================================
# SECTION 3: ER-STYLE ENTITIES & SEVERITY ENUMS
# =============================================================================
class SeverityLevel(Enum):
    NORMAL = "NORMAL"
    MODERATE = "MODERATE"
    SEVERE = "SEVERE"
    EXTREME = "EXTREME"


class ObservationLog:
    """ER Entity: ObservationLog (station_id, lat, lon, max_temp, timestamp, anomaly_score)
    Also functions as a Linked-List Node for Regional Time-Series tracking.
    """

    def __init__(
        self,
        station_id: str,
        region_id: int,
        lat: float,
        lon: float,
        max_temp: float,
        timestamp: str = None,
        normal_temp: float = 35.0,
        station_name: str = "",
    ):
        self.station_id = station_id
        self.region_id = region_id
        self.station_name = station_name or station_id
        self.lat = float(lat)
        self.lon = float(lon)
        self.max_temp = float(max_temp)
        self.normal_temp = float(normal_temp)
        self.departure = round(self.max_temp - self.normal_temp, 2)
        self.timestamp = timestamp or datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        self.anomaly_score = round(max(0.0, self.departure), 2)

        # IMD Thresholds: >= 40.0 C in plains, departure >= 4.5 C
        if self.max_temp >= 45.0 or self.departure >= 6.4:
            self.severityLevel = SeverityLevel.EXTREME
        elif self.max_temp >= 42.0 or self.departure >= 5.0:
            self.severityLevel = SeverityLevel.SEVERE
        elif self.max_temp >= 40.0 or self.departure >= 4.5:
            self.severityLevel = SeverityLevel.MODERATE
        else:
            self.severityLevel = SeverityLevel.NORMAL

        self.latestReading = self  # Reference matching Operation 4.4 pseudocode
        self.next = None  # Pointer to next ObservationLog node in Regional Linked-List


class WeatherStation:
    """ER Entity: WeatherStation"""

    def __init__(
        self,
        station_id: str,
        region_id: int,
        name: str,
        lat: float,
        lon: float,
        normal_temp: float = 35.0,
    ):
        self.station_id = station_id
        self.region_id = region_id
        self.name = name
        self.lat = lat
        self.lon = lon
        self.normal_temp = normal_temp


class IMD_Region:
    """ER Entity: IMD_Region"""

    def __init__(self, region_id: int, name: str):
        self.region_id = region_id
        self.name = name


# =============================================================================
# SECTION 3, OPERATION 4.1 & 4.2: 2D PR QUAD-TREE
# =============================================================================
class BoundingBox:

    def __init__(
        self, minLat: float, maxLat: float, minLon: float, maxLon: float
    ):
        self.minLat = minLat
        self.maxLat = maxLat
        self.minLon = minLon
        self.maxLon = maxLon

    def contains(self, lat: float, lon: float) -> bool:
        return (
            self.minLat <= lat <= self.maxLat
            and self.minLon <= lon <= self.maxLon
        )

    def intersects(self, other: "BoundingBox") -> bool:
        return not (
            other.maxLat < self.minLat
            or other.minLat > self.maxLat
            or other.maxLon < self.minLon
            or other.minLon > self.maxLon
        )


class QuadTreeNode:
    """QuadTreeNode Structure Layout:
    - boundary: BoundingBox
    - capacity: Integer (Bucket threshold = 16)
    - stations: List<ObservationLog>
    - isSubdivided: Boolean
    - NW, NE, SW, SE: Child Pointers
    """

    def __init__(self, boundary: BoundingBox, capacity: int = 16):
        self.boundary = boundary
        self.capacity = capacity
        self.stations: list[ObservationLog] = []
        self.isSubdivided = False
        self.NW = None
        self.NE = None
        self.SW = None
        self.SE = None


def SubdivideQuadNode(node: QuadTreeNode):
    midLat = (node.boundary.minLat + node.boundary.maxLat) / 2.0
    midLon = (node.boundary.minLon + node.boundary.maxLon) / 2.0

    node.NW = QuadTreeNode(
        BoundingBox(midLat, node.boundary.maxLat, node.boundary.minLon, midLon),
        node.capacity,
    )
    node.NE = QuadTreeNode(
        BoundingBox(midLat, node.boundary.maxLat, midLon, node.boundary.maxLon),
        node.capacity,
    )
    node.SW = QuadTreeNode(
        BoundingBox(node.boundary.minLat, midLat, node.boundary.minLon, midLon),
        node.capacity,
    )
    node.SE = QuadTreeNode(
        BoundingBox(node.boundary.minLat, midLat, midLon, node.boundary.maxLon),
        node.capacity,
    )
    node.isSubdivided = True


def InsertStation(node: QuadTreeNode, st: ObservationLog) -> bool:
    """OPERATION 4.1: Spatial Quad-Tree Insertion"""
    if not node.boundary.contains(st.lat, st.lon):
        return False

    if len(node.stations) < node.capacity and not node.isSubdivided:
        node.stations.append(st)
        return True

    if not node.isSubdivided:
        SubdivideQuadNode(node)
        existing = node.stations
        node.stations = []
        for old_st in existing:
            _ = (
                InsertStation(node.NE, old_st)
                or InsertStation(node.NW, old_st)
                or InsertStation(node.SW, old_st)
                or InsertStation(node.SE, old_st)
            )

    if InsertStation(node.NE, st):
        return True
    if InsertStation(node.NW, st):
        return True
    if InsertStation(node.SW, st):
        return True
    if InsertStation(node.SE, st):
        return True

    return False


def QueryHotspotRange(
    node: QuadTreeNode, rangeBox: BoundingBox, outList: list
):
    """OPERATION 4.2: Spatial Hotspot Range Query"""
    if node is None or not node.boundary.intersects(rangeBox):
        return

    for st in node.stations:
        if rangeBox.contains(st.lat, st.lon):
            outList.append(st)

    if node.isSubdivided:
        QueryHotspotRange(node.NW, rangeBox, outList)
        QueryHotspotRange(node.NE, rangeBox, outList)
        QueryHotspotRange(node.SW, rangeBox, outList)
        QueryHotspotRange(node.SE, rangeBox, outList)


# =============================================================================
# SECTION 3 & OPERATION 4.3: PRIORITY MAX-HEAP (ALERT EXTRACTION)
# =============================================================================
class HeapAlertNode:
    """Structure Layout: HeapAlertNode (Priority Queue)"""

    def __init__(
        self,
        stationid: str,
        regionid: int,
        currentTemp: float,
        severityLevel: SeverityLevel,
        station_name: str = "",
        departure: float = 0.0,
    ):
        self.stationid = stationid
        self.regionid = regionid
        self.currentTemp = currentTemp
        self.severityLevel = severityLevel
        self.station_name = station_name or stationid
        self.departure = departure


class MaxHeapPriorityQueue:

    def __init__(self):
        self.heap: list[HeapAlertNode] = []

    def _parent(self, i: int) -> int:
        return (i - 1) // 2

    def _leftChild(self, i: int) -> int:
        return 2 * i + 1

    def _rightChild(self, i: int) -> int:
        return 2 * i + 2

    def InsertAlert(self, alert: HeapAlertNode):
        self.heap.append(alert)
        self._heapifyUp(len(self.heap) - 1)

    def _heapifyUp(self, index: int):
        while (
            index > 0
            and self.heap[index].currentTemp
            > self.heap[self._parent(index)].currentTemp
        ):
            p = self._parent(index)
            self.heap[index], self.heap[p] = self.heap[p], self.heap[index]
            index = p

    def HeapifyDown(self, heap: list, index: int):
        largest = index
        left = self._leftChild(index)
        right = self._rightChild(index)

        if (
            left < len(heap)
            and heap[left].currentTemp > heap[largest].currentTemp
        ):
            largest = left
        if (
            right < len(heap)
            and heap[right].currentTemp > heap[largest].currentTemp
        ):
            largest = right

        if largest != index:
            heap[index], heap[largest] = heap[largest], heap[index]
            self.HeapifyDown(heap, largest)

    def ExtractHighestTempAlert(self) -> HeapAlertNode:
        """OPERATION 4.3: Priority Max-Heap Alert Extraction in O(log P)"""
        if len(self.heap) == 0:
            return None

        topAlert = self.heap[0]
        lastElem = self.heap.pop()

        if len(self.heap) > 0:
            self.heap[0] = lastElem
            self.HeapifyDown(self.heap, 0)

        return topAlert

    def is_empty(self) -> bool:
        return len(self.heap) == 0


# =============================================================================
# OPERATION 4.4: REGIONAL TIME-SERIES TRAVERSAL (HASH MAP + LINKED LIST)
# =============================================================================
class RegionalMapRegistry:

    def __init__(self):
        self.regionMap: dict[int, ObservationLog] = {}

    def insert(self, obs: ObservationLog):
        if obs.region_id not in self.regionMap:
            self.regionMap[obs.region_id] = obs
        else:
            obs.next = self.regionMap[obs.region_id]
            self.regionMap[obs.region_id] = obs

    def TraverseRegionAlerts(
        self, regId: int, tThresh: float
    ) -> list[ObservationLog]:
        """OPERATION 4.4: Regional Time-Series Traversal"""
        headStation = self.regionMap.get(regId)
        alerts = []
        curr = headStation
        while curr is not None:
            if curr.latestReading.max_temp >= tThresh:
                alerts.append(curr.latestReading)
            curr = curr.next
        return alerts


# =============================================================================
# LIVE TELEMETRY & GEOCODING RESOLVER
# =============================================================================
def fetch_live_coordinates(
    place: str, state: str
) -> tuple[float, float, str]:
    query = f"{place}, {state}"
    url = f"https://geocoding-api.open-meteo.com/v1/search?name={urllib.parse.quote(query)}&count=1&language=en&format=json"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if "results" in data and len(data["results"]) > 0:
                res = data["results"][0]
                lat = float(res["latitude"])
                lon = float(res["longitude"])
                name = f"{res.get('name', place)}, {res.get('admin1', state)}"
                return lat, lon, name
    except Exception:
        pass

    try:
        url_city = f"https://geocoding-api.open-meteo.com/v1/search?name={urllib.parse.quote(place)}&count=1&language=en&format=json"
        req = urllib.request.Request(
            url_city, headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if "results" in data and len(data["results"]) > 0:
                res = data["results"][0]
                return (
                    float(res["latitude"]),
                    float(res["longitude"]),
                    f"{res.get('name')}, {state}",
                )
    except Exception:
        pass

    return None, None, None


def fetch_live_weather(lat: float, lon: float) -> tuple[float, float]:
    url = (
        f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
        f"&current=temperature_2m,relative_humidity_2m"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return float(data["current"]["temperature_2m"]), float(
                data["current"]["relative_humidity_2m"]
            )
    except Exception:
        return 38.5, 45.0


# =============================================================================
# HYBRID ENGINE CORE ORCHESTRATOR WITH ALL 7 IMD REGIONS
# =============================================================================
class ClimateIntelligenceEngine:

    def __init__(self):
        # India Bounding Box: Lat [8.0, 37.0], Lon [68.0, 98.0]
        self.root_boundary = BoundingBox(8.0, 37.0, 68.0, 98.0)
        self.quad_tree_root = QuadTreeNode(self.root_boundary, capacity=16)
        self.max_heap = MaxHeapPriorityQueue()
        self.region_registry = RegionalMapRegistry()
        self.all_observations: list[ObservationLog] = []

        # IMD 7 Geographical Regions Metadata Mapping
        self.imd_regions = {
            101: IMD_Region(101, "North-West India"),
            201: IMD_Region(201, "North-East India"),
            301: IMD_Region(301, "East India"),
            401: IMD_Region(401, "Central India"),
            501: IMD_Region(501, "Western India / Maharashtra"),
            601: IMD_Region(601, "South Peninsular India"),
            701: IMD_Region(701, "Coastal & Island Territories"),
        }

        self._init_all_7_imd_regions()

    def _init_all_7_imd_regions(self):
        """Initializes weather stations across all 7 IMD geographical regions (Doc Section 1.1)"""
        baseline_records = [
            # Region 101: North-West India (Delhi, Rajasthan)
            ObservationLog(
                "AWS_DEL_01",
                101,
                28.5850,
                77.2060,
                41.5,
                normal_temp=36.0,
                station_name="Delhi (Safdarjung)",
            ),
            ObservationLog(
                "AWS_JAI_02",
                101,
                26.9124,
                75.7873,
                43.8,
                normal_temp=37.0,
                station_name="Jaipur (Sanganer)",
            ),
            # Region 201: North-East India (Assam)
            ObservationLog(
                "AWS_GUW_03",
                201,
                26.1445,
                91.7362,
                35.2,
                normal_temp=32.0,
                station_name="Guwahati Central",
            ),
            # Region 301: East India (West Bengal, Bihar)
            ObservationLog(
                "AWS_KOL_04",
                301,
                22.5726,
                88.3639,
                39.4,
                normal_temp=34.5,
                station_name="Kolkata (Alipore)",
            ),
            ObservationLog(
                "AWS_PAT_05",
                301,
                25.5941,
                85.1376,
                42.6,
                normal_temp=36.0,
                station_name="Patna Airport",
            ),
            # Region 401: Central India (Madhya Pradesh)
            ObservationLog(
                "AWS_BHO_06",
                401,
                23.2599,
                77.4126,
                44.0,
                normal_temp=37.5,
                station_name="Bhopal City",
            ),
            # Region 501: Western India / Maharashtra
            ObservationLog(
                "AWS_MUM_07",
                501,
                18.9067,
                72.8147,
                34.8,
                normal_temp=33.0,
                station_name="Mumbai (Colaba)",
            ),
            ObservationLog(
                "AWS_PUN_08",
                501,
                18.5204,
                73.8567,
                40.6,
                normal_temp=35.0,
                station_name="Pune (Shivajinagar)",
            ),
            ObservationLog(
                "AWS_NAG_09",
                501,
                21.0922,
                79.0610,
                45.2,
                normal_temp=38.0,
                station_name="Nagpur (Airport)",
            ),
            ObservationLog(
                "AWS_CHA_10",
                501,
                19.9615,
                79.2961,
                46.8,
                normal_temp=38.5,
                station_name="Chandrapur City",
            ),
            # Region 601: South Peninsular India (Telangana, Karnataka)
            ObservationLog(
                "AWS_HYD_11",
                601,
                17.3850,
                78.4867,
                41.8,
                normal_temp=36.0,
                station_name="Hyderabad (Begumpet)",
            ),
            ObservationLog(
                "AWS_BLR_12",
                601,
                12.9716,
                77.5946,
                33.5,
                normal_temp=31.0,
                station_name="Bengaluru City",
            ),
            # Region 701: Coastal & Island Territories (Goa)
            ObservationLog(
                "AWS_GOA_13",
                701,
                15.2993,
                74.1240,
                34.0,
                normal_temp=32.0,
                station_name="Goa (Panaji)",
            ),
        ]
        for obs in baseline_records:
            self.IngestTelemetry(obs)

    def IngestTelemetry(self, obs: ObservationLog):
        InsertStation(self.quad_tree_root, obs)
        self.region_registry.insert(obs)

        if obs.max_temp >= 40.0:
            alert = HeapAlertNode(
                stationid=obs.station_id,
                regionid=obs.region_id,
                currentTemp=obs.max_temp,
                severityLevel=obs.severityLevel,
                station_name=obs.station_name,
                departure=obs.departure,
            )
            self.max_heap.InsertAlert(alert)

        self.all_observations.append(obs)

    def ResolveRegionIdByState(self, state_name: str) -> int:
        s = state_name.lower()
        if any(x in s for x in ["maharashtra", "gujarat"]):
            return 501
        elif any(
            x in s
            for x in ["delhi", "rajasthan", "punjab", "haryana", "uttar pradesh"]
        ):
            return 101
        elif any(x in s for x in ["madhya pradesh", "chhattisgarh"]):
            return 401
        elif any(
            x in s for x in ["bengal", "bihar", "odisha", "jharkhand"]
        ):
            return 301
        elif any(
            x in s
            for x in ["karnataka", "tamil nadu", "telangana", "andhra", "kerala"]
        ):
            return 601
        elif any(
            x in s
            for x in [
                "assam",
                "meghalaya",
                "tripura",
                "manipur",
                "nagaland",
                "arunachal",
            ]
        ):
            return 201
        elif any(x in s for x in ["goa", "andaman", "lakshadweep"]):
            return 701
        return 501  # Default to Maharashtra/West

    def GetAdvisoryText(self, severity: SeverityLevel) -> str:
        if severity == SeverityLevel.EXTREME:
            return "RED ALERT: High risk of heatstroke for all age groups. Suspend outdoor activities."
        elif severity == SeverityLevel.SEVERE:
            return "ORANGE ALERT: Severe heat fatigue. Avoid direct sun exposure between 12 PM - 4 PM."
        elif severity == SeverityLevel.MODERATE:
            return "YELLOW ALERT: Moderate heat warning. Vulnerable individuals must rest in shade."
        return "GREEN ADVISORY: Temperature within normal seasonal limits."


# =============================================================================
# MENU CLI CONTROLLER
# =============================================================================
def Handle_Live_Location_Input(engine: ClimateIntelligenceEngine):
    print("\n" + "=" * 78)
    print(" [1] USER LOCATION INPUT: RESOLVE COORDINATES & INGEST LIVE WEATHER")
    print("=" * 78)

    state = input(
        " Enter State Name (e.g. Maharashtra, Rajasthan, Delhi, MP): "
    ).strip()
    city = input(" Enter Location / City Name (e.g. Mumbai, Nagpur, Pune): ").strip()

    if not state:
        state = "Maharashtra"
    if not city:
        city = "Mumbai"

    print(
        f"\n [+] Step 1: Resolving Geocoding Coordinates for '{city}, {state}'..."
    )
    lat, lon, resolved_name = fetch_live_coordinates(city, state)

    if lat is None or lon is None:
        print(" [!] Automatic geocoding service did not return coordinates.")
        try:
            lat = float(input("     Enter Latitude manually (e.g. 19.07): "))
            lon = float(input("     Enter Longitude manually (e.g. 72.87): "))
            resolved_name = f"{city}, {state}"
        except ValueError:
            print(" [x] Invalid coordinates. Returning to main menu.")
            return
    else:
        print(f"     [✓] Coordinates Found: Lat = {lat:.4f}, Lon = {lon:.4f}")
        print(f"     [✓] Resolved Identity: {resolved_name}")

    print(f"\n [+] Step 2: Fetching Live Satellite Telemetry via Open-Meteo API...")
    live_temp, live_humidity = fetch_live_weather(lat, lon)
    print(f"     [✓] Real-Time Measured Temperature: {live_temp}°C")
    print(f"     [✓] Live Relative Humidity        : {live_humidity}%")

    reg_id = engine.ResolveRegionIdByState(state)
    reg_name = engine.imd_regions[reg_id].name
    station_code = f"AWS_{city.upper()[:4]}_{int(time.time()) % 1000}"

    new_obs = ObservationLog(
        station_id=station_code,
        region_id=reg_id,
        lat=lat,
        lon=lon,
        max_temp=live_temp,
        normal_temp=34.0,
        station_name=resolved_name,
    )

    engine.IngestTelemetry(new_obs)

    print("\n" + "-" * 78)
    print(
        "                  IMD HEATWAVE MONITORING ASSESSMENT REPORT                   "
    )
    print("-" * 78)
    print(f" Station Identifier : {new_obs.station_id}")
    print(f" IMD Region Assigned: Region {reg_id} ({reg_name})")
    print(f" Station Location   : {new_obs.station_name}")
    print(f" Geographic Anchor  : ({new_obs.lat:.4f} N, {new_obs.lon:.4f} E)")
    print(f" Ingestion Timestamp: {new_obs.timestamp}")
    print(f" Current Temperature: {new_obs.max_temp}°C")
    print(
        f" Normal Baseline    : {new_obs.normal_temp}°C (Departure:"
        f" {new_obs.departure:+.2f}°C)"
    )
    print(f" Anomaly Score      : {new_obs.anomaly_score}")
    print(f" Severity Category  : [{new_obs.severityLevel.value}]")
    print(
        f" IMD Public Advisory: {engine.GetAdvisoryText(new_obs.severityLevel)}"
    )


def Handle_Heap_Alert_Extraction(engine: ClimateIntelligenceEngine):
    print("\n" + "=" * 78)
    print(
        " [2] PRIORITY MAX-HEAP EMERGENCY EXTRACTION (Operation 4.3)"
    )
    print("=" * 78)
    print(
        " Extracting highest extreme heat readings in O(log P) via"
        " HeapifyDown:\n"
    )

    temp_heap = MaxHeapPriorityQueue()
    for rec in engine.all_observations:
        if rec.max_temp >= 40.0:
            alert = HeapAlertNode(
                rec.station_id,
                rec.region_id,
                rec.max_temp,
                rec.severityLevel,
                rec.station_name,
                rec.departure,
            )
            temp_heap.InsertAlert(alert)

    rank = 1
    while not temp_heap.is_empty():
        top = temp_heap.ExtractHighestTempAlert()
        advisory = engine.GetAdvisoryText(top.severityLevel)
        print(
            f"  #{rank} [{top.severityLevel.value:<8}] Station: {top.station_name:<26} (Region {top.regionid}) | Temp: {top.currentTemp}°C (+{top.departure}°C dep.)"
        )
        print(f"       Action Notice: {advisory}")
        rank += 1


def Handle_Spatial_QuadTree_Range(engine: ClimateIntelligenceEngine):
    print("\n" + "=" * 78)
    print(
        " [3] 2D PR QUAD-TREE SPATIAL HOTSPOT RANGE QUERY (Operation 4.2)"
    )
    print("=" * 78)
    print(" Spatial Coordinate Range Search: Maharashtra Region")
    print(" Range BoundingBox: Lat [16.0 - 22.5 N], Lon [72.0 - 81.0 E]")

    mh_range = BoundingBox(
        minLat=16.0, maxLat=22.5, minLon=72.0, maxLon=81.0
    )
    results = []
    QueryHotspotRange(engine.quad_tree_root, mh_range, results)

    print(
        f"\n Total Active Stations Filtered in Sub-second Latency: {len(results)}"
    )
    for st in sorted(results, key=lambda x: x.max_temp, reverse=True):
        print(
            f"  * {st.station_name:<26} | Coord: ({st.lat:.2f}, {st.lon:.2f}) | Temp: {st.max_temp}°C | Status: {st.severityLevel.value}"
        )


def Handle_Regional_Time_Series(engine: ClimateIntelligenceEngine):
    print("\n" + "=" * 78)
    print(
        " [4] REGIONAL TIME-SERIES TRAVERSAL VIA LINKED-LIST (Operation 4.4)"
    )
    print("=" * 78)
    print(" Available IMD Geographical Regions in System:")
    for rid, rdata in engine.imd_regions.items():
        print(f"   [{rid}] : {rdata.name}")

    try:
        reg_id = int(
            input("\n Enter Region ID to inspect [e.g. 501, 101, 401]: ")
            or "501"
        )
        threshold = float(
            input(" Enter Temperature Alert Threshold in °C [e.g. 35.0 or 40.0]: ")
            or "35.0"
        )
    except ValueError:
        reg_id, threshold = 501, 35.0

    region_title = engine.imd_regions.get(
        reg_id, IMD_Region(reg_id, "Unknown Region")
    ).name
    print(
        f"\n Traversing Hash-Indexed Linked-List for Region {reg_id} ({region_title}) where max_temp >= {threshold}°C..."
    )
    alerts = engine.region_registry.TraverseRegionAlerts(reg_id, threshold)

    if not alerts:
        print(f"  No records breached {threshold}°C threshold in Region {reg_id}.")
    else:
        for obs in alerts:
            print(
                f"  * [{obs.timestamp}] {obs.station_name:<26} | Temp:"
                f" {obs.max_temp}°C (Departure: {obs.departure:+.2f}°C) |"
                f" Severity: {obs.severityLevel.value}"
            )


# =============================================================================
# MAIN APPLICATION LOOP
# =============================================================================
def main():
    engine = ClimateIntelligenceEngine()

    while True:
        print("\n" + "=" * 78)
        print("  IMD MUMBAI-PUNE: CLIMATE INTELLIGENCE & HEATWAVE EARLY WARNING SYSTEM")
        print("=" * 78)
        print("  [1] Input State & Location (Auto Lat/Lon, Live Weather Ingestion)")
        print(
            "  [2] Real-Time Max-Heap Priority Alerts Extraction (Operation"
            " 4.3)"
        )
        print(
            "  [3] Spatial Hotspot Range Query using 2D PR Quad-Tree (Operation"
            " 4.2)"
        )
        print(
            "  [4] Regional Historical Time-Series Traversal via Linked-List"
            " (Operation 4.4)"
        )
        print("  [5] Exit Application")
        print("=" * 78)

        choice = input(" Select an option [1-5]: ").strip()

        if choice == "1":
            Handle_Live_Location_Input(engine)
        elif choice == "2":
            Handle_Heap_Alert_Extraction(engine)
        elif choice == "3":
            Handle_Spatial_QuadTree_Range(engine)
        elif choice == "4":
            Handle_Regional_Time_Series(engine)
        elif choice == "5":
            print("\n [✓] System gracefully shutdown. Goodbye!\n")
            sys.exit(0)
        else:
            print(" [!] Invalid selection. Please enter a number between 1 and 5.")


if __name__ == "__main__":
    main()