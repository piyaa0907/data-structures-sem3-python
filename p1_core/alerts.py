"""
alerts.py - alert priority queue (binary max-heap stored in a list)

The most urgent alert is always at data[0].
For the node at index i:  parent = (i-1)//2,  children = 2i+1 and 2i+2.
push and pop are both O(log n); peek is O(1).

"More urgent" means: higher severity first; if equal, the older alert
first (it has been waiting longer); if still equal, the hotter one first.
"""
from dataclasses import dataclass


@dataclass
class Alert:
    region_id: int
    severity: int          # 1..5
    temp: float
    anomaly: float
    day: int


def _more_urgent(a, b):
    if a.severity != b.severity:
        return a.severity > b.severity
    if a.day != b.day:
        return a.day < b.day
    return a.temp > b.temp


class AlertQueue:
    def __init__(self):
        self.data = []

    def __len__(self):
        return len(self.data)

    def push(self, alert):
        data = self.data
        data.append(alert)                       # put the new alert at the end ...
        i = len(data) - 1
        while i > 0:                             # ... and sift it UP
            parent = (i - 1) // 2
            if not _more_urgent(alert, data[parent]):
                break
            data[i] = data[parent]               # pull the weaker parent down
            i = parent
        data[i] = alert

    def pop(self):
        """Remove and return the most urgent alert (None if empty)."""
        data = self.data
        if not data:
            return None
        top = data[0]
        last = data.pop()                        # move the last alert to the root ...
        n = len(data)
        if n:
            i = 0
            while True:                          # ... and sift it DOWN
                c = 2 * i + 1
                if c >= n:
                    break
                if c + 1 < n and _more_urgent(data[c + 1], data[c]):
                    c += 1
                if not _more_urgent(data[c], last):
                    break
                data[i] = data[c]
                i = c
            data[i] = last
        return top

    def peek(self):
        return self.data[0] if self.data else None
