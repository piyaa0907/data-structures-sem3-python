"""Heatwave API: read-only HTTP endpoints over the database and Person 1's engine.

Data endpoints are served from the Person 2 repository layer. Forecast, alert
and location endpoints are served from an in-memory engine that replays the
database into Person 1's quad-tree, B-tree and alert queue.
"""

__version__ = "1.0.0"
