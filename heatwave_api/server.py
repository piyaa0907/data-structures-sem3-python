"""HTTP server for the heatwave API. Standard library only.

Run from the project root::

    python -m heatwave_api.server --port 8000

The bind address defaults to 127.0.0.1 because the API has no authentication.
Responses carry ``Access-Control-Allow-Origin: *`` so a dashboard served from
another local port can call it from the browser.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Optional, Sequence

from heatwave_pipeline import config

from .engine import EngineCache
from .routes import ApiContext, dispatch

log = logging.getLogger("heatwave_api")


class ApiRequestHandler(BaseHTTPRequestHandler):
    server_version = "HeatwaveAPI/1.0"

    def do_GET(self) -> None:
        self._respond("GET")

    def do_POST(self) -> None:
        self._respond("POST")

    def do_PUT(self) -> None:
        self._respond("PUT")

    def do_DELETE(self) -> None:
        self._respond("DELETE")

    def _respond(self, method: str) -> None:
        status, payload = dispatch(self.server.context, method, self.path)
        body = json.dumps(payload, indent=2, default=str).encode("utf-8")

        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        if status == 405:
            self.send_header("Allow", "GET")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args) -> None:  # route access logs through logging
        log.info("%s - %s", self.address_string(), format % args)


def make_server(db_path, host: str = "127.0.0.1", port: int = 8000) -> ThreadingHTTPServer:
    """Create (but do not start) a server. The database must already exist.

    The engine is built here so that a broken database fails at startup, not on
    the first request.
    """
    path = Path(db_path)
    if not path.exists():
        raise FileNotFoundError(f"database not found at {path}; run 'python main.py init' first")
    cache = EngineCache(path)
    cache.get()
    server = ThreadingHTTPServer((host, port), ApiRequestHandler)
    server.context = ApiContext(path, cache)
    return server


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="heatwave-api", description="Heatwave forecast and data API.")
    parser.add_argument("--db", default=str(config.DEFAULT_DB_PATH), help="SQLite database path")
    parser.add_argument("--host", default="127.0.0.1", help="bind address (default 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="port (default 8000)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(name)s: %(message)s")
    try:
        server = make_server(args.db, args.host, args.port)
    except (FileNotFoundError, OSError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    engine = server.context.engine()
    host, port = server.server_address[:2]
    print(
        f"Serving on http://{host}:{port} (db: {args.db}; "
        f"{len(engine.locations)} locations, {engine.alerts_raised} alerts). Ctrl+C to stop."
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
