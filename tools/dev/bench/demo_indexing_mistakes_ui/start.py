#!/usr/bin/env python3
"""
Static file server + same-origin REST proxy for the demo UI.

Serves this directory's static files at /. Any request whose path starts
with /api/ is forwarded to the Weaviate cluster at CLUSTER_URL with the
API key from WCD_API_KEY injected as a Bearer token.

The browser only ever talks to http://localhost:PORT, so the cluster's
CORS policy (which only allows the console origin) doesn't apply. As a
bonus the API key never reaches the browser - it stays in the proxy
process's env.

Usage:
    WCD_API_KEY=$(wcs --dev token) ./start.py
"""

from __future__ import annotations

import http.server
import os
import socketserver
import sys
import urllib.error
import urllib.request

CLUSTER_URL = os.environ.get(
    "WCD_URL",
    "https://uqkg8qogqj6phkndzmyhww.c0.europe-west3.dev.gcp.weaviate.cloud",
).rstrip("/")
PORT = int(os.environ.get("PORT", "8089"))

# Hop-by-hop headers are not forwarded.
HOP_BY_HOP = {
    "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
    "te", "trailers", "transfer-encoding", "upgrade", "host",
}


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.startswith("/api/"):
            self._proxy("GET")
            return
        super().do_GET()

    def do_POST(self) -> None:
        if self.path.startswith("/api/"):
            self._proxy("POST")
            return
        self.send_error(404)

    def do_PUT(self) -> None:
        if self.path.startswith("/api/"):
            self._proxy("PUT")
            return
        self.send_error(404)

    def do_DELETE(self) -> None:
        if self.path.startswith("/api/"):
            self._proxy("DELETE")
            return
        self.send_error(404)

    def _proxy(self, method: str) -> None:
        upstream_url = CLUSTER_URL + self.path[len("/api"):]

        body: bytes | None = None
        content_length = self.headers.get("Content-Length")
        if content_length:
            body = self.rfile.read(int(content_length))

        req = urllib.request.Request(upstream_url, data=body, method=method)
        req.add_header("Authorization", f"Bearer {os.environ.get('WCD_API_KEY', '')}")
        for hdr in ("Content-Type", "Accept"):
            value = self.headers.get(hdr)
            if value:
                req.add_header(hdr, value)

        try:
            resp = urllib.request.urlopen(req, timeout=120)
            status = resp.status
            response_body = resp.read()
            response_headers = resp.headers
        except urllib.error.HTTPError as e:
            status = e.code
            response_body = e.read()
            response_headers = e.headers
        except urllib.error.URLError as e:
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(f'{{"error":"upstream: {e.reason}"}}'.encode())
            return

        self.send_response(status)
        for k, v in response_headers.items():
            if k.lower() in HOP_BY_HOP or k.lower() == "content-length":
                continue
            self.send_header(k, v)
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        self.wfile.write(response_body)

    def log_message(self, fmt: str, *args) -> None:
        # Quiet the default per-request log; we get plenty of signal from
        # browser devtools.
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))


class ThreadedServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


def main() -> int:
    if not os.environ.get("WCD_API_KEY"):
        print("Error: WCD_API_KEY env var not set.", file=sys.stderr)
        print("Get a token with: wcs --dev token", file=sys.stderr)
        print("Then run: WCD_API_KEY=$(wcs --dev token) ./start.py", file=sys.stderr)
        return 1

    print(f"Proxying /api/* -> {CLUSTER_URL}")
    print(f"Open http://localhost:{PORT}/")
    server = ThreadedServer(("", PORT), Handler)
    # Local-only dev UI bound to localhost: PORT — HTTP is intentional.
    # The proxy talks to {CLUSTER_URL} over HTTPS (set above); the
    # browser never leaves the loopback interface. Suppressing the
    # Sonar "Use https" hotspot because adding TLS to localhost-only
    # dev tooling is friction without security gain.
    server.serve_forever()  # NOSONAR python:S5332
    return 0


if __name__ == "__main__":
    sys.exit(main())
