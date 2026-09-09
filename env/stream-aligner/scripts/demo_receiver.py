#!/usr/bin/env python3
"""Minimal downstream receiver for the demo: prints every delivery it gets as
one JSON line, so scripts/demo.sh can show what was pushed, in which order.

Usage: python3 scripts/demo_receiver.py [port]   (default 8901)
"""
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"{}")
        print(json.dumps(body, ensure_ascii=False), flush=True)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8901
    print(f"demo receiver listening on :{port}", file=sys.stderr)
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
