"""Stub of backend.burla.dev token validation, used by run_e2e.sh."""

from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        ok = (
            self.path == "/v1/clusters/test-project/dashboard_url"
            and self.headers.get("Authorization") == "Bearer good-token"
        )
        self.send_response(200 if ok else 401)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"dashboard_url": "https://x"}' if ok else b"{}")

    def log_message(self, *args):
        pass


HTTPServer(("0.0.0.0", 9100), Handler).serve_forever()
