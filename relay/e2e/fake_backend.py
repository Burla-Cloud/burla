"""Stub of backend.burla.dev token validation, used by run_e2e.sh."""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

ROUTING = {
    ("test-project", "good-token"): {
        "dashboard_url": "https://head--test-project.relay.test",
        "relay_dashboard_url": "https://head--test-project.relay.test",
        "pending_custom_hostname": "tract.burla.test",
    },
    ("other-project", "other-token"): {
        "dashboard_url": "https://head--other-project.relay.test",
        "relay_dashboard_url": "https://head--other-project.relay.test",
        "pending_custom_hostname": None,
    },
    ("conflict-project", "conflict-token"): {
        "dashboard_url": "https://head--conflict-project.relay.test",
        "relay_dashboard_url": "https://head--conflict-project.relay.test",
        "pending_custom_hostname": "tract.burla.test",
    },
}


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        path_parts = self.path.strip("/").split("/")
        project_id = path_parts[2] if len(path_parts) == 4 else ""
        token = self.headers.get("Authorization", "").removeprefix("Bearer ")
        routing = ROUTING.get((project_id, token))
        self.send_response(200 if routing else 401)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(routing or {}).encode())

    def log_message(self, *args):
        pass


HTTPServer(("0.0.0.0", 9100), Handler).serve_forever()
