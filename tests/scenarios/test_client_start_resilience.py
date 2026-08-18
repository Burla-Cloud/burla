from __future__ import annotations

import http.client
import socket
import ssl
import threading
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from ipaddress import ip_address
from urllib.parse import urlsplit

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

pytestmark = [pytest.mark.e2e, pytest.mark.slow]


class _FaultProxy(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, upstream_url: str, tls_context: ssl.SSLContext):
        super().__init__(("127.0.0.1", 0), _ProxyHandler)
        self.upstream = urlsplit(upstream_url)
        self.tls_context = tls_context
        self.lock = threading.Lock()
        self.tls_failures_remaining = 1
        self.tls_failures_injected = 0
        self.start_job_ids: list[str] = []
        self.dropped_start_response = False

    def get_request(self):
        while True:
            connection, address = super().get_request()
            with self.lock:
                reject = self.tls_failures_remaining > 0
                if reject:
                    self.tls_failures_remaining -= 1
                    self.tls_failures_injected += 1
            if reject:
                connection.close()
                continue
            return self.tls_context.wrap_socket(connection, server_side=True), address


class _ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self._proxy()

    def do_POST(self):
        self._proxy()

    def do_PATCH(self):
        self._proxy()

    def do_PUT(self):
        self._proxy()

    def _proxy(self):
        content_length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(content_length) if content_length else None
        upstream = self.server.upstream
        connection = http.client.HTTPConnection(
            upstream.hostname,
            upstream.port,
            timeout=30,
        )
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in {"connection", "content-length", "host"}
        }
        headers["Host"] = upstream.netloc
        if body is not None:
            headers["Content-Length"] = str(len(body))
        connection.request(self.command, self.path, body=body, headers=headers)
        response = connection.getresponse()
        response_body = response.read()
        connection.close()

        drop_response = False
        if self.command == "POST" and self.path.startswith("/v1/jobs/"):
            path_parts = self.path.split("/")
            if len(path_parts) == 5 and path_parts[-1] == "start":
                with self.server.lock:
                    self.server.start_job_ids.append(path_parts[-2])
                    if not self.server.dropped_start_response:
                        self.server.dropped_start_response = True
                        drop_response = True

        if drop_response:
            self.close_connection = True
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return

        self.send_response(response.status, response.reason)
        for key, value in response.getheaders():
            if key.lower() not in {
                "connection",
                "content-length",
                "transfer-encoding",
            }:
                self.send_header(key, value)
        self.send_header("Content-Length", str(len(response_body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(response_body)

    def log_message(self, format, *args):
        return


def _tls_context(tmp_path) -> tuple[ssl.SSLContext, str]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.now(timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    certificate_path = tmp_path / "fault-proxy-cert.pem"
    key_path = tmp_path / "fault-proxy-key.pem"
    certificate_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certificate_path, key_path)
    return context, str(certificate_path)


def test_job_start_survives_tls_and_lost_response(
    rpm_subprocess,
    local_dev_cluster,
    main_http_client,
    tmp_path,
):
    tls_context, certificate_path = _tls_context(tmp_path)
    proxy = _FaultProxy(local_dev_cluster["url"], tls_context)
    proxy_thread = threading.Thread(target=proxy.serve_forever, daemon=True)
    proxy_thread.start()
    proxy_url = f"https://localhost:{proxy.server_address[1]}"

    try:
        result = rpm_subprocess(
            "def test_function(x):\n    return x * 2\n",
            [1, 2, 3],
            timeout_seconds=120,
            env_overrides={
                "BURLA_CLUSTER_DASHBOARD_URL": proxy_url,
                "SSL_CERT_FILE": certificate_path,
            },
        )
    finally:
        proxy.shutdown()
        proxy.server_close()
        proxy_thread.join(timeout=5)

    assert result["ok"], result.get("traceback")
    assert sorted(result["outputs"]) == [2, 4, 6]
    assert proxy.tls_failures_injected == 1
    assert proxy.dropped_start_response is True
    assert len(proxy.start_job_ids) == 2
    assert len(set(proxy.start_job_ids)) == 1

    job = main_http_client.get(f"/v1/jobs/{proxy.start_job_ids[0]}").json()
    assert job["status"] == "COMPLETED"
    assert job["n_inputs"] == 3
