#!/bin/bash
# End-to-end test of the relay stack: SNI passthrough routing, cluster-token
# auth, and cross-tenant subdomain rejection, all in local docker.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

echo "--- validating generated node + head startup scripts"
uv run --project ../../main_service --group dev python render_startup_scripts.py node
uv run --project ../../main_service --group dev python render_startup_scripts.py node-client-hosted
uv run --project ../../client --group dev python render_startup_scripts.py head

echo "--- generating test CA + node cert (mirrors cluster CA + node cert)"
rm -rf certs && mkdir certs
openssl ecparam -name prime256v1 -genkey -noout -out certs/ca.key
openssl req -x509 -new -key certs/ca.key -subj "/CN=Burla e2e CA" -days 7 \
    -out certs/ca.pem
openssl ecparam -name prime256v1 -genkey -noout -out certs/node.key
openssl req -new -key certs/node.key -subj "/CN=Burla node" -out certs/node.csr
openssl x509 -req -in certs/node.csr -CA certs/ca.pem -CAkey certs/ca.key \
    -CAcreateserial -days 7 -out certs/node.pem \
    -extfile <(printf "subjectAltName=DNS:burla-node-1234abcd--test-project.relay.test")
openssl ecparam -name prime256v1 -genkey -noout -out certs/head.key
openssl req -new -key certs/head.key -subj "/CN=Burla head" -out certs/head.csr
openssl x509 -req -in certs/head.csr -CA certs/ca.pem -CAkey certs/ca.key \
    -CAcreateserial -days 7 -out certs/head.pem \
    -extfile <(printf "subjectAltName=DNS:head--test-project.relay.test")

echo "--- starting relay stack"
docker compose down -v --remove-orphans >/dev/null 2>&1 || true
docker compose up -d --build

cleanup() { docker compose --profile manual down -v --remove-orphans >/dev/null 2>&1 || true; }
trap cleanup EXIT

echo "--- waiting for the tunnel to come up"
HOST="burla-node-1234abcd--test-project.relay.test"
PASSTHROUGH_OK=""
for _ in $(seq 1 30); do
    if curl -sS --max-time 2 --cacert certs/ca.pem \
        --resolve "$HOST:8443:127.0.0.1" "https://$HOST:8443/" \
        2>/dev/null | grep -q hello-from-node; then
        PASSTHROUGH_OK=1
        break
    fi
    sleep 1
done
if [ -z "$PASSTHROUGH_OK" ]; then
    echo "FAIL: tunnel never became reachable"
    docker compose logs
    exit 1
fi
echo "PASS: SNI passthrough end-to-end (client -> frps -> tunnel -> node TLS)"

echo "--- deployed head API must route through its public relay hostname"
HEAD_HOST="head--test-project.relay.test"
if ! curl -sS --max-time 5 --cacert certs/ca.pem \
    --resolve "$HEAD_HOST:8443:127.0.0.1" "https://$HEAD_HOST:8443/" \
    | grep -q hello-from-head; then
    echo "FAIL: deployed head relay route is unreachable"
    docker compose logs
    exit 1
fi
echo "PASS: deployed head API routes through the relay"

echo "--- unknown SNI must not route anywhere"
if curl -sS --max-time 5 --insecure \
    --resolve "unknown.relay.test:8443:127.0.0.1" \
    "https://unknown.relay.test:8443/" >/dev/null 2>&1; then
    echo "FAIL: unregistered subdomain unexpectedly routed"
    exit 1
fi
echo "PASS: unregistered subdomain refused"

echo "--- bad cluster token must be rejected at login"
BAD_OUTPUT=$(timeout 20 docker compose run --rm frpc-bad 2>&1 || true)
if ! echo "$BAD_OUTPUT" | grep -q "invalid cluster token"; then
    echo "FAIL: bad token was not rejected. frpc output:"
    echo "$BAD_OUTPUT"
    exit 1
fi
echo "PASS: bad token rejected"

echo "--- cross-tenant subdomain must be rejected at proxy registration"
SQUAT_OUTPUT=$(timeout 20 docker compose run --rm frpc-squat 2>&1 || true)
if ! echo "$SQUAT_OUTPUT" | grep -q "does not belong"; then
    echo "FAIL: squatted subdomain was not rejected. frpc output:"
    echo "$SQUAT_OUTPUT"
    exit 1
fi
echo "PASS: cross-tenant subdomain rejected"

echo ""
echo "ALL RELAY E2E CHECKS PASSED"
