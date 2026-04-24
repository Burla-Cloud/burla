### node_service tests

These tests run against a live `make local-dev` cluster via `httpx` against
the per-node ports exposed by main_service (see the `node_http_client`
fixture in `conftest.py`). See `client/tests/README.md` for how to start a
cluster before running them.

Invoke via `make test-service` or
`uv run --project ./client --group dev pytest node_service/tests`.
