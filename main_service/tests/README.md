### main_service tests

These tests run against a live `make local-dev` cluster via `httpx` against
`http://localhost:5001`. See `client/tests/README.md` for how to start a
cluster before running them.

Invoke via `make test-service` (all service tests) or
`uv run --project ./client --group dev pytest main_service/tests`.
