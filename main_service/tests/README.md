### main_service tests

These need a dev cluster running for this checkout. See
[`client/tests/README.md`](../../client/tests/README.md) for the full workflow.

All tests here are service-tier (marked `@pytest.mark.service`). They drive the
live main_service over HTTP via `httpx`. Start a cluster in another terminal with
`make local-dev`, then from the repo root:

```
BURLA_CLUSTER_DASHBOARD_URL=$(make -s cluster-info | awk '/dashboard/{print $2}') \
  uv run --project ./client --group dev pytest main_service/tests -m "service and not chaos"
```

`make test-service` does this for you, defaulting the dashboard URL to this
checkout's head port.
