### node_service tests

These need a `make local-dev` cluster running for this checkout, because they
drive real node containers directly. See
[`client/tests/README.md`](../../client/tests/README.md) for the full workflow.

All tests here are service-tier (marked `@pytest.mark.service`). They use the
`node_http_client` fixture in the root `conftest.py`, which discovers nodes via
`main_service`'s `/v1/cluster/state` and reaches each one at the host port it
publishes. From the repo root:

```
BURLA_CLUSTER_DASHBOARD_URL=$(make -s cluster-info | awk '/dashboard/{print $2}') \
  uv run --project ./client --group dev pytest node_service/tests -m service
```

`make test-service` does this for you, defaulting the dashboard URL to this
checkout's head port.
