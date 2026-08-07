### node_service tests

These need a `make remote-dev` cluster running for this checkout: they are
marked `remote_dev` because local-dev nodes are containers reached over plain
http on localhost, which skips the real host, TLS, and cluster-CA path these
endpoints serve. See
[`client/tests/README.md`](../../client/tests/README.md) for the full workflow.

All tests here are service-tier (marked `@pytest.mark.service`, plus
`@pytest.mark.remote_dev`). They use the
`node_http_client` fixture in the root `conftest.py`, which discovers nodes via
`main_service`'s `/v1/cluster/state` and reaches each one at the host port it
publishes. From the repo root:

```
BURLA_CLUSTER_DASHBOARD_URL=$(make -s cluster-info | awk '/dashboard/{print $2}') \
  uv run --project ./client --group dev pytest node_service/tests -m service
```

`make test-service` does this for you, defaulting the dashboard URL to this
checkout's head port.
