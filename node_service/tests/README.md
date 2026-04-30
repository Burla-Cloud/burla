### node_service tests

**Run these on a dev VM, not your laptop.** See
[`client/tests/README.md`](../../client/tests/README.md) for the full
workflow. The tests drive real node containers over HTTP; reaching them
from a laptop requires tunnel plumbing that doesn't exist, and the node
bind-mounts assume `/srv/burla` on a dev VM host.

All tests here are service-tier (marked `@pytest.mark.service`). They
use the `node_http_client` fixture in the root `conftest.py`, which
discovers nodes via `main_service`'s `/v1/cluster/state` and talks to
them over the local docker network on the VM. From inside the dev VM:

```
cd /srv/burla
BURLA_TEST_PROJECT=burla-agent-<slot> \
BURLA_CLUSTER_DASHBOARD_URL=http://localhost:5001 \
  uv run --project ./client --group dev pytest node_service/tests -m "service and not chaos"
```
