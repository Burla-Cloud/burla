### main_service tests

**Run these on a dev VM, not your laptop.** See
[`client/tests/README.md`](../../client/tests/README.md) for the full
workflow. The cluster-level tests need real Docker-in-Docker, real
Firestore access via a service account, and scratch bind-mount
directories — all of which only work reliably on a dev VM.

All tests here are service-tier (marked `@pytest.mark.service`). They
drive the live main_service over HTTP via `httpx`. From inside the dev
VM:

```
cd /srv/burla
BURLA_TEST_PROJECT=burla-agent-<slot> \
BURLA_CLUSTER_DASHBOARD_URL=http://localhost:5001 \
  uv run --project ./client --group dev pytest main_service/tests -m "service and not chaos"
```
