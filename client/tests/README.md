### How to run the tests

Every tier needs a dev cluster running for **this checkout**. One checkout gets
one cluster, and several checkouts can run clusters at the same time on one
machine without colliding, so tests must be pointed at the right one. The
`make test*` targets handle that for you.

Three targets:

- `make test-service` — service-level tests. Needs a cluster.
- `make test-e2e` — full end-to-end tests, including the 5 scenario flows.
- `make test` — both tiers.

Nothing runs in GitHub Actions.

`make 3.11-dev` through `make 3.14-dev` drop you into a shell on that
interpreter with `BURLA_ENVIRONMENT=test` set, for running these by hand.

#### Start a cluster first

In another terminal, from this checkout:

```
make local-dev      # whole cluster local: head, nodes, and workers as containers
make remote-dev     # head local, nodes are real EC2 in the Burla test AWS account
```

`make cluster-info` prints this checkout's cluster name, dashboard URL, docker
network, and node port base. The head is not on port 5001; it is on a port
derived from the checkout name, so that several clusters coexist.

In `local-dev` the head runs as a host subprocess straight from this checkout
(like `remote-dev`); only the nodes/workers are containers. That needs one node
base image, built from `node_service/Dockerfile` on first run, so no image
registry is involved. Rebuild it with `make local-images` after changing the
node Dockerfile or a `uv.lock`; service code is bind-mounted, so ordinary edits
need no rebuild.

Both dev modes need a working AWS identity and a saved cluster token, because
nodes authorize callers against the backend's user list for this cluster id.
`make local-dev` fails fast telling you to run `aws sso login` or `burla login`
if either is missing.

Use `local-dev` for anything that can be exercised with light resources, and
`remote-dev` when you need real scale or real-VM behavior, or when the machine
cannot take another local cluster. See the `burla-parallel-dev` skill for the
full decision guide.

Tear down with `make stop` (this checkout only) or `make stop-all`.

#### Running the tests

```
make test-service
make test-e2e
```

Each target defaults `BURLA_CLUSTER_DASHBOARD_URL` to this checkout's head port.
To run pytest directly, set it yourself:

```
export BURLA_CLUSTER_DASHBOARD_URL=$(make -s cluster-info | awk '/dashboard/{print $2}')
uv run --project ./client --group dev pytest -m service
```

Readiness gate: the service and e2e tiers refuse to run unless the head is
reachable and is a local dev cluster. They restart and mutate whatever they are
aimed at, so they will never touch a deployed cluster. A failure caused by the
cluster not being ready is not a test failure; start or reset the cluster and
retry.

For now, tests that call `remote_parallel_map` should pass `grow=True` so the job
boots nodes itself instead of relying on an already-started cluster.

All tests have a 120s default timeout. If output doesn't advance past
`collected N items` within 10 seconds, stop and report blocked.

#### Notes for agents

1. Work in your own worktree and run your own cluster. Never reuse another
   agent's cluster, and never `docker rm` containers by `node_*` / `worker_*`
   name prefix; that destroys other agents' clusters. Use `make stop`.
2. Prefer `local-dev` while iterating: node and worker code is bind-mounted, so
   your edits apply on save. In `remote-dev`, node VMs run your branch from
   GitHub, so `node_service` / `worker_server.py` changes need a push first.
3. Keep local clusters small. They default to 1 node; raise with
   `LOCAL_DEV_NODE_QUANTITY` only when a test needs multiple nodes.

#### What changed vs. earlier revisions

- Removed ~130 source-text grep assertions that passed regardless of whether
  the code they claimed to cover was correct. The remaining suite either
  imports and exercises the code under test, or drives it over HTTP against
  the live cluster.
- Added 5 end-to-end scenarios in `tests/scenarios/` that cover full user
  journeys: `test_full_job_lifecycle`, `test_cluster_restart_mid_job`,
  `test_grow_under_load`, `test_udf_error_propagation`,
  `test_detach_and_complete_async`.
- Deleted the Playwright dashboard-UI tests because backend coverage catches
  the regressions that matter.
