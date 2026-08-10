### How to run the tests

Every tier needs a dev cluster running for **this checkout**. One checkout gets
one cluster, and several checkouts can run clusters at the same time on one
machine without colliding, so tests must be pointed at the right one. The
`make test*` targets handle that for you.

Four targets:

- `make test-e2e` — end-to-end tests through `remote_parallel_map`, including
  the scenario flows in `tests/scenarios/`.
- `make test-dashboard` — dashboard-UI tests through real Chromium
  (`tests/dashboard/`); installs the browser on first run.
- `make test-service` — direct HTTP contract tests.
- `make test` — everything.

Which tier a behavior belongs in:

- User-visible client behavior (results, errors, messages) is tested through
  `remote_parallel_map`, asserting what the user actually sees.
- Dashboard behavior is tested through the browser: the page, its data
  fetches, and the rendered result.
- The service tier keeps only contracts neither user surface can reach or
  pin down deterministically: node/head protocol invariants, boundary
  validation a correct client can't produce (malformed versions, bad months),
  auth bypasses, and precisely seeded state transitions.

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
(like `remote-dev`). Each node is a privileged container acting as a fake VM:
it runs its own docker daemon, and its workers are containers *inside* it,
exactly like on a real VM (`docker exec node_<id> docker ps` to see them). The
node base image is built from `node_service/Dockerfile` on first run, so no
image registry is involved. Rebuild it with `make local-images` after changing
the node Dockerfile or a `uv.lock`; service code is bind-mounted, so ordinary
edits need no rebuild.

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
make test-dashboard
```

Each target defaults `BURLA_CLUSTER_DASHBOARD_URL` to this checkout's head port.
To run pytest directly, set it yourself:

```
export BURLA_CLUSTER_DASHBOARD_URL=$(make -s cluster-info | awk '/dashboard/{print $2}')
uv run --project ./client --group dev pytest -m service
```

Readiness gate: all tiers refuse to run unless the head is reachable and is a
local dev cluster. They restart and mutate whatever they are aimed at, so they
will never touch a deployed cluster. A failure caused by the cluster not being
ready is not a test failure; start or reset the cluster and retry.

For now, tests that call `remote_parallel_map` should pass `grow=True` so the job
boots nodes itself instead of relying on an already-started cluster.

All tests have a 120s default timeout. If output doesn't advance past
`collected N items` within 10 seconds, stop and report blocked.

Three exceptions, all in `tests/scenarios/`, raise their own timeouts:

- `test_node_lost_mid_job.py` runs ~4-5 minutes. A node that stops answering
  (with no lifecycle signal on the job doc) is only failed after the client's
  3-minute result-poll silence budget, so it cannot finish sooner.
- `test_cluster_shutdown_mid_job.py` keeps similar headroom but normally
  finishes fast: shutdown is recorded on the job doc, which the client reads
  as soon as its node stops answering.
- `test_node_silent_but_alive.py` freezes a node for a minute, and runs a
  workload that pins a node for several, to prove neither is mistaken for a
  dead node.

#### Notes for agents

1. Work in your own worktree and run your own cluster. Never reuse another
   agent's cluster, and never `docker rm` containers by `node_*` name prefix;
   that destroys other agents' clusters. Use `make stop`. (Workers live inside
   their node's own docker daemon and die with it.)
2. Prefer `local-dev` while iterating: node and worker code is bind-mounted, so
   your edits apply on save. In `remote-dev`, node VMs run your branch from
   GitHub, so `node_service` / `worker_server.py` changes need a push first.
3. Keep local clusters small. They default to 1 node; raise with
   `LOCAL_DEV_NODE_QUANTITY` only when a test needs multiple nodes.

#### What changed vs. earlier revisions

- Removed ~130 source-text grep assertions that passed regardless of whether
  the code they claimed to cover was correct. The remaining suite either
  exercises real behavior over HTTP or drives the product surfaces directly.
- Moved user-visible client behavior (version mismatch, image mismatch) from
  endpoint assertions to `remote_parallel_map` tests that assert the message
  the user reads, and moved dashboard happy paths into browser tests in
  `tests/dashboard/`.
- Deleted service tests that were shape-only, accepted several outcomes at
  once, or duplicated an e2e journey.
