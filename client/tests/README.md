### How to run the tests

Four tiers. Only `test-unit` runs without a live cluster.

- `make test-unit` — pure unit tests. No cluster, no GCP. Fast (~10s).
- `make test-service` — service-level tests. Requires `make local-dev`.
- `make test-e2e` — full end-to-end tests against `remote_parallel_map`, including the 5 scenario flows. Requires `make local-dev`.
- `make test-chaos` — destructive tests that restart or mutate the cluster. Run each test individually with a cluster reset between.
- `make test` — all non-chaos tiers.

Nothing runs in GitHub Actions.

#### Instructions for humans

Start a local cluster in the worktree you're editing:

```
make local-dev       # full cluster in docker on this machine
# OR
make remote-dev      # main_service local, node VMs in the cloud
```

Open `http://localhost:5001` and hit **Start** to boot nodes. Then:

```
uv sync --project ./client --group dev
make test            # or test-unit / test-service / test-e2e individually
```

Errors surface in docker desktop container logs (local-dev) or Google Cloud Logging (remote-dev, for node/worker logs).

#### Instructions for agents

1. Verify the local-dev cluster is running before every service/e2e run: `curl http://localhost:5001/version` must return 200 and `/v1/cluster/state` must show at least one `ready_nodes` entry.
2. Verify gcloud project: `gcloud config get-value project` must be `burla-test` (or whichever dev-VM project you're working in). If not, `gcloud config set project <project>`.
3. If local-dev isn't running: `make local-dev`, then open `http://localhost:5001` and press **Start**. Wait for at least one node to be READY.
4. Credentials: tests read auth headers from `burla login`'s `burla_credentials.json`. On a dev VM the agent credentials (`jakescursoragent@gmail.com`) are pre-provisioned; on your laptop run `burla login --no_browser=True` if the service tier returns 401.
5. Readiness gate: if the cluster isn't verifiably READY, stop and investigate. A failure caused by cluster-not-ready is NOT a test failure — do not report it as one.
6. Auth errors (`invalid_grant` / `Invalid JWT Signature`) → `burla login --no_browser=True` and re-authorize.
7. Always invoke pytest via uv so path/venv issues are handled:
   - `uv run --project ./client --group dev pytest -m unit`
   - `uv run --project ./client --group dev pytest -m "service and not chaos"`
   - `uv run --project ./client --group dev pytest -m "e2e and not chaos"`
8. All tests have a 120s default timeout. If output doesn't advance past `collected N items` within 10 seconds, stop and report blocked.

#### What changed vs. earlier revisions

- Removed ~130 source-text grep assertions that passed regardless of whether the code they claimed to cover was correct. The remaining suite either imports and exercises the code under test, or drives it over HTTP against the live cluster.
- Added 5 end-to-end scenarios in `tests/scenarios/` that cover full user journeys: `test_full_job_lifecycle`, `test_cluster_restart_mid_job`, `test_grow_under_load`, `test_udf_error_propagation`, `test_detach_and_complete_async`.
- Deleted the Playwright dashboard-UI tests — backend coverage catches regressions that matter; UI smoke tests are out of scope.
