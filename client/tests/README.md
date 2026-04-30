### How to run the tests

**All tiers except `test-unit` must run on a dev VM, not on your laptop.** The
cluster tests need real Docker-in-Docker, real Firestore, real GCE-style
networking, and scratch `_node_auth` / `_shared_workspace` directories that get
wiped between runs — all of which work reliably only on a dev VM. Running
`make -f makefile local-dev` on a laptop is unsupported and will break in ways that look
like test bugs (port collisions, orphaned bind mounts, macOS Docker
idiosyncrasies, laptop ADC credential expiry mid-run).

Four tiers:

- `make test-unit` — pure unit tests. No cluster, no GCP. Fast (~10s). **The
  only tier safe to run on a laptop.**
- `make test-service` — service-level tests. **Dev VM only.**
- `make test-e2e` — full end-to-end tests, including the 5 scenario flows.
  **Dev VM only.**
- `make test-chaos` — destructive tests that restart / shut down / mutate the
  cluster. Each test needs a cluster reset between runs. **Dev VM only.**
- `make test` — all non-chaos tiers. **Dev VM only.**

Nothing runs in GitHub Actions.

#### Running on a dev VM (humans)

Follow the ephemeral dev-VM workflow (see [`.cursor/skills/burla-ephemeral-dev-vm/SKILL.md`](../../.cursor/skills/burla-ephemeral-dev-vm/SKILL.md)):

```
# From the primary checkout
git worktree add -b <task-branch> ../burla-worktrees/<task-slug> main
cd ../burla-worktrees/<task-slug>

scripts/dev_vm_create.sh
scripts/dev_vm_sync_repo.sh --slot <slot> --source "$(pwd)"
scripts/dev_vm_tunnel.sh --slot <slot>
```

Then open a VM shell and run tests from there:

```
scripts/dev_vm_shell.sh --slot <slot>
make -f makefile local-dev
```

Run `make -f makefile local-dev` / `make -f makefile remote-dev` inside
`dev_vm_shell.sh`. Do not run those targets through non-interactive SSH; Docker
needs a real terminal.

Open the tunneled dashboard in the GStack browser and click Start. Force
VM-local tests and smoke jobs to hit the dev server:

```
cd /srv/burla
export BURLA_CLUSTER_DASHBOARD_URL=http://localhost:5001
```

Then run tests from the VM:

```
BURLA_TEST_PROJECT=burla-agent-<slot> \
BURLA_CLUSTER_DASHBOARD_URL=http://localhost:5001 \
  uv run --project ./client --group dev pytest -m "not chaos and not dashboard"
```

When done:

```
scripts/dev_vm_stop.sh --slot <slot>
```

#### Running on a dev VM (agents)

1. **Never run service / e2e / chaos tests on your laptop.** Provision a dev VM.
   The whole suite is designed and verified against the dev-VM environment.
2. Run `scripts/dev_vm_create.sh`. It picks an available slot, prepares the
   project if needed, and starts or creates the VM.
3. Sync the worktree explicitly with
   `scripts/dev_vm_sync_repo.sh --slot <slot> --source <worktree>`.
4. For now, tests that call `remote_parallel_map` should pass `grow=True` so the
   job boots nodes itself instead of relying on an already-started cluster.
   Do this for smoke tests too.
5. Run the tests on the VM with `scripts/dev_vm_shell.sh --slot <slot>`.
   Start the cluster with `make -f makefile local-dev`. The VM has `uv` at `/usr/local/bin/uv`;
   always invoke pytest via `uv run --project ./client --group dev pytest`.
6. Set `BURLA_CLUSTER_DASHBOARD_URL=http://localhost:5001` for VM-local
   client/test commands so they hit the dev server.
7. Set `BURLA_TEST_PROJECT=burla-agent-<slot>` so the readiness gate in
   `conftest.py` matches the active project. On a laptop it defaults to
   `burla-test`, which is wrong for agent dev VMs.
8. Readiness gate: if the cluster isn't verifiably READY, stop and investigate.
   A failure caused by cluster-not-ready is NOT a test failure — do not report
   it as one.
9. All tests have a 120s default timeout. If output doesn't advance past
   `collected N items` within 10 seconds, stop and report blocked.

#### Unit tier on a laptop

Only `make test-unit` is safe to run on a laptop. It imports real modules
and runs real logic for version parsing, signal handlers, exception
messages, package detection, and `_local_host_from` — no cluster needed:

```
uv sync --project ./client --group dev
uv run --project ./client --group dev pytest -m unit
```

#### What changed vs. earlier revisions

- Removed ~130 source-text grep assertions that passed regardless of whether
  the code they claimed to cover was correct. The remaining suite either
  imports and exercises the code under test, or drives it over HTTP against
  the live cluster.
- Added 5 end-to-end scenarios in `tests/scenarios/` that cover full user
  journeys: `test_full_job_lifecycle`, `test_cluster_restart_mid_job`,
  `test_grow_under_load`, `test_udf_error_propagation`,
  `test_detach_and_complete_async`.
- Deleted the Playwright dashboard-UI tests — backend coverage catches
  regressions that matter; UI smoke tests are out of scope.
