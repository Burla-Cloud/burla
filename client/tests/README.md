### How to run the tests

**All tiers except `test-unit` must run on a dev VM, not on your laptop.** The
cluster tests need real Docker-in-Docker, real Firestore, real GCE-style
networking, and scratch `_node_auth` / `_shared_workspace` directories that get
wiped between runs — all of which work reliably only on a dev VM. Running
`make local-dev` on a laptop is unsupported and will break in ways that look
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
scripts/dev-worktree/create.sh --task <task-slug>
cd ../burla-worktrees/<task-slug>

scripts/dev_vm_slot_acquire.sh --source "$(pwd)"
scripts/dev_vm_prepare_slot.sh --slot <slot>
scripts/dev_vm_create.sh --slot <slot>
scripts/dev_vm_wait_ssh.sh --slot <slot>
scripts/dev_vm_sync_repo.sh --slot <slot> --source "$(pwd)"
scripts/dev_vm_tunnel.sh --slot <slot>
```

Then SSH into the VM and run tests from there:

```
ssh -i ~/.ssh/burla-dev-vm/<slot>_ed25519 jakezuliani@<vm-ip>
cd /srv/burla
make local-dev
```

Open the tunneled dashboard in the GStack browser, sign in with the agent
account, and click Start. Then authorize Burla CLI inside the VM:

```
cd /srv/burla
uv run --project ./client --group dev burla login --no_browser=True
```

Open the printed URL in the same GStack browser session and click authorize.
Then run tests from the VM:

```
curl -sX POST http://localhost:5001/v1/cluster/restart \
  -H "X-User-Email: jakescursoragent@gmail.com" \
  -H "Authorization: Bearer <agent-token>"
# wait for ready_nodes >= 1 at /v1/cluster/state
BURLA_TEST_PROJECT=burla-agent-<slot> \
  uv run --project ./client --group dev pytest -m "not chaos and not dashboard"
```

When done:

```
scripts/dev_vm_slot_release.sh --slot <slot>
scripts/dev_vm_stop.sh --slot <slot>
```

#### Running on a dev VM (agents)

1. **Never run service / e2e / chaos tests on your laptop.** Provision a dev VM.
   The whole suite is designed and verified against the dev-VM environment.
2. Acquire a slot with `scripts/dev_vm_slot_acquire.sh --source <worktree>`.
   Run `scripts/dev_vm_prepare_slot.sh --slot <slot>` before first use.
   If no VM exists for that slot, run `scripts/dev_vm_create.sh --slot <slot>`
   and follow the sequence above.
3. Before every service / e2e run, verify: cluster is reachable through the
   tunnel (`curl http://localhost:<local-dashboard-port>/version` returns 200)
   and `ready_nodes` in `/v1/cluster/state` is ≥ 1.
4. Run the tests on the VM (via SSH). The VM has `uv` at `/usr/local/bin/uv`;
   always invoke pytest via `uv run --project ./client --group dev pytest`.
5. Set `BURLA_TEST_PROJECT=burla-agent-<slot>` so the readiness gate in
   `conftest.py` matches the active project. On a laptop it defaults to
   `burla-test`, which is wrong for agent dev VMs.
6. Readiness gate: if the cluster isn't verifiably READY, stop and investigate.
   A failure caused by cluster-not-ready is NOT a test failure — do not report
   it as one.
7. If the `_node_auth` bind-mount gets orphaned (seen after `dev_vm_sync_repo.sh`
   blows away `/srv/burla`): recreate the host dirs (`sudo mkdir -p /srv/burla/_node_auth
   /srv/burla/_shared_workspace /srv/burla/_worker_service_python_env
   && sudo chmod 777`, then `sudo chown -R $USER /srv/burla`), shut down the
   cluster, then restart. Otherwise nodes will 500 on `NODE_AUTH_CREDENTIALS_PATH.write_text()`.
8. Auth errors (`invalid_grant` / `Invalid JWT Signature`) → run
   `uv run --project ./client --group dev burla login --no_browser=True`
   on the VM and authorize the printed URL in the signed-in GStack browser.
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
