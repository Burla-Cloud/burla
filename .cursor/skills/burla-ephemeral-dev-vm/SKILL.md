---
name: burla-ephemeral-dev-vm
description: Provision and use isolated ephemeral GCP VMs for Burla local-dev or remote-dev. Use when the user mentions Burla dev VMs, local-dev or remote-dev on GCP, SSH tunnel dashboards, isolated agent runtimes, or ephemeral VM workflows.
---

# Burla Ephemeral Dev VM

Use Git worktrees for code isolation and VM scripts for cloud resources. Git worktrees and dev VM slots are separate resources: a worktree holds code, while a slot is reusable compute that can run any synced worktree.

## Defaults

- One active task gets one linked worktree and branch. It can use any available dev VM slot when it needs runtime verification.
- Always edit from the linked worktree, never from the primary checkout.
- Pick the dev VM slot automatically — never ask the user which slot to use. See "Slot Selection" below.
- Run `make -f makefile local-dev` or `make -f makefile remote-dev` in the VM shell after syncing code. Always pass `-f makefile`; the repo uses a lowercase makefile.
- Run VM-local client smoke jobs with `BURLA_CLUSTER_DASHBOARD_URL=http://localhost:5001` so the client talks to the dev server.
- Use the script-reported `http://localhost:<port>` URL for browser work.
- Stop the VM instead of deleting it so the next task can reuse the bootstrapped slot.
- Keep the worktree and branch until explicit cleanup so work-in-progress is not lost.

## Slot Selection

`scripts/dev_vm_create.sh` picks the lowest available slot from `00` through `10`. A slot is available only when its stable VM is missing or stopped. Pending work in a git worktree does not reserve any dev VM slot. You need the user's explicit approval in the current conversation before using any slot above `10`.

## Standard Workflow

1. From the primary checkout, run `git worktree add -b <branch-name> ../burla-worktrees/<task-slug> main`.
2. `cd` into the printed worktree path.
3. Make code changes only from that linked worktree.
4. Run `scripts/dev_vm_create.sh`; it chooses a slot, starts or creates the VM, waits for SSH/bootstrap readiness, and prints the slot state.
5. Run `scripts/dev_vm_sync_repo.sh --slot <id> --source "$(pwd)"`.
6. Run `scripts/dev_vm_shell.sh --slot <id>` and start Burla inside that TTY shell with `make -f makefile local-dev` or `make -f makefile remote-dev`. Do not start these targets through non-interactive SSH; Docker needs a real terminal.
7. Run `scripts/dev_vm_tunnel.sh --slot <id>` when you need dashboard/browser access.
8. For VM-local client smoke jobs, rely on ADC bootstrap and set `BURLA_CLUSTER_DASHBOARD_URL=http://localhost:5001`, for example: `BURLA_CLUSTER_DASHBOARD_URL=http://localhost:5001 uv run --project ./client --group dev python smoke.py`.
9. Stop the VM with `scripts/dev_vm_stop.sh --slot <id>` when the slot should go idle.
10. Remove the worktree later with `git worktree remove ../burla-worktrees/<task-slug>` only when you are done with that branch.

Switching modes on a running VM: stop the foreground make command in the VM shell, then run the other mode from `/srv/burla` with `make -f makefile local-dev` or `make -f makefile remote-dev`.

## Mode Trade-offs

| Mode | Nodes | Hot-reload | GCP cost | When to use |
|------|-------|-----------|----------|-------------|
| `local-dev` | 2x `n4-standard-2` docker containers on the same VM | `main_service` + `node_service` + `worker_service` | VM only | Default. Fast loop, no cluster to babysit. |
| `remote-dev` | Real GCE VMs in the agent's project, sized from the firestore `cluster_config` | `main_service` only (node/worker code is pinned to `CURRENT_BURLA_VERSION` on public GitHub) | VM + worker VMs | Reproducing bugs that only surface with real VM cold-starts, GPU images, multi-node grow/shrink, or real firewall/IAM paths. |

Caveats for `remote-dev`:

- Uncommitted edits under `node_service/` or `worker_server.py` do NOT reach worker VMs. The node startup script does `git fetch --depth=1 origin "{CURRENT_BURLA_VERSION}"` against the public repo. To test node-side changes remotely you must bump `CURRENT_BURLA_VERSION` in the four pinned places and push a matching tag.
- Nested `remote_parallel_map` inside a UDF fails: workers use the `cluster_dashboard_url` the client sent (e.g. `http://localhost:<tunnel_port>`), which is not reachable from a GCE VM. Top-level RPM works fine.
- Set `BURLA_CLUSTER_DASHBOARD_URL=http://localhost:5001` for tests or smoke jobs that should hit the running dev server.
- `dev_vm_stop.sh` best-effort POSTs `/v1/cluster/shutdown` before stopping the VM so worker VMs get cleaned up. The local state file is kept so the stopped VM can be restarted by the next task.

## Guardrails

- Never edit the primary checkout for an agent task.
- Never edit from the primary checkout.
- Never sync a different worktree than the one you intend to test.
- Never share a running VM slot across active agents.
- Never expose port `5001` publicly.
- Never assume the browser/tunnel URL is `http://localhost:5001`; use the URL printed by `scripts/dev_vm_create.sh`. Inside the VM, use `http://localhost:5001` for client commands that should target the dev server.
- Sync the repo before starting or restarting the main service if local code changed.
- `scripts/dev_vm_common.sh` is bash-only. If you must source it for ad hoc state helpers, run from bash, e.g. `bash -lc 'source scripts/dev_vm_common.sh && load_state_vars 02 && ...'`; do not source it from zsh.
- Use raw `gcloud` / `ssh` automation only; do not switch the workflow into Cursor Remote SSH windows.

## Script Roles

- `scripts/dev_vm_create.sh`: choose/start/create a slot VM and prepare its project if needed.
- `scripts/dev_vm_sync_repo.sh`: rsync the worktree to `/srv/burla`, including uncommitted changes and deletions while excluding generated directories.
- `scripts/dev_vm_tunnel.sh`: forward dashboard and Vite ports.
- `scripts/dev_vm_shell.sh`: open an interactive SSH shell in `/srv/burla`.
- `scripts/dev_vm_stop.sh`: shut down the cluster, stop tunnels, and stop the VM.
