---
name: burla-ephemeral-dev-vm
description: Provision and use isolated ephemeral GCP VMs for Burla local-dev or remote-dev. Use when the user mentions Burla dev VMs, local-dev or remote-dev on GCP, SSH tunnel dashboards, isolated agent runtimes, or ephemeral VM workflows.
---

# Burla Ephemeral Dev VM

Use the worktree and VM scripts instead of handwritten `git worktree`, `gcloud`, `ssh`, or `scp` sequences.

## Defaults

- One active agent task gets one fresh linked worktree, one fresh task branch, and one fresh VM.
- Always edit from the linked worktree, never from the primary checkout.
- Pick the agent slot automatically — never ask the user which slot to use. See "Slot Selection" below.
- Default to `--mode local-dev` on the VM; switch to `--mode remote-dev` when real GCE worker VMs are needed.
- Use the script-reported `http://localhost:<port>` URL for browser and client work.
- Destroy the VM when the task is complete unless the user asked to keep it.
- Keep the worktree and branch until explicit cleanup so work-in-progress is not lost.

## Slot Selection

Pick the slot without asking the user. Follow this order:

1. List existing slots in `../burla-worktrees/agent-*/`.
2. A slot is "in use" if its directory contains any task subdirectories. Pick the lowest-numbered slot whose directory is missing or empty.
3. If every existing slot is in use, create the next sequential slot (e.g. `agent-05` after `agent-04`). The `scripts/dev_vm_create.sh` step provisions the GCP project and SSH key for a new slot automatically.
4. Slot IDs are zero-padded two-digit strings (`01`, `02`, ...).

## Standard Workflow

1. From the primary checkout, run `scripts/dev-worktree/create.sh --agent <id> --task <task-slug>`.
2. `cd` into the printed worktree path.
3. Make code changes only from that linked worktree.
4. Run `scripts/dev_vm_create.sh --agent <id>`.
5. Run `scripts/dev_vm_wait_ssh.sh --agent <id>`.
6. Run `scripts/dev_vm_sync_repo.sh --agent <id>`.
7. Run `scripts/dev_vm_start.sh --agent <id> --mode <local-dev|remote-dev>`.
8. Run `scripts/dev_vm_tunnel.sh --agent <id>`.
9. Run `scripts/dev_vm_status.sh --agent <id>`.
10. For local client work, run `scripts/dev_vm_client_shell.sh --agent <id> --python <version>`.
11. When done, run `scripts/dev_vm_destroy.sh --agent <id>`.
12. Remove the worktree later with `scripts/dev-worktree/remove.sh --agent <id> --task <task-slug>` only when you are done with that branch.

Switching modes on a running VM: re-run step 7 with the other `--mode`. The start script tears down the previous `main_service` container and tmux session before starting the new mode, so only one mode runs at a time.

## Mode Trade-offs

| Mode | Nodes | Hot-reload | GCP cost | When to use |
|------|-------|-----------|----------|-------------|
| `local-dev` | 2x `n4-standard-2` docker containers on the same VM | `main_service` + `node_service` + `worker_service` | VM only | Default. Fast loop, no cluster to babysit. |
| `remote-dev` | Real GCE VMs in the agent's project, sized from the firestore `cluster_config` | `main_service` only (node/worker code is pinned to `CURRENT_BURLA_VERSION` on public GitHub) | VM + worker VMs | Reproducing bugs that only surface with real VM cold-starts, GPU images, multi-node grow/shrink, or real firewall/IAM paths. |

Caveats for `remote-dev`:

- Uncommitted edits under `node_service/` or `worker_server.py` do NOT reach worker VMs. The node startup script does `git fetch --depth=1 origin "{CURRENT_BURLA_VERSION}"` against the public repo. To test node-side changes remotely you must bump `CURRENT_BURLA_VERSION` in the four pinned places and push a matching tag.
- Nested `remote_parallel_map` inside a UDF fails: workers use the `cluster_dashboard_url` the client sent (e.g. `http://localhost:<tunnel_port>`), which is not reachable from a GCE VM. Top-level RPM works fine.
- `dev_vm_destroy.sh` best-effort POSTs `/v1/cluster/shutdown` before deleting the VM so worker VMs get cleaned up. If the VM is already unreachable, worker VMs fall back to the per-node inactivity timeout (default 10 min) or get nuked with `--delete-project`.

## Guardrails

- Never edit the primary checkout for an agent task.
- Never run the `scripts/dev_vm_*.sh` commands from the primary checkout.
- The current branch must match `agent/<id>/<task-slug>` before VM scripts run.
- Never share a VM across active agents.
- Never expose port `5001` publicly.
- Never assume the dashboard URL is `http://localhost:5001`; always read the state file or status output.
- Sync the repo before starting or restarting the main service if local code changed.
- Use raw `gcloud` / `ssh` automation only; do not switch the workflow into Cursor Remote SSH windows.

## Additional Reference

- For naming, ports, state files, and script responsibilities, read [reference.md](reference.md).
