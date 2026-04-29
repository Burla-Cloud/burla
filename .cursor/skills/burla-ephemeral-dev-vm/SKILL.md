---
name: burla-ephemeral-dev-vm
description: Provision and use isolated ephemeral GCP VMs for Burla local-dev or remote-dev. Use when the user mentions Burla dev VMs, local-dev or remote-dev on GCP, SSH tunnel dashboards, isolated agent runtimes, or ephemeral VM workflows.
---

# Burla Ephemeral Dev VM

Use the worktree and VM scripts instead of handwritten `git worktree`, `gcloud`, `ssh`, or `scp` sequences. Git worktrees and dev VM slots are separate resources: a worktree holds code, while a slot is reusable compute that can run any synced worktree.

## Defaults

- One active task gets one linked worktree and branch. It can use any available dev VM slot when it needs runtime verification.
- Always edit from the linked worktree, never from the primary checkout.
- Pick the dev VM slot automatically — never ask the user which slot to use. See "Slot Selection" below.
- Default to `--mode local-dev` on the VM; switch to `--mode remote-dev` when real GCE worker VMs are needed.
- Use the script-reported `http://localhost:<port>` URL for browser and client work.
- Release the slot lock when done. Destroy the VM when the task is complete unless the user asked to keep it warm.
- Keep the worktree and branch until explicit cleanup so work-in-progress is not lost.

## Slot Selection

Pick the slot without asking the user. Follow this order:

1. Run `scripts/dev_vm_slot_acquire.sh --source <worktree-path>` from any checkout. It picks the lowest unlocked slot from `00` through `10`.
2. A slot is unavailable only if it has an explicit lock, is being created/destroyed, or an active terminal command is using it.
3. Pending work in a git worktree does not reserve any dev VM slot.
4. You need the user's explicit approval in the current conversation before using any slot above `10`.
5. Slot IDs are zero-padded two-digit strings (`01`, `02`, ...).

## Standard Workflow

1. From the primary checkout, run `scripts/dev-worktree/create.sh --task <task-slug>` or include `--branch <branch-name>`.
2. `cd` into the printed worktree path.
3. Make code changes only from that linked worktree.
4. Acquire a slot: `scripts/dev_vm_slot_acquire.sh --source "$(pwd)"`.
5. Create the VM if needed: `scripts/dev_vm_create.sh --slot <id>`.
6. Wait for bootstrap: `scripts/dev_vm_wait_ssh.sh --slot <id>`.
7. Sync the current worktree: `scripts/dev_vm_sync_repo.sh --slot <id> --source "$(pwd)"`.
8. Start the synced code: `scripts/dev_vm_start.sh --slot <id> --mode <local-dev|remote-dev>`.
9. Run `scripts/dev_vm_tunnel.sh --slot <id>`.
10. Run `scripts/dev_vm_status.sh --slot <id>`.
11. For local client work, run `scripts/dev_vm_client_shell.sh --slot <id> --python <version>`.
12. When done, run `scripts/dev_vm_slot_release.sh --slot <id>`.
13. Destroy the VM with `scripts/dev_vm_destroy.sh --slot <id>` unless the user asked to keep it warm.
14. Remove the worktree later with `scripts/dev-worktree/remove.sh --task <task-slug>` only when you are done with that branch.

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
- Never edit from the primary checkout.
- Never sync a different worktree than the one you intend to test.
- Never share a locked VM slot across active agents.
- Never expose port `5001` publicly.
- Never assume the dashboard URL is `http://localhost:5001`; always read the state file or status output.
- Sync the repo before starting or restarting the main service if local code changed.
- Use raw `gcloud` / `ssh` automation only; do not switch the workflow into Cursor Remote SSH windows.

## Additional Reference

- For naming, ports, state files, and script responsibilities, read [reference.md](reference.md).
