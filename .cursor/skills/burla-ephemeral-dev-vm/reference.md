# Burla Ephemeral Dev VM Reference

## Worktree Contract

- Default task branch: `work/<task-slug>` unless `--branch` is supplied
- Worktree path: `../burla-worktrees/<task-slug>`
- Create and remove worktrees from the primary checkout
- Worktrees are not assigned to VM slots; any worktree can be synced to any slot

Examples:

- Task `fix-auth-flow` -> branch `work/fix-auth-flow` -> worktree `../burla-worktrees/fix-auth-flow`
- Task `improve-jobs-ui`, branch `feature/jobs-ui` -> worktree `../burla-worktrees/improve-jobs-ui`

## VM Slot Naming Contract

- Project slot: `burla-agent-<slot>`
- VM name: `burla-dev-vm-<slot>-<timestamp>`
- Dashboard port: `15000 + <slot>`
- Vite port: `18000 + <slot>`
- Local state file (non-sensitive slot metadata): `.cursor/dev-vm-state/<slot>.json`
- Slot lock file: `.cursor/dev-vm-state/<slot>.lock`
- SSH keys (private + public): `~/.ssh/burla-dev-vm/<slot>_ed25519[.pub]`

Examples:

- Slot `01` -> project `burla-agent-01` -> dashboard `http://localhost:15001` -> keys `~/.ssh/burla-dev-vm/01_ed25519`
- Slot `02` -> project `burla-agent-02` -> dashboard `http://localhost:15002` -> keys `~/.ssh/burla-dev-vm/02_ed25519`

State and keys are split deliberately: the repo's `.cursor/` folder is a common force-commit target, so SSH private keys are kept completely outside the repo tree. The absolute key path is also written into each state file's `private_key_path` / `public_key_path` fields, so any script that's already loaded state via `load_state_vars` can use `$PRIVATE_KEY_PATH` directly. Override `BURLA_DEV_VM_KEY_DIR` if you need a different location.

## Script Roles

- `scripts/dev-worktree/create.sh`: create or reopen a linked worktree and branch for the current task, independent of VM slots
- `scripts/dev-worktree/status.sh`: report whether the expected task worktree exists and which branch it is on
- `scripts/dev-worktree/remove.sh`: remove the linked worktree and optionally delete the branch
- `scripts/dev_vm_slot_acquire.sh`: lock the lowest available slot for the current source worktree
- `scripts/dev_vm_slot_release.sh`: release a slot lock
- `scripts/dev_vm_prepare_slot.sh`: create/prepare the GCP project, services, Artifact Registry repositories, and IAM for a slot
- `scripts/dev_vm_create.sh`: create or reuse the project slot, create a fresh VM, and write the local state file
- `scripts/dev_vm_wait_ssh.sh`: wait until SSH works and the startup bootstrap is complete
- `scripts/dev_vm_sync_repo.sh`: copy the selected source worktree to `/srv/burla` on the VM and record source metadata
- `scripts/dev_vm_start.sh`: deprecated compatibility command that explains how to run `make local-dev` / `make remote-dev` directly over SSH
- `scripts/dev_vm_tunnel.sh`: forward local dashboard and Vite ports to the VM
- `scripts/dev_vm_status.sh`: print the current state plus `health`, VM status, running mode, lock state, last synced source, and VM-side Burla credential status
- `scripts/dev_vm_client_shell.sh`: start a local `uv` client shell with `BURLA_CLUSTER_DASHBOARD_URL` pointed at the tunneled dashboard URL
- `scripts/dev_vm_burla_login_instructions.sh`: print the GStack/browser `burla login --no_browser=True` flow for authorizing the VM-local Burla CLI
- `scripts/dev_vm_stop.sh`: best-effort POST `/v1/cluster/shutdown` to `main_service` (deletes remote-dev worker VMs), stop the local tunnel, stop the dev VM, and keep the state file for reuse
- `scripts/dev_vm_destroy.sh`: compatibility wrapper for the stop-only lifecycle; it stops VMs and does not delete them

## Local Client Caveat

The repo’s stock `make 3.11-dev` / `make 3.12-dev` helpers hardcode `http://localhost:5001`. For ephemeral remote VMs, use `scripts/dev_vm_client_shell.sh` instead so each slot gets its own tunneled dashboard URL.

The VM scripts do not require the current branch or worktree to match the slot. Use `dev_vm_sync_repo.sh --slot <id> --source <worktree-path>` to choose the code snapshot explicitly.

## Config Knobs

These scripts accept optional environment overrides when the defaults are wrong for the current environment:

- `BURLA_DEV_VM_ORGANIZATION_ID`
- `BURLA_DEV_VM_BILLING_ACCOUNT`
- `BURLA_DEV_VM_REGION`
- `BURLA_DEV_VM_ZONE`
- `BURLA_DEV_VM_MACHINE_TYPE`
- `BURLA_DEV_VM_IMAGE_PROJECT`
- `BURLA_DEV_VM_IMAGE_FAMILY`
- `BURLA_DEV_VM_ARTIFACT_LOCATION`
- `BURLA_DEV_VM_ARTIFACT_REPOSITORY`
- `BURLA_DEV_VM_REMOTE_REPO_DIR`
- `BURLA_DEV_VM_REMOTE_LOG_PATH`
- `BURLA_DEV_VM_KEY_DIR` (default `~/.ssh/burla-dev-vm`)
- `BURLA_DEV_WORKTREE_BASE_DIR` (default sibling dir `../burla-worktrees`)
- `BURLA_DEV_WORKTREE_BASE_REF` (default `main`)

## Expected Loop

1. From the primary checkout, create or reopen the task worktree.
2. `cd` into the worktree and do all edits there.
3. Acquire an available slot.
4. Prepare the slot once if needed.
5. Create/restart the VM if the slot has no warm VM.
6. Wait for bootstrap.
7. Sync the worktree snapshot to that slot.
8. SSH into the VM and run `cd /srv/burla && make local-dev` or `make remote-dev`.
9. Start the tunnel.
10. Use the dashboard, GStack browser auth flow, and local client shell.
11. Release the slot lock when done.
12. Stop the VM when done unless the user asked to keep it running warm.
13. Remove the worktree later only when the task branch is no longer needed.
