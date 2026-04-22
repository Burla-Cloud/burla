# Burla Ephemeral Dev VM Reference

## Worktree Contract

- Task branch: `agent/<id>/<task-slug>`
- Worktree path: `../burla-worktrees/agent-<id>/<task-slug>`
- Create and remove worktrees from the primary checkout
- Run all `scripts/dev_vm_*.sh` commands from inside the linked worktree

Examples:

- Agent `01`, task `fix-auth-flow` -> branch `agent/01/fix-auth-flow` -> worktree `../burla-worktrees/agent-01/fix-auth-flow`
- Agent `02`, task `improve-jobs-ui` -> branch `agent/02/improve-jobs-ui` -> worktree `../burla-worktrees/agent-02/improve-jobs-ui`

## Naming Contract

- Project slot: `burla-agent-<id>`
- VM name: `burla-dev-vm-<id>-<timestamp>`
- Dashboard port: `15000 + <id>`
- Vite port: `18000 + <id>`
- Local state file (non-sensitive agent metadata): `.cursor/dev-vm-state/<id>.json`
- SSH keys (private + public): `~/.ssh/burla-dev-vm/<id>_ed25519[.pub]`

Examples:

- Agent `01` -> project `burla-agent-01` -> dashboard `http://localhost:15001` -> keys `~/.ssh/burla-dev-vm/01_ed25519`
- Agent `02` -> project `burla-agent-02` -> dashboard `http://localhost:15002` -> keys `~/.ssh/burla-dev-vm/02_ed25519`

State and keys are split deliberately: the repo's `.cursor/` folder is a common force-commit target, so SSH private keys are kept completely outside the repo tree. The absolute key path is also written into each state file's `private_key_path` / `public_key_path` fields, so any script that's already loaded state via `load_state_vars` can use `$PRIVATE_KEY_PATH` directly. Override `BURLA_DEV_VM_KEY_DIR` if you need a different location.

## Script Roles

- `scripts/dev-worktree/create.sh`: create or reopen a linked worktree and branch for the current task
- `scripts/dev-worktree/status.sh`: report whether the expected task worktree exists and which branch it is on
- `scripts/dev-worktree/remove.sh`: remove the linked worktree and optionally delete the branch
- `scripts/dev_vm_create.sh`: create or reuse the project slot, create a fresh VM, and write the local state file
- `scripts/dev_vm_wait_ssh.sh`: wait until SSH works and the startup bootstrap is complete
- `scripts/dev_vm_sync_repo.sh`: copy the current local repo state to `/srv/burla` on the VM
- `scripts/dev_vm_start_local_dev.sh`: build the `burla-main-service:latest` image for that project and start `make local-dev` in tmux
- `scripts/dev_vm_tunnel.sh`: forward local dashboard and Vite ports to the VM
- `scripts/dev_vm_status.sh`: print the current state plus a coarse health field
- `scripts/dev_vm_client_shell.sh`: start a local `uv` client shell with `BURLA_CLUSTER_DASHBOARD_URL` pointed at the tunneled dashboard URL
- `scripts/dev_vm_destroy.sh`: stop the local tunnel and delete the VM; delete the project slot only when explicitly requested

## Local Client Caveat

The repo’s stock `make 3.11-dev` / `make 3.12-dev` helpers hardcode `http://localhost:5001`. For ephemeral remote VMs, use `scripts/dev_vm_client_shell.sh` instead so each agent task gets its own tunneled dashboard URL.

The VM scripts also assume they are running from a linked task worktree. They should fail if they are run from the primary checkout or from a branch that does not match `agent/<id>/<task-slug>`.

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
3. Create the VM.
4. Wait for bootstrap.
5. Sync the worktree snapshot.
6. Start local-dev.
7. Start the tunnel.
8. Use the dashboard and local client shell.
9. Destroy the VM when done.
10. Remove the worktree later only when the task branch is no longer needed.
