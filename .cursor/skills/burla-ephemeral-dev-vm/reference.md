# Burla Ephemeral Dev VM Reference

## Naming Contract

- Project slot: `burla-agent-<id>`
- VM name: `burla-dev-vm-<id>-<timestamp>`
- Dashboard port: `15000 + <id>`
- Vite port: `18000 + <id>`
- Local state file: `.cursor/dev-vm-state/<id>.json`

Examples:

- Agent `01` -> project `burla-agent-01` -> dashboard `http://localhost:15001`
- Agent `02` -> project `burla-agent-02` -> dashboard `http://localhost:15002`

## Script Roles

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

## Expected Loop

1. Create the VM.
2. Wait for bootstrap.
3. Sync the repo.
4. Start local-dev.
5. Start the tunnel.
6. Use the dashboard and local client shell.
7. Destroy the VM when done.
