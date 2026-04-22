---
name: burla-ephemeral-dev-vm
description: Provision and use isolated ephemeral GCP VMs for Burla local-dev. Use when the user mentions Burla dev VMs, local-dev on GCP, SSH tunnel dashboards, isolated agent runtimes, or ephemeral VM workflows.
---

# Burla Ephemeral Dev VM

Use the `scripts/dev_vm_*.sh` commands instead of handwritten `gcloud`, `ssh`, or `scp` sequences.

## Defaults

- One active agent task gets one fresh VM.
- Reuse the dedicated project slot for that agent ID unless the user asks otherwise.
- Default to `make local-dev` on the VM.
- Use the script-reported `http://localhost:<port>` URL for browser and client work.
- Destroy the VM when the task is complete unless the user asked to keep it.

## Standard Workflow

1. `scripts/dev_vm_create.sh --agent <id>`
2. `scripts/dev_vm_wait_ssh.sh --agent <id>`
3. `scripts/dev_vm_sync_repo.sh --agent <id>`
4. `scripts/dev_vm_start_local_dev.sh --agent <id>`
5. `scripts/dev_vm_tunnel.sh --agent <id>`
6. `scripts/dev_vm_status.sh --agent <id>`
7. For local client work, run `scripts/dev_vm_client_shell.sh --agent <id> --python <version>`
8. When done, run `scripts/dev_vm_destroy.sh --agent <id>`

## Guardrails

- Never share a VM across active agents.
- Never expose port `5001` publicly.
- Never assume the dashboard URL is `http://localhost:5001`; always read the state file or status output.
- Sync the repo before starting or restarting local-dev if local code changed.
- Use raw `gcloud` / `ssh` automation only; do not switch the workflow into Cursor Remote SSH windows.

## Additional Reference

- For naming, ports, state files, and script responsibilities, read [reference.md](reference.md).
