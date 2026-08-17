# Head State and History (replaces the old Firestore schema)

Firestore was removed in 1.6.0. This file describes where every piece of state now lives and the shapes services exchange over HTTP.

## Two stores, one owner

- **Live state**: plain dicts in [main_service/src/main_service/cluster_state.py](../../../main_service/src/main_service/cluster_state.py). `NODES` (instance_name → node dict) and `JOBS` (job_id → job dict). Guarded by one `threading.RLock` because `Node.start` mutates from thread-pool threads. In-memory pub/sub (`subscribe_node_events` / `subscribe_job_events` / `subscribe_node_logs`) feeds the dashboard SSE streams.
- **History**: SQLite (WAL) via [main_service/src/main_service/history.py](../../../main_service/src/main_service/history.py) at `HISTORY_DB_PATH` (default `/var/lib/burla/history.db`). Tables: `jobs`, `job_logs`, `nodes`, `node_logs`, `resource_metrics`, `cluster_config`. Written on status transitions and batched logs/metrics; read only by dashboard/history endpoints and at head startup (`cluster_state.load_from_history` reloads active nodes + RUNNING jobs).

The client and the dashboard browser never see either store directly; everything goes through main_service HTTP/SSE.

## Node dict shape (cluster_state.NODES values)

Same field names the old Firestore docs used: `instance_name`, `status` (BOOTING/READY/RUNNING/FAILED; DELETED entries are persisted then dropped from memory), `host`, `zone`, `machine_type`, `gcp_region` (holds an AWS region on AWS), `containers`, `current_job`, `reserved_for_job`, `started_booting_at`, `inactivity_shutdown_time_sec`, `disk_size`, `num_gpus`, `spot`, `port`, `sync_gcs_bucket_name`, `ended_at`.

Status-merge rules in `cluster_state.update_node`:
- BOOTING/READY/RUNNING never overwrite terminal DELETED/FAILED.
- DELETED never overwrites FAILED (failed nodes stay visible for debugging).

## Job dict shape (cluster_state.JOBS values)

Same fields the old job docs had (`n_inputs`, `func_cpu`, `func_ram`, `func_gpu`, `image`, `packages`, `status`, `burla_client_version`, `user_python_version`, `target_parallelism`, `max_parallelism`, `user`, `function_name`, `function_size_gb`, `started_at`, `is_background_job`, `all_inputs_uploaded`, `client_has_all_results`, `fail_reason` list, `dashboard_canceled`, `cluster_shutdown`, `cluster_restarted`) plus `assigned_nodes`: `{instance_name: {current_num_results, client_contact_last_1s, last_push_at}}` maintained by node progress pushes.

`update_job` rules: FAILED/CANCELED always apply; COMPLETED only applies while RUNNING.

## The state-push exchange (replaces every on_snapshot watch)

`PUT /v1/nodes/{instance_name}/state` (cluster-token auth), body:

```json
{
  "status": "RUNNING",
  "current_job": "myfunc-abc123",
  "reserved_for_job": null,
  "started_booting_at": 1751234567.0,
  "ended_at": 1751234567.0,
  "job_progress": {"job_id": "...", "current_num_results": 42, "client_contact_last_1s": true}
}
```

All fields optional; only present keys are merged. Response:

```json
{
  "status": "RUNNING",
  "host": "http://34.72.10.5:8080",
  "reserved_for_job": null,
  "job": {
    "exists": true,
    "status": "RUNNING",
    "all_inputs_uploaded": true,
    "client_has_all_results": false,
    "dashboard_canceled": false,
    "cluster_shutdown": false,
    "cluster_restarted": false,
    "any_node_client_contact": true,
    "total_num_results": 420,
    "n_inputs": 1000
  }
}
```

Node-side: `_state_push_loop` in [node_service/src/node_service/__init__.py](../../../node_service/src/node_service/__init__.py) pushes every 1s; transition points (`on_job_start`, `reboot_containers`, `reinit_node`, shutdown) push directly. `head_client.apply_job_signals` copies the `job` view into `SELF` exactly like the old `_on_job_snapshot` callback did.

Other node → head endpoints (all in [main_service/src/main_service/endpoints/nodes.py](../../../main_service/src/main_service/endpoints/nodes.py)):
- `POST /v1/nodes/{id}/logs:batch`: `{"logs": [{"msg", "ts"}]}` (boot/error logs, also from VM startup scripts via curl)
- `POST /v1/nodes/{id}/metrics:batch`: per-second whole-node and active-task CPU, memory, network, and disk samples, batched without coarsening timestamps; task rows are attributed from each worker's `current_input`
- `POST /v1/nodes/{id}/self_delete`: the head deletes the VM (inactivity shutdown, boot failure); nodes have zero cloud-API access
- `GET /v1/jobs/{id}/peers`: `{"peers": [{"instance_name", "host"}], "booting_node_ids": [...]}`, the input-stealing ring
- `POST /v1/jobs/{id}/logs:batch`: `{"documents": [...]}` UDF log docs from `JobLogWriter` (timestamps are epoch floats)
- Nodes also reuse `GET /v1/jobs/{id}` (reservation watching) and `PATCH /v1/jobs/{id}` (status/fail_reason writes) with cluster-token auth.

## Client liveness / disconnect quorum

The only liveness channel is the direct per-node `/client-heartbeat` (0.5s, from a client subprocess). Each node folds its "heard from client in the last 1s" flag into its progress pushes; a node suspecting disconnect checks `any_node_client_contact` in the job view (head-aggregated quorum). There is no head-side client heartbeat. A client that can reach the head but zero nodes counts as disconnected.

## Status vocabulary (unchanged)

- nodes: `BOOTING`, `READY`, `RUNNING`, `FAILED`, `DELETED`
- jobs: `RUNNING`, `COMPLETED`, `FAILED`, `CANCELED`

## Gotchas

- The head is a singleton. Restarting it mid-job is survivable: it reloads active nodes/RUNNING jobs from history, and node pushes rebuild `assigned_nodes` within ~1s. Signals set while it was down are lost only if nothing re-sends them.
- `job_view.total_num_results` is the sum over `assigned_nodes`; the reaper in `cluster_state.job_reaper_loop` fails RUNNING jobs whose nodes all stopped pushing >300s ago (the old dashboard-SSE watchdog, now always on).
- History rows for RUNNING jobs are stale between transitions by design; dashboard endpoints overlay live summaries from memory.
- Ad hoc client-hosted heads reuse one account-wide history database. Explicit worktree clusters stay isolated: local-dev uses `_local_dev_state/history.db`, while remote-dev uses its namespaced head state directory.
- A first `burla deploy` migrates that account-wide database into the new deployed head: deploy pauses the local head's job admission (409 if a job is running), deletes its idle nodes, takes a WAL-safe `sqlite3` backup, and POSTs it to `/v1/cluster/import_history` (cluster-token only). The import merges ended jobs/logs/nodes plus `cluster_config` (deployed shared-workspace bucket and, on AWS, the deployed node region are preserved) and records the snapshot digest so retries are no-ops. Redeploys never re-import; the head VM's own database is authoritative from then on.
