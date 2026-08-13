---
name: burla-deep-dive
description: Detailed reference for Burla subsystems - end-to-end job lifecycle for remote_parallel_map, cluster and node lifecycle (boot, grow, reserve, shutdown), and the head's in-memory state + SQLite history stores. Use when working on Burla internals, debugging cross-service behavior in the burla repo, tracing how a job flows between client / main_service / node_service / worker_server, or answering detailed questions about how a specific Burla subsystem works.
---

# Burla Deep Dive

The high-level architecture is in the always-applied rule [burla-architecture.mdc](../../rules/burla-architecture.mdc). This skill provides the deeper subsystem-level detail needed when actually editing or debugging Burla internals.

## When to use which reference

Pick the file that matches what you need. Each is self-contained; only read what the current task requires.

- **[job-lifecycle.md](job-lifecycle.md)**: What happens when a user calls `remote_parallel_map`. Covers client-side pickling, the `POST /v1/jobs/{id}/start` single-call entry point on `main_service`, input upload, the node-side `CallHookOnJobStartMiddleware`, worker assignment and Python-version matching, the TCP command protocol to `worker_server.py`, the `job_watcher` loop driven by state-push responses, node-to-node input stealing (`/get_inputs` + `/ack_transfer` with the peer ring from `GET /v1/jobs/{id}/peers`), and how cancellation is signaled. Read this for anything touching how a job actually runs.

- **[cluster-operations.md](cluster-operations.md)**: How the cluster itself is managed. Covers `/v1/cluster/restart`, `/v1/cluster/shutdown`, the mid-job grow path that lives inside `POST /v1/jobs/{id}/start`, how `main_service` boots node VMs through the compute-provider layer (`providers/`: GCE, EC2, local Docker), the node state machine (BOOTING / READY / RUNNING / FAILED), reservations (`reserved_for_job` / `RESERVED_FOR_JOB`), and the inactivity-shutdown watchdog running inside each node.

- **[head-state.md](head-state.md)**: Where every piece of state lives since Firestore was removed (1.6.0). The `cluster_state.NODES` / `cluster_state.JOBS` dict shapes, the SQLite history store, the `PUT /v1/nodes/{id}/state` push exchange (request and response shapes, including the job signal view), the other node-to-head endpoints, the client-liveness quorum, and the status vocabulary.

## Core invariants to remember

These are assumptions the codebase relies on. Don't break them:

- **One cluster per project, one main_service (the head) per cluster.** The head is a singleton always-on VM (or one container in local-dev). Authoritative live cluster state exists only in its memory ([main_service/src/main_service/cluster_state.py](../../../main_service/src/main_service/cluster_state.py)); the only persistence is the SQLite history store ([history.py](../../../main_service/src/main_service/history.py)), written on status transitions and log batches.
- **There is no database in the coordination path.** The client never talks to a DB because there is no DB: every read/write the `burla` pypi package does goes through `main_service` HTTP endpoints (see [client/src/burla/_cluster_client.py](../../../client/src/burla/_cluster_client.py)), plus direct node HTTP for inputs/results. Nodes likewise reach all cluster state through [head_client.py](../../../node_service/src/node_service/head_client.py).
- **Signals ride push responses.** Each node PUTs its state to `/v1/nodes/{id}/state` every ~1s (`_state_push_loop`) and at every transition; the response carries the head's view back down (`host` during boot, the job signal view during a job). There are no watches or listeners anywhere; if a node needs to learn something, it arrives on the next push response (within ~1s) or via an explicit GET.
- **The head owns cloud API access.** A node that must die pushes its terminal status then calls `POST /v1/nodes/{id}/self_delete` and the head deletes the VM. The one exception is client-hosted Azure: the head continually sends each node a short-lived caller token so it can delete itself before guest poweroff would leave compute billing.
- **A node holds in-memory state in the `SELF` dict** ([node_service/src/node_service/__init__.py](../../../node_service/src/node_service/__init__.py)). Reloads reset this; never persist anything important only in `SELF`. If it must survive a node restart, it has to live on the head (`cluster_state` or history).
- **Workers speak a custom TCP byte-protocol, not HTTP** ([worker_server.py](../../../node_service/src/node_service/worker_server.py)). After a one-byte handshake the four commands are `r` (reset / kill children), `i` (install packages), `l` (load pickled function), `c` (call with one input). Any worker change has to keep this tiny protocol intact.
- **Version compatibility matters.** Client sends `burla_client_version` on every `/v1/jobs/{id}/start`; `main_service` rejects jobs where the client is outside `[MIN_COMPATIBLE_CLIENT_VERSION, CURRENT_BURLA_VERSION]`. 1.6.0 was a clean break: both constants are 1.6.0.
- **Node-to-node peer traffic uses the same auth headers as the client.** During a job, `SELF["auth_headers"]` is set from the incoming client request and replayed on the inter-node `/jobs/{id}/get_inputs` / `/jobs/{id}/ack_transfer` calls. Don't clobber it. (Node-to-head traffic instead authenticates with `CLUSTER_ID_TOKEN`, a plain env var.)
- **Local dev replaces VMs with Docker containers** but keeps the same HTTP surface and state model. If a change works against real VMs but breaks `make local-dev`, it's a bug: the contract is supposed to be identical.

## Quick pointers before diving in

When a question mentions any of these, go straight to the file below instead of exploring:

| Topic | File |
|-------|------|
| `remote_parallel_map`, `/v1/jobs/{id}/start`, job failure, cancellation, UDF errors, logs streaming | job-lifecycle.md |
| Nodes stuck in BOOTING, mid-job grow, inactivity shutdown, providers / machine catalog, local-dev containers not starting | cluster-operations.md |
| Dashboard showing wrong status, `cluster_state.NODES` / `JOBS` shapes, the state-push exchange, job signal view, SQLite history, disconnect quorum | head-state.md |

## Source-file index

- Client entry point + orchestrator: [client/src/burla/_remote_parallel_map.py](../../../client/src/burla/_remote_parallel_map.py)
- Client per-node driver: [client/src/burla/_node.py](../../../client/src/burla/_node.py)
- Client ↔ main_service HTTP wrapper: [client/src/burla/_cluster_client.py](../../../client/src/burla/_cluster_client.py)
- Client heartbeat subprocess (direct node pings only): [client/src/burla/_heartbeat.py](../../../client/src/burla/_heartbeat.py)
- Cluster deployment (head VM creation, GCP / AWS): [client/src/burla/_deploy.py](../../../client/src/burla/_deploy.py), [client/src/burla/_deploy_aws.py](../../../client/src/burla/_deploy_aws.py)
- Main-service endpoints for the pypi client (start_job, get/patch job, cluster state): [main_service/src/main_service/endpoints/client.py](../../../main_service/src/main_service/endpoints/client.py)
- Main-service endpoints for nodes (state push, logs, self_delete, peers): [main_service/src/main_service/endpoints/nodes.py](../../../main_service/src/main_service/endpoints/nodes.py)
- Main-service cluster lifecycle (restart, shutdown, `_start_nodes`): [main_service/src/main_service/endpoints/cluster_lifecycle.py](../../../main_service/src/main_service/endpoints/cluster_lifecycle.py)
- Main-service live state + pub/sub + job reaper: [main_service/src/main_service/cluster_state.py](../../../main_service/src/main_service/cluster_state.py)
- Main-service SQLite history store: [main_service/src/main_service/history.py](../../../main_service/src/main_service/history.py)
- Main-service `Node` (VM wrapper + startup script): [main_service/src/main_service/node.py](../../../main_service/src/main_service/node.py)
- Compute providers (GCE / EC2 / local Docker) + machine catalog: [main_service/src/main_service/providers/](../../../main_service/src/main_service/providers/) (`gcp.py`, `aws.py`, `local_docker.py`, `catalog.py`)
- Blob-store adapter for /workspace/shared (GCS on GCP, S3 on AWS): [main_service/src/main_service/blobstore.py](../../../main_service/src/main_service/blobstore.py)
- Main-service app, version constants, `CLOUD_PROVIDER`, auth middleware: [main_service/src/main_service/__init__.py](../../../main_service/src/main_service/__init__.py)
- Node-service app + middleware + `SELF` + `_state_push_loop`: [node_service/src/node_service/__init__.py](../../../node_service/src/node_service/__init__.py)
- Node-to-head HTTP client (`push_state`, `apply_job_signals`): [node_service/src/node_service/head_client.py](../../../node_service/src/node_service/head_client.py)
- Node-service job endpoints: [node_service/src/node_service/job_endpoints.py](../../../node_service/src/node_service/job_endpoints.py)
- Node-service lifecycle endpoints (`reboot_containers`, `_watch_reservation`): [node_service/src/node_service/lifecycle_endpoints.py](../../../node_service/src/node_service/lifecycle_endpoints.py)
- Node-service job watcher: [node_service/src/node_service/job_watcher.py](../../../node_service/src/node_service/job_watcher.py)
- Node-to-worker TCP client + `JobLogWriter`: [node_service/src/node_service/worker_client.py](../../../node_service/src/node_service/worker_client.py)
- Worker TCP server (runs in container): [node_service/src/node_service/worker_server.py](../../../node_service/src/node_service/worker_server.py)
