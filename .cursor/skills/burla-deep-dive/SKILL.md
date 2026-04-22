---
name: burla-deep-dive
description: Detailed reference for Burla subsystems - end-to-end job lifecycle for remote_parallel_map, cluster and node lifecycle (boot, grow, reserve, shutdown), and Firestore schema. Use when working on Burla internals, debugging cross-service behavior in the burla repo, tracing how a job flows between client / main_service / node_service / worker_server, or answering detailed questions about how a specific Burla subsystem works.
---

# Burla Deep Dive

The high-level architecture is in the always-applied rule [burla-architecture.mdc](../../rules/burla-architecture.mdc). This skill provides the deeper subsystem-level detail needed when actually editing or debugging Burla internals.

## When to use which reference

Pick the file that matches what you need. Each is self-contained; only read what the current task requires.

- **[job-lifecycle.md](job-lifecycle.md)** — What happens when a user calls `remote_parallel_map`. Covers client-side pickling, the `POST /v1/jobs/{id}/start` single-call entry point on `main_service`, input upload, the node-side `CallHookOnJobStartMiddleware`, worker assignment and Python-version matching, the TCP command protocol to `worker_server.py`, the `job_watcher` polling loop, node-to-node input stealing (`/get_inputs` + `/ack_transfer`), and how cancellation is signaled. Read this for anything touching how a job actually runs.

- **[cluster-operations.md](cluster-operations.md)** — How the cluster itself is managed. Covers `/v1/cluster/restart`, `/v1/cluster/shutdown`, the mid-job grow path that lives inside `POST /v1/jobs/{id}/start`, how `main_service` boots Compute Engine VMs via `Node.start`, the node state machine (BOOTING / READY / RUNNING / FAILED), reservations (`reserved_for_job` / `RESERVED_FOR_JOB`), and the inactivity-shutdown watchdog running inside each node.

- **[firestore-schema.md](firestore-schema.md)** — Document shapes for the three top-level collections: `nodes`, `cluster_config`, `jobs`. Includes subcollections (`nodes/{id}/logs`, `jobs/{id}/logs`, `jobs/{id}/assigned_nodes`), which service writes each field, and the status vocabulary used for filtering.

## Core invariants to remember

These are assumptions the codebase relies on. Don't break them:

- **One cluster per GCP project, one main_service per cluster.** Hard-coded — enforced implicitly by secrets and Firestore database naming.
- **Firestore database is always `burla`** (not the default). Every `firestore.Client(...)` call in this repo passes `database="burla"`.
- **The client never talks to Firestore directly.** Every read/write the `burla` pypi package does goes through `main_service` HTTP endpoints (see [client/src/burla/_cluster_client.py](../../../client/src/burla/_cluster_client.py)). `main_service` serves most of those from in-memory caches (`NODES_CACHE`, `CLUSTER_CONFIG_CACHE`) that are kept in sync by Firestore `on_snapshot` listeners — the client path avoids a Firestore round-trip entirely. It does still talk directly to each node over HTTP for inputs/results.
- **A node holds in-memory state in the `SELF` dict** ([node_service/src/node_service/__init__.py](../../../node_service/src/node_service/__init__.py)). Reloads reset this — never persist anything important only in `SELF`; if it must survive a restart, put it in Firestore.
- **Workers speak a custom TCP byte-protocol, not HTTP** ([worker_server.py](../../../node_service/src/node_service/worker_server.py)). After a one-byte handshake the four commands are `r` (reset / kill children), `i` (install packages), `l` (load pickled function), `c` (call with one input). Any worker change has to keep this tiny protocol intact.
- **Version compatibility matters.** Client sends `burla_client_version` on every `/v1/jobs/{id}/start`; `main_service` rejects jobs where the client is outside `[MIN_COMPATIBLE_CLIENT_VERSION, CURRENT_BURLA_VERSION]`. Bumping `CURRENT_BURLA_VERSION` without bumping the minimum is the normal path.
- **Node-to-node peer traffic uses the same auth headers as the client.** During a job, `SELF["auth_headers"]` is set from the incoming client request and replayed on the inter-node `/jobs/{id}/get_inputs` / `/jobs/{id}/ack_transfer` calls. Don't clobber it.
- **Local dev replaces VMs with Docker containers** but keeps the same HTTP surface and Firestore schema. If a change works against Compute Engine VMs but breaks `make local-dev`, it's a bug — the contract is supposed to be identical.

## Quick pointers before diving in

When a question mentions any of these, go straight to the file below instead of exploring:

| Topic | File |
|-------|------|
| `remote_parallel_map`, `/v1/jobs/{id}/start`, job failure, cancellation, UDF errors, logs streaming | job-lifecycle.md |
| Nodes stuck in BOOTING, mid-job grow, inactivity shutdown, local-dev containers not starting | cluster-operations.md |
| Dashboard showing wrong status, Firestore queries, node filters, `jobs/{id}` shape, `NODES_CACHE` | firestore-schema.md |

## Source-file index

- Client entry point + orchestrator: [client/src/burla/_remote_parallel_map.py](../../../client/src/burla/_remote_parallel_map.py)
- Client per-node driver: [client/src/burla/_node.py](../../../client/src/burla/_node.py)
- Client ↔ main_service HTTP wrapper: [client/src/burla/_cluster_client.py](../../../client/src/burla/_cluster_client.py)
- Client heartbeat subprocess: [client/src/burla/_heartbeat.py](../../../client/src/burla/_heartbeat.py)
- Main-service endpoints for the pypi client (start_job, patch_job, cluster state): [main_service/src/main_service/endpoints/client.py](../../../main_service/src/main_service/endpoints/client.py)
- Main-service cluster lifecycle (restart, shutdown, `_start_nodes`, packing): [main_service/src/main_service/endpoints/cluster_lifecycle.py](../../../main_service/src/main_service/endpoints/cluster_lifecycle.py)
- Main-service `Node` (VM wrapper): [main_service/src/main_service/node.py](../../../main_service/src/main_service/node.py)
- Main-service in-memory caches (`NODES_CACHE`, `CLUSTER_CONFIG_CACHE`): [main_service/src/main_service/__init__.py](../../../main_service/src/main_service/__init__.py)
- Node-service app + middleware + `SELF`: [node_service/src/node_service/__init__.py](../../../node_service/src/node_service/__init__.py)
- Node-service job endpoints: [node_service/src/node_service/job_endpoints.py](../../../node_service/src/node_service/job_endpoints.py)
- Node-service lifecycle endpoints: [node_service/src/node_service/lifecycle_endpoints.py](../../../node_service/src/node_service/lifecycle_endpoints.py)
- Node-service job watcher: [node_service/src/node_service/job_watcher.py](../../../node_service/src/node_service/job_watcher.py)
- Node-to-worker TCP client + `JobLogWriter`: [node_service/src/node_service/worker_client.py](../../../node_service/src/node_service/worker_client.py)
- Worker TCP server (runs in container): [node_service/src/node_service/worker_server.py](../../../node_service/src/node_service/worker_server.py)
