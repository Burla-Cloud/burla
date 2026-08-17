#### Main Service

The "main service" is a fastapi webservice deployed as a single always-on VM (the cluster's "head node"), on GCE or EC2.
This service is responsible for:

- Adding/removing/managing nodes in the cluster (via a compute-provider interface, GCP or AWS).
- Holding the cluster's live coordination state in memory (nodes push state to it over HTTP every ~1s).
- Persisting job/node history to SQLite on its disk (`/var/lib/burla/history.db`) for the dashboard.
- Hosting the cluster-management dashboard (react/ts)

There is no external database. The head is a stateful singleton:
It is currently not possible to run more than one "main-service" instance in any single cloud account.
It is currently not possible to run more than one "cluster" using a single "main-service".  

#### Resource metrics

Every node samples resources once per second and sends batches to the
cluster-token-authenticated
`POST /v1/nodes/{instance_name}/metrics:batch` endpoint. The head persists the
samples in SQLite table `resource_metrics`; they are not part of live
coordination state or displayed by the dashboard.

Each row contains `timestamp`, `duration_sec`, `instance_name`, `scope`,
`job_id`, `input_index`, `worker_id`, `cpu_seconds`, `cpu_percent`,
`memory_bytes`, `memory_percent`, `network_rx_bytes`, `network_tx_bytes`,
`disk_read_bytes`, and `disk_write_bytes`. `scope` is `node` for a whole-node
sample and `task` for a worker container that had an active input at that
sample.

- CPU percentages are normalized to the whole node's logical CPU capacity, so
  100 means every logical CPU was busy. `cpu_seconds` is the raw busy CPU time
  consumed during the row's interval.
- Memory percentages use whole-node physical memory as the denominator. Node
  bytes are total memory minus available memory; task bytes are Docker cgroup
  usage minus inactive file cache, which includes the worker and every child
  process it starts.
- Network and disk columns are raw byte-counter deltas over the preceding
  approximately one-second `duration_sec` interval. Node counters come from
  psutil; task counters come from the worker container's network namespace and
  cgroup.
- Task rows are attributed to `WorkerClient.current_input` at sample time.
  Inputs that begin and end between sampling instants do not produce a task
  row.

The unique `(instance_name, timestamp, scope, worker_id)` index makes retried
batches idempotent. The `(job_id, scope, input_index, timestamp)` index supports
ordered job and task queries.

#### Dev:

To avoid the need for CORS middleware I use a script that builds the react website every time I hit save. It takes about the same amount of time to build as the fastapi webservice takes to reload, so it dosent actually slow anything down much.  
To get this setup install the vscode extension called "Run on Save", the publisher is "emeraldwalk". After installing add the following to your `settings.json` (open this by hitting `Cmd + Shift + P`, then type `Preferences: Open Settings (JSON)` and select it):
```json
{
    // <other settings you've set will be here, add below to the main dict>
    "emeraldwalk.runonsave": {
        "commands": [
            {
                "match": "frontend/src/.*\\.(js|ts|jsx|tsx|css|scss|html)$", // Run whenever any source-code file is saved
                "cmd": "make -C ./main_service build-frontend"
            }
        ]
    }
}
```
Now the website should build everytime you hit save! (It should take <2s to build)  
To see the output of this command press `Cmd + Shift + U`, then select `Run on Save` in the dropdown.
