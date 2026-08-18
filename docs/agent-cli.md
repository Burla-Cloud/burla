# Burla management CLI

Burla's management commands are non-interactive and emit machine-readable
output. They inspect and control the same `/v1/management` resources used by
the dashboard. Job submission remains `remote_parallel_map`.

## Context and authentication

Head selection is fixed for one invocation:

1. `--head`
2. `BURLA_CLUSTER_DASHBOARD_URL`
3. The dashboard URL in saved user credentials
4. An existing local head for the saved project

The CLI never falls through to another head after selecting one. Reads never
start a head, prompt, invoke cloud discovery, authorize users, repair the relay,
or write credentials.

Credentials are selected in this order:

1. `BURLA_AUTH_TOKEN` together with `BURLA_USER_EMAIL`
2. `BURLA_CLUSTER_TOKEN`
3. Saved user credentials
4. The saved cluster token for the selected project
5. The local owner identity for a loopback head

`burla auth status` reports the selected sources and verifies them through the
existing `/version` route without printing a token.

## Commands

```text
burla auth status

burla cluster status
burla cluster watch
burla cluster start
burla cluster restart
burla cluster stop

burla nodes list [--status STATUS] [--region REGION] [--job JOB_ID]
  [--started-after TIME] [--ended-after TIME] [--sort FIELD]
  [--order asc|desc] [--limit N] [--cursor CURSOR]
burla nodes show NODE_ID
burla nodes logs NODE_ID [--before CURSOR|--after CURSOR] [--limit N] [--follow]

burla jobs list [--status STATUS] [--user EMAIL] [--function NAME]
  [--started-after TIME] [--started-before TIME] [--sort FIELD]
  [--order asc|desc] [--limit N] [--cursor CURSOR]
burla jobs show JOB_ID
burla jobs watch JOB_ID
burla jobs cancel JOB_ID
burla jobs errors JOB_ID [--limit N] [--cursor CURSOR]
burla jobs metrics JOB_ID [--raw] [--limit N] [--cursor CURSOR]

burla jobs calls list JOB_ID [--input-index N] [--status STATUS]
  [--failed-only] [--logs-only] [--has-metrics] [--sort FIELD]
  [--order asc|desc] [--limit N] [--cursor CURSOR]
burla jobs calls show JOB_ID INPUT_INDEX
burla jobs calls logs JOB_ID INPUT_INDEX [--errors-only]
  [--before CURSOR|--after CURSOR] [--limit N]
burla jobs calls metrics JOB_ID INPUT_INDEX
  [--raw] [--limit N] [--cursor CURSOR]

burla settings show
burla settings update [--image IMAGE] [--machine-type TYPE] [--quantity N]
  [--region REGION] [--disk-gb N] [--inactivity-timeout-seconds N]

burla usage show [--month YYYY-MM]
```

Existing human commands retain their normal output and behavior:

```text
burla login [--no-browser]
burla dashboard [--port PORT]
burla deploy [--cloud aws|gcp|azure]
burla config get [KEY]
burla config set KEY VALUE
burla --version
```

## Output

A non-streaming command writes exactly one JSON document:

```json
{"schema_version":1,"ok":true,"command":"cluster.status","request_id":"...","data":{}}
```

Expected errors also use one JSON document:

```json
{"schema_version":1,"ok":false,"command":"jobs.show","request_id":"...","error":{"code":"NOT_FOUND","message":"...","retryable":false,"details":{},"remediation":[]}}
```

Watches and raw metrics emit NDJSON. Cluster and job watches start with a
snapshot and then emit changed state. Raw metric lines carry an opaque cursor
that resumes with the next stored sample.

All fields use `snake_case`. Timestamps are ISO-8601 UTC. Durations are seconds,
memory is bytes, disk configuration is GB, I/O rates are bytes per second, and
costs are USD.

Exit codes are:

- `0`: success
- `2`: syntax or validation
- `3`: context, version, or authentication
- `4`: resource not found
- `5`: conflict
- `6`: timeout, transport, or provider failure
- `7`: watched job failed
- `8`: watched job was canceled
- `130`: local interruption

## Calls, logs, and errors

Every submitted input index appears in `jobs calls list`, including calls with
no utilization samples. Call records expose aggregate attempt count and overall
timing; individual retry attempts are not queryable.

Job notices such as cancellation or cluster shutdown are included by
`jobs show` and never count as failed function calls.

`jobs errors` groups matching traceback text and returns a count,
representative traceback, and sample input indexes. Individual failures are
inspected through `jobs calls list --failed-only`, `jobs calls show`, and
`jobs calls logs`.

## Utilization

Bounded job metrics match the dashboard series:

- sampled node count
- CPU and memory percentages
- network receive/transmit bytes per second
- disk read/write bytes per second
- GPU utilization and GPU memory percentage when GPU samples exist

Bounded call metrics report CPU cores, memory bytes, network and disk rates,
plus GPU utilization and GPU memory bytes when present. `--raw` streams the
stored full-fidelity samples without downsampling.

## Mutations

Cluster, cancellation, and settings mutations wait until the requested action
has completed and then return one JSON result. Restart and stop match dashboard
behavior by canceling running jobs first. No management command prompts.
