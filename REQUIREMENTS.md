# Burla Requirements

What Burla must do, as observable behavior. This is the source of truth for intent
and doubles as the manual test plan.

Conventions:

- One requirement per user-visible capability, written as "doing X results in Y".
  If two things can be checked by one action, they are one requirement.
- IDs are permanent. Never renumber; retire instead.
- No implementation details. How it works lives in `.cursor/rules/burla-architecture.mdc`
  and the `burla-deep-dive` skill. This file only says what must be true.
- Each requirement carries a `Verify` line: the shortest way to prove it by hand.
- Preconditions that are not themselves requirements: a working cloud identity
  (`aws sso login`) for every mode, including local-dev.

---

## R1. Local development cluster

Running `make local-dev` and pressing Start in the dashboard boots a complete
cluster on this machine (head, node, workers), and `remote_parallel_map` against it
returns correct results. Edits to any service take effect without a rebuild.

Verify: `make local-dev`, open the URL from `make cluster-info`, press Start, then run
an `rpm` job with `BURLA_CLUSTER_DASHBOARD_URL` set to that URL. Results correct, no
node boot failures in the head log.

## R2. Remote development cluster

Running `make remote-dev` serves the head from the working tree while nodes are real
cloud VMs, and `remote_parallel_map` against it returns correct results. Nodes run the
current branch as pushed to GitHub.

Verify: push the branch, `make remote-dev`, press Start, run an `rpm` job. Results
correct, and the booted VMs appear in the cloud console under this cluster.

## R3. Zero-setup job

With no cluster running and no deployed cluster logged into, calling
`remote_parallel_map` boots cloud VMs, runs the job, returns correct results, and
leaves no head and no nodes running when it finishes. The job is still visible in the
dashboard afterwards.

Verify: stop every local head and cluster, confirm no deployed cluster is selected, run
an `rpm` job. Results correct; afterwards no head process and no cluster VMs remain;
`burla dashboard` still lists the job.

## R4. One head per machine

When a head is already running locally, `remote_parallel_map` and `burla dashboard`
both use it instead of starting a second one, and neither shuts it down on exit.

Verify: start `burla dashboard`, note the head pid and port, then run an `rpm` job and
open `burla dashboard` again. Same pid and port throughout, still running at the end.

## R5. Deployed cluster

`burla deploy` creates at most one always-on cluster per cloud account, and after
`burla login` the client sends jobs to that cluster instead of running a head locally.
A first deploy carries this machine's client-hosted job history and settings into the
deployed cluster, refusing to start while a local job is running. Deploying again
updates the existing cluster rather than creating a second one, and preserves the
deployed history instead of re-importing local data.

Verify: run a local `rpm` job, then `burla deploy` and `burla login`; the deployed
dashboard lists the local job, and a new `rpm` job appears there too. Run
`burla deploy` again and confirm the account still has one head with its history intact.

## R6. Isolation

Several dev clusters run at the same time without interfering, and tearing one down
leaves the others untouched.

Verify: `make local-dev` in two checkouts. Two dashboards on different ports, each with
its own nodes. `make stop` in one; the other still runs jobs.

## R7. No orphans

Every VM Burla boots is eventually deleted: when the job ends, when the cluster is
stopped, when the head disappears, or after an idle timeout. Nothing bills forever.

Verify: after each of a finished job, a dashboard Stop, killing the head outright, and
leaving a cluster idle past its timeout, confirm the cloud account has no Burla VMs left
running.

## R8. Failures are legible

A failing function, a dead node, an unreachable cluster, or a missing prerequisite
produces a clear actionable error locally with the real traceback, and never hangs
indefinitely.

Verify: run a job whose function raises (real traceback locally), point the client at a
stopped cluster (clear error, not a hang), and run `make local-dev` with no cloud
identity (tells you to log in).

## R9. Auth boundary

Only authorized users of a cluster can run work on it or read its results.
Unauthenticated requests to any internet-reachable cluster endpoint are rejected.

Verify: request a node's relay hostname with no credentials and get rejected, then run
the same job as an authorized user and have it succeed.

## R10. Scale and environment fidelity

A job runs correctly at thousands of inputs across many VMs, inside the Docker image and
Python version the user asked for, with their local packages and modules available.

Verify: run a job with thousands of inputs and a custom image whose function imports both
a local module and an installed package. Results correct.

## R11. Job control

Interrupting a job stops it promptly, and a job started detached keeps running after the
client exits and can be inspected or collected later.

Verify: Ctrl-C a running job and confirm it stops and releases its nodes. Start a detached
job, exit the client, then confirm from the dashboard that it finished.

## R12. Version compatibility

A client whose version does not work with a cluster is refused with a clear message saying
so, rather than failing later in a confusing way.

Verify: point a deliberately mismatched client at a cluster and confirm the error names the
version problem.

## R13. Concurrent jobs

Several jobs running against one cluster at the same time never mix results and never take
each other's nodes.

Verify: run two jobs at once against one cluster with distinguishable inputs. Each gets
exactly its own correct results.

## R14. Resource requests honored

The CPU, RAM, and GPU requested per function call are what the function actually gets, and
a call that runs out of memory is retried with more rather than looping forever.

Verify: request GPUs and confirm the work lands on GPU machines; run a function that needs
more RAM than one slot and confirm it completes instead of OOM-looping.

## R15. Observability

While a job runs, the dashboard shows the live nodes, the job, and its logs. After it ends,
the job and its logs are still there.

Verify: watch the dashboard during a job and confirm nodes, job state, and printed output
appear live. Reload after it finishes and confirm the record and logs remain.

## R16. 1,000-CPU latency

Running a job whose function only calls `print` across 1,000 CPUs completes end-to-end in
under one second.

Verify: run the job on 1,000 CPUs and measure from calling `remote_parallel_map` until it
returns. The elapsed time is under one second.
