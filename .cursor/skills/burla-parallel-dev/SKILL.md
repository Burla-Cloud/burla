---
name: burla-parallel-dev
description: How to run an isolated Burla dev cluster for a task, and when to use local-dev vs remote-dev. Use when changing, fixing, testing, or debugging anything in the burla repo, when several agents work in parallel worktrees, when starting or stopping a dev cluster, or when merging finished work into the dev branch.
---

# Burla Parallel Dev

Many agents work on Burla at once on one machine. Each task gets its own git
worktree and its own dev cluster, and clusters never touch each other.

## Cluster identity

One worktree = one cluster. The cluster name defaults to the worktree directory
name and namespaces everything that would otherwise be shared: the docker
network, container labels, the published head port, published node ports, the
head's state directory, its relay subdomain, and the EC2 tag on its nodes.

Run `make cluster-info` in a worktree to see that worktree's cluster name,
dashboard URL, network, and node port base. Nothing needs to be passed by hand;
override `BURLA_CLUSTER_NAME` only if two worktrees ever collide.

## Choosing a mode

Both modes run `main_service` locally and hot-reload the working tree. The
difference is where nodes and workers run.

Use **`make local-dev`** when the change can be exercised with very light
resources: dashboard and frontend work, head/API behavior, anything where you
need a cluster to exist and boot but not to do real work. Each node is a
privileged container acting as a fake VM: it runs its own docker daemon and its
workers live inside it, exactly like on a real VM, so node_service runs the
same worker code path as prod. Node and worker code is bind-mounted from the
working tree, so edits apply on save with no pushing. This is the fast loop,
but every cluster costs containers on a laptop (plus one inner dockerd and
~1GB of inner image store per node), so keep clusters small (1 node by
default) and stop the ones you are not using.

Use **`make remote-dev`** when local-dev is the wrong tool:

- the machine is already saturated and cannot take another local cluster
- the change needs real scale, real parallelism, or real processing work
- the behavior only appears on real VMs: node boot and cold starts, disk images,
  GPUs, multi-node grow/shrink, spot capacity, real network and IAM paths

Nodes are real EC2 instances in the Burla test AWS account, and the head reaches
them through the relay, so many remote-dev clusters coexist on one machine.

Development happens in AWS. GCP is production-only.

## Running a cluster

From the task's worktree, in its own terminal:

```bash
make local-dev      # whole cluster local, containers on this machine
make remote-dev     # head local, nodes are real EC2 in the test account
```

Then point clients and tests at that cluster. `make test`, `make test-service`,
and `make test-e2e` already default to this worktree's head port; for ad hoc
client commands, set the worktree URL and test environment inline every time:

```bash
BURLA_CLUSTER_DASHBOARD_URL="$(make -s cluster-info | awk '/dashboard/{print $2}')" \
  BURLA_ENVIRONMENT=test uv run python your_script.py
```

Do not rely on a previous shell export or automatic cluster resolution. Either
can silently send the job to the deployed test cluster instead of this
worktree's cluster.

Stop the foreground `make local-dev` or `make remote-dev` process in its
terminal; the cluster cleans up its own nodes. `make stop-all` is emergency
machine-wide cleanup and destroys every local dev cluster.

## The one thing remote-dev cannot see

In local-dev the node and worker code is bind-mounted, so your edits are live.
In remote-dev, node VMs `git fetch` their code from GitHub at **this worktree's
current branch**, so:

- `main_service` changes are live (the head runs your working tree)
- `node_service` / `worker_server.py` changes reach nodes only after you commit
  and push the branch

`make remote-dev` warns when the branch is unpushed or missing from origin. Set
`BURLA_NODE_SOURCE_REF` to pin nodes at an already pushed ref (e.g. `dev`) when
you only care about head-side changes.

## Starting a task

`dev` is the base branch for every task in this repo, whatever branch the
checkout happens to be on. If it is not on `dev`, stop and tell Jake before
creating the worktree instead of branching off what is there.

## Finishing a task

When the work looks done, stop and ask Jake whether to merge. Never merge on
your own initiative. Once he says yes:

1. Commit the work on the worktree's branch and push it.
2. Merge the branch into `dev` and push `dev`. `dev` is the integration branch:
   everything in flight lives there, and it is what a release is cut from.
3. Stop the dev-cluster process, then remove the worktree and delete the branch
   (locally and on origin). His yes covers all of this, so don't ask again per
   step.

Until he says yes, the worktree and its cluster stay put.

## Testing what is on dev

To exercise the merged result rather than one task's branch, use a worktree
checked out on `dev` and run either mode there, or deploy `dev` to the test
account ("push this to test", see the `burla-release` skill).

## Guardrails

- Never edit the primary checkout for a task; always work in a linked worktree.
- Never run `docker rm` by container-name prefix (`node_*`); that deletes other
  agents' clusters. Stop the dev-cluster process normally; reserve `make
  stop-all` for intentional machine-wide cleanup.
- To inspect a worker in local-dev, go through its node:
  `docker exec node_<id> docker ps` / `docker exec node_<id> docker logs <worker>`.
- Never assume the head is on port 5001; ask `make cluster-info`.
- Do not point a dev cluster's client at a deployed cluster: the service and
  e2e tiers restart and mutate whatever they are aimed at.
