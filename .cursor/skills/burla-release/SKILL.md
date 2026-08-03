---
name: burla-release
description: Cut a Burla release, deploy the dev branch to the test account, or report release status. Use when Jake says "release it", "cut a release", "ship X.Y.Z", "what release are we working on", "push this to test", "deploy this to test", or asks about the dev branch / version bump / PyPI publish flow for the burla repo.
---

# Burla Release

Burla ships as one artifact: the `burla` wheel. The wheel vendors `main_service`
(including the built dashboard, via `client/hatch_build.py`), so it is both the
client and a complete cluster head. There is no head image: a `burla deploy`'d
head VM runs a stock `python:3.13` container that pip-installs `burla` and
runs `uvicorn main_service:app`. Node VMs `git fetch` their code from GitHub at
a ref. So a release is just: get the code onto `main`, tag it, publish the wheel.

Where each piece gets its code:

| Mode | Head (`main_service`) | Nodes + workers |
|---|---|---|
| `make local-dev` | local container, your working tree | local containers, your working tree |
| `make remote-dev` | local container, your working tree | real VMs, `dev` branch |
| test deploy (below) | test-account VM, `dev` branch | real VMs, `dev` branch |
| production `burla deploy` | user's VM, `burla==<version>` from PyPI | real VMs, the release tag |

Both dev modes run the head locally. Anything running on a real VM comes from a
pushed git ref, never from your working tree.

## Branch model

- `dev` is the long-lived integration branch. All work lands on `dev`. It is
  what the client uses in test mode, and what test-mode node VMs pull
  (`_DEFAULT_NODE_SOURCE_REF = "dev"`), so `dev` is "the version I'm running".
- The version number is chosen at release time, not when a branch is created.
  Never make a release-named branch.
- Releasing merges `dev` into `main`, tags `main`, and publishes. `main` only
  ever advances through a release.

## What the release CI enforces

`.github/workflows/pypi-on-release.yml` runs on a published GitHub release and
refuses to publish unless:

1. All seven version locations equal the tag (see `bump_version.py`).
2. The tagged commit is an ancestor of `origin/main`.

Then it builds the frontend + wheel and `uv publish`es to PyPI. Nothing else
gates a release.

> Hard-break caveat: the CI requires `MIN_COMPATIBLE_CLIENT_VERSION == tag`, so
> every release currently rejects all older clients. `bump_version.py` follows
> this by default. To ship a non-breaking release you must first loosen that CI
> check to allow `MIN_COMPATIBLE_CLIENT_VERSION <=` the tag, then pass
> `--min-compatible` to the bump script. Flag this to Jake if a release should
> not break compatibility.

## "What release are we working on?" (status)

Report, without changing anything:

```bash
git -C <repo> fetch origin --quiet
rg '^version = ' client/pyproject.toml                       # in-code version
curl -s https://pypi.org/pypi/burla/json | python3 -c "import json,sys;print('pypi:', json.load(sys.stdin)['info']['version'])"
gh release list --limit 3                                    # latest GitHub releases
git log --oneline origin/main..dev                           # unreleased work on dev
```

Summarize: current in-code version, latest published version, and how many
commits `dev` is ahead of `main` (the unreleased changes).

## "Push this to test" (deploy `dev` to the Burla test AWS account)

Stands up the `dev` branch as a real always-on cluster in the test AWS account:
the same code path a customer's `burla deploy` takes, pointed at the test
backend and test relay. Use it to check a deployment end-to-end before cutting a
release.

The head and the nodes both install from the `dev` branch on GitHub, so nothing
uncommitted reaches the test cluster. Push first.

1. Build the dashboard and make sure the built assets are committed. The head
   installs straight from git, so missing or stale `static/assets` means a blank
   dashboard:

   ```bash
   make -C main_service build-frontend
   git status --porcelain main_service/src/main_service/static
   ```

2. Commit and push everything to `dev`:

   ```bash
   git push origin dev
   ```

3. Point AWS at the Burla test account. Deploy uses whatever credentials are
   active, and derives `project_id` as `aws-<account_id>`:

   ```bash
   export AWS_PROFILE=<burla test profile>
   aws sts get-caller-identity      # confirm this is the test account
   ```

4. Deploy from the isolated test environment (test backend, test relay, node ref
   `dev`, separate credentials):

   ```bash
   make test-shell
   burla config set cloud aws
   burla deploy --cloud=aws
   ```

5. Confirm it is really running dev-branch code. `burla deploy` already polls
   `/version` and fails if it does not match the local client, so also open the
   dashboard and check it renders rather than serving a blank page.

The first deploy into a fresh region spends ~10 minutes building the node AMI.
To redeploy after new commits, push to `dev` and run `burla deploy --cloud=aws`
again: it shuts the existing cluster down and restarts the head on the new code.

## "Release it [as X.Y.Z]" (the release)

Releasing pushes to `origin/main`, `origin/dev`, and a new tag, and publishes a
public GitHub release + PyPI package. The "release it" instruction is the
authorization for those pushes. Run from a clean worktree that has all the work
on `dev`.

If Jake did not give a version, propose one (major = breaking, minor = new
feature, patch = fix) based on `git log origin/main..dev`, and confirm it.

1. Preflight. Fetch, confirm `dev` is checked out and clean, and capture the
   range to write notes from before the merge erases it:

   ```bash
   git fetch origin --quiet
   git switch dev && git pull --ff-only origin dev
   PREV=$(git rev-parse origin/main)
   git log --oneline "$PREV"..dev          # this is what ships
   ```

2. Bump every version location, refresh the uv locks, and rebuild the dashboard
   so the committed assets match the frontend source (test heads install those
   assets straight from git):

   ```bash
   python3 .cursor/skills/burla-release/bump_version.py X.Y.Z
   make -C main_service build-frontend
   ```

3. Guard: version invariants + unit tests must pass (fast, laptop-safe):

   ```bash
   uv run --project ./client --group dev pytest -m unit -s --disable-warnings
   ```

4. Commit the bump on `dev` and push:

   ```bash
   git commit -am "Release X.Y.Z"
   git push origin dev
   ```

5. Merge into `main` and push (fast-forwards in the normal case):

   ```bash
   git switch main && git pull --ff-only origin main
   git merge dev
   git push origin main
   ```

6. Draft release notes in the house style (see below) from `"$PREV"..dev`, then
   create the release. `--target main` tags the pushed `main` HEAD and triggers
   the PyPI workflow:

   ```bash
   gh release create X.Y.Z --target main --title "X.Y.Z" --notes-file <notes.md>
   ```

7. Watch the publish and verify it landed on PyPI:

   ```bash
   gh run watch $(gh run list --workflow=pypi-on-release.yml --limit 1 --json databaseId -q '.[0].databaseId') --exit-status
   curl -s https://pypi.org/pypi/burla/json | python3 -c "import json,sys;print(json.load(sys.stdin)['info']['version'])"
   ```

   The curl must print `X.Y.Z`. If the workflow fails, read its logs
   (`gh run view --log-failed`), fix, and re-release; do not report success.

8. Resync `dev` to `main` so it stays "main + in-flight" for the next cycle:

   ```bash
   git switch dev && git merge --ff-only main && git push origin dev
   ```

9. Report done: the version is live on PyPI, plus the GitHub release URL and the
   Actions run URL.

## Release notes house style

Match the existing releases (`gh release view <last> --json body`). User-facing
prose grouped by theme, not a raw commit list. Auto-generate and publish without
pausing for edits.

```
**Burla Release X.Y.Z**
_"The simplest way to scale Python"_

- <user-facing change, phrased as a benefit>
- <...>

To update run `pip install burla==X.Y.Z`

Made with [Cursor](https://cursor.com)
```
