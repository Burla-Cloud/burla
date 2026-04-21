### How to run the tests

#### Instructions for Humans:
These steps assume you're already a contributer with a Google Cloud Project prepared with the required resources to run a burla cluster.  
Email `jake@burla.dev` if you're interested in contributing, or just [put time on my calendar](https://cal.com/jakez/burla?duration=30) :)   

&nbsp;

1. Start a cluster:
    - run `make local-dev-cluster` (runs entire cluster on your local machine)
    - OR: run `make remote-dev-cluster` (main_service runs locally, nodes run in the cloud)
2. Run `make test`

If running in `local-dev-mode` errors will be visible in docker desktop container logs.  
If running in `remote-dev-mode` errors will be visible in the terminal where the `main_service`
is running, or google-cloud-logging for errors from the `node_service` or `worker_service`.

#### Instructions for Agents:

1. Before running tests, check if local-dev cluster is already running.
2. Before starting local-dev, verify active gcloud project is `burla-test`.
   If not, switch with `gcloud config set project burla-test`.
3. If local-dev is not running:
   - start it with `make local-dev`
   - open `http://localhost:5001` in browser automation
   - if login page appears, login with:
     - email: `JakesCursorAgent@gmail.com`
     - password: Google Cloud Secret `JakesCursorAgent-gmail-password`
   - press the Start button in the Burla UI
4. Readiness gate: if you cannot verify local-dev cluster is on and ready, stop.
   Do not run tests in that state. Investigate why cluster boot failed and report
   a clear diagnosis (what failed, where it failed, and the likely fix).
   A run only counts as "running the tests" when this readiness gate is passed.
   Any failure caused by cluster-not-ready state does not count as a test run.
   - Local-dev recovery: if tests fail with connection errors (for example
     `Cannot connect to host localhost:8081`) right after containers were killed,
     nodes may still be marked ready in Firestore while containers are down.
     Open the cluster dashboard and click **Restart** to recreate containers, then
     rerun the test command.
     Treat this as a readiness failure, not a test failure.
5. If tests fail with auth errors like `invalid_grant` or `Invalid JWT Signature`:
   - run `burla login --no_browser=True`
   - open the printed login URL in browser automation
   - complete login and click the Authorize button
   - then rerun the test command
6. `make test` is not reliable in fresh shells because `pytest` may not be on `PATH`.
   Always run tests with uv from repo root:
   - `uv sync --project ./client --group dev`
   - `uv run --project ./client --group dev pytest client/tests/test.py -s -x --disable-warnings`
7. Hard timeout rule: if test output does not advance to pass/fail within 10 seconds after
   `collected 1 item`, stop the test process and report it as blocked. Never wait longer.
8. After test run, verify logs for the latest test job show `"hi"` once per input.

