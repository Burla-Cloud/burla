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

1. Before running tests, ensure the cluster is on, ready, and the machine is authorized.
2. If the cluster is not running, open the dashboard with the browser tool and press Start to boot it.
3. Run `make test`.
4. After test run, use the browser tool to open the dashboard job details page for the latest test job.
5. Verify that logs on the job details page show `"hi"` exactly once per input.

