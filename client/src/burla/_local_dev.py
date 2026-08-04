"""`make local-dev`: this checkout's whole cluster as local docker containers.

Run as `python -m burla._local_dev` rather than a `burla` subcommand, so dev
entry points never show up in the CLI, not even from a source checkout.
"""

from burla._local_head import run_local_dev_head

if __name__ == "__main__":
    run_local_dev_head()
