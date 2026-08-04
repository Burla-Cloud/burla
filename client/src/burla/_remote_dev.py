"""`make remote-dev`: this checkout's main_service here, nodes as real cloud VMs.

Run as `python -m burla._remote_dev` rather than a `burla` subcommand, so dev
entry points never show up in the CLI, not even from a source checkout.
"""

from burla._local_head import run_remote_dev_head

if __name__ == "__main__":
    run_remote_dev_head()
