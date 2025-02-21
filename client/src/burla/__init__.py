import os
import tomli
import pathlib
import subprocess
from fire import Fire


_BURLA_GCP_PROJECT = "burla-prod"
_BURLA_SERVICE_URL = "https://cluster.burla.dev"
_BURLA_BACKEND_URL = "https://backend.burla.dev"

custom_host = os.environ.get("BURLA_API_URL")
custom_project = os.environ.get("BURLA_GCP_PROJECT")

if custom_host or custom_project and not (custom_host and custom_project):
    msg = "Env variables `BURLA_API_URL` and `BURLA_GCP_PROJECT` must both be set, or neither."
    raise Exception(msg)
elif custom_host or custom_project:
    _BURLA_SERVICE_URL = custom_host
    _BURLA_GCP_PROJECT = custom_project


# Detect if in dev mode by searching for the `IN_DEV` flag in a nearby pyproject.toml
pyproject_path = pathlib.Path(__file__).resolve().parent.parent.parent / "pyproject.toml"
pyproject_config = tomli.loads(pyproject_path.read_text()) if pyproject_path.exists() else {}
IN_DEV = pyproject_config.get("tool", {}).get("burla", {}).get("config", {}).get("in_dev", False)
if IN_DEV:
    cmd = ["gcloud", "config", "get-value", "project"]
    _BURLA_GCP_PROJECT = subprocess.run(cmd, capture_output=True, text=True).stdout.strip()
    _BURLA_SERVICE_URL = "http://127.0.0.1:5001"

# needed so main_service can associate a client version with a request
__version__ = "0.9.4"

from burla._auth import login
from burla._remote_parallel_map import remote_parallel_map


def init_cli():
    Fire({"login": login, "version": lambda: print(__version__)})
