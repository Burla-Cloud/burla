import tomli
import pathlib
import subprocess
from fire import Fire

pyproject_path = pathlib.Path(__file__).resolve().parent.parent.parent / "pyproject.toml"
pyproject_config = tomli.loads(pyproject_path.read_text()) if pyproject_path.exists() else {}
IN_DEV = pyproject_config.get("tool", {}).get("burla", {}).get("config", {}).get("in_dev", False)
if IN_DEV:
    cmd = ["gcloud", "config", "get-value", "project"]
    _BURLA_GCP_PROJECT = subprocess.run(cmd, capture_output=True, text=True).stdout.strip()
    _BURLA_SERVICE_URL = "http://127.0.0.1:5001"
else:
    _BURLA_GCP_PROJECT = "burla-prod"
    _BURLA_SERVICE_URL = "https://cluster.burla.dev"

_BURLA_BACKEND_URL = "https://backend.burla.dev"

# needed for service to record client version
__version__ = "0.8.14"

from burla._auth import login
from burla._remote_parallel_map import remote_parallel_map


def init_cli():
    Fire({"login": login, "version": lambda: print(__version__)})
