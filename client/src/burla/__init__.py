import json
import os
from pathlib import Path

from fire import Fire
from platformdirs import user_config_dir

# needed so main_service can associate a client version with a request
__version__ = "1.5.6"
_BURLA_BACKEND_URL = "https://backend.burla.dev"

_appdata_dir = Path(user_config_dir(appname="burla", appauthor="burla"))
CONFIG_PATH = _appdata_dir / Path("burla_credentials.json")


def get_cluster_dashboard_url() -> str:
    """
    Resolve the main_service URL. `BURLA_CLUSTER_DASHBOARD_URL` wins if set -
    this is how `make local-dev` / `make remote-dev` point the client at the
    in-shell dev server without mutating the user's credentials file.
    """
    override = os.environ.get("BURLA_CLUSTER_DASHBOARD_URL")
    if override:
        return override.rstrip("/")
    return json.loads(CONFIG_PATH.read_text())["cluster_dashboard_url"].rstrip("/")


from burla._auth import login
from burla._install import install
from burla._remote_parallel_map import remote_parallel_map

worker_cache = {}


def version():
    """Print current Burla client version."""
    print(__version__)


def init_cli():
    Fire(
        {
            "login": login,
            "install": install,
            "--version": version,
            "-v": version,
        }
    )
