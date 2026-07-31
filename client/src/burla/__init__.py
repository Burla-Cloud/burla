import json
import os
from pathlib import Path
from urllib.parse import urlparse

from fire import Fire
from platformdirs import user_config_dir

# needed so main_service can associate a client version with a request
__version__ = "1.6.1"
_BURLA_BACKEND_URL = os.environ.get(
    "BURLA_BACKEND_URL", "https://backend.burla.dev"
).rstrip("/")

_appdata_dir = Path(user_config_dir(appname="burla", appauthor="burla"))
CONFIG_PATH = _appdata_dir / Path("burla_credentials.json")


def _local_dashboard_url(url: str) -> bool:
    hostname = urlparse(url).hostname
    return hostname in {"localhost", "127.0.0.1", "main_service"} or bool(
        hostname and hostname.startswith("node_")
    )


def get_cluster_dashboard_url() -> str:
    """
    Resolve the main_service URL. `BURLA_CLUSTER_DASHBOARD_URL` wins if set -
    this is how `make local-dev` / `make remote-dev` point the client at the
    in-shell dev server without mutating the user's credentials file.

    With no deployed cluster (the default), Burla runs the cluster head on
    this machine: see `burla._local_head`.
    """
    override = os.environ.get("BURLA_CLUSTER_DASHBOARD_URL")
    if override:
        override = override.rstrip("/")
        if not override.startswith("https://") and not _local_dashboard_url(override):
            raise ValueError("BURLA_CLUSTER_DASHBOARD_URL must use HTTPS")
        return override

    if not CONFIG_PATH.exists():
        # A deployed cluster is used when one is reachable (via `burla login`
        # or the ADC bootstrap); otherwise default to client-hosted mode.
        from burla._auth import (
            ADCBootstrapException,
            ADCProjectException,
            ADCSecretPermissionException,
            BurlaNotInstalledException,
            bootstrap_from_adc,
        )
        from google.auth.exceptions import DefaultCredentialsError

        try:
            bootstrap_from_adc()
        except (
            ADCBootstrapException,
            ADCProjectException,
            ADCSecretPermissionException,
            BurlaNotInstalledException,
            DefaultCredentialsError,
        ):
            from burla._local_head import ensure_local_head

            return ensure_local_head()

    config = json.loads(CONFIG_PATH.read_text())
    if config.get("mode") == "client_hosted" or not config.get("cluster_dashboard_url"):
        from burla._local_head import ensure_local_head

        return ensure_local_head()

    dashboard_url = config["cluster_dashboard_url"].rstrip("/")
    if not dashboard_url.startswith("https://") and not _local_dashboard_url(
        dashboard_url
    ):
        import requests
        from burla._auth import get_auth_headers

        response = requests.get(
            f"{_BURLA_BACKEND_URL}/v1/clusters/{config['project_id']}/dashboard_url",
            headers=get_auth_headers(),
            timeout=20,
        )
        response.raise_for_status()
        dashboard_url = response.json()["dashboard_url"].rstrip("/")
        if not dashboard_url.startswith("https://"):
            raise ValueError("Backend returned a non-HTTPS dashboard URL")
        config["cluster_dashboard_url"] = dashboard_url
        CONFIG_PATH.write_text(json.dumps(config))
    return dashboard_url


from burla._auth import login
from burla._deploy import deploy
from burla._remote_parallel_map import remote_parallel_map

worker_cache = {}


def version():
    """Print current Burla client version."""
    print(__version__)


def dashboard(cloud: str = None):
    """Open the Burla dashboard, hosted on this machine.

    Starts the cluster head locally if it isn't already running - from there
    you can boot nodes, watch jobs, and change settings. No deployment or
    special cloud permissions needed (only permission to boot VMs).
    """
    import webbrowser

    from burla._local_head import ensure_local_head

    url = ensure_local_head(cloud=cloud)
    print(f"Burla dashboard is running at {url}")
    webbrowser.open(url)


def install(cloud: str = "gcp"):
    """Deprecated: use `burla deploy` (or nothing at all - `remote_parallel_map`
    and `burla dashboard` now work with zero deployment)."""
    print("`burla install` is now `burla deploy`, deploying ...")
    deploy(cloud=cloud)


def init_cli():
    Fire(
        {
            "login": login,
            "deploy": deploy,
            "dashboard": dashboard,
            "install": install,
            "--version": version,
            "-v": version,
        }
    )
