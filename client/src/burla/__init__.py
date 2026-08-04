import json
import os
from pathlib import Path
from urllib.parse import urlparse

from fire import Fire
from platformdirs import user_config_dir

# needed so main_service can associate a client version with a request
__version__ = "1.6.1"

_SOURCE_ROOT = Path(__file__).resolve().parents[3]
_IN_SOURCE_CHECKOUT = (
    (_SOURCE_ROOT / ".git").exists()
    and (_SOURCE_ROOT / "client" / "pyproject.toml").exists()
)
_BURLA_ENVIRONMENT = os.environ.get("BURLA_ENVIRONMENT", "production").lower()
if _BURLA_ENVIRONMENT not in {"production", "test"}:
    raise ValueError("BURLA_ENVIRONMENT must be `production` or `test`.")
if _BURLA_ENVIRONMENT == "test" and not _IN_SOURCE_CHECKOUT:
    raise RuntimeError(
        "Burla's internal test environment is only available from an editable "
        "source checkout."
    )

_BURLA_APP_NAME = "burla-test" if _BURLA_ENVIRONMENT == "test" else "burla"
_DEFAULT_BACKEND_URL = (
    "https://test.backend.burla.dev"
    if _BURLA_ENVIRONMENT == "test"
    else "https://backend.burla.dev"
)
_DEFAULT_RELAY_HOST = (
    "relay.test-clusters.burla.dev"
    if _BURLA_ENVIRONMENT == "test"
    else "relay.burla.dev"
)
_DEFAULT_NODE_SOURCE_REF = "dev" if _BURLA_ENVIRONMENT == "test" else __version__
_BURLA_BACKEND_URL = os.environ.get(
    "BURLA_BACKEND_URL", _DEFAULT_BACKEND_URL
).rstrip("/")
if _BURLA_BACKEND_URL != "https://backend.burla.dev" and not _IN_SOURCE_CHECKOUT:
    raise RuntimeError(
        "Non-production Burla backends are only available from an editable "
        "source checkout."
    )
_BURLA_RELAY_HOST = os.environ.get("BURLA_RELAY_HOST", _DEFAULT_RELAY_HOST)
_BURLA_NODE_SOURCE_REF = os.environ.get(
    "BURLA_NODE_SOURCE_REF", _DEFAULT_NODE_SOURCE_REF
)

_appdata_dir = Path(user_config_dir(appname=_BURLA_APP_NAME, appauthor="burla"))
CONFIG_PATH = _appdata_dir / Path("burla_credentials.json")
SETTINGS_PATH = _appdata_dir / Path("config.json")


def get_cloud() -> str:
    override = os.environ.get("BURLA_CLOUD")
    if override:
        return override.lower()
    if SETTINGS_PATH.exists():
        return json.loads(SETTINGS_PATH.read_text())["cloud"]
    return "aws"


def set_config(key: str, value: str) -> str:
    if key != "cloud":
        raise ValueError("The only supported config key is `cloud`.")
    cloud = value.lower()
    if cloud not in ("aws", "gcp"):
        raise ValueError("cloud must be `aws` or `gcp`.")

    SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    SETTINGS_PATH.write_text(json.dumps({"cloud": cloud}))
    return f"cloud = {cloud}"


def get_config(key: str = None):
    config = {
        "cloud": get_cloud(),
        "environment": _BURLA_ENVIRONMENT,
        "backend": _BURLA_BACKEND_URL,
    }
    if key is None:
        return config
    if key not in config:
        raise ValueError(f"Unknown config key: {key}")
    return config[key]


def _local_dashboard_url(url: str) -> bool:
    hostname = urlparse(url).hostname
    return hostname in {"localhost", "127.0.0.1", "main_service"} or bool(
        hostname and hostname.startswith("node_")
    )


def _env_dashboard_url() -> str | None:
    """`BURLA_CLUSTER_DASHBOARD_URL`, how `make local-dev` / `make remote-dev`
    point the client at the in-shell dev server without touching the creds
    file. Highest precedence."""
    override = os.environ.get("BURLA_CLUSTER_DASHBOARD_URL")
    if not override:
        return None
    override = override.rstrip("/")
    if not override.startswith("https://") and not _local_dashboard_url(override):
        raise ValueError("BURLA_CLUSTER_DASHBOARD_URL must use HTTPS")
    return override


def _deployed_dashboard_url() -> str | None:
    """The deployed cluster from `burla login` (mode="deployed"), or None.

    A head you start locally wins over this while it is running; this is the
    fallback once no local head is up.
    """
    if not CONFIG_PATH.exists():
        return None
    config = json.loads(CONFIG_PATH.read_text())
    if config.get("mode") != "deployed" or not config.get("cluster_dashboard_url"):
        return None

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


def get_cluster_dashboard_url() -> str:
    """Resolve the main_service URL for the cluster this machine should use.

    Precedence: `BURLA_CLUSTER_DASHBOARD_URL`, then a head already running for
    this checkout's namespace, then a deployed cluster from `burla login`,
    then a head started on this machine. See `burla._local_head`.
    """
    from burla._local_head import ensure_local_head, running_head_url

    return (
        _env_dashboard_url()
        or running_head_url()
        or _deployed_dashboard_url()
        or ensure_local_head()
    )


from burla._auth import login
from burla._deploy import deploy
from burla._remote_parallel_map import remote_parallel_map

worker_cache = {}


def version():
    """Print current Burla client version."""
    print(__version__)


def dashboard():
    """Open the Burla dashboard hosted on this machine.

    Reuses a healthy cluster head without restarting it. If none is running,
    starts one in this terminal until Ctrl-C.
    """
    import webbrowser

    from burla._local_head import run_local_head_for_dashboard

    def open_dashboard(url: str, is_foreground: bool):
        print(f"Burla dashboard is running at {url}")
        if is_foreground:
            print("Press Ctrl-C to stop it.")
        webbrowser.open(url)

    run_local_head_for_dashboard(on_ready=open_dashboard)


def install(cloud: str = "gcp"):
    """Deprecated: use `burla deploy` (or nothing at all - `remote_parallel_map`
    and `burla dashboard` now work with zero deployment)."""
    print("`burla install` is now `burla deploy`, deploying ...")
    deploy(cloud=cloud)


def init_cli():
    commands = {
        "login": login,
        "dashboard": dashboard,
        "config": {
            "set": set_config,
            "get": get_config,
        },
        "--version": version,
        "-v": version,
    }
    commands["deploy"] = deploy
    if _BURLA_ENVIRONMENT == "production":
        commands["install"] = install
    Fire(commands)
