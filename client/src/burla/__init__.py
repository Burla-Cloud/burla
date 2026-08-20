import json
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from platformdirs import user_config_dir

# needed so main_service can associate a client version with a request
__version__ = "1.7.6"

# In a checkout this file is <root>/client/src/burla/__init__.py. Installed
# flat (e.g. /worker_service_python_env/burla/ on nodes) there is no parents[3],
# so guard the depth or importing burla crashes there.
_parents = Path(__file__).resolve().parents
_SOURCE_ROOT = _parents[3] if len(_parents) > 3 else None
_IN_SOURCE_CHECKOUT = _SOURCE_ROOT is not None and (
    (_SOURCE_ROOT / ".git").exists()
    and (_SOURCE_ROOT / "client" / "pyproject.toml").exists()
)
_IN_HEAD_RUNTIME = os.environ.get("BURLA_HEAD_RUNTIME") == "True"
_BURLA_ENVIRONMENT = os.environ.get("BURLA_ENVIRONMENT", "production").lower()
if _BURLA_ENVIRONMENT not in {"production", "test"}:
    raise ValueError("BURLA_ENVIRONMENT must be `production` or `test`.")
if _BURLA_ENVIRONMENT == "test" and not (_IN_SOURCE_CHECKOUT or _IN_HEAD_RUNTIME):
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
if _BURLA_BACKEND_URL != "https://backend.burla.dev" and not (
    _IN_SOURCE_CHECKOUT or _IN_HEAD_RUNTIME
):
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
    from burla._cloud_select import choose_cloud

    return choose_cloud()


def set_config(key: str, value: str) -> str:
    if key != "cloud":
        raise ValueError("The only supported config key is `cloud`.")
    cloud = value.lower()
    if cloud not in ("aws", "gcp", "azure"):
        raise ValueError("cloud must be `aws`, `gcp`, or `azure`.")

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
    from burla._auth import deployed_dashboard_url

    return deployed_dashboard_url()


# The cluster a job has already committed to. Set for the duration of a
# `remote_parallel_map` call: without it a later lookup whose health probe times
# out would fall through to starting a *new* head, killing the one the job is
# running against (observed mid-job: "Shutting down" then a second head on a
# different port). Process-wide, like the resolution it short-circuits.
_pinned_cluster_url: str | None = None
# Whether the pinned head outlives this machine (deployed or env-pointed), so a
# concurrent `remote_parallel_map(detach=True)` in this process resolving to the
# pin is gated correctly.
_pinned_head_supports_detach: bool = False


@dataclass
class _HeadHandle:
    url: str
    owned: bool
    pid: int | None = None
    supports_detach: bool = False


def _pin_cluster_url(url: str, supports_detach: bool = False):
    global _pinned_cluster_url, _pinned_head_supports_detach
    _pinned_cluster_url = url
    _pinned_head_supports_detach = supports_detach


def _unpin_cluster_url():
    global _pinned_cluster_url, _pinned_head_supports_detach
    _pinned_cluster_url = None
    _pinned_head_supports_detach = False


def _remote_head_handle() -> _HeadHandle | None:
    if _pinned_cluster_url:
        return _HeadHandle(
            url=_pinned_cluster_url,
            owned=False,
            supports_detach=_pinned_head_supports_detach,
        )
    url = _env_dashboard_url()
    if url:
        return _HeadHandle(url=url, owned=False, supports_detach=True)
    url = _deployed_dashboard_url()
    if url:
        return _HeadHandle(url=url, owned=False, supports_detach=True)
    return None


def _existing_cluster_dashboard_url() -> str | None:
    handle = _remote_head_handle()
    if handle:
        return handle.url

    from burla._local_head import running_head_url

    return running_head_url()


def get_cluster_dashboard_url() -> str:
    """Resolve the main_service URL for the cluster this machine should use.

    Precedence: a cluster a running job already committed to, then
    `BURLA_CLUSTER_DASHBOARD_URL`, then the deployed cluster registered for the
    active cloud account, then an account-wide ad hoc head already running, then
    a head started on this machine. See `burla._local_head`.
    """
    url = _existing_cluster_dashboard_url()
    if url:
        return url

    from burla._local_head import ensure_local_head

    return ensure_local_head()


def _acquire_head_for_job(for_background_job: bool = False) -> _HeadHandle:
    handle = _remote_head_handle()
    if handle is None:
        from burla._local_head import running_head_url

        url = running_head_url()
        if url:
            handle = _HeadHandle(url=url, owned=False)

    if for_background_job and (handle is None or not handle.supports_detach):
        from burla._node import DetachRequiresDeployedCluster

        raise DetachRequiresDeployedCluster()

    if handle is None:
        from burla._local_head import start_local_head_for_job

        url, pid = start_local_head_for_job()
        handle = _HeadHandle(url=url, owned=True, pid=pid)
    return handle


def _release_head_for_job(handle: _HeadHandle):
    if not handle.owned:
        return
    from burla._local_head import release_head

    release_head(handle)


from burla._auth import login
from burla._deploy import deploy
from burla._remote_parallel_map import remote_parallel_map

worker_cache = {}


def dashboard(port: int | None = None):
    """Open the dashboard. Local dashboards use port 5001 unless overridden."""
    import webbrowser

    from burla._reporting import log_dashboard_start_telemetry

    def open_dashboard(url: str, is_foreground: bool):
        print(f"Burla dashboard is running at {url}")
        if is_foreground:
            print("Press Ctrl-C to stop it.")
        webbrowser.open(url)
        log_dashboard_start_telemetry(__version__, is_local=is_foreground)

    remote_head = _remote_head_handle()
    if remote_head:
        from burla._auth import LocalHeadError

        if port is not None:
            raise LocalHeadError("--port can only be used with a local dashboard.")
        open_dashboard(remote_head.url, False)
        return

    from burla._local_head import run_local_head_for_dashboard

    run_local_head_for_dashboard(on_ready=open_dashboard, port=port)


def init_cli():
    from burla._cli import main

    raise SystemExit(main())
