"""
Client-hosted mode: runs main_service on this machine instead of a head VM.

This is the default way Burla runs. `remote_parallel_map` starts main_service
as a detached subprocess using the code vendored inside this package. The
explicit `burla dashboard` command reuses that service when healthy or runs it
in the foreground. Both modes also start an frpc tunnel so node VMs can reach
the head through the relay. Node VMs are booted with the user's own cloud
credentials and carry none of their own, so the only permissions needed are
"can boot VMs". `burla deploy` remains the upgrade path to an always-on, shared
head VM.
"""

import configparser
import json
import os
import platform
import re
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from time import sleep, time
from uuid import uuid4

import requests
from platformdirs import user_data_dir

from burla import (
    _BURLA_APP_NAME,
    _BURLA_BACKEND_URL,
    _BURLA_NODE_SOURCE_REF,
    _BURLA_RELAY_HOST,
    __version__,
)

RELAY_HOST = _BURLA_RELAY_HOST.strip().lower()
RELAY_SERVER_ADDR = os.environ.get("BURLA_RELAY_SERVER_ADDR", RELAY_HOST)
RELAY_SERVER_PORT = os.environ.get("BURLA_RELAY_SERVER_PORT", "7000")
FRP_VERSION = "0.70.1"

PREFERRED_HEAD_PORT = 5001  # the browser login flow redirects to localhost:5001

STATE_ROOT = (
    Path(user_data_dir(appname=_BURLA_APP_NAME, appauthor="burla")) / "clusters"
)


class LocalHeadError(Exception):
    pass


def _state_dir(project_id: str) -> Path:
    directory = STATE_ROOT / project_id
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def cluster_namespace() -> str:
    """Which explicit worktree dev cluster this process belongs to.

    Several dev heads run against one cloud account (one per checkout), and they
    must not share a head port, relay subdomain, or history db. `make remote-dev`
    passes this namespace to `_run_local_head`. Ad hoc client-hosted heads ignore
    it so notebooks and shells reuse one account-wide head and history database.
    """
    name = os.environ.get("BURLA_CLUSTER_NAME", "").strip().lower()
    return re.sub(r"[^a-z0-9-]+", "-", name).strip("-")


def _head_state_dir(project_id: str, namespace: str = "") -> Path:
    """Per-cluster head state: ports, relay subdomain, history db, TLS. The
    cluster token lives in the project dir above this instead, because it is
    account-wide and every head for the account shares it."""
    directory = _state_dir(project_id)
    if namespace:
        directory = directory / f"cluster-{namespace}"
        directory.mkdir(parents=True, exist_ok=True)
    return directory


# ------------------------------------------------------------------ cloud


def detect_cloud() -> tuple[str, str, str | None]:
    """Returns (cloud, project_id, region) for the configured cloud.
    region is None on GCP (nodes' region comes from cluster config there)."""
    from burla import get_cloud

    cloud = get_cloud()
    if cloud == "azure":
        if not shutil.which("az"):
            raise LocalHeadError(
                "Azure is selected, but the az CLI is not installed. "
                "Install it or run `burla config set cloud aws`."
            )
        result = subprocess.run(
            ["az", "account", "show", "--query", "id", "--output", "tsv"],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0 or not result.stdout.strip():
            raise LocalHeadError(
                "Azure is selected, but its credentials are not active. "
                "Run `az login`, then retry."
            )
        subscription_id = result.stdout.strip()
        from burla._deploy_azure import _azure_region

        # Keeps the GUID's dashes: the longest relay label this produces is
        # exactly the 63-char DNS limit (see _deploy_azure.deploy_azure).
        return "azure", f"azure-{subscription_id}", _azure_region()

    if cloud == "gcp":
        if not shutil.which("gcloud"):
            raise LocalHeadError(
                "GCP is selected, but gcloud is not installed. "
                "Install it or run `burla config set cloud aws`."
            )
        result = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            capture_output=True,
            text=True,
        )
        gcp_project = result.stdout.strip()
        if gcp_project and gcp_project != "(unset)":
            return "gcp", gcp_project, None
        raise LocalHeadError(
            "GCP is selected, but no gcloud project is set. "
            "Run `gcloud config set project <id>`."
        )

    if not shutil.which("aws"):
        raise LocalHeadError(
            "AWS is selected, but the AWS CLI is not installed. "
            "Install it or run `burla config set cloud gcp`."
        )
    result = subprocess.run(
        ["aws", "sts", "get-caller-identity", "--query", "Account", "--output", "text"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise LocalHeadError(
            "AWS is selected, but its credentials are not active. "
            "Run `aws configure` or `aws sso login`, then retry."
        )
    region_result = subprocess.run(
        ["aws", "configure", "get", "region"], capture_output=True, text=True
    )
    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or region_result.stdout.strip()
        or "us-east-1"
    )
    return "aws", f"aws-{result.stdout.strip()}", region


def _cloud_account_name(cloud: str, project_id: str) -> str:
    """Human-readable account label for the dashboard's settings page."""
    if cloud == "aws":
        return _aws_account_name(project_id.removeprefix("aws-"))
    if cloud == "azure":
        result = subprocess.run(
            ["az", "account", "show", "--query", "name", "--output", "tsv"],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    return project_id


def _aws_account_name(account_id: str) -> str:
    result = subprocess.run(
        [
            "aws",
            "account",
            "get-account-information",
            "--query",
            "AccountName",
            "--output",
            "text",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode == 0 and result.stdout.strip() not in ("", "None"):
        return result.stdout.strip()

    config = configparser.ConfigParser()
    config.read(Path.home() / ".aws" / "config")
    for section in config.sections():
        if config[section].get("sso_account_id") == account_id:
            return section.removeprefix("profile ")
    return account_id


def _gcp_ownership_payload() -> dict:
    try:
        import google.auth
        from google.auth.transport.requests import Request

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        credentials.refresh(Request())
        access_token = credentials.token
    except Exception:
        result = subprocess.run(
            ["gcloud", "auth", "print-access-token"],
            capture_output=True,
            text=True,
            check=True,
        )
        access_token = result.stdout.strip()
    return {"cloud": "gcp", "access_token": access_token}


def _ownership_payload(cloud: str, region: str | None) -> dict:
    if cloud == "aws":
        from burla._deploy_aws import _aws_ownership_payload

        return _aws_ownership_payload(region)
    if cloud == "azure":
        from burla._deploy_azure import _azure_ownership_payload

        return _azure_ownership_payload()
    return _gcp_ownership_payload()


# ------------------------------------------------------------------ token


def read_saved_cluster_token(project_id: str) -> str | None:
    token_path = _state_dir(project_id) / "cluster_token"
    if token_path.exists():
        return token_path.read_text().strip()
    return None


def save_cluster_token(project_id: str, token: str):
    token_path = _state_dir(project_id) / "cluster_token"
    token_path.write_text(token)
    token_path.chmod(0o600)


def get_or_register_cluster_token(cloud: str, project_id: str, aws_region: str | None) -> str:
    token = read_saved_cluster_token(project_id)
    if token:
        return token

    response = requests.post(
        f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}",
        json=_ownership_payload(cloud, aws_region),
        timeout=30,
    )
    if response.status_code == 200:
        token = response.json()["token"]
        save_cluster_token(project_id, token)
        return token

    # Cluster already registered by an old `burla install`, which stored its
    # token in Secret Manager (GCP) / SSM (AWS) - read it from there so
    # upgrades stay seamless. No Azure equivalent: Azure support postdates
    # the move to backend-held tokens.
    if response.status_code in (403, 409):
        command = None
        if cloud == "gcp":
            secret = os.environ.get("BURLA_CLUSTER_TOKEN_SECRET", "burla-cluster-id-token")
            command = ["gcloud", "secrets", "versions", "access", "latest", f"--secret={secret}"]
        elif cloud == "aws":
            parameter = os.environ.get("BURLA_CLUSTER_TOKEN_PARAMETER", "/burla/cluster-id-token")
            command = [
                *("aws", "ssm", "get-parameter", "--region", aws_region),
                *("--name", parameter, "--with-decryption"),
                *("--query", "Parameter.Value", "--output", "text"),
            ]
        if command:
            result = subprocess.run(command, capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip():
                token = result.stdout.strip()
                save_cluster_token(project_id, token)
                return token

        # Authorized users (e.g. this user's other laptop) may fetch the
        # token from the backend after a browser `burla login`.
        from burla import CONFIG_PATH
        from burla._auth import get_auth_headers

        if CONFIG_PATH.exists():
            response = requests.get(
                f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}/token",
                headers=get_auth_headers(),
                timeout=30,
            )
            if response.status_code == 200:
                token = response.json()["token"]
                save_cluster_token(project_id, token)
                return token

    raise LocalHeadError(
        f"The cluster [{project_id}] is already registered with Burla, but this machine "
        "doesn't have its token. Run `burla login` (as an authorized user of that cluster) "
        "and retry, or email jake@burla.dev to recover access."
    )


def ensure_user_authorized(
    cloud: str,
    project_id: str,
    cluster_token: str,
):
    """Registers this user against the cluster and mints client credentials,
    without needing Secret Manager (the token came from local state)."""
    from burla import CONFIG_PATH
    from burla._auth import _write_auth_config

    if CONFIG_PATH.exists():
        auth_info = json.loads(CONFIG_PATH.read_text())
        if auth_info["project_id"] == project_id:
            return

    if cloud == "aws":
        result = subprocess.run(
            ["aws", "sts", "get-caller-identity", "--query", "Arn", "--output", "text"],
            capture_output=True,
            text=True,
            check=True,
        )
        identity = result.stdout.strip().rsplit("/", 1)[-1]
    elif cloud == "azure":
        result = subprocess.run(
            ["az", "account", "show", "--query", "user.name", "--output", "tsv"],
            capture_output=True,
            text=True,
            check=True,
        )
        identity = result.stdout.strip()
    else:
        import google.auth
        from google.auth.transport.requests import Request

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        credentials.refresh(Request())
        response = requests.get(
            "https://oauth2.googleapis.com/tokeninfo",
            params={"access_token": credentials.token},
            timeout=10,
        )
        response.raise_for_status()
        identity = response.json()["email"]

    headers = {"Authorization": f"Bearer {cluster_token}"}
    users_url = f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}/users"
    requests.post(users_url, json={"new_user": identity}, headers=headers, timeout=30)

    response = requests.post(
        f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}/adc:exchange",
        headers=headers,
        json={"email": identity},
        timeout=30,
    )
    response.raise_for_status()
    auth_info = response.json()
    _write_auth_config(auth_info)


# ------------------------------------------------------------------ frpc


def _frpc_download_url() -> tuple[str, str]:
    """(url, archive member path) for this machine's frpc build."""
    machine = platform.machine().lower()
    arch = "arm64" if machine in ("arm64", "aarch64") else "amd64"
    system = platform.system().lower()
    operating_system = {"darwin": "darwin", "linux": "linux", "windows": "windows"}[system]
    extension = "zip" if operating_system == "windows" else "tar.gz"
    directory = f"frp_{FRP_VERSION}_{operating_system}_{arch}"
    url = (
        "https://github.com/fatedier/frp/releases/download/"
        f"v{FRP_VERSION}/{directory}.{extension}"
    )
    binary = "frpc.exe" if operating_system == "windows" else "frpc"
    return url, f"{directory}/{binary}"


def ensure_frpc_binary() -> Path:
    binary_name = "frpc.exe" if platform.system() == "Windows" else "frpc"
    binary_path = STATE_ROOT.parent / "bin" / f"{binary_name}-{FRP_VERSION}"
    if binary_path.exists():
        return binary_path

    binary_path.parent.mkdir(parents=True, exist_ok=True)
    url, member = _frpc_download_url()
    with tempfile.TemporaryDirectory() as tmp:
        archive_path = Path(tmp) / url.split("/")[-1]
        with requests.get(url, stream=True, timeout=120) as response:
            response.raise_for_status()
            with open(archive_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
        if archive_path.suffix == ".zip":
            with zipfile.ZipFile(archive_path) as archive:
                archive.extract(member, tmp)
        else:
            with tarfile.open(archive_path) as archive:
                archive.extract(member, tmp)
        extracted = Path(tmp) / member
        shutil.move(str(extracted), binary_path)
    binary_path.chmod(0o755)
    return binary_path


def _write_frpc_config(
    state_dir: Path, project_id: str, cluster_token: str, subdomain: str, tls_port: int
) -> Path:
    config = f"""serverAddr = "{RELAY_SERVER_ADDR}"
serverPort = {RELAY_SERVER_PORT}
loginFailExit = false
user = "{project_id}"
metadatas.token = "{cluster_token}"
transport.poolCount = 4

[[proxies]]
name = "{subdomain}"
type = "https"
localIP = "127.0.0.1"
localPort = {tls_port}
subdomain = "{subdomain}"
"""
    config_path = state_dir / "frpc.toml"
    config_path.write_text(config)
    config_path.chmod(0o600)
    return config_path


# ------------------------------------------------------------------ head process


def _free_port(preferred: int | None = None) -> int:
    # SO_REUSEADDR matches how uvicorn binds, so a just-killed head's
    # TIME_WAIT socket doesn't push us off the preferred port.
    if preferred is not None:
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", preferred))
                return preferred
            except OSError:
                pass
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (OSError, TypeError):
        return False


def _main_service_pythonpath() -> str | None:
    """The wheel vendors main_service next to burla; a repo checkout keeps it
    in main_service/src. The repo copy wins when both exist (editable installs
    leave a stale snapshot of main_service in site-packages)."""
    import importlib.util

    repo_root = Path(__file__).resolve().parents[3]
    repo_copy = repo_root / "main_service" / "src" / "main_service"
    if repo_copy.exists():
        return str(repo_copy.parent)
    if importlib.util.find_spec("main_service") is not None:
        return None
    raise LocalHeadError(
        "The main_service package is missing from this Burla installation. "
        "Reinstall with `pip install --force-reinstall burla`."
    )


def _head_matches(
    url: str, project_id: str, cluster_token: str, expected_namespace: str
) -> bool:
    # /version sits behind the auth middleware like everything else;
    # unauthenticated requests get the login page instead of JSON.
    headers = {"Authorization": f"Bearer {cluster_token}"}
    try:
        response = requests.get(f"{url}/version", headers=headers, timeout=2)
        info = response.json()
        return (
            info["version"] == __version__
            and info["project"] == project_id
            and info.get("namespace", "default") == expected_namespace
        )
    except Exception:
        return False


def _prepare_aws(project_id: str, aws_region: str):
    """First-run AWS prep a laptop head needs before booting nodes: internal
    security groups + the node AMI. Both are plain EC2 operations."""
    from yaspin import yaspin

    from burla._deploy_aws import _create_security_groups, _ensure_node_ami

    marker = _state_dir(project_id) / f"aws_prepped_{__version__}"
    if marker.exists():
        return
    with yaspin() as spinner:
        _create_security_groups(spinner, aws_region)
        _ensure_node_ami(spinner, aws_region, node_profile=None)
    marker.write_text("done")


def _prepare_azure(project_id: str, region: str):
    """First-run Azure prep a laptop head needs before booting nodes:
    resource providers, the burla resource group + network, the burla-node
    identity (nodes must be able to delete themselves on Azure), and the node
    image. Creating the identity's role needs Owner on the subscription once;
    afterwards Contributor is enough to run clusters."""
    from yaspin import yaspin

    from burla._deploy_azure import (
        ensure_network,
        ensure_node_identity,
        ensure_node_image,
        ensure_resource_group,
        register_resource_providers,
    )

    marker = _state_dir(project_id) / f"azure_prepped_{__version__}"
    if marker.exists():
        return
    subscription_id = project_id.removeprefix("azure-")
    with yaspin() as spinner:
        spinner.text = "Preparing Azure subscription ... "
        spinner.start()
        register_resource_providers()
        ensure_resource_group(region)
        ensure_network(region)
        ensure_node_identity(subscription_id)
        spinner.text = "Preparing Azure subscription ... Done."
        spinner.ok("✓")
        ensure_node_image(spinner, region)
    marker.write_text("done")


def ensure_local_head() -> str:
    """Starts (or reuses) a detached main_service and returns its URL."""
    return _run_local_head(detached=True)


def running_head_url() -> str | None:
    """URL of the account-wide ad hoc client head, or None.

    Explicit worktree clusters are reached through BURLA_CLUSTER_DASHBOARD_URL
    instead. A stale `head.json` entry returns None.
    """
    try:
        _, project_id, _ = detect_cloud()
    except Exception:
        return None
    state_dir = _head_state_dir(project_id)
    head_state_path = state_dir / "head.json"
    if not head_state_path.exists():
        return None
    head_state = json.loads(head_state_path.read_text())
    url = head_state.get("url")
    saved_token = read_saved_cluster_token(project_id)
    if url and saved_token and _head_matches(
        url, project_id, saved_token, expected_namespace="default"
    ):
        if not _pid_alive(head_state.get("frpc_pid")):
            _respawn_frpc(state_dir, head_state)
        return url
    return None


@dataclass
class HeadHandle:
    url: str
    owned: bool  # True only for a head this process started (and must stop).


def acquire_head_for_job() -> HeadHandle:
    """Resolve the cluster for one job, matching `get_cluster_dashboard_url`'s
    precedence. Reuses an env-pointed, already-running, or deployed cluster
    (owned=False); only when nothing is available does it start a head here,
    which the caller must stop with `release_head`."""
    from burla import _existing_cluster_dashboard_url

    url = _existing_cluster_dashboard_url()
    if url:
        return HeadHandle(url=url, owned=False)
    return HeadHandle(url=ensure_local_head(), owned=True)


# How long to wait for the head to delete this cluster's nodes before killing
# it anyway. Deleting is just one terminate/delete API call per node, so this
# only needs to cover a slow control plane, and it bounds how long a finished
# `remote_parallel_map` can sit in teardown.
CLUSTER_SHUTDOWN_TIMEOUT_SEC = 30


def release_head(handle: HeadHandle):
    """Stop a head started by `acquire_head_for_job` (no-op if not owned).

    Deletes this cluster's nodes first, while the head still holds the cloud
    credentials to do it: otherwise the nodes outlive the job and only clean
    themselves up once their own timers fire. History persists on disk, so
    `burla dashboard` can still show the job afterwards.
    """
    if not handle.owned:
        return
    try:
        _, project_id, _ = detect_cloud()
    except Exception:
        return

    _shutdown_cluster_via_head(handle.url, project_id)

    head_state_path = _head_state_dir(project_id) / "head.json"
    if not head_state_path.exists():
        return
    head_state = json.loads(head_state_path.read_text())
    head_pid = head_state.get("head_pid")
    frpc_pid = head_state.get("frpc_pid")
    _terminate_pid(head_pid)
    _terminate_pid(frpc_pid)
    _clear_process_state(head_state_path, head_pid, frpc_pid)


def _shutdown_cluster_via_head(url: str, project_id: str):
    """Ask the head to delete every node it booted. Best-effort: the head is
    about to be killed either way, and nodes still self-delete on their own
    timers if this never lands."""
    cluster_token = read_saved_cluster_token(project_id)
    headers = {"Authorization": f"Bearer {cluster_token}"} if cluster_token else {}
    try:
        requests.post(
            f"{url}/v1/cluster/shutdown",
            headers=headers,
            timeout=CLUSTER_SHUTDOWN_TIMEOUT_SEC,
        )
    except Exception:
        pass


# ------------------------------------------------------------------ deploy migration


def prepare_history_migration(project_id: str) -> tuple[Path | None, str | None]:
    """Quiesce the account-wide ad hoc head (if one is running) and snapshot
    its history db for a first `burla deploy`.

    Returns (snapshot_path, paused_head_url); snapshot_path is None when this
    machine has no local history. Only the account-wide database migrates:
    namespaced worktree dev clusters are never touched. The snapshot uses
    sqlite's backup API so rows still sitting in the WAL are included.
    """
    cluster_token = read_saved_cluster_token(project_id)
    state_dir = _head_state_dir(project_id)
    head_state_path = state_dir / "head.json"
    head_url = None
    if head_state_path.exists() and cluster_token:
        url = json.loads(head_state_path.read_text()).get("url")
        if url and _head_matches(
            url, project_id, cluster_token, expected_namespace="default"
        ):
            head_url = url

    if head_url:
        response = requests.post(
            f"{head_url}/v1/cluster/pause_job_admission",
            headers={"Authorization": f"Bearer {cluster_token}"},
            timeout=30,
        )
        if response.status_code == 409:
            raise LocalHeadError(
                "A job is currently running on this machine's Burla cluster. "
                "Wait for it to finish, then re-run `burla deploy`."
            )
        response.raise_for_status()
        # Delete idle nodes now, while a head with cloud credentials is still
        # up, so their DELETED records land in the snapshot.
        _shutdown_cluster_via_head(head_url, project_id)

    db_path = state_dir / "history.db"
    if not db_path.exists():
        return None, head_url

    descriptor, snapshot_path = tempfile.mkstemp(suffix=".db")
    os.close(descriptor)
    source = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    snapshot = sqlite3.connect(snapshot_path)
    with snapshot:
        source.backup(snapshot)
    snapshot.close()
    source.close()
    return Path(snapshot_path), head_url


def resume_history_migration(project_id: str, head_url: str):
    """Deploy failed: let the paused local head admit jobs again."""
    cluster_token = read_saved_cluster_token(project_id)
    try:
        requests.post(
            f"{head_url}/v1/cluster/resume_job_admission",
            headers={"Authorization": f"Bearer {cluster_token}"},
            timeout=10,
        )
    except requests.RequestException:
        pass  # deploy is already failing; don't mask its error


def finish_history_migration(project_id: str):
    """The deployed cluster owns the history now: stop the local head so
    nothing writes to the migrated database afterwards."""
    head_state_path = _head_state_dir(project_id) / "head.json"
    if not head_state_path.exists():
        return
    head_state = json.loads(head_state_path.read_text())
    head_pid = head_state.get("head_pid")
    frpc_pid = head_state.get("frpc_pid")
    _terminate_pid(head_pid)
    _terminate_pid(frpc_pid)
    _clear_process_state(head_state_path, head_pid, frpc_pid)


def _reap(pid: int):
    """Clear the zombie left behind when we kill a head we spawned ourselves.
    Until it is reaped the pid still exists, so `_pid_alive` keeps saying yes."""
    try:
        os.waitpid(pid, os.WNOHANG)
    except (ChildProcessError, OSError):
        pass  # not our child (a head some earlier process started)


def _terminate_pid(pid: int | None):
    if not _pid_alive(pid):
        return
    os.kill(pid, signal.SIGTERM)
    for _ in range(50):  # up to ~5s for a graceful uvicorn shutdown
        _reap(pid)
        if not _pid_alive(pid):
            return
        sleep(0.1)
    os.kill(pid, signal.SIGKILL)
    for _ in range(20):
        _reap(pid)
        if not _pid_alive(pid):
            return
        sleep(0.1)


def run_local_head_for_dashboard(
    on_ready: Callable[[str, bool], None] | None = None,
) -> None:
    """Reuse a healthy head or run one here until it exits or Ctrl-C."""
    _run_local_head(detached=False, on_ready=on_ready)


def _run_local_head(
    detached: bool,
    on_ready: Callable[[str, bool], None] | None = None,
    node_source_ref: str | None = None,
    reload_dir: str | None = None,
    namespace: str = "",
) -> str:
    cloud, project_id, aws_region = detect_cloud()

    state_dir = _head_state_dir(project_id, namespace)
    head_namespace = namespace or "default"
    head_state_path = state_dir / "head.json"
    head_state = {}
    if head_state_path.exists():
        head_state = json.loads(head_state_path.read_text())

    url = head_state.get("url")
    saved_token = read_saved_cluster_token(project_id)
    if url and saved_token and _head_matches(
        url, project_id, saved_token, expected_namespace=head_namespace
    ):
        if not _pid_alive(head_state.get("frpc_pid")):
            _respawn_frpc(state_dir, head_state)
        if on_ready:
            on_ready(url, False)
        return url

    cluster_token = get_or_register_cluster_token(cloud, project_id, aws_region)
    ensure_user_authorized(cloud, project_id, cluster_token)
    if cloud == "aws":
        _prepare_aws(project_id, aws_region)
    if cloud == "azure":
        _prepare_azure(project_id, aws_region)

    from burla import CONFIG_PATH

    auth_info = json.loads(CONFIG_PATH.read_text())

    # Stale processes from an old version/session die before their ports are reused.
    for pid_key in ("head_pid", "frpc_pid"):
        if _pid_alive(head_state.get(pid_key)):
            os.kill(head_state[pid_key], 15)
        sleep(0.2)

    head_port = _free_port(preferred=PREFERRED_HEAD_PORT)
    tls_port = _free_port()
    # Stable per project so node certs / head certs stay valid across restarts.
    subdomain = head_state.get("subdomain") or f"head-{uuid4().hex[:8]}--{project_id}"
    url = f"http://127.0.0.1:{head_port}"

    environment = {
        **os.environ,
        "IN_CLIENT_HOSTED_MODE": "True",
        "BURLA_CLUSTER_NAME": head_namespace,
        "PROJECT_ID": project_id,
        "CLOUD_PROVIDER": cloud,
        "CLOUD_ACCOUNT_NAME": _cloud_account_name(cloud, project_id),
        "CLUSTER_ID_TOKEN": cluster_token,
        "BURLA_BACKEND_URL": _BURLA_BACKEND_URL,
        "BURLA_RELAY_HOST": RELAY_HOST,
        "BURLA_RELAY_SERVER_ADDR": RELAY_SERVER_ADDR,
        "BURLA_RELAY_SERVER_PORT": str(RELAY_SERVER_PORT),
        "BURLA_NODE_SOURCE_REF": node_source_ref or _BURLA_NODE_SOURCE_REF,
        "MAIN_SERVICE_URL_FOR_NODES": f"https://{subdomain}.{RELAY_HOST}",
        "PORT": str(head_port),
        "INTERNAL_TLS_PORT": str(tls_port),
        "HISTORY_DB_PATH": str(state_dir / "history.db"),
        "BURLA_TLS_DIR": str(state_dir / "tls"),
        "REDIRECT_LOCALLY_ON_LOGIN": "True",
        # The owner's real credentials: local dashboard/browser requests are
        # stamped with these instead of being asked to log in.
        "BURLA_LOCAL_USER_EMAIL": auth_info["email"],
        "BURLA_LOCAL_USER_TOKEN": auth_info["auth_token"],
    }
    if cloud == "aws" and aws_region:
        environment["AWS_REGION"] = aws_region
    if cloud == "azure":
        environment["AZURE_SUBSCRIPTION_ID"] = project_id.removeprefix("azure-")
        environment["AZURE_REGION"] = aws_region
        environment["AZURE_RESOURCE_GROUP"] = "burla"
    extra_pythonpath = _main_service_pythonpath()
    if extra_pythonpath:
        existing = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            f"{extra_pythonpath}{os.pathsep}{existing}" if existing else extra_pythonpath
        )

    head_process = _spawn_head(
        state_dir=state_dir,
        head_port=head_port,
        environment=environment,
        detached=detached,
        reload_dir=reload_dir,
    )

    # Written before the readiness wait so a failed boot never leaks the
    # process (the next attempt kills whatever pids are recorded here).
    head_state = {
        "url": url,
        "head_pid": head_process.pid,
        "subdomain": subdomain,
        "tls_port": tls_port,
        "project_id": project_id,
        "cloud": cloud,
    }
    head_state_path.write_text(json.dumps(head_state))

    if detached:
        _wait_for_head_ready(
            head_process,
            url,
            project_id,
            cluster_token,
            head_namespace,
            state_dir,
            detached=True,
        )
        _respawn_frpc(state_dir, head_state, cluster_token=cluster_token)
        return url

    frpc_process = None
    try:
        _wait_for_head_ready(
            head_process,
            url,
            project_id,
            cluster_token,
            head_namespace,
            state_dir,
            detached=False,
        )
        frpc_process = _respawn_frpc(
            state_dir, head_state, cluster_token=cluster_token
        )
        if on_ready:
            on_ready(url, True)
        return_code = head_process.wait()
        if return_code != 0:
            raise LocalHeadError(
                f"Burla's local service exited with status {return_code}."
            )
    except KeyboardInterrupt:
        return url
    finally:
        _stop_process(head_process)
        if frpc_process is not None:
            _stop_process(frpc_process)
        _clear_process_state(
            head_state_path,
            head_process.pid,
            frpc_process.pid if frpc_process is not None else None,
        )
    return url


def _current_branch(repo_root: Path) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo_root), "branch", "--show-current"],
        capture_output=True,
        text=True,
    )
    branch = result.stdout.strip()
    if not branch:
        raise LocalHeadError(
            "remote-dev needs a checked-out branch, but this repo is on a detached HEAD."
        )
    return branch


def _warn_if_branch_unpushed(repo_root: Path, branch: str):
    """Node VMs git-fetch this branch from GitHub, so anything not pushed (every
    uncommitted edit included) never reaches them."""
    on_origin = subprocess.run(
        ["git", "-C", str(repo_root), "rev-parse", "--verify", f"origin/{branch}"],
        capture_output=True,
        text=True,
    )
    if on_origin.returncode != 0:
        print(
            f"WARNING: branch `{branch}` is not on origin yet, so nodes cannot "
            f"fetch it. Push it before booting nodes.",
            flush=True,
        )
        return
    unpushed = subprocess.run(
        [
            *("git", "-C", str(repo_root)),
            *("rev-list", "--count", f"origin/{branch}..{branch}"),
        ],
        capture_output=True,
        text=True,
    )
    count = unpushed.stdout.strip()
    if count and count != "0":
        print(
            f"WARNING: {count} commit(s) on `{branch}` are not pushed. Nodes will "
            f"run the pushed version, not your working tree.",
            flush=True,
        )


def run_local_dev_head() -> None:
    """`make local-dev`: run this checkout's main_service on the docker host in
    local-dev mode, hot-reloading on save. Nodes and workers are still
    containers; the head reaches them on their published 127.0.0.1 ports and
    they reach the head at host.docker.internal. Foreground until Ctrl-C.

    Unlike remote-dev this does not use the relay/frpc/TLS stack: nodes are on
    this machine. All cluster settings come from the env the makefile sets
    (PROJECT_ID, CLUSTER_ID_TOKEN, LOCAL_DEV_*, PORT, HOST_PWD, ...).
    """
    from burla import _IN_SOURCE_CHECKOUT, _SOURCE_ROOT

    if not _IN_SOURCE_CHECKOUT:
        raise LocalHeadError("local-dev requires an editable Burla source checkout.")

    environment = dict(os.environ)
    extra_pythonpath = _main_service_pythonpath()
    if extra_pythonpath:
        existing = environment.get("PYTHONPATH")
        environment["PYTHONPATH"] = (
            f"{extra_pythonpath}{os.pathsep}{existing}" if existing else extra_pythonpath
        )

    head_port = os.environ.get("PORT", str(PREFERRED_HEAD_PORT))
    main_service_dir = _SOURCE_ROOT / "main_service"
    command = [
        sys.executable,
        "-m",
        "uvicorn",
        "main_service:app",
        # 0.0.0.0 (not 127.0.0.1) so node containers can reach the head via
        # host.docker.internal; local-dev is dev-only and single-machine.
        "--host",
        "0.0.0.0",
        "--port",
        str(head_port),
        "--reload",
        "--reload-dir",
        str(main_service_dir / "src"),
        # Relative to cwd (main_service); uvicorn rejects absolute exclude globs.
        "--reload-exclude",
        "frontend/node_modules/*",
        "--timeout-keep-alive",
        "600",
    ]
    # cwd matches the container's old WORKDIR so the head's cwd-relative reads
    # (e.g. .frontend_last_built_at.txt) resolve.
    result = subprocess.run(command, cwd=str(main_service_dir), env=environment)
    if result.returncode:
        raise SystemExit(result.returncode)


def run_remote_dev_head() -> None:
    """`make remote-dev`: run this checkout's main_service here, hot-reloading on
    save, while nodes boot as real cloud VMs running this checkout's branch."""
    from burla import _IN_SOURCE_CHECKOUT, _SOURCE_ROOT

    if not _IN_SOURCE_CHECKOUT:
        raise LocalHeadError("remote-dev requires an editable Burla source checkout.")

    # Nodes git-fetch their code from GitHub, so the ref has to exist there.
    # Defaults to this checkout's branch; override to pin nodes at an already
    # pushed ref (e.g. `dev`) while iterating on head-only changes.
    branch = os.environ.get("BURLA_NODE_SOURCE_REF") or _current_branch(_SOURCE_ROOT)
    _warn_if_branch_unpushed(_SOURCE_ROOT, branch)
    namespace = cluster_namespace()

    def announce(url: str, is_foreground: bool):
        lines = [
            f"\nBurla remote-dev cluster [{namespace or 'default'}]",
            f"  dashboard: {url}",
            f"  node code: branch `{branch}` on GitHub",
        ]
        if is_foreground:
            lines.append("  Press Ctrl-C to stop it.\n")
        print("\n".join(lines), flush=True)

    _run_local_head(
        detached=False,
        on_ready=announce,
        node_source_ref=branch,
        reload_dir=str(_SOURCE_ROOT / "main_service" / "src"),
        namespace=namespace,
    )


def _wait_for_head_ready(
    head_process: subprocess.Popen,
    url: str,
    project_id: str,
    cluster_token: str,
    expected_namespace: str,
    state_dir: Path,
    detached: bool,
):
    start = time()
    while time() - start < 90:
        if _head_matches(url, project_id, cluster_token, expected_namespace):
            return
        if head_process.poll() is not None:
            if detached:
                log_tail = (state_dir / "head.log").read_text()[-3000:]
                raise LocalHeadError(
                    f"Burla's local service failed to start:\n{log_tail}"
                )
            raise LocalHeadError("Burla's local service failed to start.")
        sleep(0.5)

    message = "Burla's local service never became ready"
    if detached:
        message += f" (see {state_dir / 'head.log'})."
    else:
        message += "."
    raise LocalHeadError(message)


def _spawn_head(
    state_dir: Path,
    head_port: int,
    environment: dict[str, str],
    detached: bool,
    reload_dir: str | None = None,
) -> subprocess.Popen:
    command = [
        sys.executable,
        "-m",
        "uvicorn",
        "main_service:app",
        "--host",
        "127.0.0.1",
        "--port",
        str(head_port),
        "--timeout-keep-alive",
        "600",
    ]
    if reload_dir:
        command += ["--reload", "--reload-dir", reload_dir]
    if not detached:
        return subprocess.Popen(command, env=environment)

    with open(state_dir / "head.log", "ab") as head_log:
        return subprocess.Popen(
            command,
            env=environment,
            stdout=head_log,
            stderr=head_log,
            start_new_session=True,
        )


def _stop_process(process: subprocess.Popen):
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _clear_process_state(
    head_state_path: Path, head_pid: int | None, frpc_pid: int | None
):
    head_state = json.loads(head_state_path.read_text())
    if head_pid is not None and head_state.get("head_pid") == head_pid:
        head_state.pop("head_pid")
    if frpc_pid is not None and head_state.get("frpc_pid") == frpc_pid:
        head_state.pop("frpc_pid")
    head_state_path.write_text(json.dumps(head_state))


def _respawn_frpc(
    state_dir: Path, head_state: dict, cluster_token: str = None
) -> subprocess.Popen:
    cluster_token = cluster_token or read_saved_cluster_token(head_state["project_id"])
    frpc_binary = ensure_frpc_binary()
    config_path = _write_frpc_config(
        state_dir,
        head_state["project_id"],
        cluster_token,
        head_state["subdomain"],
        head_state["tls_port"],
    )
    frpc_log = open(state_dir / "frpc.log", "ab")
    frpc_process = subprocess.Popen(
        [str(frpc_binary), "-c", str(config_path)],
        stdout=frpc_log,
        stderr=frpc_log,
        start_new_session=True,
    )
    head_state["frpc_pid"] = frpc_process.pid
    (state_dir / "head.json").write_text(json.dumps(head_state))
    return frpc_process
