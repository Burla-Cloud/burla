"""
Client-hosted mode: runs main_service on this machine instead of a head VM.

This is the default way Burla runs. `remote_parallel_map` starts main_service
as a detached subprocess using the code vendored inside this package. The
explicit `burla dashboard` command runs that same service in the foreground.
Both modes also start an frpc tunnel so node VMs can reach the head through the
relay. Node VMs are booted with the user's own cloud credentials and carry none
of their own, so the only permissions needed are "can boot VMs". `burla deploy`
remains the upgrade path to an always-on, shared head VM.
"""

import configparser
import json
import os
import platform
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
import zipfile
from collections.abc import Callable
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


# ------------------------------------------------------------------ cloud


def detect_cloud() -> tuple[str, str, str | None]:
    """Returns (cloud, project_id, aws_region) for the configured cloud."""
    from burla import get_cloud

    cloud = get_cloud()
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


def _ownership_payload(cloud: str, aws_region: str | None) -> dict:
    if cloud == "aws":
        from burla._deploy_aws import _aws_ownership_payload

        return _aws_ownership_payload(aws_region)
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
    # upgrades stay seamless.
    if response.status_code in (403, 409):
        if cloud == "gcp":
            secret = os.environ.get("BURLA_CLUSTER_TOKEN_SECRET", "burla-cluster-id-token")
            command = ["gcloud", "secrets", "versions", "access", "latest", f"--secret={secret}"]
        else:
            parameter = os.environ.get("BURLA_CLUSTER_TOKEN_PARAMETER", "/burla/cluster-id-token")
            command = [
                *("aws", "ssm", "get-parameter", "--region", aws_region),
                *("--name", parameter, "--with-decryption"),
                *("--query", "Parameter.Value", "--output", "text"),
            ]
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
            if auth_info.get("mode") != "client_hosted":
                auth_info["mode"] = "client_hosted"
                _write_auth_config(auth_info)
            return

    if cloud == "aws":
        result = subprocess.run(
            ["aws", "sts", "get-caller-identity", "--query", "Arn", "--output", "text"],
            capture_output=True,
            text=True,
            check=True,
        )
        identity = result.stdout.strip().rsplit("/", 1)[-1]
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
    # Marks this machine as running its own cluster head, so
    # `get_cluster_dashboard_url` keeps resolving to it.
    auth_info["mode"] = "client_hosted"
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


def _head_matches(url: str, project_id: str, cluster_token: str) -> bool:
    # /version sits behind the auth middleware like everything else;
    # unauthenticated requests get the login page instead of JSON.
    headers = {"Authorization": f"Bearer {cluster_token}"}
    try:
        response = requests.get(f"{url}/version", headers=headers, timeout=2)
        info = response.json()
        return info["version"] == __version__ and info["project"] == project_id
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


def ensure_local_head() -> str:
    """Starts (or reuses) a detached main_service and returns its URL."""
    return _run_local_head(detached=True)


def run_local_head_foreground(
    on_ready: Callable[[str], None] | None = None,
) -> None:
    """Run main_service attached to this terminal until it exits or Ctrl-C."""
    _run_local_head(detached=False, on_ready=on_ready)


def _run_local_head(
    detached: bool,
    on_ready: Callable[[str], None] | None = None,
) -> str:
    cloud, project_id, aws_region = detect_cloud()

    state_dir = _state_dir(project_id)
    head_state_path = state_dir / "head.json"
    head_state = {}
    if head_state_path.exists():
        head_state = json.loads(head_state_path.read_text())

    url = head_state.get("url")
    saved_token = read_saved_cluster_token(project_id)
    if (
        detached
        and url
        and saved_token
        and _head_matches(url, project_id, saved_token)
    ):
        if not _pid_alive(head_state.get("frpc_pid")):
            _respawn_frpc(state_dir, head_state)
        return url

    cluster_token = get_or_register_cluster_token(cloud, project_id, aws_region)
    ensure_user_authorized(cloud, project_id, cluster_token)
    if cloud == "aws":
        _prepare_aws(project_id, aws_region)

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
        "PROJECT_ID": project_id,
        "CLOUD_PROVIDER": cloud,
        "CLOUD_ACCOUNT_NAME": (
            _aws_account_name(project_id.removeprefix("aws-"))
            if cloud == "aws"
            else project_id
        ),
        "CLUSTER_ID_TOKEN": cluster_token,
        "BURLA_BACKEND_URL": _BURLA_BACKEND_URL,
        "BURLA_RELAY_HOST": RELAY_HOST,
        "BURLA_RELAY_SERVER_ADDR": RELAY_SERVER_ADDR,
        "BURLA_RELAY_SERVER_PORT": str(RELAY_SERVER_PORT),
        "BURLA_NODE_SOURCE_REF": _BURLA_NODE_SOURCE_REF,
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
    if aws_region:
        environment["AWS_REGION"] = aws_region
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
            head_process, url, project_id, cluster_token, state_dir, detached=True
        )
        _respawn_frpc(state_dir, head_state, cluster_token=cluster_token)
        return url

    frpc_process = None
    try:
        _wait_for_head_ready(
            head_process, url, project_id, cluster_token, state_dir, detached=False
        )
        frpc_process = _respawn_frpc(
            state_dir, head_state, cluster_token=cluster_token
        )
        if on_ready:
            on_ready(url)
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


def _wait_for_head_ready(
    head_process: subprocess.Popen,
    url: str,
    project_id: str,
    cluster_token: str,
    state_dir: Path,
    detached: bool,
):
    start = time()
    while time() - start < 90:
        if _head_matches(url, project_id, cluster_token):
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
    head_state_path: Path, head_pid: int, frpc_pid: int | None
):
    if not head_state_path.exists():
        return
    head_state = json.loads(head_state_path.read_text())
    if head_state.get("head_pid") == head_pid:
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
