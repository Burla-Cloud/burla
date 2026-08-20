import base64
import configparser
import json
import os
import shutil
import subprocess
import webbrowser
from functools import cache
from pathlib import Path
from time import sleep
from uuid import uuid4

import requests
from platformdirs import user_data_dir
from yaspin import yaspin

from burla import _BURLA_APP_NAME, _BURLA_BACKEND_URL, CONFIG_PATH
from burla._helpers import run_command

AUTH_TIMEOUT_SECONDS = 180
IN_COLAB = os.getenv("COLAB_RELEASE_TAG") is not None
CLUSTER_TOKEN_SECRET = os.environ.get(
    "BURLA_CLUSTER_TOKEN_SECRET", "burla-cluster-id-token"
)
STATE_ROOT = (
    Path(user_data_dir(appname=_BURLA_APP_NAME, appauthor="burla")) / "clusters"
)


class LocalHeadError(Exception):
    pass


class AuthTimeoutException(Exception):
    def __init__(self):
        super().__init__("Timed out waiting for authentication flow to complete.")


class AuthException(Exception):
    def __init__(self):
        super().__init__(
            "You are not logged in! Please run `burla login` to create an account or login."
        )


class ADCProjectException(Exception):
    def __init__(self):
        super().__init__(
            "Burla found Google Application Default Credentials, but could not determine "
            "the active GCP project.\n\n"
            "Set GOOGLE_CLOUD_PROJECT to the project that has Burla installed, or run `burla login`."
        )


class BurlaNotInstalledException(Exception):
    def __init__(self, project_id: str):
        super().__init__(
            f"No Burla cluster was found for the active GCP project [{project_id}].\n\n"
            "To use Burla, do one of these:\n"
            "- Just call `remote_parallel_map` (or run `burla dashboard`) - Burla runs "
            f"from this machine with no deployment needed.\n"
            "- Run `burla login` to authorize this machine against a deployed Burla "
            "cluster you have access to.\n"
            f"- Run `burla deploy` while [{project_id}] is selected to deploy a shared cluster."
        )


class ADCBootstrapException(Exception):
    pass


class ADCSecretPermissionException(Exception):
    def __init__(self, project_id: str):
        super().__init__(
            f"Burla found Google Application Default Credentials for [{project_id}], "
            "but they cannot read the Burla cluster token secret.\n\n"
            f"Grant this identity access to Secret Manager secret `{CLUSTER_TOKEN_SECRET}`, "
            "or run `burla login`."
        )


def _state_dir(project_id: str) -> Path:
    directory = STATE_ROOT / project_id
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def detect_cloud() -> tuple[str, str, str | None]:
    """Returns (cloud, project_id, region) for the configured cloud.
    region is None on GCP (nodes' region comes from cluster config there)."""
    from burla import get_cloud

    cloud = get_cloud()
    if cloud == "azure":
        executable = shutil.which("az")
        if not executable:
            raise LocalHeadError(
                "Azure is selected, but the az CLI is not installed. "
                "Install it or run `burla config set cloud aws`."
            )
        result = subprocess.run(
            [executable, "account", "show", "--query", "id", "--output", "tsv"],
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
        executable = shutil.which("gcloud")
        if not executable:
            raise LocalHeadError(
                "GCP is selected, but gcloud is not installed. "
                "Install it or run `burla config set cloud aws`."
            )
        result = subprocess.run(
            [executable, "config", "get-value", "project"],
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

    executable = shutil.which("aws")
    if not executable:
        raise LocalHeadError(
            "AWS is selected, but the AWS CLI is not installed. "
            "Install it or run `burla config set cloud gcp`."
        )
    result = subprocess.run(
        [
            executable,
            "sts",
            "get-caller-identity",
            "--query",
            "Account",
            "--output",
            "text",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise LocalHeadError(
            "AWS is selected, but its credentials are not active. "
            "Run `aws configure` or `aws sso login`, then retry."
        )
    region_result = subprocess.run(
        [executable, "configure", "get", "region"], capture_output=True, text=True
    )
    region = (
        os.environ.get("AWS_REGION")
        or os.environ.get("AWS_DEFAULT_REGION")
        or region_result.stdout.strip()
        or "us-east-1"
    )
    return "aws", f"aws-{result.stdout.strip()}", region


def cloud_account_name(cloud: str, project_id: str) -> str:
    """Human-readable account label for the dashboard's settings page."""
    if cloud == "aws":
        return aws_account_name(project_id.removeprefix("aws-"))
    if cloud == "azure":
        result = subprocess.run(
            [
                shutil.which("az"),
                "account",
                "show",
                "--query",
                "name",
                "--output",
                "tsv",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    return project_id


def aws_account_name(account_id: str) -> str:
    result = subprocess.run(
        [
            shutil.which("aws"),
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
            [shutil.which("gcloud"), "auth", "print-access-token"],
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


def read_saved_cluster_token(project_id: str) -> str | None:
    token_path = _state_dir(project_id) / "cluster_token"
    if token_path.exists():
        return token_path.read_text().strip()
    return None


def save_cluster_token(project_id: str, token: str):
    token_path = _state_dir(project_id) / "cluster_token"
    token_path.write_text(token)
    token_path.chmod(0o600)


def get_or_register_cluster_token(
    cloud: str, project_id: str, region: str | None
) -> str:
    token = read_saved_cluster_token(project_id)
    if token:
        return token

    response = requests.post(
        f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}",
        json=_ownership_payload(cloud, region),
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
            secret = os.environ.get(
                "BURLA_CLUSTER_TOKEN_SECRET", "burla-cluster-id-token"
            )
            command = [
                shutil.which("gcloud"),
                "secrets",
                "versions",
                "access",
                "latest",
                f"--secret={secret}",
            ]
        elif cloud == "aws":
            parameter = os.environ.get(
                "BURLA_CLUSTER_TOKEN_PARAMETER", "/burla/cluster-id-token"
            )
            command = [
                *(shutil.which("aws"), "ssm", "get-parameter", "--region", region),
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
    if CONFIG_PATH.exists():
        auth_info = json.loads(CONFIG_PATH.read_text())
        if auth_info["project_id"] == project_id:
            return

    if cloud == "aws":
        result = subprocess.run(
            [
                shutil.which("aws"),
                "sts",
                "get-caller-identity",
                "--query",
                "Arn",
                "--output",
                "text",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        identity = result.stdout.strip().rsplit("/", 1)[-1]
    elif cloud == "azure":
        result = subprocess.run(
            [
                shutil.which("az"),
                "account",
                "show",
                "--query",
                "user.name",
                "--output",
                "tsv",
            ],
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
    _write_auth_config(response.json())


def deployed_dashboard_url() -> str | None:
    config = json.loads(CONFIG_PATH.read_text()) if CONFIG_PATH.exists() else {}
    configured_url = (config.get("cluster_dashboard_url") or "").rstrip("/")
    if configured_url:
        return configured_url

    try:
        cloud, project_id, region = detect_cloud()
    except LocalHeadError:
        return None

    cluster_token = get_or_register_cluster_token(cloud, project_id, region)
    response = requests.get(
        f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}/dashboard_url",
        headers={"Authorization": f"Bearer {cluster_token}"},
        timeout=20,
    )
    if response.status_code == 409:
        return None
    response.raise_for_status()

    dashboard_url = response.json()["dashboard_url"].rstrip("/")
    if not dashboard_url.startswith("https://"):
        raise ValueError("Backend returned a non-HTTPS dashboard URL")

    ensure_user_authorized(cloud, project_id, cluster_token)
    config = json.loads(CONFIG_PATH.read_text())
    config["cluster_dashboard_url"] = dashboard_url
    config.pop("mode", None)
    _write_auth_config(config)
    return dashboard_url


def _write_auth_config(auth_info: dict):
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(auth_info))
    _get_auth_info.cache_clear()


def _get_adc_identity() -> tuple[str, str, str]:
    import google.auth
    from google.auth.transport.requests import Request

    credentials, project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    project_id = (
        project_id or os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCLOUD_PROJECT")
    )
    if not project_id:
        raise ADCProjectException()
    credentials.refresh(Request())
    access_token = credentials.token
    email = getattr(credentials, "service_account_email", None)
    if email:
        return access_token, project_id, email

    response = requests.get(
        "https://oauth2.googleapis.com/tokeninfo",
        params={"access_token": access_token},
        timeout=10,
    )
    response.raise_for_status()
    email = response.json().get("email")
    if not email:
        raise ADCBootstrapException(
            "Burla could not determine the email for these Google Application Default Credentials. "
            "Run `burla login` instead."
        )
    return access_token, project_id, email


def _get_cluster_token(access_token: str, project_id: str) -> str:
    # `burla deploy` and client-hosted mode save the token locally; only
    # clusters installed before 1.7 still keep it in Secret Manager.
    saved_token = read_saved_cluster_token(project_id)
    if saved_token:
        return saved_token

    response = requests.get(
        "https://secretmanager.googleapis.com/v1/"
        f"projects/{project_id}/secrets/{CLUSTER_TOKEN_SECRET}/versions/latest:access",
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=20,
    )
    if response.status_code == 404:
        raise BurlaNotInstalledException(project_id)
    if response.status_code == 403:
        raise ADCSecretPermissionException(project_id)
    response.raise_for_status()
    encoded_token = response.json()["payload"]["data"]
    return base64.b64decode(encoded_token).decode("utf-8")


def bootstrap_from_adc() -> dict:
    access_token, project_id, email = _get_adc_identity()
    cluster_token = _get_cluster_token(access_token, project_id)
    response = requests.post(
        f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}/adc:exchange",
        headers={"Authorization": f"Bearer {cluster_token}"},
        json={"email": email},
        timeout=20,
    )
    if response.status_code == 404:
        raise BurlaNotInstalledException(project_id)
    if response.status_code >= 400:
        try:
            detail = response.json().get("detail")
        except Exception:
            detail = response.text
        raise ADCBootstrapException(detail)
    response.raise_for_status()
    auth_info = response.json()
    _write_auth_config(auth_info)
    return auth_info


@cache
def _get_auth_info() -> tuple[str, str]:
    if not CONFIG_PATH.exists():
        bootstrap_from_adc()
    auth_info = json.loads(CONFIG_PATH.read_text())
    return auth_info["email"], auth_info["auth_token"]


def get_auth_headers() -> dict[str, str]:
    try:
        email, auth_token = _get_auth_info()
    except Exception:
        # No user identity available (e.g. never logged in and the ADC
        # bootstrap failed). Both the head and the nodes accept the cluster
        # token, so fall back to it when a local copy exists; otherwise
        # re-raise the original, more actionable bootstrap error.
        cluster_token = _saved_cluster_token()
        if not cluster_token:
            raise
        return {"Authorization": f"Bearer {cluster_token}"}
    return {
        "X-User-Email": email,
        "Authorization": f"Bearer {auth_token}",
    }


def _saved_cluster_token() -> str | None:
    try:
        _, project_id, _ = detect_cloud()
        return read_saved_cluster_token(project_id)
    except Exception:
        return None


def save_deployed_cluster_config(
    cloud: str, project_id: str, cluster_token: str, dashboard_url: str
):
    """After `burla deploy`, point this machine at the deployed cluster so
    `remote_parallel_map` uses it without a separate `burla login`. Best-effort:
    deploy has already succeeded, and the deploy message still tells the user
    they can `burla login`."""
    try:
        ensure_user_authorized(cloud, project_id, cluster_token)
        config = json.loads(CONFIG_PATH.read_text())
        config["cluster_dashboard_url"] = dashboard_url.rstrip("/")
        _write_auth_config(config)
    except Exception:
        pass


def _get_login_response(client_id, spinner, attempt=0):
    if attempt == AUTH_TIMEOUT_SECONDS / 2:
        raise AuthTimeoutException()

    response = requests.get(f"{_BURLA_BACKEND_URL}/v2/login/client/{client_id}/token")

    if response.status_code == 404:
        sleep(2)
        return _get_login_response(client_id, spinner, attempt=attempt + 1)
    elif response.status_code == 202:
        sleep(2)
        if spinner.text != "Waiting for dashboard login ...":
            spinner.text = "Waiting for Google login response ... Response recieved."
            spinner.ok("✓")
            spinner.start()
            spinner.text = "Waiting for dashboard login ..."
        return _get_login_response(client_id, spinner, attempt=attempt + 1)
    elif response.status_code == 408:
        spinner.text = "Waiting for dashboard login ... Timed out after 3 minutes."
        spinner.fail("✗")
        response.raise_for_status()
    elif response.status_code != 200:
        spinner.fail("✗")
        response.raise_for_status()
    else:
        spinner.text = "Waiting for dashboard login ... Done."
        spinner.ok("✓")
        return (
            response.json()["token"],
            response.json()["email"],
            response.json()["project_id"],
            response.json()["cluster_dashboard_url"],
        )


def login(no_browser: bool = False):
    # for dev: if main service is running locally, redirect to it instead of the backend
    cmd = "docker container list --filter name=main_service-"
    result = run_command(cmd, raise_error=False)
    redirect_locally = (result.returncode == 0) and (
        len(result.stdout.strip().splitlines()) > 1
    )

    client_id = uuid4().hex
    login_url = f"{_BURLA_BACKEND_URL}/v2/login/client/{client_id}"
    login_url += f"?redirect_locally={redirect_locally}"
    if IN_COLAB or no_browser:
        print(f"Please navigate to the following URL to login:\n\n    {login_url}\n")
        if IN_COLAB:
            print(
                f"(We are unable to automatically open this from a Google Colab notebook)"
            )
    else:
        print(f"Your browser has been opened to visit:\n\n    {login_url}\n")
        webbrowser.open(login_url)

    with yaspin() as spinner:
        spinner.text = "Waiting for Google login response ..."
        auth_token, email, project_id, cluster_dashboard_url = _get_login_response(
            client_id, spinner
        )

    print(f"\nYou are now logged in to [{project_id}] as [{email}].")
    print("Please email jake@burla.dev with any questions!\n")
    config = {
        "auth_token": auth_token,
        "email": email,
        "project_id": project_id,
        "cluster_dashboard_url": cluster_dashboard_url,
    }
    _write_auth_config(config)
