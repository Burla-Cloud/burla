import os
import json
import base64
import webbrowser
import requests
from functools import cache
from time import sleep
from uuid import uuid4

from yaspin import yaspin

from burla import _BURLA_BACKEND_URL, CONFIG_PATH
from burla._helpers import run_command

AUTH_TIMEOUT_SECONDS = 180
IN_COLAB = os.getenv("COLAB_RELEASE_TAG") is not None
CLUSTER_TOKEN_SECRET = os.environ.get(
    "BURLA_CLUSTER_TOKEN_SECRET", "burla-cluster-id-token"
)


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
    from burla._local_head import read_saved_cluster_token

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
        from burla._local_head import detect_cloud, read_saved_cluster_token

        _, project_id, _ = detect_cloud()
        return read_saved_cluster_token(project_id)
    except Exception:
        return None


def save_deployed_cluster_config(
    cloud: str, project_id: str, cluster_token: str, dashboard_url: str
):
    """After `burla deploy`, point this machine at the deployed cluster so
    `remote_parallel_map` uses it without a separate `burla login`. Mints
    client credentials from the local cloud identity, then pins mode=deployed.
    Best-effort: deploy has already succeeded, and the deploy message still
    tells the user they can `burla login`."""
    from burla._local_head import ensure_user_authorized

    try:
        ensure_user_authorized(cloud, project_id, cluster_token)
        config = json.loads(CONFIG_PATH.read_text())
        config["cluster_dashboard_url"] = dashboard_url.rstrip("/")
        config["mode"] = "deployed"
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
    # `mode="deployed"` pins remote_parallel_map to this deployed cluster.
    # A head you start locally (burla dashboard / local-dev / remote-dev)
    # still wins while it is running; stopping it falls back here.
    config = {
        "auth_token": auth_token,
        "email": email,
        "project_id": project_id,
        "cluster_dashboard_url": cluster_dashboard_url,
        "mode": "deployed",
    }
    _write_auth_config(config)
