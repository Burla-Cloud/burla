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
            f"Burla is not installed in the active GCP project [{project_id}].\n\n"
            "To use Burla, do one of these:\n"
            f"- Run `burla install` while [{project_id}] is selected.\n"
            "- Switch your Google Cloud project to one where Burla is already installed.\n"
            "- Run `burla login` to authorize this machine against the Burla deployment you most "
            "recently logged into in your browser."
        )


class ADCBootstrapException(Exception):
    pass


class ADCSecretPermissionException(Exception):
    def __init__(self, project_id: str):
        super().__init__(
            f"Burla found Google Application Default Credentials for [{project_id}], "
            "but they cannot read the Burla cluster token secret.\n\n"
            "Grant this identity access to Secret Manager secret `burla-cluster-id-token`, "
            "or run `burla login`."
        )


def _write_auth_config(auth_info: dict):
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(json.dumps(auth_info))
    _get_auth_info.cache_clear()


def _get_adc_credentials():
    import google.auth
    from google.auth.transport.requests import Request

    credentials, project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    project_id = project_id or os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCLOUD_PROJECT")
    if not project_id:
        raise ADCProjectException()
    credentials.refresh(Request())
    return credentials, credentials.token, project_id


def _get_adc_email(credentials, access_token: str) -> str:
    service_account_email = getattr(credentials, "service_account_email", None)
    if service_account_email:
        return service_account_email

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
    return email


def _get_cluster_token(access_token: str, project_id: str) -> str:
    response = requests.get(
        "https://secretmanager.googleapis.com/v1/"
        f"projects/{project_id}/secrets/burla-cluster-id-token/versions/latest:access",
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
    credentials, access_token, project_id = _get_adc_credentials()
    cluster_token = _get_cluster_token(access_token, project_id)
    email = _get_adc_email(credentials, access_token)
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
    email, auth_token = _get_auth_info()
    return {
        "X-User-Email": email,
        "Authorization": f"Bearer {auth_token}",
    }


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
    # for dev: if main service is running locally, redirect to it instead of deployed cloud run
    main_svc_image_name = "us-docker.pkg.dev/burla-test/burla-main-service/burla-main-service"
    cmd = f"docker container list --filter ancestor={main_svc_image_name}"
    result = run_command(cmd, raise_error=False)
    redirect_locally = (result.returncode == 0) and (len(result.stdout.strip().splitlines()) > 1)

    client_id = uuid4().hex
    login_url = f"{_BURLA_BACKEND_URL}/v2/login/client/{client_id}"
    login_url += f"?redirect_locally={redirect_locally}"
    if IN_COLAB or no_browser:
        print(f"Please navigate to the following URL to login:\n\n    {login_url}\n")
        if IN_COLAB:
            print(f"(We are unable to automatically open this from a Google Colab notebook)")
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
