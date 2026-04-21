import os
import json
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


@cache
def _get_auth_info() -> tuple[str, str]:
    if not CONFIG_PATH.exists():
        raise AuthException()
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
    if not CONFIG_PATH.exists():
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.touch()
    config = {
        "auth_token": auth_token,
        "email": email,
        "project_id": project_id,
        "cluster_dashboard_url": cluster_dashboard_url,
    }
    CONFIG_PATH.write_text(json.dumps(config))
    _get_auth_info.cache_clear()
