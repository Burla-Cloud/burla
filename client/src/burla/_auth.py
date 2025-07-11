import os
import json
import webbrowser
import requests
from time import sleep
from uuid import uuid4
from pathlib import Path
from typing import Tuple

import google.auth
from appdirs import user_config_dir

from burla import _BURLA_BACKEND_URL
from burla._install import main_service_url

AUTH_TIMEOUT_SECONDS = 180
BURLA_APPDATA_DIR = Path(user_config_dir(appname="burla", appauthor="burla"))
CONFIG_PATH = BURLA_APPDATA_DIR / Path("burla_credentials.json")
IN_COLAB = os.getenv("COLAB_RELEASE_TAG") is not None


class AuthTimeoutException(Exception):
    def __init__(self):
        super().__init__("Timed out waiting for authentication flow to complete.")


class AuthException(Exception):
    def __init__(self):
        super().__init__(
            "You are not logged in! Please run `burla login` to create an account or login."
        )


def get_auth_headers() -> Tuple[str, str]:
    if not CONFIG_PATH.exists():
        raise AuthException()
    else:
        auth_info = json.loads(CONFIG_PATH.read_text())
        return {
            "X-User-Email": auth_info["email"],
            "Authorization": f"Bearer {auth_info['auth_token']}",
        }


def _get_login_response(client_id, attempt=0):
    if attempt == AUTH_TIMEOUT_SECONDS / 2:
        raise AuthTimeoutException()

    sleep(2)
    response = requests.get(f"{_BURLA_BACKEND_URL}/v1/login/{client_id}/token")

    if response.status_code == 404:
        return _get_login_response(client_id, attempt=attempt + 1)
    else:
        response.raise_for_status()
        return response.json()["token"], response.json()["email"]


def login():
    client_id = uuid4().hex
    dashboard_url = f"{main_service_url()}/auth-success"
    login_url = f"{_BURLA_BACKEND_URL}/v1/login/{client_id}?redirect_url={dashboard_url}"
    _, PROJECT_ID = google.auth.default()

    if IN_COLAB:
        print(f"Please navigate to the following URL to login:\n\n    {login_url}\n")
        print(f"(We are unable to automatically open this from a Google Colab notebook)")
    else:
        print(f"Your browser has been opened to visit:\n\n    {login_url}\n")
        webbrowser.open(login_url)

    auth_token, email = _get_login_response(client_id)
    validate_url = f"{_BURLA_BACKEND_URL}/v1/projects/{PROJECT_ID}/users:validate"
    headers = {"Authorization": f"Bearer {auth_token}", "X-User-Email": email}
    response = requests.get(validate_url, headers=headers)
    if response.status_code == 200:
        print(f"You are now logged in as [{email}].")
        print("Please email jake@burla.dev with any questions!\n")
        if not CONFIG_PATH.exists():
            CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            CONFIG_PATH.touch()
        CONFIG_PATH.write_text(json.dumps({"auth_token": auth_token, "email": email}))
    elif response.status_code == 401:
        print("Access denied.")
        print(f"[{email}] is not authorized to access the deployment in project: [{PROJECT_ID}]")
        print(f"Contact your admin to request access, then login again.\n")
    else:
        response.raise_for_status()
