import os
import json
import webbrowser
import requests
from time import sleep
from uuid import uuid4
from pathlib import Path
from typing import Tuple, Optional

import google.auth
from google.oauth2 import service_account
from appdirs import user_config_dir
from IPython.core.display import Javascript
from IPython.display import display, clear_output

from burla import _BURLA_BACKEND_URL, IN_DEV

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


def get_gcs_credentials(burla_auth_headers: dict):
    if IN_DEV:
        credentials, _ = google.auth.default()
    else:
        url = f"{_BURLA_BACKEND_URL}/v1/private/svc_account"
        response = requests.get(url, headers=burla_auth_headers)
        response.raise_for_status()
        service_account_info = json.loads(response.json())
        credentials = service_account.Credentials.from_service_account_info(service_account_info)
        return credentials


def get_auth_headers(api_key: Optional[str] = None) -> Tuple[str, str]:
    login_credentials_missing = not CONFIG_PATH.exists()

    if api_key:
        return {"Authorization": f"Bearer {api_key}"}
    elif login_credentials_missing:
        raise AuthException()
    else:
        auth_info = json.loads(CONFIG_PATH.read_text())
        return {"email": auth_info["email"], "Authorization": f"Bearer {auth_info['auth_token']}"}


def _get_auth_creds(client_id, attempt=0):
    if attempt == AUTH_TIMEOUT_SECONDS / 2:
        raise AuthTimeoutException()

    sleep(2)
    response = requests.get(f"{_BURLA_BACKEND_URL}/v1/login/{client_id}/token")

    if response.status_code == 404:
        return _get_auth_creds(client_id, attempt=attempt + 1)
    else:
        response.raise_for_status()
        return response.json()["token"], response.json()["email"]


def login_cmd():
    if IN_COLAB:
        raise SystemExit(
            (
                "\nUnable to login using this command from inside a Google Colab notebook!\n"
                "To login simply call `burla.remote_parallel_map`, or `burla.login`, eg:\n"
                "```\n"
                "from burla import login\n"
                "login()\n"
                "```"
            )
        )
    else:
        login()


def login():
    client_id = uuid4().hex
    login_url = f"{_BURLA_BACKEND_URL}/v1/login/{client_id}"

    print(f"Your browser has been opened to visit:\n\n    {login_url}\n")

    if IN_COLAB:
        display(Javascript(f'window.open("{login_url}");'))
        sleep(1)  # give js a second to run before removing it
        clear_output()  # prevents js from re-running automatically when notebook opened
    else:
        webbrowser.open(login_url)
    auth_token, email = _get_auth_creds(client_id)

    message = f"Thank you for registering with Burla! You are now logged in as [{email}].\n"
    message += "Please email jake@burla.dev with any questions!\n"
    print(message)

    if not CONFIG_PATH.exists():
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.touch()
    CONFIG_PATH.write_text(json.dumps({"auth_token": auth_token, "email": email}))
