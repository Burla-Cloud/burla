import json
import os
from dataclasses import dataclass
from pathlib import Path
from time import sleep
from urllib.parse import urlparse

import requests
from platformdirs import user_data_dir

from burla import CONFIG_PATH, _BURLA_APP_NAME


ERROR_EXIT_CODES = {
    "INVALID_ARGUMENT": 2,
    "INVALID_CURSOR": 2,
    "CONTEXT_REQUIRED": 3,
    "HEAD_UNREACHABLE": 3,
    "HEAD_UPGRADE_REQUIRED": 3,
    "AUTH_REQUIRED": 3,
    "FORBIDDEN": 3,
    "NOT_FOUND": 4,
    "CLUSTER_BUSY": 5,
    "TIMEOUT": 6,
    "TRANSPORT_ERROR": 6,
    "PROVIDER_ERROR": 6,
    "JOB_FAILED": 7,
    "JOB_CANCELED": 8,
}


class ManagementError(Exception):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        retryable: bool = False,
        details: dict | None = None,
        remediation: list | None = None,
        request_id: str | None = None,
    ):
        super().__init__(message)
        self.code = code
        self.message = message
        self.retryable = retryable
        self.details = details or {}
        self.remediation = remediation or []
        self.request_id = request_id
        self.exit_code = ERROR_EXIT_CODES.get(code, 6)

    def as_dict(self) -> dict:
        return {
            "code": self.code,
            "message": self.message,
            "retryable": self.retryable,
            "details": self.details,
            "remediation": self.remediation,
        }


@dataclass(frozen=True)
class ManagementContext:
    head_url: str | None
    head_source: str | None
    project_id: str | None
    auth_source: str | None
    principal: str | None
    headers: dict[str, str]


def _is_local_url(url: str) -> bool:
    hostname = urlparse(url).hostname or ""
    return hostname in {"localhost", "127.0.0.1", "main_service"} or hostname.startswith(
        "node_"
    )


def _normalize_head_url(url: str) -> str:
    url = url.rstrip("/")
    parsed = urlparse(url)
    if not parsed.scheme or not parsed.hostname:
        raise ManagementError("INVALID_ARGUMENT", f"Invalid head URL: {url!r}")
    if parsed.scheme != "https" and not _is_local_url(url):
        raise ManagementError(
            "INVALID_ARGUMENT",
            "Head URLs must use HTTPS unless they point to a local Burla service.",
        )
    return url


def _saved_credentials() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    return json.loads(CONFIG_PATH.read_text())


def _saved_head_url(credentials: dict) -> str | None:
    configured = credentials.get("cluster_dashboard_url")
    if configured:
        return _normalize_head_url(configured)

    project_id = credentials.get("project_id")
    if not project_id:
        return None
    state_root = (
        Path(user_data_dir(appname=_BURLA_APP_NAME, appauthor="burla")) / "clusters"
    )
    state_path = state_root / project_id / "head.json"
    if not state_path.exists():
        return None
    state = json.loads(state_path.read_text())
    url = state.get("url")
    return _normalize_head_url(url) if url else None


def _saved_cluster_token(project_id: str | None) -> str | None:
    if not project_id:
        return None
    state_root = (
        Path(user_data_dir(appname=_BURLA_APP_NAME, appauthor="burla")) / "clusters"
    )
    token_path = state_root / project_id / "cluster_token"
    return token_path.read_text().strip() if token_path.exists() else None


def resolve_management_context(
    explicit_head: str | None,
    *,
    allow_missing: bool = False,
) -> ManagementContext:
    credentials = _saved_credentials()
    configured_url = credentials.get("cluster_dashboard_url")
    env_url = os.environ.get("BURLA_CLUSTER_DASHBOARD_URL")

    if explicit_head:
        head_url = _normalize_head_url(explicit_head)
        head_source = "explicit"
    elif env_url:
        head_url = _normalize_head_url(env_url)
        head_source = "environment"
    else:
        head_url = _saved_head_url(credentials)
        head_source = "saved_credentials" if configured_url else "saved_local_head"

    if not head_url:
        if allow_missing:
            return ManagementContext(None, None, None, None, None, {})
        raise ManagementError(
            "CONTEXT_REQUIRED",
            "No Burla head is selected. Set --head or BURLA_CLUSTER_DASHBOARD_URL, "
            "or run `burla dashboard`.",
        )

    auth_token = os.environ.get("BURLA_AUTH_TOKEN")
    user_email = os.environ.get("BURLA_USER_EMAIL")
    if bool(auth_token) != bool(user_email):
        raise ManagementError(
            "INVALID_ARGUMENT",
            "BURLA_AUTH_TOKEN and BURLA_USER_EMAIL must be set together.",
        )

    project_id = credentials.get("project_id")
    if auth_token and user_email:
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "X-User-Email": user_email,
        }
        auth_source = "environment_user"
        principal = user_email
    elif os.environ.get("BURLA_CLUSTER_TOKEN"):
        headers = {
            "Authorization": f"Bearer {os.environ['BURLA_CLUSTER_TOKEN']}",
        }
        auth_source = "environment_cluster"
        principal = None
    elif credentials.get("auth_token") and credentials.get("email"):
        headers = {
            "Authorization": f"Bearer {credentials['auth_token']}",
            "X-User-Email": credentials["email"],
        }
        auth_source = "saved_user"
        principal = credentials["email"]
    else:
        cluster_token = _saved_cluster_token(project_id)
        if cluster_token:
            headers = {"Authorization": f"Bearer {cluster_token}"}
            auth_source = "saved_cluster"
            principal = None
        elif _is_local_url(head_url):
            headers = {}
            auth_source = "local_owner"
            principal = None
        elif allow_missing:
            headers = {}
            auth_source = None
            principal = None
        else:
            raise ManagementError(
                "AUTH_REQUIRED",
                "No Burla credentials are available. Run `burla login --no-browser` "
                "or set BURLA_AUTH_TOKEN and BURLA_USER_EMAIL.",
            )

    return ManagementContext(
        head_url=head_url,
        head_source=head_source,
        project_id=project_id,
        auth_source=auth_source,
        principal=principal,
        headers=headers,
    )


class ManagementClient:
    def __init__(self, context: ManagementContext):
        if context.head_url is None:
            raise ManagementError("CONTEXT_REQUIRED", "No Burla head is selected.")
        self.context = context
        self.session = requests.Session()
        self.session.headers.update(context.headers)

    def _url(self, path: str) -> str:
        return f"{self.context.head_url}{path}"

    def _error_from_response(self, response: requests.Response) -> ManagementError:
        request_id = response.headers.get("X-Request-ID")
        content_type = response.headers.get("content-type", "")
        if "text/html" in content_type:
            return ManagementError(
                "AUTH_REQUIRED",
                "The Burla head returned its browser login page.",
                request_id=request_id,
            )

        try:
            body = response.json()
        except ValueError:
            body = {}
        error = body.get("error") if isinstance(body, dict) else None
        if isinstance(error, dict) and error.get("code"):
            return ManagementError(
                error["code"],
                error.get("message") or response.reason,
                retryable=bool(error.get("retryable")),
                details=error.get("details"),
                remediation=error.get("remediation"),
                request_id=body.get("request_id") or request_id,
            )

        detail = body.get("detail") if isinstance(body, dict) else None
        message = detail if isinstance(detail, str) else response.reason
        code = {
            400: "INVALID_ARGUMENT",
            401: "AUTH_REQUIRED",
            403: "FORBIDDEN",
            404: "NOT_FOUND",
            409: "CLUSTER_BUSY",
            412: "CLUSTER_BUSY",
            422: "INVALID_ARGUMENT",
        }.get(response.status_code)
        if code is None:
            code = "PROVIDER_ERROR" if response.status_code >= 500 else "TRANSPORT_ERROR"
        return ManagementError(code, message, request_id=request_id)

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict | None = None,
        body: dict | None = None,
        long_running: bool = False,
    ) -> dict:
        timeout = (10, None) if long_running else (10, 30)
        try:
            response = self.session.request(
                method,
                self._url(path),
                params=params,
                json=body,
                timeout=timeout,
            )
        except requests.Timeout as error:
            raise ManagementError("TIMEOUT", str(error), retryable=True) from error
        except requests.RequestException as error:
            raise ManagementError(
                "HEAD_UNREACHABLE",
                f"Could not reach the Burla head at {self.context.head_url}: {error}",
                retryable=True,
            ) from error

        if response.status_code >= 400:
            error = self._error_from_response(response)
            try:
                normalized_error = isinstance(response.json().get("error"), dict)
            except (ValueError, AttributeError):
                normalized_error = False
            if (
                response.status_code == 404
                and path.startswith("/v1/management")
                and not normalized_error
            ):
                try:
                    version_response = self.session.get(
                        self._url("/version"), timeout=(10, 30)
                    )
                except requests.RequestException:
                    version_response = None
                if version_response is not None and version_response.status_code == 200:
                    error = ManagementError(
                        "HEAD_UPGRADE_REQUIRED",
                        "This Burla head does not support the management API.",
                    )
            raise error

        content_type = response.headers.get("content-type", "")
        if "text/html" in content_type:
            raise self._error_from_response(response)
        if not response.content:
            return {}
        return response.json()

    def stream_sse(self, path: str, *, params: dict | None = None):
        last_event_id = None
        connected = False
        while True:
            headers = {"Last-Event-ID": last_event_id} if last_event_id else None
            try:
                response = self.session.get(
                    self._url(path),
                    params=params,
                    headers=headers,
                    timeout=(10, None),
                    stream=True,
                )
            except requests.RequestException as error:
                if not connected:
                    raise ManagementError(
                        "HEAD_UNREACHABLE",
                        f"Could not reach the Burla head at {self.context.head_url}: {error}",
                        retryable=True,
                    ) from error
                sleep(1)
                continue
            if response.status_code >= 400:
                raise self._error_from_response(response)
            connected = True

            event_name = "update"
            event_id = None
            data_lines = []
            try:
                for line in response.iter_lines(decode_unicode=True):
                    if line == "":
                        if data_lines:
                            payload = json.loads("\n".join(data_lines))
                            if event_id:
                                last_event_id = event_id
                            yield {
                                "event": event_name,
                                "cursor": event_id,
                                "data": payload,
                            }
                        event_name = "update"
                        event_id = None
                        data_lines = []
                        continue
                    if line.startswith(":") or line.startswith("retry:"):
                        continue
                    if line.startswith("event:"):
                        event_name = line.removeprefix("event:").strip()
                    elif line.startswith("id:"):
                        event_id = line.removeprefix("id:").strip()
                    elif line.startswith("data:"):
                        data_lines.append(line.removeprefix("data:").lstrip())
            except requests.RequestException:
                sleep(1)
            finally:
                response.close()

    def stream_ndjson(self, path: str, *, params: dict | None = None):
        try:
            response = self.session.get(
                self._url(path),
                params=params,
                timeout=(10, None),
                stream=True,
            )
        except requests.RequestException as error:
            raise ManagementError(
                "HEAD_UNREACHABLE",
                f"Could not reach the Burla head at {self.context.head_url}: {error}",
                retryable=True,
            ) from error
        if response.status_code >= 400:
            raise self._error_from_response(response)
        try:
            for line in response.iter_lines(decode_unicode=True):
                if line:
                    yield json.loads(line)
        finally:
            response.close()
