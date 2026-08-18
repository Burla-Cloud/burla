import os
import sys
import shutil
import textwrap
import traceback
import requests
import json
import base64
import tempfile
from time import sleep, time
from urllib.parse import urlparse

from yaspin import yaspin

from burla import (
    _BURLA_BACKEND_URL,
    _BURLA_ENVIRONMENT,
    _BURLA_NODE_SOURCE_REF,
    _BURLA_RELAY_HOST,
    __version__,
)
from burla._helpers import run_command, VerboseCalledProcessError
from burla._reporting import log_telemetry

HEAD_VM_NAME = "burla-main-service"
HEAD_MACHINE_TYPE = "e2-small"
HEAD_REGION = "us-central1"
HEAD_ZONE = "us-central1-a"

# All cluster traffic flows through Burla's frp relay: nodes + head dial out
# to it, so no inbound firewall rules are ever needed in the user's project.
RELAY_HOST = _BURLA_RELAY_HOST.strip().lower()
RELAY_SERVER_ADDR = os.environ.get("BURLA_RELAY_SERVER_ADDR", RELAY_HOST)
RELAY_SERVER_PORT = os.environ.get("BURLA_RELAY_SERVER_PORT", "7000")
FRP_VERSION = "0.70.1"


def head_install_spec() -> str:
    """What the head VM pip-installs. Production heads install the published
    release; test heads install the same ref the nodes run, so a test deploy is
    exactly the dev branch (the built dashboard is committed, so this works)."""
    if _BURLA_ENVIRONMENT == "production":
        return f"burla=={__version__}"
    return (
        "burla @ git+https://github.com/Burla-Cloud/burla.git"
        f"@{_BURLA_NODE_SOURCE_REF}#subdirectory=client"
    )


def _gcp_ownership_payload() -> dict:
    access_token = run_command("gcloud auth print-access-token").stdout.decode().strip()
    return {"cloud": "gcp", "access_token": access_token}


def _register_dashboard(
    project_id: str,
    cluster_id_token: str,
    public_ipv4: str,
    ownership: dict,
    dashboard_url: str,
):
    """Registers the cluster's relay-hosted dashboard URL so `burla login`
    hands it out to clients."""
    headers = {"Authorization": f"Bearer {cluster_id_token}"}
    url = f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}/dashboard"
    payload = {
        "public_ipv4": public_ipv4,
        "ownership": ownership,
        "dashboard_url": dashboard_url,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()


def _shutdown_cluster_for_upgrade(
    project_id: str,
    cluster_id_token: str,
    fallback_url: str,
):
    headers = {"Authorization": f"Bearer {cluster_id_token}"}
    response = requests.get(
        f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}/dashboard_url",
        headers=headers,
        timeout=20,
    )
    dashboard_url = (
        response.json()["dashboard_url"]
        if response.status_code == 200
        else fallback_url
    )
    urls = [dashboard_url]
    if _BURLA_BACKEND_URL != "https://backend.burla.dev" and fallback_url not in urls:
        urls.append(fallback_url)

    timeout_sec = 10 if _BURLA_BACKEND_URL != "https://backend.burla.dev" else 60
    for candidate_url in urls:
        if not candidate_url.startswith("https://") and (
            _BURLA_BACKEND_URL == "https://backend.burla.dev"
        ):
            raise ValueError(
                "Refusing to send the cluster token over HTTP during upgrade"
            )
        start = time()
        while time() - start < timeout_sec:
            try:
                response = requests.post(
                    f"{candidate_url}/v1/cluster/shutdown",
                    headers=headers,
                    timeout=timeout_sec,
                )
                response.raise_for_status()
                return
            except requests.RequestException:
                sleep(5)
    if _BURLA_BACKEND_URL != "https://backend.burla.dev":
        return
    raise Exception(f"Existing cluster did not shut down through {urls}")


def _head_startup_script(
    project_id: str,
    cluster_id_token: str,
    dashboard_hostname: str,
) -> str:
    node_source_ref = _BURLA_NODE_SOURCE_REF
    install_spec = head_install_spec()
    relay_subdomain = f"head--{project_id}"
    caddy_config = f"""{dashboard_hostname} {{
  reverse_proxy burla-main-service:5001
}}
"""
    caddy_config_b64 = base64.b64encode(caddy_config.encode()).decode()

    frpc_config = f"""serverAddr = "{RELAY_SERVER_ADDR}"
serverPort = {RELAY_SERVER_PORT}
loginFailExit = false
user = "{project_id}"
metadatas.token = "{cluster_id_token}"
transport.poolCount = 4

[[proxies]]
name = "{relay_subdomain}"
type = "https"
localIP = "burla-head-caddy"
localPort = 443
subdomain = "{relay_subdomain}"
"""
    frpc_config_b64 = base64.b64encode(frpc_config.encode()).decode()

    script = f"""#!/bin/bash
    set -euo pipefail
    mkdir -p /var/lib/burla/tls /var/lib/burla/caddy /etc/burla
    docker pull python:3.13
    docker pull caddy:2.10.2-alpine
    docker network create burla-head || true
    old_containers=$(docker ps -aq --filter name=klt-)
    [ -z "$old_containers" ] || docker rm -f $old_containers
    docker rm -f burla-main-service burla-head-caddy burla-head-frpc || true
    docker run -d --restart=always --network=burla-head --name=burla-main-service \\
      -v /var/lib/burla:/var/lib/burla \\
      -e PROJECT_ID="{project_id}" \\
      -e CLUSTER_ID_TOKEN="{cluster_id_token}" \\
      -e BURLA_HEAD_RUNTIME=True \\
      -e CLOUD_PROVIDER=gcp \\
      -e BIND_HOST=0.0.0.0 \\
      -e PORT=5001 \\
      -e HISTORY_DB_PATH=/var/lib/burla/history.db \\
      -e SHARED_WORKSPACE_BUCKET="{project_id}-burla-shared-workspace" \\
      -e BURLA_BACKEND_URL="{_BURLA_BACKEND_URL}" \\
      -e BURLA_RELAY_HOST="{RELAY_HOST}" \\
      -e BURLA_RELAY_SERVER_ADDR="{RELAY_SERVER_ADDR}" \\
      -e BURLA_RELAY_SERVER_PORT="{RELAY_SERVER_PORT}" \\
      -e BURLA_NODE_SOURCE_REF="{node_source_ref}" \\
      python:3.13 \\
      sh -c 'pip install --no-cache-dir "{install_spec}" && exec python -m uvicorn main_service:app --host 0.0.0.0 --port 5001 --workers 1 --timeout-keep-alive 60'
    until docker exec burla-main-service \\
      python -c 'import urllib.request; urllib.request.urlopen("http://127.0.0.1:5001/version")' \\
      >/dev/null 2>&1; do
      sleep 1
    done
    rm -rf /etc/burla/Caddyfile
    echo "{caddy_config_b64}" | base64 -d > /etc/burla/Caddyfile
    docker run -d --restart=always --network=burla-head --name=burla-head-caddy \\
      -v /etc/burla/Caddyfile:/etc/caddy/Caddyfile:ro \\
      -v /var/lib/burla/caddy:/data \\
      caddy:2.10.2-alpine caddy run --config /etc/caddy/Caddyfile --adapter caddyfile
    sleep 3
    if [ "$(docker inspect --format '{{{{.State.Running}}}}' burla-head-caddy)" != "true" ]; then
      docker logs burla-head-caddy
      exit 1
    fi
    echo "{frpc_config_b64}" | base64 -d > /etc/burla/frpc.toml
    chmod 600 /etc/burla/frpc.toml
    docker pull fatedier/frpc:v{FRP_VERSION}
    docker run -d --restart=always --network=burla-head --name=burla-head-frpc \\
      -v /etc/burla/frpc.toml:/etc/frp/frpc.toml:ro \\
      fatedier/frpc:v{FRP_VERSION} -c /etc/frp/frpc.toml
    """
    return textwrap.dedent(script)


class InstallError(Exception):
    def __init__(self):
        message = f"\n\nIf you're not sure what to do, please email jake@burla.dev!\n"
        message += f"We take errors very seriously, and would really like to help you get Burla installed!\n-"
        super().__init__(message)


class AuthError(Exception):
    def __init__(self):
        message = "This cluster is already registered, but this machine doesn't have its token.\n"
        message += "Because of this we cannot verify that you are the owner of this cluster.\n"
        message += "Please email jake@burla.dev, "
        message += "or DM @jake__z in our Discord to regain access!"
        super().__init__(message)


# A first deploy carries this machine's client-hosted job history and settings
# into the new deployed cluster. Redeploys never re-import: the head VM keeps
# its database on disk, and that database is authoritative from then on.


def _snapshot_local_history(spinner, project_id):
    """Pause the local head (refusing if a job is running) and snapshot its
    account-wide history db. Returns (snapshot_path, paused_head_url)."""
    from burla._local_head import prepare_history_migration

    spinner.text = "Snapshotting local job history ... "
    spinner.start()
    snapshot_path, paused_head_url = prepare_history_migration(project_id)
    suffix = "Done." if snapshot_path else "No local history found."
    spinner.text = f"Snapshotting local job history ... {suffix}"
    spinner.ok("✓")
    return snapshot_path, paused_head_url


def _upload_local_history(spinner, dashboard_url, cluster_id_token, snapshot_path):
    spinner.text = "Copying local job history to the deployed cluster ... "
    spinner.start()
    with open(snapshot_path, "rb") as snapshot:
        response = requests.post(
            f"{dashboard_url}/v1/cluster/import_history",
            headers={"Authorization": f"Bearer {cluster_id_token}"},
            data=snapshot,
            timeout=120,
        )
    response.raise_for_status()
    spinner.text = "Copying local job history to the deployed cluster ... Done."
    spinner.ok("✓")


def deploy(cloud: str = None):
    """Deploy (or update) an always-on, shared Burla cluster with a head VM.

    Burla works with zero deployment: `remote_parallel_map` and
    `burla dashboard` run the cluster head on this machine. Deploy when you
    want the dashboard and cluster to stay up for your whole team.

    Deploys into the cloud selected by `burla config set cloud <aws|gcp|azure>`,
    unless `--cloud` is passed.

    - On AWS, deploys into your current default AWS account/region.
    - On GCP, deploys into your current default Google Cloud project.
      Run: `gcloud config get project` to view your default project.
      Run: `gcloud config set project <new-project-id>` to change your default project.
    - On Azure, deploys into your current default Azure subscription.
    """
    from burla import get_cloud

    cloud = (cloud or get_cloud()).lower()
    try:
        with yaspin() as spinner:
            if cloud == "aws":
                from burla._deploy_aws import deploy_aws

                deploy_aws(spinner)
            elif cloud == "azure":
                from burla._deploy_azure import deploy_azure

                deploy_azure(spinner)
            elif cloud == "gcp":
                _deploy_gcp(spinner)
            else:
                raise ValueError(
                    f"Unknown cloud: {cloud!r}. Use 'gcp', 'aws', or 'azure'."
                )
    except Exception as e:
        # Report errors back to Burla's cloud.
        exc_type, exc_value, exc_traceback = sys.exc_info()
        traceback_details = traceback.format_exception(
            exc_type, exc_value, exc_traceback
        )
        traceback_str = "".join(traceback_details)
        log_telemetry(str(exc_type), "ERROR", traceback=traceback_str)

        # reraise
        if isinstance(e, VerboseCalledProcessError) or isinstance(e, AuthError):
            raise e
        else:
            # Raises error with a super clear message at the end of the traceback.
            # yes this is hacky but I need to make sure users of all skill levels see this message.
            message = str(InstallError())
            try:
                exc_cls = e.__class__
                old_str = exc_cls.__str__

                def new_str(self):
                    return f"{old_str(self)}\n\n{message}"

                if getattr(exc_cls, "_burla_str_patched", False) is False:
                    exc_cls.__str__ = new_str
                    exc_cls._burla_str_patched = True
            except Exception:
                raise InstallError() from e
            raise e


def _deploy_gcp(spinner):
    log_telemetry("Somebody is running `burla deploy`!")
    _check_gcloud_is_installed(spinner)

    PROJECT_ID = _get_gcloud_GCP_project_id(spinner)
    log_telemetry("Deployer has gcloud and is logged in.", project_id=PROJECT_ID)

    spinner.text = "Enabling required services ... "
    spinner.start()
    run_command("gcloud services enable compute.googleapis.com")
    run_command("gcloud services enable cloudresourcemanager.googleapis.com")
    run_command("gcloud services enable storage.googleapis.com")
    run_command("gcloud services enable iamcredentials.googleapis.com")
    spinner.text = "Enabling required services... Done."
    spinner.ok("✓")

    _create_gcs_bucket(spinner, PROJECT_ID)

    # create service accounts: main-service, compute-engine-default
    main_svc_account_email = _create_service_accounts(spinner, PROJECT_ID)

    cluster_id_token = _register_cluster_and_save_cluster_id_token(spinner, PROJECT_ID)

    describe_cmd = (
        f"gcloud compute instances describe {HEAD_VM_NAME} "
        f"--zone={HEAD_ZONE} --format='value(status)'"
    )
    first_deploy = run_command(describe_cmd, raise_error=False).returncode != 0
    snapshot_path, paused_head_url = (None, None)
    if first_deploy:
        snapshot_path, paused_head_url = _snapshot_local_history(spinner, PROJECT_ID)

    try:
        dashboard_url = _deploy_head_vm(
            spinner, PROJECT_ID, main_svc_account_email, cluster_id_token
        )
        if snapshot_path:
            _upload_local_history(
                spinner, dashboard_url, cluster_id_token, snapshot_path
            )
        if first_deploy:
            from burla._local_head import finish_history_migration

            finish_history_migration(PROJECT_ID)
    except BaseException:
        if paused_head_url:
            from burla._local_head import resume_history_migration

            resume_history_migration(PROJECT_ID, paused_head_url)
        raise
    finally:
        if snapshot_path:
            os.remove(snapshot_path)

    # remove the old Cloud Run deployment if upgrading from a pre-1.6 cluster.
    cmd = "gcloud run services delete burla-main-service --region=us-central1 --quiet"
    run_command(cmd, raise_error=False)

    headers = {"Authorization": f"Bearer {cluster_id_token}"}
    url = f"{_BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/version"
    response = requests.put(url, json={"version": __version__}, headers=headers)
    response.raise_for_status()

    # Point this machine's client at the freshly deployed cluster.
    from burla._auth import save_deployed_cluster_config

    save_deployed_cluster_config("gcp", PROJECT_ID, cluster_id_token, dashboard_url)

    # print success message
    msg = f"\nSuccessfully deployed Burla v{__version__}!\n"
    msg += f"Quickstart:\n"
    msg += f"  1. Open your new cluster dashboard: {dashboard_url}\n"
    msg += f'  2. Hit "⏻ Start" to boot some machines.\n'
    msg += f"  3. Run `burla login` to connect your laptop to the cluster.\n"
    msg += f"  4. Import and call `remote_parallel_map`!\n\n"
    msg += f"Don't hesitate to E-Mail jake@burla.dev, thank you for using Burla!"
    spinner.write(msg)

    log_telemetry("Burla successfully deployed!", project_id=PROJECT_ID)


def _deploy_head_vm(spinner, PROJECT_ID, main_svc_account_email, cluster_id_token):
    """One small always-on VM runs main_service. It holds live cluster state
    in memory and job/node history in SQLite on its disk (/var/lib/burla)."""
    spinner.text = "Deploying burla-main-service VM ... "
    spinner.start()

    # Static IP so the dashboard URL survives VM restarts.
    cmd = f"gcloud compute addresses create {HEAD_VM_NAME} --region={HEAD_REGION}"
    result = run_command(cmd, raise_error=False)
    if result.returncode != 0 and "already exists" not in result.stderr.decode():
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)
    cmd = (
        f"gcloud compute addresses describe {HEAD_VM_NAME} "
        f"--region={HEAD_REGION} --format='value(address)'"
    )
    static_ip = run_command(cmd).stdout.decode().strip()
    describe_cmd = (
        f"gcloud compute instances describe {HEAD_VM_NAME} "
        f"--zone={HEAD_ZONE} --format='value(status)'"
    )
    existing = run_command(describe_cmd, raise_error=False)
    existing_status = existing.stdout.decode().strip()
    dashboard_url = f"https://head--{PROJECT_ID}.{RELAY_HOST}"
    if existing.returncode == 0:
        if existing_status != "RUNNING":
            run_command(
                f"gcloud compute instances start {HEAD_VM_NAME} " f"--zone={HEAD_ZONE}"
            )
        _shutdown_cluster_for_upgrade(
            PROJECT_ID,
            cluster_id_token,
            dashboard_url,
        )

    _register_dashboard(
        PROJECT_ID,
        cluster_id_token,
        static_ip,
        _gcp_ownership_payload(),
        dashboard_url,
    )
    startup_script = _head_startup_script(
        PROJECT_ID,
        cluster_id_token,
        urlparse(dashboard_url).hostname,
    )
    with tempfile.NamedTemporaryFile("w", suffix=".sh") as startup_file:
        startup_file.write(startup_script)
        startup_file.flush()
        if existing.returncode == 0:
            run_command(
                f"gcloud compute instances stop {HEAD_VM_NAME} "
                f"--zone={HEAD_ZONE} --quiet"
            )
            run_command(
                f"gcloud compute instances remove-metadata {HEAD_VM_NAME} "
                f"--zone={HEAD_ZONE} --keys=gce-container-declaration",
                raise_error=False,
            )
            run_command(
                f"gcloud compute instances add-metadata {HEAD_VM_NAME} "
                f"--zone={HEAD_ZONE} --metadata-from-file=startup-script={startup_file.name}"
            )
            run_command(
                f"gcloud compute instances start {HEAD_VM_NAME} " f"--zone={HEAD_ZONE}"
            )
        else:
            create_cmd = (
                f"gcloud compute instances create {HEAD_VM_NAME} "
                f"--zone={HEAD_ZONE} "
                f"--machine-type={HEAD_MACHINE_TYPE} "
                f"--address={static_ip} "
                f"--tags=burla-head "
                f"--service-account={main_svc_account_email} "
                f"--scopes=https://www.googleapis.com/auth/cloud-platform "
                f"--boot-disk-size=20GB "
                f"--image-family=cos-stable "
                f"--image-project=cos-cloud "
                f"--metadata-from-file=startup-script={startup_file.name}"
            )
            run_command(create_cmd)

    spinner.text = (
        "Deploying burla-main-service VM ... waiting for it to serve traffic ..."
    )
    start = time()
    timeout_sec = 90 if _BURLA_BACKEND_URL != "https://backend.burla.dev" else 300
    while time() - start < timeout_sec:
        try:
            response = requests.get(
                f"{dashboard_url}/version",
                headers={"Authorization": f"Bearer {cluster_id_token}"},
                timeout=3,
            )
            # Subset match: /version also reports fields like `namespace`.
            info = response.json() if response.status_code == 200 else {}
            if info.get("version") == __version__ and info.get("project") == PROJECT_ID:
                break
        except requests.RequestException:
            pass
        sleep(5)
    else:
        spinner.fail("✗")
        raise Exception(
            f"burla-main-service VM never became reachable at {dashboard_url}"
        )

    spinner.text = "Deploying burla-main-service VM ... Done."
    spinner.ok("✓")
    return dashboard_url


def _check_gcloud_is_installed(spinner):
    spinner.text = "Checking for gcloud ... "
    spinner.start()
    if shutil.which("gcloud") is None:
        spinner.fail("✗")
        msg = "Error: Google Cloud SDK (gcloud) is not installed or not in your PATH.\n"
        msg += "Please install the Google Cloud SDK from: https://cloud.google.com/sdk/docs/install"
        print(msg, file=sys.stderr)
        log_telemetry("User does not have gcloud installed.")
        sys.exit(1)
    spinner.text = "Checking for gcloud ... Done."
    spinner.ok("✓")


def _get_gcloud_GCP_project_id(spinner):
    spinner.text = "Checking for gcloud project ... "
    spinner.start()
    result = run_command("gcloud config get-value project 2>/dev/null")
    PROJECT_ID = result.stdout.decode().strip()
    if PROJECT_ID == "":
        spinner.fail("✗")
        msg = "ERROR: No project is set.\n"
        msg += "Please run 'gcloud config set project <YOUR_PROJECT_ID>' before installing Burla."
        print("")
        print(msg, file=sys.stderr)
        log_telemetry("User is logged in but does not have a project set.")
        sys.exit(1)
    spinner.text = f"Checking for gcloud project ... Using project: {PROJECT_ID}"
    spinner.ok("✓")
    return PROJECT_ID


def _create_gcs_bucket(spinner, PROJECT_ID):
    spinner.text = "Creating GCS bucket ... "
    spinner.start()
    cmd = f"gcloud storage buckets create gs://{PROJECT_ID}-burla-shared-workspace"
    result = run_command(cmd, raise_error=False)
    already_exists = False
    if result.returncode != 0 and "HTTPError 409:" in result.stderr.decode():
        already_exists = True
    elif result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)

    cors_config = [
        {
            "origin": ["*"],
            "method": ["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            "responseHeader": [
                "Content-Type",
                "Content-Length",
                "Location",
                "x-goog-resumable",
            ],
            "maxAgeSeconds": 3600,
        }
    ]
    with tempfile.NamedTemporaryFile("w") as cors_file:
        json.dump(cors_config, cors_file)
        cors_file.flush()
        cmd = f"gcloud storage buckets update gs://{PROJECT_ID}-burla-shared-workspace "
        cmd += f"--cors-file='{cors_file.name}'"
        run_command(cmd)

    if already_exists:
        spinner.text = "Creating GCS bucket ... Bucket already exists."
    else:
        spinner.text = "Creating GCS bucket ... Done."
    spinner.ok("✓")


def _register_cluster_and_save_cluster_id_token(spinner, PROJECT_ID):
    """The cluster token lives in Burla's local state dir (and, for clusters
    installed before 1.7, in Secret Manager, which is read as a fallback)."""
    from burla._local_head import LocalHeadError, get_or_register_cluster_token

    spinner.text = "Registering cluster ... "
    spinner.start()

    try:
        cluster_id_token = get_or_register_cluster_token(
            "gcp", PROJECT_ID, aws_region=None
        )
    except LocalHeadError:
        spinner.fail("✗")
        raise AuthError()

    # ensure deployer is authorized
    headers = {"Authorization": f"Bearer {cluster_id_token}"}
    cmd = f'gcloud auth list --filter=status:ACTIVE --format="value(account)"'
    cluster_owner_email = run_command(cmd).stdout.decode().strip()
    users_url = f"{_BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/users"
    response = requests.post(
        users_url, json={"new_user": cluster_owner_email}, headers=headers
    )
    response.raise_for_status()

    spinner.text = "Registering cluster ... Done."
    spinner.ok("✓")
    return cluster_id_token


def _create_service_accounts(spinner, PROJECT_ID):
    # initiate create MAIN SERVICE svc account
    spinner.text = "Creating service accounts ... "
    spinner.start()
    main_svc_account_name = "burla-main-service"
    main_svc_email = f"{main_svc_account_name}@{PROJECT_ID}.iam.gserviceaccount.com"
    cmd = f"gcloud iam service-accounts create {main_svc_account_name} "
    cmd += f" --display-name='{main_svc_account_name}'"
    result = run_command(cmd, raise_error=False)
    if result.returncode != 0 and "already exists" in result.stderr.decode():
        main_svc_svc_account_already_exists = True
    elif result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)
    else:
        main_svc_svc_account_already_exists = False

    # Get reference to COMPUTE ENGINE service-account (GCP project num)
    result = run_command(
        f"gcloud projects describe {PROJECT_ID} --format='value(projectNumber)'"
    )
    gcp_project_num = result.stdout.decode().strip()
    compute_engine_email = f"{gcp_project_num}-compute@developer.gserviceaccount.com"

    # wait for both service accounts to exist:
    start = time()
    all_accounts_exist = False
    while not all_accounts_exist:
        sleep(1)
        for email in [main_svc_email, compute_engine_email]:
            cmd = f"gcloud iam service-accounts describe {email}"
            all_accounts_exist = run_command(cmd, raise_error=False).returncode == 0
        if (time() - start) > 120:
            spinner.fail("✗")
            raise Exception("svc account not found 120s after successful create cmd.")

    # wait service account to propagate, it sonetimes still does not exist even after
    # `gcloud iam service-accounts describe` succeeds because it has yet to propagate fully.
    sleep(5)

    # apply roles to burla-main-service svc account:
    cmd = f"gcloud projects add-iam-policy-binding {PROJECT_ID}"
    cmd += f" --member=serviceAccount:{main_svc_email} --role=roles/compute.instanceAdmin.v1"
    cmd += f" --condition=None"
    run_command(cmd)
    cmd = f"gcloud projects add-iam-policy-binding {PROJECT_ID}"
    cmd += f" --member=serviceAccount:{main_svc_email} --role=roles/storage.objectUser"
    cmd += f" --condition=None"
    run_command(cmd)
    cmd = f"gcloud projects add-iam-policy-binding {PROJECT_ID}"
    cmd += f" --member=serviceAccount:{main_svc_email} --role=roles/artifactregistry.reader"
    cmd += f" --condition=None"
    run_command(cmd)
    # allow main-service to create signed GCS url's for uploading/downloading from filemanager
    cmd = f"gcloud iam service-accounts add-iam-policy-binding {main_svc_email}"
    cmd += f" --member=serviceAccount:{main_svc_email} --role=roles/iam.serviceAccountTokenCreator"
    cmd += f" --condition=None"
    run_command(cmd)

    # allow dashboard to create vm instances having the default compute engine service account
    cmd = f"gcloud iam service-accounts add-iam-policy-binding {compute_engine_email}"
    cmd += f' --member="serviceAccount:{main_svc_email}"'
    cmd += f' --role="roles/iam.serviceAccountUser"'
    run_command(cmd)

    if main_svc_svc_account_already_exists:
        spinner.text = "Creating service accounts ... Accounts already exist."
    else:
        spinner.text = "Creating service accounts ... Done."
    spinner.ok("✓")
    return main_svc_email
