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

from burla import _BURLA_BACKEND_URL, __version__
from burla._helpers import run_command, VerboseCalledProcessError
from burla._reporting import log_telemetry

HEAD_VM_NAME = "burla-main-service"
HEAD_MACHINE_TYPE = "e2-small"
HEAD_REGION = "us-central1"
HEAD_ZONE = "us-central1-a"
CLUSTER_TOKEN_SECRET = os.environ.get(
    "BURLA_CLUSTER_TOKEN_SECRET", "burla-cluster-id-token"
)


def _main_service_image(project_id: str) -> str:
    override = os.environ.get("BURLA_MAIN_SERVICE_IMAGE")
    if override:
        if _BURLA_BACKEND_URL == "https://backend.burla.dev":
            raise ValueError(
                "BURLA_MAIN_SERVICE_IMAGE requires a non-production backend"
            )
        return override
    return (
        "us-docker.pkg.dev/burla-prod/burla-main-service/"
        f"burla-main-service:{__version__}"
    )


def _register_dashboard(
    project_id: str, cluster_id_token: str, public_ipv4: str
) -> str:
    headers = {"Authorization": f"Bearer {cluster_id_token}"}
    url = f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}/dashboard"
    response = requests.post(
        url,
        json={"public_ipv4": public_ipv4},
        headers=headers,
    )
    response.raise_for_status()
    return response.json()["dashboard_url"]


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

    for candidate_url in urls:
        if not candidate_url.startswith("https://") and (
            _BURLA_BACKEND_URL == "https://backend.burla.dev"
        ):
            raise ValueError(
                "Refusing to send the cluster token over HTTP during upgrade"
            )
        start = time()
        while time() - start < 60:
            try:
                response = requests.post(
                    f"{candidate_url}/v1/cluster/shutdown",
                    headers=headers,
                    timeout=60,
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
    image: str,
    dashboard_hostname: str,
) -> str:
    node_source_ref = os.environ.get("BURLA_NODE_SOURCE_REF", __version__)
    caddy_config = f"""{dashboard_hostname} {{
  reverse_proxy 127.0.0.1:5001
}}
https://$PRIVATE_IP:8443 {{
  tls /etc/burla/tls/head.pem /etc/burla/tls/head.key
  reverse_proxy 127.0.0.1:5001
}}
"""
    caddy_config_b64 = base64.b64encode(caddy_config.encode()).decode()
    script = f"""#!/bin/bash
    set -euo pipefail
    export DOCKER_CONFIG=/var/lib/burla/docker-config
    mkdir -p "$DOCKER_CONFIG" /var/lib/burla/tls /var/lib/burla/caddy /etc/burla
    ACCESS_TOKEN=$(curl -sS -H "Metadata-Flavor: Google" \\
      http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token \\
      | python3 -c 'import json,sys; print(json.load(sys.stdin)["access_token"])')
    echo "$ACCESS_TOKEN" | docker login -u oauth2accesstoken --password-stdin \\
      https://us-docker.pkg.dev
    docker pull "{image}"
    docker pull caddy:2.10.2-alpine
    old_containers=$(docker ps -aq --filter name=klt-)
    [ -z "$old_containers" ] || docker rm -f $old_containers
    docker rm -f burla-main-service burla-head-caddy || true
    docker run -d --restart=always --network=host --name=burla-main-service \\
      -v /var/lib/burla:/var/lib/burla \\
      -e PROJECT_ID="{project_id}" \\
      -e CLUSTER_ID_TOKEN="{cluster_id_token}" \\
      -e CLOUD_PROVIDER=gcp \\
      -e BIND_HOST=127.0.0.1 \\
      -e PORT=5001 \\
      -e INTERNAL_TLS_PORT=8443 \\
      -e HISTORY_DB_PATH=/var/lib/burla/history.db \\
      -e BURLA_BACKEND_URL="{_BURLA_BACKEND_URL}" \\
      -e BURLA_NODE_SOURCE_REF="{node_source_ref}" \\
      "{image}"
    until curl --fail --silent http://127.0.0.1:5001/version >/dev/null; do
      sleep 1
    done
    PRIVATE_IP=$(curl -sS -H "Metadata-Flavor: Google" \\
      http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip)
    echo "{caddy_config_b64}" | base64 -d > /etc/burla/Caddyfile
    sed -i "s/\\$PRIVATE_IP/$PRIVATE_IP/g" /etc/burla/Caddyfile
    docker run -d --restart=always --network=host --name=burla-head-caddy \\
      -v /etc/burla/Caddyfile:/etc/caddy/Caddyfile:ro \\
      -v /var/lib/burla/tls/head.pem:/etc/burla/tls/head.pem:ro \\
      -v /var/lib/burla/tls/head.key:/etc/burla/tls/head.key:ro \\
      -v /var/lib/burla/caddy:/data \\
      caddy:2.10.2-alpine caddy run --config /etc/caddy/Caddyfile --adapter caddyfile
    """
    return textwrap.dedent(script)


class InstallError(Exception):
    def __init__(self):
        message = f"\n\nIf you're not sure what to do, please email jake@burla.dev!\n"
        message += f"We take errors very seriously, and would really like to help you get Burla installed!\n-"
        super().__init__(message)


class AuthError(Exception):
    def __init__(self):
        message = "Cluster ID secret is missing, but this deployment has already been registered.\n"
        message += "Because this secret is missing, we cannot verify that you are the owner of this cluster.\n"
        message += "Please email jake@burla.dev, "
        message += "or DM @jake__z in our Discord to regain access!"
        super().__init__(message)


def install(cloud: str = "gcp"):
    """Install or Update the Burla cluster.

    - `burla install` installs into your current default Google Cloud project.
      Run: `gcloud config get project` to view your default project.
      Run: `gcloud config set project <new-project-id>` to change your default project.
    - `burla install --cloud=aws` installs into your current default AWS account/region.
    """
    try:
        with yaspin() as spinner:
            if cloud == "aws":
                from burla._install_aws import install_aws

                install_aws(spinner)
            elif cloud == "gcp":
                _install_gcp(spinner)
            else:
                raise ValueError(f"Unknown cloud: {cloud!r}. Use 'gcp' or 'aws'.")
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


def _install_gcp(spinner):
    log_telemetry("Somebody is running `burla install`!")
    _check_gcloud_is_installed(spinner)

    PROJECT_ID = _get_gcloud_GCP_project_id(spinner)
    log_telemetry("Installer has gcloud and is logged in.", project_id=PROJECT_ID)

    spinner.text = "Enabling required services ... "
    spinner.start()
    run_command("gcloud services enable compute.googleapis.com")
    run_command("gcloud services enable cloudresourcemanager.googleapis.com")
    run_command("gcloud services enable secretmanager.googleapis.com")
    run_command("gcloud services enable storage.googleapis.com")
    run_command("gcloud services enable iamcredentials.googleapis.com")
    spinner.text = "Enabling required services... Done."
    spinner.ok("✓")

    _open_port_8080_to_VMs_with_tag_burla_cluster_node(spinner)
    _open_head_ports(spinner)

    _create_gcs_bucket(spinner, PROJECT_ID)

    # create cluster id token secret (must exist for service accounts to be created)
    # The secret is only read by `burla login`'s ADC bootstrap path - services
    # receive the token as an env var.
    cmd = (
        f"gcloud secrets create {CLUSTER_TOKEN_SECRET} "
        '--replication-policy="automatic"'
    )
    create_cmd_result = run_command(cmd, raise_error=False)
    cmd_threw_error = create_cmd_result.returncode != 0
    if cmd_threw_error and ("already exists" not in create_cmd_result.stderr.decode()):
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, create_cmd_result.stderr)

    # create service accounts: main-service, compute-engine-default
    main_svc_account_email = _create_service_accounts(spinner, PROJECT_ID)

    cluster_id_token = _register_cluster_and_save_cluster_id_token(spinner, PROJECT_ID)

    dashboard_url = _deploy_head_vm(
        spinner, PROJECT_ID, main_svc_account_email, cluster_id_token
    )

    # remove the old Cloud Run deployment if upgrading from a pre-1.6 cluster.
    cmd = "gcloud run services delete burla-main-service --region=us-central1 --quiet"
    run_command(cmd, raise_error=False)

    headers = {"Authorization": f"Bearer {cluster_id_token}"}
    url = f"{_BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/version"
    response = requests.put(url, json={"version": __version__}, headers=headers)
    response.raise_for_status()

    # print success message
    msg = f"\nSuccessfully installed Burla v{__version__}!\n"
    msg += f"Quickstart:\n"
    msg += f"  1. Open your new cluster dashboard: {dashboard_url}\n"
    msg += f'  2. Hit "⏻ Start" to boot some machines.\n'
    msg += f"  3. Run `burla login` to connect your laptop to the cluster.\n"
    msg += f"  4. Import and call `remote_parallel_map`!\n\n"
    msg += f"Don't hesitate to E-Mail jake@burla.dev, thank you for using Burla!"
    spinner.write(msg)

    log_telemetry("Burla successfully installed!", project_id=PROJECT_ID)


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
    if existing.returncode == 0:
        if existing_status != "RUNNING":
            run_command(
                f"gcloud compute instances start {HEAD_VM_NAME} " f"--zone={HEAD_ZONE}"
            )
            existing_status = "RUNNING"
        _shutdown_cluster_for_upgrade(
            PROJECT_ID,
            cluster_id_token,
            f"http://{static_ip}",
        )

    dashboard_url = _register_dashboard(PROJECT_ID, cluster_id_token, static_ip)
    image_name = _main_service_image(PROJECT_ID)
    startup_script = _head_startup_script(
        PROJECT_ID,
        cluster_id_token,
        image_name,
        urlparse(dashboard_url).hostname,
    )
    with tempfile.NamedTemporaryFile("w", suffix=".sh") as startup_file:
        startup_file.write(startup_script)
        startup_file.flush()
        if existing.returncode == 0:
            if existing_status == "RUNNING":
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
    while time() - start < 300:
        try:
            response = requests.get(
                f"{dashboard_url}/version",
                headers={"Authorization": f"Bearer {cluster_id_token}"},
                timeout=3,
            )
            expected = {"version": __version__, "project": PROJECT_ID}
            if response.status_code == 200 and response.json() == expected:
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


def _open_port_8080_to_VMs_with_tag_burla_cluster_node(spinner):
    spinner.text = "Opening port 8080 to VM's with tag 'burla-cluster-node' ... "
    spinner.start()
    cmd = (
        "gcloud compute firewall-rules create burla-cluster-node-firewall "
        "--direction=INGRESS "
        "--priority=1000 "
        "--network=default "
        "--action=ALLOW "
        "--rules=tcp:8080 "
        "--target-tags=burla-cluster-node"
    )
    result = run_command(cmd, raise_error=False)
    if result.returncode != 0 and "already exists" in result.stderr.decode():
        msg = "Opening port 8080 to VM's with tag 'burla-cluster-node' ... "
        msg += "Rule already exists."
        spinner.text = msg
        spinner.ok("✓")
    elif result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)
    else:
        spinner.text = (
            "Opening port 8080 to VM's with tag 'burla-cluster-node' ... Done."
        )
        spinner.ok("✓")


def _open_head_ports(spinner):
    spinner.text = "Opening HTTPS ports to the VM with tag 'burla-head' ... "
    spinner.start()
    existing = run_command(
        "gcloud compute firewall-rules describe burla-head-firewall",
        raise_error=False,
    )
    if existing.returncode == 0:
        run_command(
            "gcloud compute firewall-rules update burla-head-firewall "
            "--rules=tcp:80,tcp:443"
        )
    else:
        run_command(
            "gcloud compute firewall-rules create burla-head-firewall "
            "--direction=INGRESS --priority=1000 --network=default "
            "--action=ALLOW --rules=tcp:80,tcp:443 --target-tags=burla-head"
        )

    internal = run_command(
        "gcloud compute firewall-rules describe burla-head-internal-firewall",
        raise_error=False,
    )
    if internal.returncode == 0:
        run_command(
            "gcloud compute firewall-rules update burla-head-internal-firewall "
            "--rules=tcp:8443 --source-tags=burla-cluster-node"
        )
    else:
        run_command(
            "gcloud compute firewall-rules create burla-head-internal-firewall "
            "--direction=INGRESS --priority=1000 --network=default "
            "--action=ALLOW --rules=tcp:8443 --source-tags=burla-cluster-node "
            "--target-tags=burla-head"
        )
    spinner.text = "Opening HTTPS ports to the VM with tag 'burla-head' ... Done."
    spinner.ok("✓")


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
    spinner.text = "Registering cluster ... "
    spinner.start()

    # get cluster_id_token secret value
    cluster_id_token = None
    cmd = f"gcloud secrets versions access latest --secret={CLUSTER_TOKEN_SECRET}"
    result = run_command(cmd, raise_error=False)
    if result.returncode != 0 and "NOT_FOUND" in result.stderr.decode():
        # means secret exists, but no `latest` version created yet
        pass
    elif result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)
    else:
        cluster_id_token = result.stdout.decode().strip()

    new_cluster = False
    access_token = run_command("gcloud auth print-access-token").stdout.decode().strip()
    response = requests.post(
        f"{_BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}",
        json={"cloud": "gcp", "access_token": access_token},
    )
    if response.status_code == 403:
        spinner.fail("✗")
        raise AuthError()
    elif response.status_code == 200:
        cluster_id_token = response.json()["token"]
        new_cluster = True
    elif response.status_code != 409:
        spinner.fail("✗")
        raise Exception(
            f"Error registering cluster: {response.status_code} {response.text}"
        )

    if cluster_id_token is None:
        spinner.fail("✗")
        raise AuthError()

    headers = {"Authorization": f"Bearer {cluster_id_token}"}

    if new_cluster:
        with tempfile.NamedTemporaryFile("w") as token_file:
            token_file.write(cluster_id_token)
            token_file.flush()
            run_command(
                f"gcloud secrets versions add {CLUSTER_TOKEN_SECRET} "
                f"--data-file={token_file.name}"
            )

    # ensure installer is authorized
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
