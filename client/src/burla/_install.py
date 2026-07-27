import sys
import shutil
import traceback
import requests
import json
import tempfile
from time import sleep, time

from yaspin import yaspin

from burla import _BURLA_BACKEND_URL, __version__
from burla._helpers import run_command, VerboseCalledProcessError
from burla._reporting import log_telemetry

HEAD_VM_NAME = "burla-main-service"
HEAD_MACHINE_TYPE = "e2-small"
HEAD_REGION = "us-central1"
HEAD_ZONE = "us-central1-a"


class InstallError(Exception):
    def __init__(self):
        message = f"\n\nIf you're not sure what to do, please email jake@burla.dev!\n"
        message += (
            f"We take errors very seriously, and would really like to help you get Burla installed!\n-"
        )
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
        traceback_details = traceback.format_exception(exc_type, exc_value, exc_traceback)
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
    _open_port_80_to_head_vm(spinner)

    _create_gcs_bucket(spinner, PROJECT_ID)

    # create cluster id token secret (must exist for service accounts to be created)
    # The secret is only read by `burla login`'s ADC bootstrap path - services
    # receive the token as an env var.
    cmd = 'gcloud secrets create burla-cluster-id-token --replication-policy="automatic"'
    create_cmd_result = run_command(cmd, raise_error=False)
    cmd_threw_error = create_cmd_result.returncode != 0
    if cmd_threw_error and ("already exists" not in create_cmd_result.stderr.decode()):
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, create_cmd_result.stderr)

    # create service accounts: main-service, compute-engine-default
    main_svc_account_email = _create_service_accounts(spinner, PROJECT_ID)

    cluster_id_token = _register_cluster_and_save_cluster_id_token(spinner, PROJECT_ID)

    dashboard_url = _deploy_head_vm(spinner, PROJECT_ID, main_svc_account_email, cluster_id_token)

    # remove the old Cloud Run deployment if upgrading from a pre-1.6 cluster.
    cmd = "gcloud run services delete burla-main-service --region=us-central1 --quiet"
    run_command(cmd, raise_error=False)

    # register dashboard url so burla website login page can send user to this instance.
    url = f"{_BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/dashboard_url"
    headers = {"Authorization": f"Bearer {cluster_id_token}"}
    response = requests.post(url, json={"dashboard_url": dashboard_url}, headers=headers)
    response.raise_for_status()

    # update cluster version recorded in burla's cloud
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

    image_name = f"us-docker.pkg.dev/burla-prod/burla-main-service/burla-main-service:{__version__}"
    container_env = (
        f"PROJECT_ID={PROJECT_ID},"
        f"CLUSTER_ID_TOKEN={cluster_id_token},"
        f"CLOUD_PROVIDER=gcp,"
        f"PORT=80,"
        f"HISTORY_DB_PATH=/var/lib/burla/history.db"
    )
    create_cmd = (
        f"gcloud compute instances create-with-container {HEAD_VM_NAME} "
        f"--zone={HEAD_ZONE} "
        f"--machine-type={HEAD_MACHINE_TYPE} "
        f"--address={static_ip} "
        f"--tags=burla-head "
        f"--service-account={main_svc_account_email} "
        f"--scopes=https://www.googleapis.com/auth/cloud-platform "
        f"--boot-disk-size=20GB "
        f"--container-image={image_name} "
        f"--container-restart-policy=always "
        f"--container-env='{container_env}' "
        f"--container-mount-host-path=mount-path=/var/lib/burla,host-path=/var/lib/burla,mode=rw"
    )
    result = run_command(create_cmd, raise_error=False)
    if result.returncode != 0 and "already exists" in result.stderr.decode():
        # Upgrading: point the existing VM at the new image + env.
        update_cmd = (
            f"gcloud compute instances update-container {HEAD_VM_NAME} "
            f"--zone={HEAD_ZONE} "
            f"--container-image={image_name} "
            f"--container-env='{container_env}'"
        )
        run_command(update_cmd)
    elif result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(create_cmd, result.stderr)

    dashboard_url = f"http://{static_ip}"

    spinner.text = "Deploying burla-main-service VM ... waiting for it to serve traffic ..."
    start = time()
    while time() - start < 300:
        try:
            response = requests.get(f"{dashboard_url}/version", timeout=3)
            if response.status_code == 200:
                break
        except requests.RequestException:
            pass
        sleep(5)
    else:
        spinner.fail("✗")
        raise Exception(f"burla-main-service VM never became reachable at {dashboard_url}")

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
        spinner.text = "Opening port 8080 to VM's with tag 'burla-cluster-node' ... Done."
        spinner.ok("✓")


def _open_port_80_to_head_vm(spinner):
    spinner.text = "Opening port 80 to the VM with tag 'burla-head' ... "
    spinner.start()
    cmd = (
        "gcloud compute firewall-rules create burla-head-firewall "
        "--direction=INGRESS "
        "--priority=1000 "
        "--network=default "
        "--action=ALLOW "
        "--rules=tcp:80 "
        "--target-tags=burla-head"
    )
    result = run_command(cmd, raise_error=False)
    if result.returncode != 0 and "already exists" in result.stderr.decode():
        spinner.text = "Opening port 80 to the VM with tag 'burla-head' ... Rule already exists."
        spinner.ok("✓")
    elif result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)
    else:
        spinner.text = "Opening port 80 to the VM with tag 'burla-head' ... Done."
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
            "responseHeader": ["Content-Type", "Content-Length", "Location", "x-goog-resumable"],
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
    spinner.text = "Creating/Rotating secrets ... "
    spinner.start()

    # get cluster_id_token secret value
    cluster_id_token = None
    cmd = "gcloud secrets versions access latest --secret=burla-cluster-id-token"
    result = run_command(cmd, raise_error=False)
    if result.returncode != 0 and "NOT_FOUND" in result.stderr.decode():
        # means secret exists, but no `latest` version created yet
        pass
    elif result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)
    else:
        cluster_id_token = result.stdout.decode().strip()

    # register cluster
    response = requests.post(f"{_BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}")
    if response.status_code == 403:
        spinner.fail("✗")
        raise AuthError()
    elif response.status_code == 200:
        cluster_id_token = response.json()["token"]
    elif response.status_code != 409:
        spinner.fail("✗")
        raise Exception(f"Error registering cluster: {response.status_code} {response.text}")

    # rotate cluster token
    headers = {"Authorization": f"Bearer {cluster_id_token}"}
    url = f"{_BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/token"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    cluster_id_token = response.json()["token"]
    headers = {"Authorization": f"Bearer {cluster_id_token}"}

    # save/update token as secret
    cmd = f'printf "%s" "{cluster_id_token}" | gcloud secrets versions add burla-cluster-id-token --data-file=-'
    run_command(cmd)

    # ensure installer is authorized
    cmd = f'gcloud auth list --filter=status:ACTIVE --format="value(account)"'
    cluster_owner_email = run_command(cmd).stdout.decode().strip()
    users_url = f"{_BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/users"
    response = requests.post(users_url, json={"new_user": cluster_owner_email}, headers=headers)
    response.raise_for_status()

    spinner.text = "Creating/Rotating secrets ...  Done."
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
    result = run_command(f"gcloud projects describe {PROJECT_ID} --format='value(projectNumber)'")
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
