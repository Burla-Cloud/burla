import os
import sys
import shutil
import subprocess
import traceback
import requests
from time import time, sleep

from yaspin import yaspin
from google.cloud.firestore import Client
from google.api_core.exceptions import NotFound

from burla import _BURLA_BACKEND_URL
from burla._helpers import log_telemetry


python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
DEFAULT_CLUSTER_CONFIG = {
    "Nodes": [
        {
            "containers": [
                {
                    "image": f"python:{python_version}",
                    "python_version": python_version,
                }
            ],
            "inactivity_shutdown_time_sec": 300,
            "machine_type": "n4-standard-4",
            "quantity": 1,
            "disk_size_gb": 50,
        }
    ]
}


class VerboseCalledProcessError(Exception):
    """This exists to include stderr in the exception message, CalledProcessError does not"""

    def __init__(self, cmd: str, stderr: str):
        msg = "SubCommand failed with non-zero exit code!\n"
        msg += f'Command = "{cmd}"\n'
        msg += f"Command Stderr--------------------------------------------------------\n"
        msg += f"{stderr}\n"
        msg += f"--------------------------------------------------------\n"
        msg += f"If you're not sure what to do, please email jake@burla.dev, or call me at 508-320-8778!\n"
        msg += f"We take errors very seriously, and would really like to help you get Burla installed!\n"
        super().__init__(msg)


class InstallError(Exception):
    pass


class BackendError(Exception):
    pass


class AuthError(Exception):
    pass


def _run_command(command, raise_error=True):
    result = subprocess.run(command, shell=True, capture_output=True)

    if result.returncode != 0 and raise_error:
        print("")
        raise VerboseCalledProcessError(command, result.stderr)
    else:
        return result


def main_service_url():
    main_svc_image_name = "us-docker.pkg.dev/burla-test/burla-main-service/burla-main-service"
    cmd = f"docker container list --filter ancestor={main_svc_image_name}"
    result = _run_command(cmd, raise_error=False)
    if result.returncode == 0 and len(result.stdout.strip().splitlines()) > 1:
        return "http://localhost:5001"
    else:
        cmd = "gcloud run services describe burla-main-service --region us-central1"
        result = _run_command(cmd)
        for line in result.stdout.decode().splitlines():
            if line.startswith("URL:"):
                return line.split()[1]


def install():
    """Install or Update the Burla cluster in your current default Google Cloud Project.

    - Run: `gcloud config get project` to view your default project.
    - Run: `gcloud config set project <new-project-id>` to change your default project.
    """
    try:
        with yaspin() as spinner:
            _install(spinner)
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
            msg = f"If you're not sure what to do, please email jake@burla.dev, or call me at 508-320-8778!\n"
            msg += f"We take errors very seriously, and would really like to help you get Burla installed!\n"
            raise InstallError(msg) from e


def _install(spinner):
    log_telemetry("Somebody is running `burla install`!")

    # check gcloud is installed:
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

    # check user is logged in:
    spinner.text = "Checking for gcloud credentials ... "
    spinner.start()
    cmd = "gcloud auth list --filter=status:ACTIVE --format='value(account)'"
    result = _run_command(cmd, raise_error=False)
    if result.returncode != 0 and result.stdout == b"":
        spinner.fail("✗")
        msg = "ERROR: You are not logged in with gcloud.\n"
        msg += "Please run 'gcloud auth login' before installing Burla."
        print("")
        print(msg, file=sys.stderr)
        log_telemetry("User has gcloud but is not logged in.")
        sys.exit(1)

    cmd = "gcloud auth application-default print-access-token 2>/dev/null"
    result = _run_command(cmd, raise_error=False)
    if result.returncode != 0 and result.stdout == b"":
        spinner.fail("✗")
        msg = "ERROR: Application default credentials not found.\n"
        msg += "Please run 'gcloud auth application-default login' before installing Burla."
        print("")
        print(msg, file=sys.stderr)
        log_telemetry("User has gcloud but is not logged in with application-default credentials.")
        sys.exit(1)
    spinner.text = "Checking for gcloud credentials ... Done."
    spinner.ok("✓")

    # check user has project set:
    spinner.text = "Checking for gcloud project ... "
    spinner.start()
    result = _run_command("gcloud config get-value project 2>/dev/null")
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

    log_telemetry("Installer has gcloud and is logged in.", project_id=PROJECT_ID)

    # Enable required services
    spinner.text = "Enabling required services ... "
    spinner.start()
    _run_command("gcloud services enable compute.googleapis.com")
    _run_command("gcloud services enable run.googleapis.com")
    _run_command("gcloud services enable firestore.googleapis.com")
    _run_command("gcloud services enable cloudresourcemanager.googleapis.com")
    _run_command("gcloud services enable secretmanager.googleapis.com")
    _run_command("gcloud services enable logging.googleapis.com")
    spinner.text = "Enabling required services... Done."
    spinner.ok("✓")

    # Open port 8080
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
    result = _run_command(cmd, raise_error=False)
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

    # Register cluster and save token
    spinner.text = "Creating secrets ... "
    spinner.start()
    cmd = "gcloud secrets versions access latest --secret=burla-cluster-id-token"
    result = _run_command(cmd, raise_error=False)
    if result.returncode != 0 and "NOT_FOUND" in result.stderr.decode():
        already_created = False
        # create secret
        cmd = 'gcloud secrets create burla-cluster-id-token --replication-policy="automatic"'
        _run_command(cmd)
        # register cluster
        response = requests.post(f"{_BURLA_BACKEND_URL}/v1/projects/{PROJECT_ID}")
        if response.status_code == 403:
            spinner.fail("✗")
            msg = "Cluster ID secret is missing, but this deployment has already been registered.\n"
            msg += "Because this secret is missing, we cannot verify that you are the owner of this cluster.\n"
            msg += "Please call Jake at 508-320-8778, email jake@burla.dev, "
            msg += "or DM @jake__z in our Discord to regain access!"
            raise AuthError(msg)
        elif response.status_code != 200:
            spinner.fail("✗")
            raise BackendError(f"Error registering cluster: {response.status_code} {response.text}")
        cluster_id_token = response.json()["token"]

    elif result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)
    else:
        already_created = True
        cluster_id_token = result.stdout.decode().strip()

    # ensure installer is authorized
    cmd = f'gcloud auth list --filter=status:ACTIVE --format="value(account)"'
    cluster_owner_email = _run_command(cmd).stdout.decode().strip()
    users_url = f"{_BURLA_BACKEND_URL}/v1/projects/{PROJECT_ID}/users"
    headers = {"Authorization": f"Bearer {cluster_id_token}"}
    response = requests.post(users_url, json={"new_user": cluster_owner_email}, headers=headers)
    response.raise_for_status()

    # save/update token as secret
    cmd = f'printf "%s" "{cluster_id_token}" | gcloud secrets versions add burla-cluster-id-token --data-file=-'
    _run_command(cmd)

    if already_created:
        spinner.text = "Creating secret ... Secret already exists."
    else:
        spinner.text = "Creating secret ... Done."
    spinner.ok("✓")

    # create custom service account for main service
    spinner.text = "Creating service account ... "
    spinner.start()
    SERVICE_ACCOUNT_NAME = "burla-main-service"
    SA_EMAIL = f"{SERVICE_ACCOUNT_NAME}@{PROJECT_ID}.iam.gserviceaccount.com"
    cmd = f"gcloud iam service-accounts create {SERVICE_ACCOUNT_NAME} --display-name='Burla Main Service'"
    result = _run_command(cmd, raise_error=False)
    if result.returncode != 0 and "already exists" in result.stderr.decode():
        already_exists = True
    elif result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)
    else:
        already_exists = False

    # wait for burla-main-service service account to exist:
    start = time()
    while time() - start < 30:
        cmd = f"gcloud iam service-accounts describe {SA_EMAIL}"
        if _run_command(cmd, raise_error=False).returncode == 0:
            break
        sleep(1)
    result = _run_command(f"gcloud iam service-accounts describe {SA_EMAIL}", raise_error=False)
    if result.returncode != 0:
        spinner.fail("✗")
        raise Exception("Burla-main-service service account not found after 30s.")

    # apply required roles to new svc account:
    project_level_roles = ["datastore.user", "logging.logWriter", "compute.instanceAdmin.v1"]
    for role in project_level_roles:
        cmd = f"gcloud projects add-iam-policy-binding {PROJECT_ID} --member=serviceAccount:{SA_EMAIL} --role=roles/{role}"
        _run_command(cmd)
    cmd = f"gcloud secrets add-iam-policy-binding burla-cluster-id-token "
    cmd += f"--member=serviceAccount:{SA_EMAIL} --role=roles/secretmanager.secretAccessor"
    _run_command(cmd)

    # Project number needed to reference the default Compute Engine service-account
    result = _run_command(f"gcloud projects describe {PROJECT_ID} --format='value(projectNumber)'")
    PROJECT_NUMBER = result.stdout.decode().strip()
    COMPUTE_SA = f"{PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

    # wait for compute engine default service account to exist:
    start = time()
    while time() - start < 30:
        cmd = f"gcloud iam service-accounts describe {COMPUTE_SA}"
        if _run_command(cmd, raise_error=False).returncode == 0:
            break
        sleep(1)
    result = _run_command(f"gcloud iam service-accounts describe {COMPUTE_SA}", raise_error=False)
    if result.returncode != 0:
        spinner.fail("✗")
        raise Exception("Compute engine default service account not found after 30s.")

    # allow compute engine service account to use burla token secret
    cmd = f"gcloud secrets add-iam-policy-binding burla-cluster-id-token "
    cmd += f'--member="serviceAccount:{COMPUTE_SA}" --role="roles/secretmanager.secretAccessor"'
    _run_command(cmd)
    # allow dashboard to create vm instances having the default compute engine account.
    cmd = f"gcloud iam service-accounts add-iam-policy-binding {COMPUTE_SA} "
    cmd += f'--member="serviceAccount:{SA_EMAIL}" --role="roles/iam.serviceAccountUser"'
    _run_command(cmd)

    if already_exists:
        spinner.text = "Creating service account ... Service account already exists."
    else:
        spinner.text = "Creating service account ... Done."
    spinner.ok("✓")

    # Create Firestore database
    spinner.text = "Creating Firestore database ... "
    spinner.start()
    cmd = "gcloud firestore databases create --database=burla --location=us-central1 --type=firestore-native"
    result = _run_command(cmd, raise_error=False)
    if result.returncode != 0 and "already exists" in result.stderr.decode():
        try:
            collection = Client(database="burla").collection("cluster_config")
            collection.document("cluster_config").update(DEFAULT_CLUSTER_CONFIG)
        except NotFound:
            collection.document("cluster_config").set(DEFAULT_CLUSTER_CONFIG)
        spinner.text = "Creating Firestore database ... Database already exists."
        spinner.ok("✓")
    elif result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)
    else:
        # wait for db to exist
        start = time()
        while True:
            try:
                collection = Client(database="burla").collection("cluster_config")
                collection.document("cluster_config").set(DEFAULT_CLUSTER_CONFIG)
                break
            except NotFound as e:
                sleep(1)
                if time() - start >= 30:
                    raise e
        spinner.text = "Creating Firestore database ... Done."
        spinner.ok("✓")

    # Deploy cloud run service
    spinner.text = "Deploying burla-main-service to Google Cloud Run ... "
    spinner.start()
    _run_command(
        f"gcloud run deploy burla-main-service "
        f"--image=burlacloud/main-service:latest "
        f"--project {PROJECT_ID} "
        f"--region=us-central1 "
        f"--service-account {SA_EMAIL} "
        f"--min-instances 1 "
        f"--max-instances 5 "
        f"--memory 4Gi "
        f"--cpu 1 "
        f"--timeout 10 "
        f"--concurrency 20 "
        f"--allow-unauthenticated"
    )
    _run_command(
        f"gcloud run services update-traffic burla-main-service "
        f"--project {PROJECT_ID} "
        f"--region=us-central1 "
        f"--to-latest"
    )
    spinner.text = "Deploying Burla-Main-Service to Google Cloud Run ... Done."
    spinner.ok("✓")

    # print success message
    msg = f"\nSuccess! To view your new dashboard run `burla login`\n"
    msg += f"Quickstart:\n"
    msg += f'  1. Start your cluster by hitting "⏻ Start" in the dashboard\n'
    msg += f"  2. Import and call `remote_parallel_map`!\n\n"
    msg += f"Don't hesitate to E-Mail jake@burla.dev, or call me at 508-320-8778, thank you for using Burla!"
    spinner.write(msg)

    log_telemetry("Burla successfully installed!", project_id=PROJECT_ID)
