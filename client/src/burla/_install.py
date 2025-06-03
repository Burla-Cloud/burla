import sys
import shutil
import subprocess
import traceback
import requests
from yaspin import yaspin
from google.cloud.firestore import Client

from burla import _BURLA_BACKEND_URL
from burla._helpers import _log_telemetry


python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
DEFAULT_CLUSTER_CONFIG = {
    "Nodes": [
        {
            "containers": [
                {
                    "image": f"burlacloud/default-image-py{python_version}:latest",
                    "python_version": python_version,
                }
            ],
            "inactivity_shutdown_time_sec": 300,
            "machine_type": "n4-standard-4",
            "quantity": 1,
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


def _run_command(command, raise_error=True):
    result = subprocess.run(command, shell=True, capture_output=True)

    if result.returncode != 0 and raise_error:
        print("")
        raise VerboseCalledProcessError(command, result.stderr)
    else:
        return result


def main_service_url():
    result = _run_command(f"gcloud run services describe burla-main-service --region us-central1")
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
        _log_telemetry(str(exc_type), "ERROR", traceback=traceback_str)

        # reraise
        if isinstance(e, VerboseCalledProcessError):
            raise e
        else:
            msg = f"If you're not sure what to do, please email jake@burla.dev, or call me at 508-320-8778!\n"
            msg += f"We take errors very seriously, and would really like to help you get Burla installed!\n"
            raise InstallError(msg) from e


def _install(spinner):
    _log_telemetry("Somebody is running `burla install`!")

    # check gcloud is installed:
    spinner.text = "Checking for gcloud ... "
    spinner.start()
    if shutil.which("gcloud") is None:
        spinner.fail("✗")
        msg = "Error: Google Cloud SDK (gcloud) is not installed or not in your PATH.\n"
        msg += "Please install the Google Cloud SDK from: https://cloud.google.com/sdk/docs/install"
        print(msg, file=sys.stderr)
        _log_telemetry("User does not have gcloud installed.")
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
        _log_telemetry("User has gcloud but is not logged in.")
        sys.exit(1)

    cmd = "gcloud auth application-default print-access-token 2>/dev/null"
    result = _run_command(cmd, raise_error=False)
    if result.returncode != 0 and result.stdout == b"":
        spinner.fail("✗")
        msg = "ERROR: Application default credentials not found.\n"
        msg += "Please run 'gcloud auth application-default login' before installing Burla."
        print("")
        print(msg, file=sys.stderr)
        _log_telemetry("User has gcloud but is not logged in with application-default credentials.")
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
        _log_telemetry("User is logged in but does not have a project set.")
        sys.exit(1)
    spinner.text = f"Checking for gcloud project ... Using project: {PROJECT_ID}"
    spinner.ok("✓")

    _log_telemetry("Installer has gcloud and is logged in.", project_id=PROJECT_ID)

    # Enable required services
    spinner.text = "Enabling required services ... "
    spinner.start()
    _run_command("gcloud services enable compute.googleapis.com")
    _run_command("gcloud services enable run.googleapis.com")
    _run_command("gcloud services enable firestore.googleapis.com")
    _run_command("gcloud services enable cloudresourcemanager.googleapis.com")
    _run_command("gcloud services enable secretmanager.googleapis.com")
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
    cmd = 'gcloud secrets describe burla-cluster-id-token --format="value(name)"'
    result = _run_command(cmd, raise_error=False)
    if result.returncode == 1 and "NOT_FOUND" in result.stderr.decode():
        # get user's email from gcloud
        cmd = f'gcloud auth list --filter=status:ACTIVE --format="value(account)"'
        result = _run_command(cmd)
        cluster_owner_email = result.stdout.decode().strip()

        # tell backend service that email is authorized to run jobs in this project
        url = f"{_BURLA_BACKEND_URL}/v1/projects/{PROJECT_ID}"
        response = requests.post(url, headers={"X-User-Email": cluster_owner_email})
        if response.status_code == 200:
            cluster_id_token = response.json()["token"]
            # save token as secret
            cmd = 'gcloud secrets create burla-cluster-id-token --replication-policy="automatic"'
            result = _run_command(cmd)
            cmd = f'printf "%s" "{cluster_id_token}" | gcloud secrets versions add burla-cluster-id-token --data-file=-'
            result = _run_command(cmd)
            spinner.text = "Creating secrets ... Done."
            spinner.ok("✓")
    else:
        spinner.text = "Creating secrets ... Done."
        spinner.ok("✓")

    # Create Firestore database
    spinner.text = "Creating Firestore database ... "
    spinner.start()
    cmd = "gcloud firestore databases create --database=burla --location=us-central1 --type=firestore-native"
    result = _run_command(cmd, raise_error=False)
    if result.returncode != 0 and "already exists" in result.stderr.decode():
        spinner.text = "Creating Firestore database ... Database already exists."
        spinner.ok("✓")
    elif result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)
    else:
        db = Client(database="burla")
        db.collection("cluster_config").document("cluster_config").set(DEFAULT_CLUSTER_CONFIG)
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
        f"--min-instances 1 "
        f"--max-instances 20 "
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
    msg = f"\nSuccess! To view your new dashboard run `burla dashboard`\n"
    msg += f"Quickstart:\n"
    msg += f'  1. Start your cluster by hitting "⏻ Start" in the dashboard\n'
    msg += f"  2. Import and call `remote_parallel_map`!\n\n"
    msg += f"Don't hesitate to E-Mail jake@burla.dev, or call me at 508-320-8778, thank you for using Burla!"
    spinner.write(msg)

    _log_telemetry("Burla successfully installed!", project_id=PROJECT_ID)
