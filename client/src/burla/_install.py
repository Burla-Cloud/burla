import sys
import shutil
import traceback
import requests
import json
import tempfile
from pathlib import Path
from time import time, sleep

from yaspin import yaspin
from google.cloud.firestore import Client
from google.api_core.exceptions import NotFound

from burla import _BURLA_BACKEND_URL, __version__
from burla._helpers import log_telemetry, run_command, VerboseCalledProcessError

_python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
DEFAULT_CLUSTER_CONFIG = {
    "Nodes": [
        {
            "containers": [
                {
                    "image": f"python:{_python_version}",
                    "python_version": _python_version,
                }
            ],
            "inactivity_shutdown_time_sec": 900,
            "machine_type": "n4-standard-80",
            "quantity": 13,
            "disk_size_gb": 50,
            "gcp_region": "us-central1",
        }
    ]
}


class InstallError(Exception):
    pass


class AuthError(Exception):
    pass


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
            # Raises error with a super clear message at the end of the traceback.
            # yes this is hacky but I need to make sure users of all skill levels see this message.
            msg = f"\n\nIf you're not sure what to do, please email jake@burla.dev!\n"
            msg += f"We take errors very seriously, and would really like to help you get Burla installed!\n-"
            try:
                exc_cls = e.__class__
                old_str = exc_cls.__str__

                def new_str(self):
                    return f"{old_str(self)}\n\n{msg}"

                if getattr(exc_cls, "_burla_str_patched", False) is False:
                    exc_cls.__str__ = new_str
                    exc_cls._burla_str_patched = True
            except Exception:
                raise InstallError(msg) from e
            raise e


def _install(spinner):
    log_telemetry("Somebody is running `burla install`!")
    _check_gcloud_is_installed(spinner)

    # TODO: re-enable
    # If I remember correctly this was disabled because in the case that the user is not logged in,
    # instead of throwing an error, gcloud simple freezes for almost 2 minutes.
    # I could be wrong I don't fully remember why I commented this out.
    # _check_gcloud_is_logged_in(spinner)

    PROJECT_ID = _get_gcloud_GCP_project_id(spinner)
    log_telemetry("Installer has gcloud and is logged in.", project_id=PROJECT_ID)

    spinner.text = "Enabling required services ... "
    spinner.start()
    run_command("gcloud services enable compute.googleapis.com")
    run_command("gcloud services enable run.googleapis.com")
    run_command("gcloud services enable firestore.googleapis.com")
    run_command("gcloud services enable cloudresourcemanager.googleapis.com")
    run_command("gcloud services enable secretmanager.googleapis.com")
    run_command("gcloud services enable storage.googleapis.com")
    run_command("gcloud services enable logging.googleapis.com")
    run_command("gcloud services enable iamcredentials.googleapis.com")
    spinner.text = "Enabling required services... Done."
    spinner.ok("✓")

    _open_port_8080_to_VMs_with_tag_burla_cluster_node(spinner)

    _create_gcs_bucket(spinner, PROJECT_ID)

    # create cluster id token secret (must exist for service accounts to be created)
    cmd = 'gcloud secrets create burla-cluster-id-token --replication-policy="automatic"'
    create_cmd_result = run_command(cmd, raise_error=False)
    cmd_threw_error = create_cmd_result.returncode != 0
    if cmd_threw_error and ("already exists" not in create_cmd_result.stderr.decode()):
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, create_cmd_result.stderr)

    # create service accounts: main-service, compute-engine-default, client-user
    main_svc_account_email, client_svc_account_key = _create_service_accounts(spinner, PROJECT_ID)

    _create_firestore_database(spinner, PROJECT_ID)

    cluster_id_token = _register_cluster_and_save_cluster_id_token(
        spinner, PROJECT_ID, client_svc_account_key
    )

    # Deploy dashboard as google cloud run service
    spinner.text = "Deploying burla-main-service to Google Cloud Run ... "
    spinner.start()
    image_name = f"us-docker.pkg.dev/burla-prod/burla-main-service/burla-main-service:{__version__}"
    run_command(
        f"gcloud run deploy burla-main-service "
        f"--image={image_name} "
        f"--project {PROJECT_ID} "
        f"--region=us-central1 "
        f"--service-account {main_svc_account_email} "
        f"--min-instances 1 "
        f"--max-instances 5 "
        f"--memory 4Gi "
        f"--cpu 1 "
        f"--timeout 60 "
        f"--concurrency 20 "
        f"--allow-unauthenticated"
    )
    run_command(
        f"gcloud run services update-traffic burla-main-service "
        f"--project {PROJECT_ID} "
        f"--region=us-central1 "
        f"--to-latest"
    )

    # register dashboard url with backend service
    result = run_command("gcloud run services describe burla-main-service --region us-central1")
    dashboard_url = None
    for line in result.stdout.decode().splitlines():
        if line.startswith("URL:"):
            dashboard_url = line.split()[1]
    if not dashboard_url:
        spinner.fail("✗")
        raise Exception("Dashboard URL not returned by command: gcloud run services describe ...")
    url = f"{_BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/dashboard_url"
    headers = {"Authorization": f"Bearer {cluster_id_token}"}
    response = requests.post(url, json={"dashboard_url": dashboard_url}, headers=headers)
    response.raise_for_status()
    spinner.text = "Deploying Burla-Main-Service to Google Cloud Run ... Done."
    spinner.ok("✓")

    # update cluster version recorded in burla's cloud
    cmd = "gcloud secrets versions access latest --secret=burla-cluster-id-token"
    result = run_command(cmd, raise_error=False)
    if result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)
    else:
        headers = {"Authorization": f"Bearer {result.stdout.decode().strip()}"}
        url = f"{_BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/version"
        response = requests.put(url, json={"version": __version__}, headers=headers)
        response.raise_for_status()

    # print success message
    msg = f"\nSuccessfully installed Burla v{__version__}!\n"
    msg += f"Quickstart:\n"
    msg += f"  1. Open your new cluster dashboard: {dashboard_url}\n"
    msg += f'  2. Hit "⏻ Start" to boot some machines!\n'
    msg += f"  3. Run `burla login` to connect your laptop to the cluster.\n"
    msg += f"  4. Import and call `remote_parallel_map`!\n\n"
    msg += f"Don't hesitate to E-Mail jake@burla.dev, thank you for using Burla!"
    spinner.write(msg)

    log_telemetry("Burla successfully installed!", project_id=PROJECT_ID)


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


def _check_gcloud_is_logged_in(spinner):
    spinner.text = "Checking for gcloud credentials ... "
    spinner.start()
    cmd = "gcloud auth list --filter=status:ACTIVE --format='value(account)'"
    result = run_command(cmd, raise_error=False)
    if result.returncode != 0 and result.stdout == b"":
        spinner.fail("✗")
        msg = "ERROR: You are not logged in with gcloud.\n"
        msg += "Please run 'gcloud auth login' before installing Burla."
        print("")
        print(msg, file=sys.stderr)
        log_telemetry("User has gcloud but is not logged in.")
        sys.exit(1)

    cmd = "gcloud auth application-default print-access-token 2>/dev/null"
    result = run_command(cmd, raise_error=False)
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


def _register_cluster_and_save_cluster_id_token(spinner, PROJECT_ID, client_svc_account_key):
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
    cluster_info = {"client_svc_account_key": client_svc_account_key}
    response = requests.post(f"{_BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}", json=cluster_info)
    if response.status_code == 403:
        spinner.fail("✗")
        msg = "Cluster ID secret is missing, but this deployment has already been registered.\n"
        msg += "Because this secret is missing, we cannot verify that you are the owner of this cluster.\n"
        msg += "Please email jake@burla.dev, "
        msg += "or DM @jake__z in our Discord to regain access!"
        raise AuthError(msg)
    elif response.status_code == 200:
        cluster_id_token = response.json()["token"]
    elif response.status_code != 409:
        spinner.fail("✗")
        raise Exception(f"Error registering cluster: {response.status_code} {response.text}")

    # rotate cluster token / service account key
    headers = {"Authorization": f"Bearer {cluster_id_token}"}
    url = f"{_BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/token"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    cluster_id_token = response.json()["token"]
    # rotate client key
    headers = {"Authorization": f"Bearer {cluster_id_token}"}  # dont remove this, look closer first
    cluster_info = {"client_svc_account_key": client_svc_account_key}
    url = f"{_BURLA_BACKEND_URL}/v1/clusters/{PROJECT_ID}/client_key"
    response = requests.post(url, json=cluster_info, headers=headers)
    response.raise_for_status()

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

    # initiate create CLIENT USER svc account
    client_svc_account_name = "burla-client-user"
    client_svc_email = f"{client_svc_account_name}@{PROJECT_ID}.iam.gserviceaccount.com"
    cmd = f"gcloud iam service-accounts create {client_svc_account_name} "
    cmd += f" --display-name='{client_svc_account_name}'"
    result = run_command(cmd, raise_error=False)
    if result.returncode != 0 and "already exists" in result.stderr.decode():
        client_svc_account_already_exists = True
    elif result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)
    else:
        client_svc_account_already_exists = False

    # Get reference to COMPUTE ENGINE service-account (GCP project num)
    result = run_command(f"gcloud projects describe {PROJECT_ID} --format='value(projectNumber)'")
    gcp_project_num = result.stdout.decode().strip()
    compute_engine_email = f"{gcp_project_num}-compute@developer.gserviceaccount.com"

    # wait for all 3 service accounts to exist:
    start = time()
    all_accounts_exist = False
    while not all_accounts_exist:
        sleep(1)
        for email in [main_svc_email, client_svc_email, compute_engine_email]:
            cmd = f"gcloud iam service-accounts describe {email}"
            all_accounts_exist = run_command(cmd, raise_error=False).returncode == 0
        if (time() - start) > 120:
            spinner.fail("✗")
            raise Exception("svc account not found 120s after successful create cmd.")

    # wait service account to propagate, it sonetimes still does not exist even after
    # `gcloud iam service-accounts describe` succeeds because it has yet to propagate fully.
    sleep(5)

    # apply roles to burla-main-service svc account:
    # can't attach `burla db only` condition to this, in addition to others for some reason:
    cmd = f"gcloud projects add-iam-policy-binding {PROJECT_ID}"
    cmd += f" --member=serviceAccount:{main_svc_email} --role=roles/datastore.user"
    cmd += f" --condition=None"
    run_command(cmd)
    cmd = f"gcloud projects add-iam-policy-binding {PROJECT_ID}"
    cmd += f" --member=serviceAccount:{main_svc_email} --role=roles/logging.logWriter"
    cmd += f" --condition=None"
    run_command(cmd)
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
    cmd = f"gcloud secrets add-iam-policy-binding burla-cluster-id-token"
    cmd += f' --member="serviceAccount:{main_svc_email}"'
    cmd += f' --role="roles/secretmanager.secretAccessor"'
    run_command(cmd)

    # apply roles to burla-client-user svc account:
    condition_title = "Allow Firestore access to burla db only"
    condition_expression = f'resource.name=="projects/{PROJECT_ID}/databases/burla"'
    cmd = (
        f"gcloud projects add-iam-policy-binding {PROJECT_ID} "
        f"--member=serviceAccount:{client_svc_email} "
        f"--role=roles/datastore.user "
        f"--condition='expression={condition_expression},title={condition_title}'"
    )
    run_command(cmd)

    # delete any existing keys before creating new one (rotate the keys)
    cmd = "gcloud iam service-accounts keys list "
    cmd += f"--iam-account={client_svc_email} --project={PROJECT_ID} --format=json"
    sa_keys = json.loads(run_command(cmd).stdout.decode() or "[]")
    key_ids = [k["name"].split("/")[-1] for k in sa_keys if k.get("keyType") == "USER_MANAGED"]
    for key_id in key_ids:
        cmd = f"gcloud iam service-accounts keys delete {key_id} "
        cmd += f"--iam-account={client_svc_email} --project={PROJECT_ID} --quiet"
        run_command(cmd)

    # issue service account key for burla-client-user
    with tempfile.TemporaryDirectory() as temporary_directory:
        key_path = Path(temporary_directory) / "key.json"
        run_command(
            f"gcloud iam service-accounts keys create {str(key_path)} "
            f"--iam-account={client_svc_email} --project={PROJECT_ID}"
        )
        client_svc_account_key = json.loads(key_path.read_text())

    # allow compute engine service account to use burla token secret
    cmd = f"gcloud secrets add-iam-policy-binding burla-cluster-id-token"
    cmd += f' --member="serviceAccount:{compute_engine_email}"'
    cmd += f' --role="roles/secretmanager.secretAccessor"'
    run_command(cmd)
    # allow dashboard to create vm instances having the default compute engine service account
    cmd = f"gcloud iam service-accounts add-iam-policy-binding {compute_engine_email}"
    cmd += f' --member="serviceAccount:{main_svc_email}"'
    cmd += f' --role="roles/iam.serviceAccountUser"'
    run_command(cmd)

    if main_svc_svc_account_already_exists and client_svc_account_already_exists:
        spinner.text = "Creating service accounts ... Accounts already exist."
    else:
        spinner.text = "Creating service accounts ... Done."
    spinner.ok("✓")
    return main_svc_email, client_svc_account_key


def _create_firestore_database(spinner, PROJECT_ID):
    spinner.text = "Creating Firestore database ... "
    spinner.start()
    client = Client(database="burla", project=PROJECT_ID)

    # cannot do this at the top because PROJECT_ID is required
    DEFAULT_CLUSTER_CONFIG["gcs_bucket_name"] = f"{PROJECT_ID}-burla-shared-workspace"

    cmd = "gcloud firestore databases create --database=burla"
    cmd += f" --location=us-central1 --type=firestore-native"
    result = run_command(cmd, raise_error=False)
    already_exists = False
    if result.returncode != 0 and "already exists" in result.stderr.decode():
        already_exists = True
    elif result.returncode != 0:
        spinner.fail("✗")
        raise VerboseCalledProcessError(cmd, result.stderr)

    try:
        collection = client.collection("cluster_config")
        collection.document("cluster_config").set(DEFAULT_CLUSTER_CONFIG)
    except NotFound as e:
        # retry until db is ready or 30s
        start = time()
        while True:
            try:
                collection = client.collection("cluster_config")
                collection.document("cluster_config").set(DEFAULT_CLUSTER_CONFIG)
                break
            except NotFound as e:
                sleep(1)
                if time() - start >= 30:
                    raise e

    if already_exists:
        spinner.text = "Creating Firestore database ... Database already exists."
    else:
        spinner.text = "Creating Firestore database ... Done."
    spinner.ok("✓")
