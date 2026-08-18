"""
`burla deploy --cloud=aws`.

Mirrors the GCP deploy: IAM roles, security groups, an S3 shared-workspace
bucket, and one small always-on head EC2 instance running main_service.
Node VMs use Burla's public regional AMIs.
"""

import json
import os
import shutil
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from time import sleep, time
from urllib.parse import urlparse

import requests

from burla import _BURLA_BACKEND_URL, _BURLA_NODE_SOURCE_REF, __version__
from burla._aws_amis import node_ami_hash as _node_ami_hash
from burla._aws_amis import node_ami_name_prefix, public_node_ami_id
from burla._aws_amis import node_ami_setup_script as _node_ami_setup_script
from burla._deploy import (
    FRP_VERSION,
    RELAY_HOST,
    RELAY_SERVER_ADDR,
    RELAY_SERVER_PORT,
    head_install_spec,
)
from burla._helpers import VerboseCalledProcessError, run_command
from burla._reporting import log_telemetry

HEAD_INSTANCE_TYPE = "t3.small"
AMI_BUILDER_INSTANCE_TYPE = "t3.large"


def _head_setup_commands(
    project_id: str,
    region: str,
    dashboard_hostname: str,
    cluster_id_token: str,
    account_name: str,
) -> list[str]:
    node_source_ref = _BURLA_NODE_SOURCE_REF
    install_spec = head_install_spec()
    relay_subdomain = f"head--{project_id}"
    return [
        "set -eu",
        "export DEBIAN_FRONTEND=noninteractive",
        "apt-get update",
        "apt-get install -y docker.io awscli",
        "systemctl enable --now docker",
        "systemctl enable --now snap.amazon-ssm-agent.amazon-ssm-agent.service || true",
        "systemctl disable --now burla-main-service.service || true",
        "mkdir -p /var/lib/burla/tls /var/lib/burla/caddy /etc/burla",
        "docker pull python:3.13",
        "docker pull caddy:2.10.2-alpine",
        f'CLUSTER_ID_TOKEN="{cluster_id_token}"',
        "docker rm -f burla-main-service burla-head-caddy burla-head-frpc || true",
        (
            "docker run -d --restart=always --network=host --name=burla-main-service "
            "-v /var/lib/burla:/var/lib/burla "
            f'-e PROJECT_ID="{project_id}" '
            '-e CLUSTER_ID_TOKEN="$CLUSTER_ID_TOKEN" '
            "-e BURLA_HEAD_RUNTIME=True "
            "-e CLOUD_PROVIDER=aws "
            f'-e AWS_REGION="{region}" '
            f'-e CLOUD_ACCOUNT_NAME="{account_name}" '
            "-e BIND_HOST=127.0.0.1 "
            "-e PORT=5001 "
            "-e HISTORY_DB_PATH=/var/lib/burla/history.db "
            f'-e SHARED_WORKSPACE_BUCKET="{project_id}-burla-shared-workspace" '
            f'-e BURLA_BACKEND_URL="{_BURLA_BACKEND_URL}" '
            f'-e BURLA_RELAY_HOST="{RELAY_HOST}" '
            f'-e BURLA_RELAY_SERVER_ADDR="{RELAY_SERVER_ADDR}" '
            f'-e BURLA_RELAY_SERVER_PORT="{RELAY_SERVER_PORT}" '
            f'-e BURLA_NODE_SOURCE_REF="{node_source_ref}" '
            "python:3.13 "
            f'sh -c \'pip install --no-cache-dir "{install_spec}" '
            "&& exec python -m uvicorn main_service:app "
            "--host 127.0.0.1 --port 5001 --workers 1 --timeout-keep-alive 60'"
        ),
        (
            "until curl --fail --silent http://127.0.0.1:5001/version >/dev/null; "
            "do sleep 1; done"
        ),
        "rm -rf /etc/burla/Caddyfile",
        (
            "cat > /etc/burla/Caddyfile <<EOF\n"
            f"{dashboard_hostname} {{\n"
            "  reverse_proxy 127.0.0.1:5001\n"
            "}\n"
            "EOF"
        ),
        (
            "docker run -d --restart=always --network=host --name=burla-head-caddy "
            "-v /etc/burla/Caddyfile:/etc/caddy/Caddyfile:ro "
            "-v /var/lib/burla/caddy:/data "
            "caddy:2.10.2-alpine caddy run --config /etc/caddy/Caddyfile --adapter caddyfile"
        ),
        (
            "cat > /etc/burla/frpc.toml <<EOF\n"
            f'serverAddr = "{RELAY_SERVER_ADDR}"\n'
            f"serverPort = {RELAY_SERVER_PORT}\n"
            "loginFailExit = false\n"
            f'user = "{project_id}"\n'
            'metadatas.token = "$CLUSTER_ID_TOKEN"\n'
            "transport.poolCount = 4\n"
            "\n"
            "[[proxies]]\n"
            f'name = "{relay_subdomain}"\n'
            'type = "https"\n'
            'localIP = "127.0.0.1"\n'
            "localPort = 443\n"
            f'subdomain = "{relay_subdomain}"\n'
            "EOF"
        ),
        "chmod 600 /etc/burla/frpc.toml",
        (
            "docker run -d --restart=always --network=host --name=burla-head-frpc "
            "-v /etc/burla/frpc.toml:/etc/frp/frpc.toml:ro "
            f"fatedier/frpc:v{FRP_VERSION} -c /etc/frp/frpc.toml"
        ),
    ]


def _aws(cmd: str, parse_json: bool = True, raise_error: bool = True):
    result = run_command(f"aws {cmd}", raise_error=raise_error)
    if result.returncode != 0:
        return None
    stdout = result.stdout.decode().strip()
    if parse_json and stdout:
        return json.loads(stdout)
    return stdout


def deploy_aws(spinner):
    log_telemetry("Somebody is running `burla deploy --cloud=aws`!")

    spinner.text = "Checking for aws CLI ... "
    spinner.start()
    if shutil.which("aws") is None:
        spinner.fail("✗")
        msg = "Error: The AWS CLI is not installed or not in your PATH.\n"
        msg += "Please install it from: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html"
        print(msg, file=sys.stderr)
        log_telemetry("User does not have the aws CLI installed.")
        sys.exit(1)

    identity = _aws("sts get-caller-identity")
    account_id = identity["Account"]
    region = (
        _aws("configure get region", parse_json=False, raise_error=False) or "us-east-1"
    )
    # Cluster id: what backend.burla.dev and the dashboard know this cluster as.
    project_id = f"aws-{account_id}"
    from burla._local_head import _aws_account_name

    account_name = _aws_account_name(account_id)
    spinner.text = f"Checking for aws CLI ... Using account {account_id} in {region}."
    spinner.ok("✓")
    log_telemetry("Installer has aws CLI and is logged in.", project_id=project_id)

    bucket_name = f"{project_id}-burla-shared-workspace"
    _create_s3_bucket(spinner, bucket_name, region)
    _create_iam(spinner, account_id, bucket_name)
    head_sg_id = _create_head_security_group(spinner, region)
    cluster_id_token = _register_cluster_and_save_token(spinner, project_id, region)

    from burla._deploy import _snapshot_local_history, _upload_local_history

    first_deploy = _existing_head_instance(region) is None
    snapshot_path, paused_head_url = (None, None)
    if first_deploy:
        snapshot_path, paused_head_url = _snapshot_local_history(spinner, project_id)

    try:
        dashboard_url = _deploy_head_instance(
            spinner, project_id, region, head_sg_id, cluster_id_token, account_name
        )
        if snapshot_path:
            _upload_local_history(
                spinner, dashboard_url, cluster_id_token, snapshot_path
            )
        if first_deploy:
            from burla._local_head import finish_history_migration

            finish_history_migration(project_id)
    except BaseException:
        if paused_head_url:
            from burla._local_head import resume_history_migration

            resume_history_migration(project_id, paused_head_url)
        raise
    finally:
        if snapshot_path:
            os.remove(snapshot_path)

    headers = {"Authorization": f"Bearer {cluster_id_token}"}
    url = f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}/version"
    response = requests.put(url, json={"version": __version__}, headers=headers)
    response.raise_for_status()

    # Point this machine's client at the freshly deployed cluster.
    from burla._auth import save_deployed_cluster_config

    save_deployed_cluster_config("aws", project_id, cluster_id_token, dashboard_url)

    msg = f"\nSuccessfully deployed Burla v{__version__} on AWS!\n"
    msg += "Quickstart:\n"
    msg += f"  1. Open your new cluster dashboard: {dashboard_url}\n"
    msg += '  2. Hit "⏻ Start" to boot some machines.\n'
    msg += "  3. Run `burla login` to connect your laptop to the cluster.\n"
    msg += "  4. Import and call `remote_parallel_map`!\n\n"
    msg += "Don't hesitate to E-Mail jake@burla.dev, thank you for using Burla!"
    spinner.write(msg)

    log_telemetry("Burla successfully deployed on AWS!", project_id=project_id)


def _create_s3_bucket(spinner, bucket_name, region):
    spinner.text = "Creating S3 bucket ... "
    spinner.start()
    location_arg = (
        ""
        if region == "us-east-1"
        else (f" --create-bucket-configuration LocationConstraint={region}")
    )
    result = run_command(
        f"aws s3api create-bucket --bucket {bucket_name} --region {region}{location_arg}",
        raise_error=False,
    )
    stderr = result.stderr.decode()
    already_exists = (
        "BucketAlreadyOwnedByYou" in stderr or "BucketAlreadyExists" in stderr
    )
    if result.returncode != 0 and not already_exists:
        spinner.fail("✗")
        raise VerboseCalledProcessError("aws s3api create-bucket", result.stderr)

    cors = {
        "CORSRules": [
            {
                "AllowedOrigins": ["*"],
                "AllowedMethods": ["GET", "HEAD", "POST", "PUT", "DELETE"],
                "AllowedHeaders": ["*"],
                "ExposeHeaders": ["Content-Type", "Content-Length", "Location"],
                "MaxAgeSeconds": 3600,
            }
        ]
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json") as cors_file:
        json.dump(cors, cors_file)
        cors_file.flush()
        run_command(
            f"aws s3api put-bucket-cors --bucket {bucket_name} "
            f"--cors-configuration file://{cors_file.name}"
        )

    suffix = "Bucket already exists." if already_exists else "Done."
    spinner.text = f"Creating S3 bucket ... {suffix}"
    spinner.ok("✓")


def _create_role_with_policy(role_name, policy):
    trust = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    with tempfile.NamedTemporaryFile("w", suffix=".json") as trust_file:
        json.dump(trust, trust_file)
        trust_file.flush()
        result = run_command(
            f"aws iam create-role --role-name {role_name} "
            f"--assume-role-policy-document file://{trust_file.name}",
            raise_error=False,
        )
        if (
            result.returncode != 0
            and "EntityAlreadyExists" not in result.stderr.decode()
        ):
            raise VerboseCalledProcessError(
                f"aws iam create-role {role_name}", result.stderr
            )

    with tempfile.NamedTemporaryFile("w", suffix=".json") as policy_file:
        json.dump(policy, policy_file)
        policy_file.flush()
        run_command(
            f"aws iam put-role-policy --role-name {role_name} "
            f"--policy-name {role_name}-policy --policy-document file://{policy_file.name}"
        )

    result = run_command(
        f"aws iam create-instance-profile --instance-profile-name {role_name}",
        raise_error=False,
    )
    if result.returncode != 0 and "EntityAlreadyExists" not in result.stderr.decode():
        raise VerboseCalledProcessError(
            f"aws iam create-instance-profile {role_name}", result.stderr
        )
    run_command(
        f"aws iam add-role-to-instance-profile --instance-profile-name {role_name} "
        f"--role-name {role_name}",
        raise_error=False,  # fails harmlessly when the role is already attached
    )


def _create_iam(spinner, account_id, bucket_name):
    spinner.text = "Creating IAM roles ... "
    spinner.start()

    bucket_arns = [f"arn:aws:s3:::{bucket_name}", f"arn:aws:s3:::{bucket_name}/*"]

    node_policy = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "s3:*", "Resource": bucket_arns}],
    }
    _create_role_with_policy("burla-node", node_policy)

    head_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "ec2:RunInstances",
                    "ec2:TerminateInstances",
                    "ec2:DescribeInstances",
                    "ec2:DescribeImages",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeSecurityGroups",
                    "ec2:CreateTags",
                ],
                "Resource": "*",
            },
            {
                "Effect": "Allow",
                "Action": "iam:PassRole",
                "Resource": f"arn:aws:iam::{account_id}:role/burla-node",
            },
            {
                "Effect": "Allow",
                "Action": [
                    "ecr:GetAuthorizationToken",
                    "ecr:BatchCheckLayerAvailability",
                    "ecr:GetDownloadUrlForLayer",
                    "ecr:BatchGetImage",
                ],
                "Resource": "*",
            },
            {"Effect": "Allow", "Action": "s3:*", "Resource": bucket_arns},
        ],
    }
    _create_role_with_policy("burla-main-service", head_policy)
    run_command(
        "aws iam attach-role-policy --role-name burla-main-service "
        "--policy-arn arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
    )

    # IAM is eventually consistent; instance-profile propagation takes a bit.
    sleep(10)
    spinner.text = "Creating IAM roles ... Done."
    spinner.ok("✓")


def _get_or_create_security_group(name, description, region):
    existing = _aws(
        f"ec2 describe-security-groups --region {region} "
        f'--filters Name=group-name,Values={name} --query "SecurityGroups[0].GroupId" --output json',
        raise_error=False,
    )
    if existing:
        return existing
    return _aws(
        f"ec2 create-security-group --region {region} --group-name {name} "
        f'--description "{description}" --query GroupId --output json'
    )


def _create_head_security_group(spinner, region):
    """The head and nodes dial out to the relay, so the head needs no ingress."""
    spinner.text = "Creating head security group ... "
    spinner.start()
    head_sg = _get_or_create_security_group(
        "burla-head", "Burla main_service head VM", region
    )
    spinner.text = "Creating head security group ... Done."
    spinner.ok("✓")
    return head_sg


def _aws_ownership_payload(region: str) -> dict:
    import boto3

    sts_url = boto3.client("sts", region_name=region).generate_presigned_url(
        "get_caller_identity",
        ExpiresIn=60,
    )
    ec2 = boto3.client("ec2", region_name=region)
    create_security_group_params = {
        "GroupName": "burla-ownership-check",
        "Description": "Burla ownership permission check",
        "DryRun": True,
    }
    ec2_dry_run_url = ec2.generate_presigned_url(
        "create_security_group",
        Params=create_security_group_params,
        ExpiresIn=60,
    )
    return {
        "cloud": "aws",
        "sts_url": sts_url,
        "ec2_dry_run_url": ec2_dry_run_url,
    }


def _register_cluster_and_save_token(spinner, project_id, region):
    """The cluster token lives in Burla's local state dir (and, for clusters
    installed before 1.7, in SSM, which is read as a fallback)."""
    from burla._deploy import AuthError
    from burla._local_head import LocalHeadError, get_or_register_cluster_token

    spinner.text = "Registering cluster ... "
    spinner.start()

    try:
        cluster_id_token = get_or_register_cluster_token("aws", project_id, region)
    except LocalHeadError:
        spinner.fail("✗")
        raise AuthError()

    # ensure deployer is authorized
    installer_email = None
    arn = _aws("sts get-caller-identity --query Arn --output json")
    if arn and "@" in arn:
        installer_email = arn.split("/")[-1]
    if installer_email:
        headers = {"Authorization": f"Bearer {cluster_id_token}"}
        users_url = f"{_BURLA_BACKEND_URL}/v1/clusters/{project_id}/users"
        requests.post(users_url, json={"new_user": installer_email}, headers=headers)
    else:
        msg = "Could not infer your email from your AWS identity. After deploying, run "
        msg += "`burla login` to authorize yourself against this cluster."
        spinner.write(msg)

    spinner.text = "Registering cluster ... Done."
    spinner.ok("✓")
    return cluster_id_token


def _latest_ubuntu_ami(region) -> str:
    return _aws(
        f"ssm get-parameter --region {region} "
        f"--name /aws/service/canonical/ubuntu/server/22.04/stable/current/amd64/hvm/ebs-gp2/ami-id "
        f'--query "Parameter.Value" --output json'
    )


def _existing_node_ami(region: str, gpu: bool) -> str | None:
    variant = "gpu" if gpu else "nogpu"
    return _aws(
        f"ec2 describe-images --region {region} --owners self "
        f"--filters Name=name,Values=burla-node-{variant}-* "
        f"Name=tag:burla-node-image,Values=true "
        f"Name=tag:burla-node-image-hash,Values={_node_ami_hash(gpu)} "
        f"Name=state,Values=available "
        f'--query "sort_by(Images, &CreationDate)[-1].ImageId" --output json',
        raise_error=False,
    )


def _build_node_ami(region: str, gpu: bool) -> str:
    variant = "gpu" if gpu else "nogpu"
    ami_hash = _node_ami_hash(gpu)
    base_ami = _latest_ubuntu_ami(region)
    with tempfile.NamedTemporaryFile("w", suffix=".sh") as user_data_file:
        user_data_file.write(_node_ami_setup_script(gpu))
        user_data_file.flush()
        instance = _aws(
            f"ec2 run-instances --region {region} --image-id {base_ami} "
            f"--instance-type {AMI_BUILDER_INSTANCE_TYPE} "
            f"--block-device-mappings "
            f'\'[{{"DeviceName":"/dev/sda1","Ebs":{{"VolumeSize":20,"VolumeType":"gp3"}}}}]\' '
            f"--user-data file://{user_data_file.name} "
            f"--tag-specifications "
            f"'ResourceType=instance,Tags=[{{Key=Name,Value=burla-ami-builder-{variant}}}]'"
        )
    builder_id = instance["Instances"][0]["InstanceId"]

    try:
        # run-instances is eventually consistent: an immediate describe/wait can
        # get InvalidInstanceID.NotFound and abort, so wait for visibility first.
        run_command(
            f"aws ec2 wait instance-exists --region {region} --instance-ids {builder_id}",
            raise_error=False,
        )
        # Direct polling lets slow package mirrors exceed AWS's 10-minute waiter cap.
        deadline = time() + 3600
        state = None
        while time() < deadline:
            state = _aws(
                f"ec2 describe-instances --region {region} --instance-ids {builder_id} "
                f'--query "Reservations[0].Instances[0].State.Name" --output json'
            )
            if state == "stopped":
                break
            sleep(15)
        if state != "stopped":
            raise Exception(
                f"AMI builder instance {builder_id} ({variant}) never stopped "
                f"(state={state}). Check its console output, then terminate it "
                "and re-run deploy."
            )

        image = _aws(
            f"ec2 create-image --region {region} --instance-id {builder_id} "
            f'--name "{node_ami_name_prefix(gpu)}-{int(time())}" --output json'
        )
        ami_id = image["ImageId"]
        run_command(
            f"aws ec2 create-tags --region {region} --resources {ami_id} "
            f"--tags Key=burla-node-image,Value=true "
            f"Key=burla-node-image-hash,Value={ami_hash}"
        )
        run_command(
            f"aws ec2 wait image-available --region {region} --image-ids {ami_id}"
        )
    finally:
        run_command(
            f"aws ec2 terminate-instances --region {region} --instance-ids {builder_id}",
            raise_error=False,
        )
    return ami_id


def _ensure_node_ami(spinner, region):
    """Maintainer helper used by scripts/publish_aws_amis.py."""
    missing = [gpu for gpu in (False, True) if not _existing_node_ami(region, gpu)]
    if not missing:
        spinner.text = "Node AMIs ... using existing."
        spinner.ok("✓")
        return

    variants = ", ".join("gpu" if gpu else "nogpu" for gpu in missing)
    spinner.text = (
        f"Building node AMI(s): {variants} "
        "(takes ~10 minutes, only when the image changes) ... "
    )
    spinner.start()

    with ThreadPoolExecutor(max_workers=len(missing)) as pool:
        futures = [pool.submit(_build_node_ami, region, gpu) for gpu in missing]
    # the with-block waits for every build; surface the first failure after all
    # builders have terminated so none is left running.
    for future in futures:
        try:
            future.result()
        except BaseException:
            spinner.fail("✗")
            raise

    spinner.text = f"Building node AMI(s): {variants} ... Done."
    spinner.ok("✓")


def _run_head_update(region: str, instance_id: str, commands: list[str]):
    start = time()
    while time() - start < 180:
        managed = _aws(
            f"ssm describe-instance-information --region {region} "
            f"--filters Key=InstanceIds,Values={instance_id} "
            '--query "InstanceInformationList[0].InstanceId" --output json',
            raise_error=False,
        )
        if managed:
            break
        sleep(5)
    else:
        raise Exception(f"Head instance {instance_id} did not register with SSM")

    parameters = {"commands": commands}
    with tempfile.NamedTemporaryFile("w", suffix=".json") as parameters_file:
        json.dump(parameters, parameters_file)
        parameters_file.flush()
        response = _aws(
            f"ssm send-command --region {region} --instance-ids {instance_id} "
            "--document-name AWS-RunShellScript "
            f"--parameters file://{parameters_file.name}"
        )
    command_id = response["Command"]["CommandId"]
    start = time()
    invocation = None
    while time() - start < 900:
        invocation = _aws(
            f"ssm get-command-invocation --region {region} "
            f"--command-id {command_id} --instance-id {instance_id}",
            raise_error=False,
        )
        if invocation and invocation["Status"] in {
            "Success",
            "Cancelled",
            "Failed",
            "TimedOut",
        }:
            break
        sleep(5)
    if invocation is None:
        raise Exception(f"SSM command {command_id} did not finish")
    if invocation["Status"] != "Success":
        raise Exception(invocation["StandardErrorContent"])


def _existing_head_instance(region) -> dict | None:
    return _aws(
        f"ec2 describe-instances --region {region} "
        f"--filters Name=tag:Name,Values=burla-main-service "
        f"Name=instance-state-name,Values=pending,running,stopping,stopped "
        f'--query "Reservations[0].Instances[0]" --output json',
        raise_error=False,
    )


def _deploy_head_instance(
    spinner, project_id, region, head_sg_id, cluster_id_token, account_name
) -> str:
    spinner.text = "Deploying burla-main-service instance ... "
    spinner.start()

    existing = _existing_head_instance(region)
    allocation = _aws(
        f"ec2 describe-addresses --region {region} "
        f"--filters Name=tag:Name,Values=burla-main-service "
        f'--query "Addresses[0]" --output json',
        raise_error=False,
    )
    if not allocation:
        allocation = _aws(
            f"ec2 allocate-address --region {region} --domain vpc "
            f"--tag-specifications "
            f"'ResourceType=elastic-ip,Tags=[{{Key=Name,Value=burla-main-service}}]'"
        )
    allocation_id = allocation["AllocationId"]
    public_ip = allocation["PublicIp"]
    if existing and existing["State"]["Name"] != "running":
        run_command(
            f"aws ec2 start-instances --region {region} "
            f"--instance-ids {existing['InstanceId']}"
        )
        run_command(
            f"aws ec2 wait instance-running --region {region} "
            f"--instance-ids {existing['InstanceId']}"
        )
        existing["State"]["Name"] = "running"

    from burla._deploy import _register_dashboard, _shutdown_cluster_for_upgrade

    dashboard_url = f"https://head--{project_id}.{RELAY_HOST}"
    if existing:
        _shutdown_cluster_for_upgrade(
            project_id,
            cluster_id_token,
            dashboard_url,
        )
    _register_dashboard(
        project_id,
        cluster_id_token,
        public_ip,
        _aws_ownership_payload(region),
        dashboard_url,
    )
    commands = _head_setup_commands(
        project_id,
        region,
        urlparse(dashboard_url).hostname,
        cluster_id_token,
        account_name,
    )
    if existing:
        head_id = existing["InstanceId"]
        _run_head_update(region, head_id, commands)
    else:
        user_data = "#!/bin/bash\n" + "\n".join(commands) + "\n"
        base_ami = _latest_ubuntu_ami(region)
        with tempfile.NamedTemporaryFile("w", suffix=".sh") as user_data_file:
            user_data_file.write(user_data)
            user_data_file.flush()
            instance = _aws(
                f"ec2 run-instances --region {region} --image-id {base_ami} "
                f"--instance-type {HEAD_INSTANCE_TYPE} "
                f"--iam-instance-profile Name=burla-main-service "
                f"--security-group-ids {head_sg_id} "
                f"--user-data file://{user_data_file.name} "
                f"--tag-specifications "
                f"'ResourceType=instance,Tags=[{{Key=Name,Value=burla-main-service}}]'"
            )
        head_id = instance["Instances"][0]["InstanceId"]
        run_command(
            f"aws ec2 wait instance-exists --region {region} --instance-ids {head_id}",
            raise_error=False,
        )
        run_command(
            f"aws ec2 wait instance-running --region {region} --instance-ids {head_id}"
        )

    if allocation.get("InstanceId") != head_id:
        run_command(
            f"aws ec2 associate-address --region {region} "
            f"--instance-id {head_id} --allocation-id {allocation_id} --allow-reassociation"
        )

    spinner.text = (
        "Deploying burla-main-service instance ... waiting for it to serve traffic ..."
    )
    start = time()
    while time() - start < 600:
        try:
            response = requests.get(
                f"{dashboard_url}/version",
                headers={"Authorization": f"Bearer {cluster_id_token}"},
                timeout=3,
            )
            # Subset match: /version also reports fields like `namespace`.
            info = response.json() if response.status_code == 200 else {}
            if info.get("version") == __version__ and info.get("project") == project_id:
                break
        except requests.RequestException:
            pass
        sleep(5)
    else:
        spinner.fail("✗")
        raise Exception(f"burla-main-service never became reachable at {dashboard_url}")

    spinner.text = "Deploying burla-main-service instance ... Done."
    spinner.ok("✓")
    return dashboard_url
