import os
from time import sleep

import boto3
from botocore.exceptions import ClientError
from burla._aws_amis import public_node_ami_id

from main_service import CLUSTER_NAME, IN_CLIENT_HOSTED_MODE
from main_service.providers import NoCapacity

# Capacity errors worth trying the next AZ for; anything else is a real error.
_CAPACITY_ERROR_CODES = (
    "InsufficientInstanceCapacity",
    "SpotMaxPriceTooLow",
    "InsufficientFreeAddressesInSubnet",
    "Unsupported",
)


class AWSProvider:
    """EC2 twin of the GCP provider. Nodes use Burla's public regional AMIs."""

    def __init__(self):
        self.region = (
            os.environ.get("AWS_REGION") or boto3.session.Session().region_name
        )

    def _ec2(self, region: str):
        return boto3.client("ec2", region_name=region)

    def _ami_id(self, ec2, gpu: bool) -> str:
        override = os.environ.get("BURLA_NODE_AMI")
        if override:
            return override
        return public_node_ami_id(ec2, gpu)

    def _subnets(self, ec2) -> list[dict]:
        subnet_id = os.environ.get("AWS_SUBNET_ID")
        if subnet_id:
            return ec2.describe_subnets(SubnetIds=[subnet_id])["Subnets"]
        response = ec2.describe_subnets(
            Filters=[{"Name": "default-for-az", "Values": ["true"]}]
        )
        return response["Subnets"]

    def create_instance(
        self,
        instance_name: str,
        machine_type: str,
        region: str,
        disk_size: int,
        spot: bool,
        num_gpus: int,
        port: int,
        startup_script: str,
        shutdown_script: str,
        on_log,
        needs_cloud_credentials: bool = False,
    ) -> tuple[str | None, str, str]:
        """Create the EC2 instance, iterating AZs on capacity exhaustion.
        Returns (public_ip, private_ip, availability_zone)."""
        ec2 = self._ec2(region)
        ami_id = self._ami_id(ec2, gpu=num_gpus > 0)
        subnets = self._subnets(ec2)
        if not subnets:
            raise Exception(
                f"No default subnets found in region {region}. "
                "Set AWS_SUBNET_ID to an existing subnet."
            )
        subnets_by_az = {subnet["AvailabilityZone"]: subnet for subnet in subnets}
        security_group_id = os.environ.get("AWS_SECURITY_GROUP_ID")
        if not security_group_id:
            security_group_id = _security_group_id(ec2, subnets[0]["VpcId"])

        # EC2 has no shutdown-script metadata; the AMI ships a systemd unit
        # (burla-shutdown-hook) that POSTs /shutdown on system halt, so the
        # shutdown_script arg is intentionally unused here.
        run_kwargs = dict(
            ImageId=ami_id,
            InstanceType=machine_type,
            MinCount=1,
            MaxCount=1,
            UserData=startup_script,
            BlockDeviceMappings=[
                {
                    "DeviceName": "/dev/sda1",
                    "Ebs": {
                        # EC2 cannot shrink the public AMI's 20 GB snapshot.
                        "VolumeSize": max(disk_size, 20),
                        "VolumeType": "gp3",
                        "DeleteOnTermination": True,
                    },
                }
            ],
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name", "Value": instance_name},
                        {"Key": "burla-cluster-node", "Value": "true"},
                        # Several dev clusters boot nodes into one AWS account,
                        # so every destructive lookup below filters on this.
                        {"Key": "burla-cluster-id", "Value": CLUSTER_NAME},
                    ],
                }
            ],
        )
        # The instance profile only grants shared-workspace S3 access; without
        # it nodes are credential-less and whoever boots them needs no
        # iam:PassRole (the client-hosted default).
        if needs_cloud_credentials:
            run_kwargs["IamInstanceProfile"] = {"Name": "burla-node"}
        if spot:
            run_kwargs["InstanceMarketOptions"] = {
                "MarketType": "spot",
                "SpotOptions": {"SpotInstanceType": "one-time"},
            }
        else:
            # An in-VM `poweroff` (inactivity shutdown, dead head) fully
            # terminates the instance - no credentials needed to clean up.
            # (One-time spot instances already terminate on shutdown, and
            # the API rejects this parameter for them.)
            run_kwargs["InstanceInitiatedShutdownBehavior"] = "terminate"

        exhausted_azs = []
        instance_id = None
        instance = None
        associate_public_ip = False
        for az, subnet in sorted(subnets_by_az.items()):
            on_log(f"Attempting to provision {machine_type} in AZ: {az}")
            try:
                response = ec2.run_instances(
                    **run_kwargs,
                    NetworkInterfaces=[
                        {
                            "DeviceIndex": 0,
                            "SubnetId": subnet["SubnetId"],
                            "AssociatePublicIpAddress": subnet["MapPublicIpOnLaunch"],
                            "Groups": [security_group_id],
                        }
                    ],
                )
                instance = response["Instances"][0]
                instance_id = instance["InstanceId"]
                associate_public_ip = subnet["MapPublicIpOnLaunch"]
                zone = az
                break
            except ClientError as error:
                code = error.response["Error"]["Code"]
                if code in _CAPACITY_ERROR_CODES:
                    exhausted_azs.append(az)
                    on_log(f"No available capacity for {machine_type} in AZ: {az}")
                else:
                    raise

        if instance_id is None:
            msg = f"INSUFFICIENT_INSTANCE_CAPACITY: {exhausted_azs} currently have no "
            msg += f"available capacity for instance type {machine_type}\n"
            raise NoCapacity(msg)

        # Instance ids are the only handle EC2 gives us; remember it so
        # delete_instance doesn't need a lookup.
        _instance_ids[instance_name] = (instance_id, region)

        public_ip = None
        if associate_public_ip:
            for _ in range(60):
                try:
                    description = ec2.describe_instances(InstanceIds=[instance_id])
                except ClientError as error:
                    # run_instances is eventually consistent: the new id can be
                    # invisible to describe_instances for a few seconds.
                    if error.response["Error"]["Code"] == "InvalidInstanceID.NotFound":
                        sleep(2)
                        continue
                    raise
                instance = description["Reservations"][0]["Instances"][0]
                public_ip = instance.get("PublicIpAddress")
                if public_ip:
                    break
                sleep(2)
        if associate_public_ip and not public_ip:
            raise Exception(
                f"Instance {instance_name} ({instance_id}) never got a public IP."
            )
        private_ip = instance["PrivateIpAddress"]

        on_log(
            f"Successfully provisioned {machine_type} in AZ: {zone}\nWaiting for startup script ..."
        )
        return public_ip, private_ip, zone

    def delete_instance(self, instance_name: str, zone: str | None = None):
        cached = _instance_ids.pop(instance_name, None)
        if cached:
            instance_id, region = cached
            ec2 = self._ec2(region)
            # A just-created id can be invisible for a few seconds (same
            # eventual consistency as describe_instances in create_instance).
            for attempt in range(5):
                try:
                    ec2.terminate_instances(InstanceIds=[instance_id])
                    ec2.get_waiter("instance_terminated").wait(
                        InstanceIds=[instance_id]
                    )
                    return
                except ClientError as error:
                    if error.response["Error"]["Code"] != "InvalidInstanceID.NotFound":
                        raise
                    sleep(3)
            return
        region = zone[:-1] if zone else self.region
        ec2 = self._ec2(region)
        response = ec2.describe_instances(
            Filters=[
                {"Name": "tag:Name", "Values": [instance_name]},
                {"Name": "tag:burla-cluster-id", "Values": [CLUSTER_NAME]},
                {
                    "Name": "instance-state-name",
                    "Values": ["pending", "running", "stopping", "stopped"],
                },
            ]
        )
        instance_ids = [
            instance["InstanceId"]
            for reservation in response["Reservations"]
            for instance in reservation["Instances"]
        ]
        if instance_ids:
            ec2.terminate_instances(InstanceIds=instance_ids)
            ec2.get_waiter("instance_terminated").wait(InstanceIds=instance_ids)

    def delete_stopped_instances(self):
        """Nothing to reap on AWS.

        A node that wants to be gone powers off, and
        `InstanceInitiatedShutdownBehavior=terminate` turns that into a full
        termination, so a *stopped* AWS node was never a self-delete attempt:
        someone stopped it deliberately (console, CLI). This used to terminate
        those, which destroyed a user's node behind their back.
        """
        return

    def mount_shared_workspace_script(self, bucket_name: str) -> str:
        return f"""
        mkdir -p /workspace/shared
        mount-s3 --allow-overwrite --allow-delete {bucket_name} /workspace/shared
        """


_instance_ids: dict[str, tuple[str, str]] = {}
_cached_sg_ids: dict[tuple[str, str, str], str] = {}


def _security_group_id(ec2, vpc_id: str) -> str:
    group_name = "default" if IN_CLIENT_HOSTED_MODE else "burla-cluster-node"
    cache_key = (ec2.meta.region_name, vpc_id, group_name)
    if cache_key not in _cached_sg_ids:
        response = ec2.describe_security_groups(
            Filters=[
                {"Name": "group-name", "Values": [group_name]},
                {"Name": "vpc-id", "Values": [vpc_id]},
            ]
        )
        groups = response["SecurityGroups"]
        if not groups:
            raise Exception(f"Security group `{group_name}` not found in VPC {vpc_id}.")
        _cached_sg_ids[cache_key] = groups[0]["GroupId"]
    return _cached_sg_ids[cache_key]
