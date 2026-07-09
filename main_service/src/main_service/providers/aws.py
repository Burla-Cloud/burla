import os
from time import sleep

import boto3
from botocore.exceptions import ClientError

from main_service.providers import NoCapacity

# Capacity errors worth trying the next AZ for; anything else is a real error.
_CAPACITY_ERROR_CODES = (
    "InsufficientInstanceCapacity",
    "SpotMaxPriceTooLow",
    "InsufficientFreeAddressesInSubnet",
    "Unsupported",
)


class AWSProvider:
    """EC2 twin of the GCP provider. Nodes are plain EC2 instances built from
    the burla node AMI (see scripts/build_aws_node_ami.sh), tagged
    `burla-cluster-node`, in a security group opening the node port."""

    def __init__(self):
        self.region = os.environ.get("AWS_REGION") or boto3.session.Session().region_name

    def _ec2(self, region: str):
        return boto3.client("ec2", region_name=region)

    def _ami_id(self, ec2) -> str:
        """The node AMI is looked up by tag so `burla install --cloud aws` can
        publish new AMI versions without touching cluster config."""
        override = os.environ.get("BURLA_NODE_AMI")
        if override:
            return override
        response = ec2.describe_images(
            Owners=["self"],
            Filters=[{"Name": "tag:burla-node-image", "Values": ["true"]}],
        )
        images = sorted(response["Images"], key=lambda i: i["CreationDate"], reverse=True)
        if not images:
            raise Exception(
                "No burla node AMI found in this region. "
                "Run `burla install --cloud aws` to build one, or set BURLA_NODE_AMI."
            )
        return images[0]["ImageId"]

    def _subnets_by_az(self, ec2) -> dict[str, str]:
        response = ec2.describe_subnets(
            Filters=[{"Name": "default-for-az", "Values": ["true"]}]
        )
        return {s["AvailabilityZone"]: s["SubnetId"] for s in response["Subnets"]}

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
    ) -> tuple[str, str]:
        """Create the EC2 instance, iterating AZs on capacity exhaustion.
        Returns (public_ip, availability_zone)."""
        if num_gpus > 0:
            raise Exception(
                "GPU nodes are not supported on AWS yet (the burla node AMI "
                "does not include GPU drivers)."
            )

        ec2 = self._ec2(region)
        ami_id = self._ami_id(ec2)
        subnets_by_az = self._subnets_by_az(ec2)
        if not subnets_by_az:
            raise Exception(f"No default subnets found in region {region}.")

        # EC2 has no shutdown-script metadata; the AMI ships a systemd unit
        # (burla-shutdown-hook) that POSTs /shutdown on system halt, so the
        # shutdown_script arg is intentionally unused here.
        run_kwargs = dict(
            ImageId=ami_id,
            InstanceType=machine_type,
            MinCount=1,
            MaxCount=1,
            UserData=startup_script,
            IamInstanceProfile={"Name": "burla-node"},
            BlockDeviceMappings=[
                {
                    "DeviceName": "/dev/sda1",
                    "Ebs": {"VolumeSize": disk_size, "VolumeType": "gp3", "DeleteOnTermination": True},
                }
            ],
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": "Name", "Value": instance_name},
                        {"Key": "burla-cluster-node", "Value": "true"},
                    ],
                }
            ],
        )
        if spot:
            run_kwargs["InstanceMarketOptions"] = {
                "MarketType": "spot",
                "SpotOptions": {"SpotInstanceType": "one-time"},
            }

        exhausted_azs = []
        instance_id = None
        for az, subnet_id in sorted(subnets_by_az.items()):
            on_log(f"Attempting to provision {machine_type} in AZ: {az}")
            try:
                security_group_id = _security_group_id(ec2)
                response = ec2.run_instances(
                    **run_kwargs,
                    NetworkInterfaces=[
                        {
                            "DeviceIndex": 0,
                            "SubnetId": subnet_id,
                            "AssociatePublicIpAddress": True,
                            "Groups": [security_group_id],
                        }
                    ],
                )
                instance_id = response["Instances"][0]["InstanceId"]
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
        for _ in range(60):
            description = ec2.describe_instances(InstanceIds=[instance_id])
            instance = description["Reservations"][0]["Instances"][0]
            public_ip = instance.get("PublicIpAddress")
            if public_ip:
                break
            sleep(2)
        if not public_ip:
            raise Exception(f"Instance {instance_name} ({instance_id}) never got a public IP.")

        on_log(f"Successfully provisioned {machine_type} in AZ: {zone}\nWaiting for startup script ...")
        return public_ip, zone

    def delete_instance(self, instance_name: str, zone: str | None = None):
        cached = _instance_ids.pop(instance_name, None)
        if cached:
            instance_id, region = cached
            self._ec2(region).terminate_instances(InstanceIds=[instance_id])
            return
        ec2 = self._ec2(self.region)
        response = ec2.describe_instances(
            Filters=[
                {"Name": "tag:Name", "Values": [instance_name]},
                {"Name": "instance-state-name", "Values": ["pending", "running", "stopping", "stopped"]},
            ]
        )
        instance_ids = [
            instance["InstanceId"]
            for reservation in response["Reservations"]
            for instance in reservation["Instances"]
        ]
        if instance_ids:
            ec2.terminate_instances(InstanceIds=instance_ids)

    def mount_shared_workspace_script(self, bucket_name: str) -> str:
        return f"""
        mkdir -p /workspace/shared
        mount-s3 --allow-overwrite --allow-delete {bucket_name} /workspace/shared
        """


_instance_ids: dict[str, tuple[str, str]] = {}
_cached_sg_id = None


def _security_group_id(ec2) -> str:
    global _cached_sg_id
    if _cached_sg_id is None:
        response = ec2.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": ["burla-cluster-node"]}]
        )
        groups = response["SecurityGroups"]
        if not groups:
            raise Exception(
                "Security group `burla-cluster-node` not found. Run `burla install --cloud aws`."
            )
        _cached_sg_id = groups[0]["GroupId"]
    return _cached_sg_id
