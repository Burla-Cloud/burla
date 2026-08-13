#!/usr/bin/env python3

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep

import boto3
from botocore.exceptions import WaiterError
from burla._aws_amis import (
    PUBLIC_AMI_OWNER_ID,
    node_ami_hash,
    node_ami_name_prefix,
)
from burla._deploy_aws import _ensure_node_ami
from yaspin import yaspin

TARGET_REGIONS = (
    "af-south-1",
    "ap-east-1",
    "ap-east-2",
    "ap-northeast-1",
    "ap-northeast-2",
    "ap-northeast-3",
    "ap-south-1",
    "ap-south-2",
    "ap-southeast-1",
    "ap-southeast-2",
    "ap-southeast-3",
    "ap-southeast-4",
    "ap-southeast-5",
    "ap-southeast-6",
    "ap-southeast-7",
    "ca-central-1",
    "eu-central-1",
    "eu-central-2",
    "eu-north-1",
    "eu-south-1",
    "eu-south-2",
    "eu-west-1",
    "eu-west-2",
    "eu-west-3",
    "il-central-1",
    "me-central-1",
    "mx-central-1",
    "sa-east-1",
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
)
PUBLIC_AMI_QUOTA_CODE = "L-0E3CBAB9"
DEFAULT_PUBLIC_AMI_QUOTA = 5


def _owned_node_amis(ec2, gpu: bool) -> list[dict]:
    response = ec2.describe_images(
        Owners=["self"],
        Filters=[
            {"Name": "name", "Values": [f"{node_ami_name_prefix(gpu)}-*"]},
        ],
    )
    return sorted(
        response["Images"], key=lambda image: image["CreationDate"], reverse=True
    )


def _owned_node_ami(ec2, gpu: bool) -> dict | None:
    return next(
        (
            image
            for image in _owned_node_amis(ec2, gpu)
            if image["State"] == "available"
        ),
        None,
    )


def _copy_node_ami(
    profile: str,
    source_region: str,
    source_image: dict,
    destination_region: str,
    gpu: bool,
) -> tuple[str, str]:
    ec2 = boto3.Session(profile_name=profile).client(
        "ec2", region_name=destination_region
    )
    images = _owned_node_amis(ec2, gpu)
    existing = next((image for image in images if image["State"] == "available"), None)
    if existing:
        return destination_region, existing["ImageId"]

    pending = next((image for image in images if image["State"] == "pending"), None)
    if pending:
        try:
            ec2.get_waiter("image_available").wait(
                ImageIds=[pending["ImageId"]],
                WaiterConfig={"Delay": 15, "MaxAttempts": 240},
            )
            return destination_region, pending["ImageId"]
        except WaiterError:
            ec2.deregister_image(ImageId=pending["ImageId"])

    for failed in images:
        if failed["State"] in ("failed", "error"):
            ec2.deregister_image(ImageId=failed["ImageId"])

    response = ec2.copy_image(
        SourceRegion=source_region,
        SourceImageId=source_image["ImageId"],
        Name=source_image["Name"],
        Description="Burla node image",
        TagSpecifications=[
            {
                "ResourceType": "image",
                "Tags": [
                    {"Key": "burla-node-image", "Value": "true"},
                    {
                        "Key": "burla-node-image-hash",
                        "Value": node_ami_hash(gpu),
                    },
                ],
            }
        ],
    )
    image_id = response["ImageId"]
    ec2.get_waiter("image_available").wait(
        ImageIds=[image_id],
        WaiterConfig={"Delay": 15, "MaxAttempts": 240},
    )
    return destination_region, image_id


def _make_public(profile: str, region: str, image_id: str):
    ec2 = boto3.Session(profile_name=profile).client("ec2", region_name=region)
    ec2.modify_image_attribute(
        ImageId=image_id,
        LaunchPermission={"Add": [{"Group": "all"}]},
    )


def _wait_for_public_access_state(ec2, expected: str):
    for _ in range(120):
        state = ec2.get_image_block_public_access_state()["ImageBlockPublicAccessState"]
        if state == expected:
            return
        sleep(5)
    raise RuntimeError(
        f"EC2 image public-access state in {ec2.meta.region_name} "
        f"did not become {expected}."
    )


def _check_public_ami_quotas(session):
    prefixes = [node_ami_name_prefix(gpu) for gpu in (False, True)]

    def shortfall(region: str) -> str | None:
        images = session.client("ec2", region_name=region).describe_images(
            Owners=["self"]
        )["Images"]
        public_images = [image for image in images if image["Public"]]
        missing = sum(
            not any(image["Name"].startswith(prefix) for image in public_images)
            for prefix in prefixes
        )
        required_public_images = len(public_images) + missing
        if required_public_images <= DEFAULT_PUBLIC_AMI_QUOTA:
            return None
        quota = session.client("service-quotas", region_name=region).get_service_quota(
            ServiceCode="ec2",
            QuotaCode=PUBLIC_AMI_QUOTA_CODE,
        )["Quota"]["Value"]
        if required_public_images <= quota:
            return None
        return region

    with ThreadPoolExecutor(max_workers=8) as pool:
        shortfalls = [
            region for region in pool.map(shortfall, TARGET_REGIONS) if region
        ]
    if shortfalls:
        raise RuntimeError(
            f"Increase the EC2 Public AMIs quota ({PUBLIC_AMI_QUOTA_CODE}) "
            "to at least 20 in these regions, then rerun: " + ", ".join(shortfalls)
        )


def publish(profile: str, source_region: str) -> dict:
    os.environ["AWS_PROFILE"] = profile
    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
    ):
        os.environ.pop(name, None)
    session = boto3.Session(profile_name=profile)
    account_id = session.client("sts").get_caller_identity()["Account"]
    if account_id != PUBLIC_AMI_OWNER_ID:
        raise RuntimeError(
            f"Profile {profile} uses AWS account {account_id}, not "
            f"the public AMI account {PUBLIC_AMI_OWNER_ID}."
        )

    source_ec2 = session.client("ec2", region_name=source_region)
    region_status = {
        region["RegionName"]: region["OptInStatus"]
        for region in source_ec2.describe_regions(AllRegions=True)["Regions"]
    }
    disabled_regions = [
        region
        for region in TARGET_REGIONS
        if region_status[region] not in ("opt-in-not-required", "opted-in")
    ]
    if disabled_regions:
        raise RuntimeError(
            f"Enable these regions for AWS profile {profile}, then rerun: "
            + ", ".join(disabled_regions)
        )

    _check_public_ami_quotas(session)

    with open(os.devnull, "w") as quiet_output:
        spinner_output = sys.stdout if sys.stdout.isatty() else quiet_output
        with yaspin(stream=spinner_output) as spinner:
            _ensure_node_ami(spinner, source_region)

    source_images = {gpu: _owned_node_ami(source_ec2, gpu) for gpu in (False, True)}
    regions = TARGET_REGIONS
    published = {
        source_region: {
            "cpu": source_images[False]["ImageId"],
            "gpu": source_images[True]["ImageId"],
        }
    }

    copies = {}
    with ThreadPoolExecutor(max_workers=8) as pool:
        for gpu, source_image in source_images.items():
            for region in regions:
                if region == source_region:
                    continue
                future = pool.submit(
                    _copy_node_ami,
                    profile,
                    source_region,
                    source_image,
                    region,
                    gpu,
                )
                copies[future] = gpu

        for future in as_completed(copies):
            region, image_id = future.result()
            variant = "gpu" if copies[future] else "cpu"
            published.setdefault(region, {})[variant] = image_id
            print(f"{region} {variant}: {image_id}", flush=True)

    previous_public_access_states = {}
    try:
        for region in published:
            ec2 = session.client("ec2", region_name=region)
            state = ec2.get_image_block_public_access_state()[
                "ImageBlockPublicAccessState"
            ]
            previous_public_access_states[region] = state
            if state != "unblocked":
                ec2.disable_image_block_public_access()
        for region in published:
            _wait_for_public_access_state(
                session.client("ec2", region_name=region), "unblocked"
            )

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [
                pool.submit(_make_public, profile, region, image_id)
                for region, variants in published.items()
                for image_id in variants.values()
            ]
            for future in as_completed(futures):
                future.result()
    finally:
        for region, state in previous_public_access_states.items():
            if state == "block-new-sharing":
                session.client(
                    "ec2", region_name=region
                ).enable_image_block_public_access(
                    ImageBlockPublicAccessState="block-new-sharing"
                )
        for region, state in previous_public_access_states.items():
            if state == "block-new-sharing":
                _wait_for_public_access_state(
                    session.client("ec2", region_name=region), state
                )

    return dict(sorted(published.items()))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", required=True)
    parser.add_argument("--source-region", default="us-east-1")
    args = parser.parse_args()
    print(json.dumps(publish(args.profile, args.source_region), indent=2))


if __name__ == "__main__":
    main()
