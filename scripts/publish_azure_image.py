#!/usr/bin/env python3

import argparse
import json
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from time import sleep

from burla._azure_images import (
    NODE_IMAGE_HASH,
    PUBLIC_GALLERY_NAME,
    PUBLIC_IMAGE_NAME,
    PUBLIC_IMAGE_REGIONS,
    public_node_image_version,
)
from burla._deploy_azure import (
    ensure_network,
    ensure_node_image,
    ensure_resource_group,
    register_resource_providers,
)
from yaspin import yaspin

GALLERY_RESOURCE_GROUP = "burla-images"
GALLERY_NAME = "burla_node_images"

DEFAULT_TARGET_REGIONS = PUBLIC_IMAGE_REGIONS


def _az(*args: str):
    result = subprocess.run(
        ["az", *args, "--output", "json"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip())
    return json.loads(result.stdout) if result.stdout.strip() else None


def _ensure_gallery(source_region: str):
    _az(
        "group", "create", "--name", GALLERY_RESOURCE_GROUP, "--location", source_region
    )
    galleries = _az(
        "sig",
        "list",
        "--resource-group",
        GALLERY_RESOURCE_GROUP,
    )
    if not any(gallery["name"] == GALLERY_NAME for gallery in galleries):
        _az(
            "sig",
            "create",
            "--resource-group",
            GALLERY_RESOURCE_GROUP,
            "--gallery-name",
            GALLERY_NAME,
            "--location",
            source_region,
            "--permissions",
            "Community",
            "--publisher-uri",
            "https://burla.dev",
            "--publisher-email",
            "jake@burla.dev",
            "--eula",
            "https://github.com/Burla-Cloud/burla/blob/main/LICENSE",
            "--public-name-prefix",
            "Burla",
        )

    gallery = _az(
        "sig",
        "show",
        "--resource-group",
        GALLERY_RESOURCE_GROUP,
        "--gallery-name",
        GALLERY_NAME,
    )
    gallery_region = gallery["location"].lower().replace(" ", "")
    if gallery_region != source_region:
        raise RuntimeError(
            f"Gallery is in {gallery_region}, not source region {source_region}."
        )
    public_names = gallery["sharingProfile"]["communityGalleryInfo"]["publicNames"]
    if public_names != [PUBLIC_GALLERY_NAME]:
        raise RuntimeError(
            f"Gallery public name is {public_names}, expected {PUBLIC_GALLERY_NAME}."
        )


def _ensure_image_definition():
    definitions = _az(
        "sig",
        "image-definition",
        "list",
        "--resource-group",
        GALLERY_RESOURCE_GROUP,
        "--gallery-name",
        GALLERY_NAME,
    )
    definition = next(
        (
            definition
            for definition in definitions
            if definition["name"] == PUBLIC_IMAGE_NAME
        ),
        None,
    )
    if definition:
        disk_controller_feature = next(
            (
                feature
                for feature in definition["features"]
                if feature["name"] == "DiskControllerTypes"
            ),
            None,
        )
        if disk_controller_feature:
            return
        versions = _az(
            "sig",
            "image-version",
            "list",
            "--resource-group",
            GALLERY_RESOURCE_GROUP,
            "--gallery-name",
            GALLERY_NAME,
            "--gallery-image-definition",
            PUBLIC_IMAGE_NAME,
        )
        starts_at_version = min(
            versions,
            key=lambda version: tuple(int(part) for part in version["name"].split(".")),
        )["name"]
        features = [
            *definition["features"],
            {
                "name": "DiskControllerTypes",
                "value": "SCSI,NVMe",
                "startsAtVersion": starts_at_version,
            },
        ]
        _az(
            "sig",
            "image-definition",
            "update",
            "--resource-group",
            GALLERY_RESOURCE_GROUP,
            "--gallery-name",
            GALLERY_NAME,
            "--gallery-image-definition",
            PUBLIC_IMAGE_NAME,
            "--set",
            "allow_update_image=true",
            f"features={json.dumps(features, separators=(',', ':'))}",
        )
        return

    _az(
        "sig",
        "image-definition",
        "create",
        "--resource-group",
        GALLERY_RESOURCE_GROUP,
        "--gallery-name",
        GALLERY_NAME,
        "--gallery-image-definition",
        PUBLIC_IMAGE_NAME,
        "--publisher",
        "Burla",
        "--offer",
        "burla-node",
        "--sku",
        "cpu-v2",
        "--os-type",
        "Linux",
        "--os-state",
        "generalized",
        "--hyper-v-generation",
        "V2",
        "--architecture",
        "x64",
        "--features",
        "DiskControllerTypes=SCSI,NVMe",
    )


def _published_version(version_name: str) -> dict | None:
    versions = _az(
        "sig",
        "image-version",
        "list",
        "--resource-group",
        GALLERY_RESOURCE_GROUP,
        "--gallery-name",
        GALLERY_NAME,
        "--gallery-image-definition",
        PUBLIC_IMAGE_NAME,
    )
    for version in versions:
        if version["name"] == version_name:
            if (version.get("tags") or {}).get(
                "burla-node-image-hash"
            ) != NODE_IMAGE_HASH:
                raise RuntimeError(
                    f"Azure image version {version_name} has the wrong hash."
                )
            return version
    return None


def _enable_community_sharing():
    _az(
        "sig",
        "share",
        "enable-community",
        "--resource-group",
        GALLERY_RESOURCE_GROUP,
        "--gallery-name",
        GALLERY_NAME,
    )


def _verify_community_visibility(version_name: str, target_regions: list[str]):
    def is_visible(region: str) -> bool:
        try:
            _az(
                "sig",
                "image-version",
                "show-community",
                "--location",
                region,
                "--public-gallery-name",
                PUBLIC_GALLERY_NAME,
                "--gallery-image-definition",
                PUBLIC_IMAGE_NAME,
                "--gallery-image-version",
                version_name,
            )
            return True
        except RuntimeError:
            return False

    missing = set(target_regions)
    for _ in range(30):
        regions = list(missing)
        with ThreadPoolExecutor(max_workers=8) as pool:
            visible = dict(zip(regions, pool.map(is_visible, regions), strict=True))
        missing = {region for region, found in visible.items() if not found}
        if not missing:
            return
        sleep(10)
    raise RuntimeError(
        f"Azure image {version_name} is not public in: " + ", ".join(sorted(missing))
    )


def _publish_version(
    managed_image_id: str,
    source_region: str,
    version_name: str,
    target_regions: list[str],
) -> dict:
    target_regions = list(dict.fromkeys((source_region, *target_regions)))
    existing = _published_version(version_name)
    if existing and existing["provisioningState"] != "Succeeded":
        raise RuntimeError(
            f"Azure image {version_name} is {existing['provisioningState']}; "
            "advance its regional revision before rerunning."
        )

    if not existing:
        _az(
            "sig",
            "image-version",
            "create",
            "--resource-group",
            GALLERY_RESOURCE_GROUP,
            "--gallery-name",
            GALLERY_NAME,
            "--gallery-image-definition",
            PUBLIC_IMAGE_NAME,
            "--gallery-image-version",
            version_name,
            "--managed-image",
            managed_image_id,
            "--replica-count",
            "1",
            "--storage-account-type",
            "Standard_LRS",
            "--exclude-from-latest",
            "true",
            "--target-regions",
            *target_regions,
            "--allow-replicated-location-deletion",
            "true",
            "--block-deletion-before-end-of-life",
            "false",
            "--tags",
            f"burla-node-image-hash={NODE_IMAGE_HASH}",
        )

    published = _published_version(version_name)
    published_regions = {
        region["name"].lower().replace(" ", "")
        for region in published["publishingProfile"]["targetRegions"]
    }
    if (
        published["provisioningState"] != "Succeeded"
        or not set(target_regions) <= published_regions
    ):
        raise RuntimeError(
            f"Azure image {version_name} did not finish all regional replicas."
        )
    return published


def _region_groups(target_regions: list[str]) -> dict[str, list[str]]:
    groups = {}
    for region in target_regions:
        groups.setdefault(public_node_image_version(region), []).append(region)
    return groups


def _publish_selected(
    subscription_id: str, source_region: str, target_regions: list[str]
) -> dict:
    account = _az("account", "show")
    if account["id"] != subscription_id:
        raise RuntimeError(f"Azure CLI did not select subscription {subscription_id}.")
    source_region = source_region.lower().replace(" ", "")
    target_regions = list(
        dict.fromkeys(
            region.lower().replace(" ", "")
            for region in (source_region, *target_regions)
        )
    )

    register_resource_providers()
    ensure_resource_group(source_region)
    ensure_network(source_region)
    _ensure_gallery(source_region)
    _ensure_image_definition()

    with open(os.devnull, "w") as quiet_output:
        spinner_output = sys.stdout if sys.stdout.isatty() else quiet_output
        with yaspin(stream=spinner_output) as spinner:
            managed_image_id = ensure_node_image(spinner, source_region)

    groups = _region_groups(target_regions)
    with ThreadPoolExecutor(max_workers=3) as pool:
        published = list(
            pool.map(
                lambda item: _publish_version(
                    managed_image_id, source_region, item[0], item[1]
                ),
                groups.items(),
            )
        )
    _enable_community_sharing()
    for version_name, regions in groups.items():
        _verify_community_visibility(version_name, regions)
    return {"versions": published}


def publish(
    subscription_id: str, source_region: str, target_regions: list[str]
) -> dict:
    previous_subscription = _az("account", "show")["id"]
    _az("account", "set", "--subscription", subscription_id)
    try:
        return _publish_selected(subscription_id, source_region, target_regions)
    finally:
        _az("account", "set", "--subscription", previous_subscription)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--subscription-id", required=True)
    parser.add_argument("--source-region", default="eastus")
    parser.add_argument(
        "--target-region",
        action="append",
        dest="target_regions",
        help="Repeat to override the default regional replica set.",
    )
    args = parser.parse_args()
    target_regions = args.target_regions or list(DEFAULT_TARGET_REGIONS)
    print(
        json.dumps(
            publish(args.subscription_id, args.source_region, target_regions),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
