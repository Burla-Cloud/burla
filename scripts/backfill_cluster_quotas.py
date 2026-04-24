#!/usr/bin/env python3
"""
One-off backfill: populate `cluster_quota/cluster_quota` in every Burla
project's `burla` Firestore DB with per-(region, machine_type) VM-count
limits translated from live GCP quotas.

Sources:
  1. Project list     <- firestore (burla-prod / backend-service / clusters).project_id
  2. Legacy quotas    <- compute v1 RegionsClient().get(project, region).quotas
                         (A2_CPUS, NVIDIA_A100_GPUS, NVIDIA_A100_80GB_GPUS)
  3. Modern quotas    <- cloudquotas v1 CloudQuotasClient().list_quota_infos(...)
                         (CPUS-PER-VM-FAMILY for N4, GPUS-PER-GPU-FAMILY
                          for H100 / H200)

Output (per project): firestore (<project_id> / burla / cluster_quota/cluster_quota):
    {
      "us-central1": {"machine_type_limits": {"n4-standard-8": 2, "a3-highgpu-1g": 0, ...}},
      "us-east1":    {"machine_type_limits": {...}},
      ...
    }

Re-run idempotent. Use `--dry-run` to see the computed doc per project
without writing.

Usage:
    uv run --with google-cloud-firestore --with google-cloud-compute \\
           --with google-cloud-quotas \\
        scripts/backfill_cluster_quotas.py [--dry-run] [--project PROJECT_ID]...
"""

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Iterable, Optional

from google.api_core.exceptions import GoogleAPICallError, PermissionDenied
from google.cloud import cloudquotas_v1, compute_v1, firestore


BACKEND_PROJECT = "burla-prod"
BACKEND_DB = "backend-service"
CLUSTERS_COLLECTION = "clusters"
QUOTA_DB = "burla"
QUOTA_COLLECTION = "cluster_quota"
QUOTA_DOC_ID = "cluster_quota"


# Every region the dashboard's region selector currently exposes
# (main_service/frontend/src/components/SettingsForm.tsx). Populating the
# full set means the UI's quota lookup always finds an entry, even after
# a customer switches region.
REGIONS = sorted({
    "us-central1", "us-east1", "us-east4", "us-east5", "us-west1", "us-west3",
    "us-west4", "us-south1",
    "northamerica-northeast2", "northamerica-south1",
    "europe-west1", "europe-west2", "europe-west3", "europe-west4",
    "europe-west9", "europe-southwest1", "europe-north2",
    "me-west1",
    "asia-northeast1", "asia-northeast3", "asia-south1", "asia-south2",
    "asia-southeast1",
    "australia-southeast1",
})


@dataclass(frozen=True)
class MachineSpec:
    name: str
    vcpus: int
    # Where we read CPU capacity from. "legacy:A2_CPUS" -> from
    # RegionsClient. "vm_family:N4" -> from CloudQuotasClient.
    cpu_source: str
    # Optional GPU info. `gpu_source` is analogous to `cpu_source`:
    # "legacy:NVIDIA_A100_GPUS" or "gpu_family:NVIDIA_H100" /
    # "gpu_family:NVIDIA_H200".
    gpu_source: Optional[str] = None
    gpus: int = 0


MACHINES: list[MachineSpec] = [
    # n4 CPU-only. N4 has no legacy quota; it's in
    # CPUS-PER-VM-FAMILY dimension vm_family=N4.
    MachineSpec("n4-standard-2",   2,   "vm_family:N4"),
    MachineSpec("n4-standard-4",   4,   "vm_family:N4"),
    MachineSpec("n4-standard-8",   8,   "vm_family:N4"),
    MachineSpec("n4-standard-16",  16,  "vm_family:N4"),
    MachineSpec("n4-standard-32",  32,  "vm_family:N4"),
    MachineSpec("n4-standard-64",  64,  "vm_family:N4"),
    MachineSpec("n4-standard-80",  80,  "vm_family:N4"),

    # a2 - A100 40GB.
    MachineSpec("a2-highgpu-1g",   12,  "legacy:A2_CPUS", "legacy:NVIDIA_A100_GPUS", 1),
    MachineSpec("a2-highgpu-2g",   24,  "legacy:A2_CPUS", "legacy:NVIDIA_A100_GPUS", 2),
    MachineSpec("a2-highgpu-4g",   48,  "legacy:A2_CPUS", "legacy:NVIDIA_A100_GPUS", 4),
    MachineSpec("a2-highgpu-8g",   96,  "legacy:A2_CPUS", "legacy:NVIDIA_A100_GPUS", 8),

    # a2 - A100 80GB.
    MachineSpec("a2-ultragpu-1g",  12,  "legacy:A2_CPUS", "legacy:NVIDIA_A100_80GB_GPUS", 1),
    MachineSpec("a2-ultragpu-2g",  24,  "legacy:A2_CPUS", "legacy:NVIDIA_A100_80GB_GPUS", 2),
    MachineSpec("a2-ultragpu-4g",  48,  "legacy:A2_CPUS", "legacy:NVIDIA_A100_80GB_GPUS", 4),
    MachineSpec("a2-ultragpu-8g",  96,  "legacy:A2_CPUS", "legacy:NVIDIA_A100_80GB_GPUS", 8),

    # a3 - H100 80GB. A3 has no per-VM-family CPU quota; CPU capacity is
    # unconstrained relative to the GPU cap, so we let GPU be the sole
    # limit by setting `cpu_source` to the sentinel 'unbounded'.
    MachineSpec("a3-highgpu-1g",   26,  "unbounded", "gpu_family:NVIDIA_H100", 1),
    MachineSpec("a3-highgpu-2g",   52,  "unbounded", "gpu_family:NVIDIA_H100", 2),
    MachineSpec("a3-highgpu-4g",   104, "unbounded", "gpu_family:NVIDIA_H100", 4),
    MachineSpec("a3-highgpu-8g",   208, "unbounded", "gpu_family:NVIDIA_H100", 8),

    # a3 - H200 141GB.
    MachineSpec("a3-ultragpu-8g",  224, "unbounded", "gpu_family:NVIDIA_H200", 8),
]


def list_project_ids() -> list[str]:
    db = firestore.Client(project=BACKEND_PROJECT, database=BACKEND_DB)
    project_ids: list[str] = []
    for doc in db.collection(CLUSTERS_COLLECTION).stream():
        data = doc.to_dict() or {}
        pid = data.get("project_id")
        if isinstance(pid, str) and pid:
            project_ids.append(pid)
    return sorted(set(project_ids))


def fetch_legacy_quotas(
    regions_client: compute_v1.RegionsClient, project_id: str, region: str
) -> Optional[dict[str, int]]:
    """Returns {metric: limit} from the legacy compute.v1 region quotas
    or None when the region is unreachable for this project."""
    try:
        info = regions_client.get(project=project_id, region=region)
    except PermissionDenied:
        return None
    except GoogleAPICallError as exc:
        print(
            f"  WARN: failed to read legacy quotas for {project_id}/{region}: {exc!r}",
            file=sys.stderr,
        )
        return None
    out: dict[str, int] = {}
    for q in info.quotas:
        try:
            out[q.metric] = int(q.limit)
        except (AttributeError, TypeError, ValueError):
            continue
    return out


def fetch_modern_quotas(
    quotas_client: cloudquotas_v1.CloudQuotasClient, project_id: str
) -> dict[str, dict]:
    """Returns nested per-region lookups from cloudquotas v1:
        {
          "cpus_per_vm_family": {region: {vm_family: limit}},
          "gpus_per_gpu_family": {region: {gpu_family: limit}},
        }
    Empty dicts on PermissionDenied / API errors so the caller can still
    degrade to just legacy quotas for this project."""
    out = {"cpus_per_vm_family": {}, "gpus_per_gpu_family": {}}
    parent = f"projects/{project_id}/locations/global/services/compute.googleapis.com"
    try:
        iterator = quotas_client.list_quota_infos(parent=parent)
    except (PermissionDenied, GoogleAPICallError) as exc:
        print(
            f"  WARN: cloudquotas list failed for {project_id}: {exc!r}",
            file=sys.stderr,
        )
        return out

    for qi in iterator:
        name = qi.name.rsplit("/", 1)[-1]
        if name == "CPUS-PER-VM-FAMILY-per-project-region":
            for di in qi.dimensions_infos:
                region = di.dimensions.get("region")
                vm_family = di.dimensions.get("vm_family")
                if not region or not vm_family:
                    continue
                try:
                    limit = int(di.details.value)
                except (AttributeError, TypeError, ValueError):
                    continue
                out["cpus_per_vm_family"].setdefault(region, {})[vm_family] = limit
        elif name == "GPUS-PER-GPU-FAMILY-per-project-region":
            # This quota has a single value per gpu_family that applies
            # uniformly to every region in applicable_locations.
            for di in qi.dimensions_infos:
                gpu_family = di.dimensions.get("gpu_family")
                if not gpu_family:
                    continue
                try:
                    limit = int(di.details.value)
                except (AttributeError, TypeError, ValueError):
                    continue
                for region in di.applicable_locations:
                    out["gpus_per_gpu_family"].setdefault(region, {})[gpu_family] = limit
    return out


def cpu_limit_for(
    spec: MachineSpec,
    legacy: dict[str, int],
    modern: dict[str, dict],
    region: str,
) -> Optional[int]:
    """Returns the CPU quota that gates `spec` in `region`. None means
    'no CPU-side constraint', which for us means 'let GPU quota decide'
    (used by A3 machines)."""
    kind, _, key = spec.cpu_source.partition(":")
    if kind == "legacy":
        return legacy.get(key, 0)
    if kind == "vm_family":
        return (modern.get("cpus_per_vm_family") or {}).get(region, {}).get(key, 0)
    if kind == "unbounded":
        return None
    raise ValueError(f"unknown cpu_source {spec.cpu_source!r}")


def gpu_limit_for(
    spec: MachineSpec,
    legacy: dict[str, int],
    modern: dict[str, dict],
    region: str,
) -> Optional[int]:
    """Returns the GPU quota that gates `spec` in `region`, or None if
    the spec is CPU-only."""
    if spec.gpu_source is None or spec.gpus == 0:
        return None
    kind, _, key = spec.gpu_source.partition(":")
    if kind == "legacy":
        return legacy.get(key, 0)
    if kind == "gpu_family":
        return (modern.get("gpus_per_gpu_family") or {}).get(region, {}).get(key, 0)
    raise ValueError(f"unknown gpu_source {spec.gpu_source!r}")


def build_machine_type_limits(
    legacy_by_region: dict[str, dict[str, int]],
    modern: dict[str, dict],
    region: str,
) -> Optional[dict[str, int]]:
    """Returns the per-machine-type VM count map for one region, or None
    if the legacy API didn't return data for this region at all (e.g.
    region is unreachable for this project)."""
    legacy = legacy_by_region.get(region)
    if legacy is None:
        return None
    out: dict[str, int] = {}
    for spec in MACHINES:
        cpu_limit = cpu_limit_for(spec, legacy, modern, region)
        gpu_limit = gpu_limit_for(spec, legacy, modern, region)
        vm_count: Optional[int] = None
        if cpu_limit is not None:
            vm_count = cpu_limit // spec.vcpus if cpu_limit > 0 else 0
        if gpu_limit is not None:
            gpu_vm_count = gpu_limit // spec.gpus if gpu_limit > 0 else 0
            vm_count = gpu_vm_count if vm_count is None else min(vm_count, gpu_vm_count)
        if vm_count is None:
            vm_count = 0
        out[spec.name] = max(0, vm_count)
    return out


def build_quota_doc(
    regions_client: compute_v1.RegionsClient,
    quotas_client: cloudquotas_v1.CloudQuotasClient,
    project_id: str,
    regions: Iterable[str],
) -> dict[str, dict]:
    legacy_by_region: dict[str, dict[str, int]] = {}
    for region in regions:
        fetched = fetch_legacy_quotas(regions_client, project_id, region)
        if fetched is not None:
            legacy_by_region[region] = fetched
    modern = fetch_modern_quotas(quotas_client, project_id)

    doc: dict[str, dict] = {}
    for region in regions:
        limits = build_machine_type_limits(legacy_by_region, modern, region)
        if limits is None:
            continue
        doc[region] = {"machine_type_limits": limits}
    return doc


def write_quota_doc(project_id: str, doc: dict[str, dict]) -> None:
    db = firestore.Client(project=project_id, database=QUOTA_DB)
    db.collection(QUOTA_COLLECTION).document(QUOTA_DOC_ID).set(doc)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the computed doc per project without writing.",
    )
    parser.add_argument(
        "--project",
        action="append",
        default=[],
        help="Only process this project_id. May be passed multiple times. "
        "If omitted, every project in backend-service/clusters is processed.",
    )
    args = parser.parse_args()

    if args.project:
        project_ids = sorted(set(args.project))
        print(f"Processing {len(project_ids)} project(s) from --project flag.")
    else:
        project_ids = list_project_ids()
        print(
            f"Processing {len(project_ids)} project(s) from "
            f"{BACKEND_PROJECT}/{BACKEND_DB}/{CLUSTERS_COLLECTION}."
        )

    regions_client = compute_v1.RegionsClient()
    quotas_client = cloudquotas_v1.CloudQuotasClient()
    failures: list[str] = []
    for project_id in project_ids:
        print(f"\n=== {project_id} ===")
        try:
            doc = build_quota_doc(regions_client, quotas_client, project_id, REGIONS)
        except Exception as exc:
            print(f"  ERROR building quota doc for {project_id}: {exc!r}", file=sys.stderr)
            failures.append(project_id)
            continue
        if not doc:
            print(f"  No reachable regions for {project_id}; skipping.")
            continue
        if args.dry_run:
            print(json.dumps(doc, indent=2, sort_keys=True))
            continue
        try:
            write_quota_doc(project_id, doc)
        except Exception as exc:
            print(f"  ERROR writing quota doc for {project_id}: {exc!r}", file=sys.stderr)
            failures.append(project_id)
            continue
        summary = ", ".join(
            f"{r}({sum(v['machine_type_limits'].values())})"
            for r, v in sorted(doc.items())
        )
        print(f"  Wrote quota doc: {summary}")

    if failures:
        print(
            f"\nBackfill finished with {len(failures)} failure(s): {failures}",
            file=sys.stderr,
        )
        return 1
    print("\nBackfill finished cleanly.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
