#!/usr/bin/env python3
"""
One-off backfill: seed every Burla-managed project's `cluster_quota/cluster_quota`
Firestore doc from the project's live GCP Compute Engine per-region quotas.

The per-region per-machine-type quota doc is read by:
- main_service `POST /v1/settings`     (dashboard save guard)
- main_service `POST /v1/jobs/{id}/start` with `grow=True`  (grow guard)

Without entries in that doc, both guards treat the limit as unlimited, so the
dashboard will happily accept a config that silently fails to boot and the
grow path will ask GCP to boot more VMs than quota allows. This script
populates those entries from the single source of truth (the live quotas GCP
has granted the project) so the guards have something real to enforce.

Usage
-----

  # Dry-run against all projects registered in backend-service:
  python scripts/backfill_cluster_quotas.py --dry-run

  # Actually write:
  python scripts/backfill_cluster_quotas.py

  # One project only:
  python scripts/backfill_cluster_quotas.py --project burla-agent-02

Requirements: ADC that can `firestore.*` and `compute.regions.get` on every
target project.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import Optional

from google.api_core.exceptions import Forbidden, NotFound, PermissionDenied
from google.cloud import firestore
from google.cloud.compute_v1 import RegionsClient


# Burla-supported machine types -> (family, vCPUs, GPU metric or None, GPUs/VM).
# Family is also the prefix of the GCP CPU quota metric: "N4" -> "N4_CPUS", etc.
@dataclass(frozen=True)
class _MachineType:
    name: str
    family: str
    vcpus: int
    gpu_metric: Optional[str]
    gpus_per_vm: int


_MACHINE_TYPES: tuple[_MachineType, ...] = (
    # n4-standard CPU nodes
    _MachineType("n4-standard-2",    "N4", 2,   None, 0),
    _MachineType("n4-standard-4",    "N4", 4,   None, 0),
    _MachineType("n4-standard-8",    "N4", 8,   None, 0),
    _MachineType("n4-standard-16",   "N4", 16,  None, 0),
    _MachineType("n4-standard-32",   "N4", 32,  None, 0),
    _MachineType("n4-standard-64",   "N4", 64,  None, 0),
    _MachineType("n4-standard-80",   "N4", 80,  None, 0),
    # a2 highgpu = A100 40G
    _MachineType("a2-highgpu-1g",    "A2", 12,  "NVIDIA_A100_GPUS",     1),
    _MachineType("a2-highgpu-2g",    "A2", 24,  "NVIDIA_A100_GPUS",     2),
    _MachineType("a2-highgpu-4g",    "A2", 48,  "NVIDIA_A100_GPUS",     4),
    _MachineType("a2-highgpu-8g",    "A2", 96,  "NVIDIA_A100_GPUS",     8),
    # a2 ultragpu = A100 80G
    _MachineType("a2-ultragpu-1g",   "A2", 12,  "NVIDIA_A100_80GB_GPUS", 1),
    _MachineType("a2-ultragpu-2g",   "A2", 24,  "NVIDIA_A100_80GB_GPUS", 2),
    _MachineType("a2-ultragpu-4g",   "A2", 48,  "NVIDIA_A100_80GB_GPUS", 4),
    _MachineType("a2-ultragpu-8g",   "A2", 96,  "NVIDIA_A100_80GB_GPUS", 8),
    # a3 highgpu = H100 80G
    _MachineType("a3-highgpu-1g",    "A3", 26,  "NVIDIA_H100_GPUS",      1),
    _MachineType("a3-highgpu-2g",    "A3", 52,  "NVIDIA_H100_GPUS",      2),
    _MachineType("a3-highgpu-4g",    "A3", 104, "NVIDIA_H100_GPUS",      4),
    _MachineType("a3-highgpu-8g",    "A3", 208, "NVIDIA_H100_GPUS",      8),
    # a3 ultragpu = H200 141G
    _MachineType("a3-ultragpu-8g",   "A3", 224, "NVIDIA_H200_GPUS",      8),
)


# Union of every region the dashboard exposes in its SettingsForm REGION_OPTIONS
# (SettingsForm.tsx lines ~152-216). Deduped.
_REGIONS: tuple[str, ...] = (
    "us-central1",
    "us-east1",
    "us-east4",
    "us-east5",
    "us-south1",
    "us-west1",
    "us-west3",
    "us-west4",
    "northamerica-northeast2",
    "northamerica-south1",
    "europe-west1",
    "europe-west2",
    "europe-west3",
    "europe-west4",
    "europe-west9",
    "europe-north2",
    "europe-southwest1",
    "asia-northeast1",
    "asia-northeast3",
    "asia-south1",
    "asia-south2",
    "asia-southeast1",
    "australia-southeast1",
    "me-west1",
)


def _list_project_ids() -> list[str]:
    """Read all registered project_ids from the backend_service's `clusters`
    collection (burla-prod / backend-service DB)."""
    db = firestore.Client(project="burla-prod", database="backend-service")
    ids: list[str] = []
    for snap in db.collection("clusters").stream():
        data = snap.to_dict() or {}
        pid = data.get("project_id")
        if isinstance(pid, str) and pid:
            ids.append(pid)
    return sorted(set(ids))


def _region_quotas(
    client: RegionsClient, project_id: str, region: str
) -> dict[str, int]:
    """Returns {metric_name: int(limit)} for one region, or {} if the region
    doesn't exist / this project can't read it."""
    try:
        region_proto = client.get(project=project_id, region=region)
    except (NotFound, Forbidden, PermissionDenied):
        return {}
    out: dict[str, int] = {}
    for quota in region_proto.quotas:
        try:
            out[quota.metric] = int(quota.limit)
        except (TypeError, ValueError):
            continue
    return out


def _per_machine_type_vm_counts(
    quotas: dict[str, int],
) -> dict[str, int]:
    """Translate a region's raw GCP quota metrics into per-machine-type VM
    counts for the Burla-supported machine type list. A machine type is set to
    0 when the corresponding CPU or GPU quota is 0 / missing, so the dashboard
    guard's revert-on-zero branch has something to key on."""
    result: dict[str, int] = {}
    for mt in _MACHINE_TYPES:
        cpu_metric = f"{mt.family}_CPUS"
        cpu_limit = quotas.get(cpu_metric, 0)
        vm_count = cpu_limit // mt.vcpus if mt.vcpus else 0
        if mt.gpu_metric is not None and mt.gpus_per_vm:
            gpu_limit = quotas.get(mt.gpu_metric, 0)
            vm_count = min(vm_count, gpu_limit // mt.gpus_per_vm)
        result[mt.name] = max(0, vm_count)
    return result


def _build_doc_for_project(project_id: str) -> dict:
    """Call Compute for every supported region and assemble the full
    cluster_quota doc."""
    client = RegionsClient()
    doc: dict = {}
    for region in _REGIONS:
        raw = _region_quotas(client, project_id, region)
        if not raw:
            continue
        machine_type_limits = _per_machine_type_vm_counts(raw)
        doc[region] = {"machine_type_limits": machine_type_limits}
    return doc


def _write_doc(project_id: str, doc: dict) -> None:
    db = firestore.Client(project=project_id, database="burla")
    db.collection("cluster_quota").document("cluster_quota").set(doc)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the computed doc per project; do not write.",
    )
    parser.add_argument(
        "--project",
        help=(
            "Only backfill this single project_id. "
            "When omitted, backfill every project registered in backend-service."
        ),
    )
    args = parser.parse_args()

    if args.project:
        project_ids = [args.project]
    else:
        project_ids = _list_project_ids()
        print(f"Found {len(project_ids)} projects in burla-prod/backend-service.")

    for pid in project_ids:
        print(f"\n=== {pid} ===")
        try:
            doc = _build_doc_for_project(pid)
        except Exception as exc:
            print(f"  ERROR building doc: {exc}", file=sys.stderr)
            continue

        if not doc:
            print("  no reachable regions (missing quota:read or compute disabled)")
            continue

        regions_with_zero_cpu_family: list[str] = []
        for region, region_body in doc.items():
            limits = region_body["machine_type_limits"]
            if all(v == 0 for v in limits.values()):
                regions_with_zero_cpu_family.append(region)

        print(f"  regions covered: {len(doc)}")
        if regions_with_zero_cpu_family:
            print(
                f"  all-zero regions (no N4/A2/A3 quota granted): "
                f"{regions_with_zero_cpu_family}"
            )

        if args.dry_run:
            print(json.dumps(doc, indent=2, sort_keys=True))
            continue

        try:
            _write_doc(pid, doc)
        except Exception as exc:
            print(f"  ERROR writing firestore doc: {exc}", file=sys.stderr)
            continue
        print(f"  wrote cluster_quota/cluster_quota ({len(doc)} regions)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
