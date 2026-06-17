from collections import Counter
from dataclasses import dataclass
from functools import lru_cache
from urllib.parse import quote

import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud.compute_v1 import RegionsClient
from google.cloud.resourcemanager_v3 import ProjectsClient

ACTIVE_QUOTA_NODE_STATUSES = {"BOOTING", "READY", "RUNNING"}


@dataclass(frozen=True)
class QuotaBucket:
    key: str
    display_name: str
    unit_name: str
    source: str
    compute_metric: str | None = None
    service_metric: str | None = None
    dimensions: tuple[tuple[str, str], ...] = ()


@dataclass(frozen=True)
class QuotaRequirement:
    bucket: QuotaBucket
    amount: int


@dataclass(frozen=True)
class QuotaCap:
    machine_type: str
    region: str
    requested: int
    allowed: int
    limit: int
    used: int
    available: int
    quota: str
    units: str

    def to_dict(self) -> dict:
        return {
            "type": "quota_capped",
            "machine_type": self.machine_type,
            "region": self.region,
            "requested": self.requested,
            "allowed": self.allowed,
            "count_unit": "machines",
            "limit": self.limit,
            "used": self.used,
            "available": self.available,
            "quota": self.quota,
            "units": self.units,
        }


@dataclass(frozen=True)
class QuotaPlan:
    machine_types: list[str]
    caps: list[QuotaCap]

    @property
    def warnings(self) -> list[dict]:
        return [cap.to_dict() for cap in self.caps]


INSTANCE_BUCKET = QuotaBucket(
    key="compute:INSTANCES",
    display_name="VM instances",
    unit_name="instances",
    source="compute",
    compute_metric="INSTANCES",
)
N4_CPU_BUCKET = QuotaBucket(
    key="serviceusage:compute.googleapis.com/cpus_per_vm_family:vm_family=N4",
    display_name="N4 vCPUs",
    unit_name="vCPUs",
    source="serviceusage",
    service_metric="compute.googleapis.com/cpus_per_vm_family",
    dimensions=(("vm_family", "N4"),),
)
H100_GPU_BUCKET = QuotaBucket(
    key="serviceusage:compute.googleapis.com/gpus_per_gpu_family:gpu_family=NVIDIA_H100",
    display_name="H100 GPUs",
    unit_name="GPUs",
    source="serviceusage",
    service_metric="compute.googleapis.com/gpus_per_gpu_family",
    dimensions=(("gpu_family", "NVIDIA_H100"),),
)
H200_GPU_BUCKET = QuotaBucket(
    key="serviceusage:compute.googleapis.com/gpus_per_gpu_family:gpu_family=NVIDIA_H200",
    display_name="H200 GPUs",
    unit_name="GPUs",
    source="serviceusage",
    service_metric="compute.googleapis.com/gpus_per_gpu_family",
    dimensions=(("gpu_family", "NVIDIA_H200"),),
)
A100_GPU_BUCKET = QuotaBucket(
    key="compute:NVIDIA_A100_GPUS",
    display_name="A100 40GB GPUs",
    unit_name="GPUs",
    source="compute",
    compute_metric="NVIDIA_A100_GPUS",
)
A100_80GB_GPU_BUCKET = QuotaBucket(
    key="compute:NVIDIA_A100_80GB_GPUS",
    display_name="A100 80GB GPUs",
    unit_name="GPUs",
    source="compute",
    compute_metric="NVIDIA_A100_80GB_GPUS",
)


def gpu_count(machine_type: str) -> int:
    return int(machine_type.split("-")[-1][:-1])


def n4_cpu_count(machine_type: str) -> int:
    return int(machine_type.split("-")[-1])


def machine_type_requirements(machine_type: str) -> list[QuotaRequirement]:
    requirements = [QuotaRequirement(INSTANCE_BUCKET, 1)]
    if machine_type.startswith("n4-standard-"):
        requirements.append(QuotaRequirement(N4_CPU_BUCKET, n4_cpu_count(machine_type)))
    elif machine_type.startswith("a3-highgpu-"):
        requirements.append(QuotaRequirement(H100_GPU_BUCKET, gpu_count(machine_type)))
    elif machine_type == "a3-ultragpu-8g":
        requirements.append(QuotaRequirement(H200_GPU_BUCKET, 8))
    elif machine_type.startswith("a2-highgpu-"):
        requirements.append(QuotaRequirement(A100_GPU_BUCKET, gpu_count(machine_type)))
    elif machine_type.startswith("a2-ultragpu-"):
        requirements.append(QuotaRequirement(A100_80GB_GPU_BUCKET, gpu_count(machine_type)))
    else:
        raise ValueError(f"Invalid machine type: {machine_type}")
    return requirements


def aggregate_requirements(machine_types: list[str]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for machine_type in machine_types:
        for requirement in machine_type_requirements(machine_type):
            totals[requirement.bucket.key] = totals.get(requirement.bucket.key, 0) + requirement.amount
    return totals


def buckets_for_machine_types(machine_types: list[str]) -> dict[str, QuotaBucket]:
    buckets = {}
    for machine_type in machine_types:
        for requirement in machine_type_requirements(machine_type):
            buckets[requirement.bucket.key] = requirement.bucket
    return buckets


@lru_cache(maxsize=1)
def project_id() -> str:
    from main_service import PROJECT_ID

    return PROJECT_ID


@lru_cache(maxsize=1)
def project_number() -> str:
    return ProjectsClient().get_project(name=f"projects/{project_id()}").name.split("/")[-1]


def quota_session() -> AuthorizedSession:
    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    return AuthorizedSession(credentials)


def quota_value_for_dimensions(quota_info: dict, region: str, dimensions: tuple[tuple[str, str], ...]) -> int:
    target_dimensions = {"region": region, **dict(dimensions)}
    best_score = -1
    best_value = 0
    for limit in quota_info["consumerQuotaLimits"]:
        if "{region}" not in limit["unit"]:
            continue
        for bucket in limit.get("quotaBuckets", []):
            bucket_dimensions = bucket.get("dimensions") or {}
            if any(target_dimensions.get(key) != value for key, value in bucket_dimensions.items()):
                continue
            raw_value = bucket.get("effectiveLimit")
            if raw_value is None:
                continue
            score = len(bucket_dimensions)
            if score > best_score:
                best_score = score
                best_value = int(float(raw_value))
    return best_value


def fetch_service_usage_quota_limit(bucket: QuotaBucket, region: str) -> int:
    encoded_metric = quote(bucket.service_metric or "", safe="")
    url = (
        "https://serviceusage.googleapis.com/v1beta1/"
        f"projects/{project_number()}/services/compute.googleapis.com/"
        f"consumerQuotaMetrics/{encoded_metric}"
    )
    response = quota_session().get(url, timeout=5)
    response.raise_for_status()
    value = quota_value_for_dimensions(response.json(), region, bucket.dimensions)
    return value


def fetch_compute_quota_limits(
    buckets: list[QuotaBucket], region: str, regions_client: RegionsClient | None = None
) -> dict[str, int]:
    client = regions_client or RegionsClient()
    region_resource = client.get(project=project_id(), region=region)
    quotas = {quota.metric: int(quota.limit) for quota in region_resource.quotas}
    return {bucket.key: quotas.get(bucket.compute_metric, 0) for bucket in buckets}


def fetch_quota_limits(
    buckets: dict[str, QuotaBucket], region: str, regions_client: RegionsClient | None = None
) -> dict[str, int]:
    limits: dict[str, int] = {}
    compute_buckets = [bucket for bucket in buckets.values() if bucket.source == "compute"]
    if compute_buckets:
        limits.update(fetch_compute_quota_limits(compute_buckets, region, regions_client))
    for bucket in buckets.values():
        if bucket.source == "serviceusage":
            limits[bucket.key] = fetch_service_usage_quota_limit(bucket, region)
    return limits


def active_machine_types_for_region(nodes: list[dict], region: str) -> list[str]:
    return [
        node["machine_type"]
        for node in nodes
        if node.get("status") in ACTIVE_QUOTA_NODE_STATUSES
        and node.get("gcp_region") == region
    ]


def cap_boot_machine_types(
    requested_machine_types: list[str],
    region: str,
    active_machine_types: list[str],
    regions_client: RegionsClient | None = None,
) -> QuotaPlan:
    buckets = buckets_for_machine_types(requested_machine_types + active_machine_types)
    limits = fetch_quota_limits(buckets, region, regions_client)
    used = aggregate_requirements(active_machine_types)
    remaining = {
        key: max(0, limit - used.get(key, 0))
        for key, limit in limits.items()
    }

    kept = []
    for machine_type in requested_machine_types:
        requirements = machine_type_requirements(machine_type)
        if all(remaining[requirement.bucket.key] >= requirement.amount for requirement in requirements):
            kept.append(machine_type)
            for requirement in requirements:
                remaining[requirement.bucket.key] -= requirement.amount

    requested_counts = Counter(requested_machine_types)
    allowed_counts = Counter(kept)
    caps = []
    for machine_type, requested_count in requested_counts.items():
        allowed_count = allowed_counts[machine_type]
        if allowed_count >= requested_count:
            continue
        limiting_requirements = sorted(
            machine_type_requirements(machine_type),
            key=lambda requirement: remaining[requirement.bucket.key] // requirement.amount,
        )
        bucket = limiting_requirements[0].bucket
        limit = limits[bucket.key]
        caps.append(
            QuotaCap(
                machine_type=machine_type,
                region=region,
                requested=requested_count,
                allowed=allowed_count,
                limit=limit,
                used=used.get(bucket.key, 0),
                available=max(0, limit - used.get(bucket.key, 0)),
                quota=bucket.display_name,
                units=bucket.unit_name,
            )
        )
    return QuotaPlan(kept, caps)


def quota_status(
    bucket: QuotaBucket,
    region: str,
    active_machine_types: list[str],
    regions_client: RegionsClient | None = None,
) -> dict:
    buckets = {bucket.key: bucket}
    for active_machine_type in active_machine_types:
        for requirement in machine_type_requirements(active_machine_type):
            if requirement.bucket.key == bucket.key:
                buckets[bucket.key] = requirement.bucket
    limit = fetch_quota_limits(buckets, region, regions_client)[bucket.key]
    used = aggregate_requirements(active_machine_types).get(bucket.key, 0)
    return {
        "limit": limit,
        "used": used,
        "available": max(0, limit - used),
        "quota": bucket.display_name,
        "units": bucket.unit_name,
    }
