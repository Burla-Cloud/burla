import importlib.util
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


QUOTA_PATH = Path(__file__).parents[1] / "src" / "main_service" / "quota.py"
SPEC = importlib.util.spec_from_file_location("quota_under_test", QUOTA_PATH)
quota = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(quota)


def test_machine_type_requirements_cover_burla_machine_families():
    assert quota.machine_type_requirements("n4-standard-8")[-1] == quota.QuotaRequirement(
        quota.N4_CPU_BUCKET, 8
    )
    assert quota.machine_type_requirements("a3-highgpu-8g")[-1] == quota.QuotaRequirement(
        quota.H100_GPU_BUCKET, 8
    )
    assert quota.machine_type_requirements("a3-ultragpu-8g")[-1] == quota.QuotaRequirement(
        quota.H200_GPU_BUCKET, 8
    )
    assert quota.machine_type_requirements("a2-highgpu-4g")[-1] == quota.QuotaRequirement(
        quota.A100_GPU_BUCKET, 4
    )
    assert quota.machine_type_requirements("a2-ultragpu-4g")[-1] == quota.QuotaRequirement(
        quota.A100_80GB_GPU_BUCKET, 4
    )


def test_quota_value_for_dimensions_picks_specific_region_and_family():
    quota_info = {
        "consumerQuotaLimits": [
            {
                "unit": "1/{project}/{region}/{gpu_family}",
                "quotaBuckets": [
                    {
                        "effectiveLimit": "0",
                        "dimensions": {"gpu_family": "NVIDIA_H100"},
                    },
                    {
                        "effectiveLimit": "100",
                        "dimensions": {"region": "us-central1", "gpu_family": "NVIDIA_H100"},
                    },
                ],
            },
            {
                "unit": "1/{project}/{zone}/{gpu_family}",
                "quotaBuckets": [{"effectiveLimit": "-1"}],
            },
        ]
    }

    value = quota.quota_value_for_dimensions(
        quota_info,
        "us-central1",
        (("gpu_family", "NVIDIA_H100"),),
    )

    assert value == 100


def test_fetch_compute_quota_limits_reads_regional_compute_quotas():
    regions_client = SimpleNamespace(
        get=lambda project, region: SimpleNamespace(
            quotas=[
                SimpleNamespace(metric="INSTANCES", limit=10),
                SimpleNamespace(metric="NVIDIA_A100_GPUS", limit=16),
            ]
        )
    )

    limits = quota.fetch_compute_quota_limits(
        [quota.INSTANCE_BUCKET, quota.A100_GPU_BUCKET],
        "us-central1",
        regions_client,
    )

    assert limits[quota.INSTANCE_BUCKET.key] == 10
    assert limits[quota.A100_GPU_BUCKET.key] == 16


def test_cap_boot_machine_types_counts_active_burla_usage():
    def fake_fetch_quota_limits(buckets, region, regions_client=None):
        return {
            key: 16 if key == quota.H100_GPU_BUCKET.key else 100
            for key, bucket in buckets.items()
        }

    with patch.object(quota, "fetch_quota_limits", fake_fetch_quota_limits):
        plan = quota.cap_boot_machine_types(
            ["a3-highgpu-8g", "a3-highgpu-8g"],
            "us-central1",
            active_machine_types=["a3-highgpu-8g"],
        )

    assert plan.machine_types == ["a3-highgpu-8g"]
    assert plan.caps[0].requested == 2
    assert plan.caps[0].allowed == 1
    assert plan.caps[0].limit == 16
    assert plan.caps[0].used == 8
    assert plan.caps[0].available == 8


def test_active_machine_types_for_region_uses_only_active_nodes():
    nodes = [
        {"machine_type": "n4-standard-8", "gcp_region": "us-central1", "status": "READY"},
        {"machine_type": "n4-standard-8", "gcp_region": "us-central1", "status": "BOOTING"},
        {"machine_type": "n4-standard-8", "gcp_region": "us-central1", "status": "FAILED"},
        {"machine_type": "n4-standard-8", "gcp_region": "us-east1", "status": "READY"},
    ]

    assert quota.active_machine_types_for_region(nodes, "us-central1") == [
        "n4-standard-8",
        "n4-standard-8",
    ]


def test_n4_pack_up_to_respects_min_machine_size():
    from main_service.endpoints.cluster_lifecycle import _pack_n4_standard_machines_up_to

    assert _pack_n4_standard_machines_up_to(79, min_size=16) == ["n4-standard-64"]


if __name__ == "__main__":
    for test in [
        test_machine_type_requirements_cover_burla_machine_families,
        test_quota_value_for_dimensions_picks_specific_region_and_family,
        test_fetch_compute_quota_limits_reads_regional_compute_quotas,
        test_cap_boot_machine_types_counts_active_burla_usage,
        test_active_machine_types_for_region_uses_only_active_nodes,
        test_n4_pack_up_to_respects_min_machine_size,
    ]:
        test()
