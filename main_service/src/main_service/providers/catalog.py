"""
Machine catalog: everything the scheduler needs to know about machine types
on both clouds, in one place. Node selection, growth packing, and GPU mapping
all go through these functions so the rest of main_service never parses
machine-type strings itself.

GCP CPU nodes are n4-standard-N (4 GB RAM per vCPU).
AWS CPU nodes are m7i.<size> (also 4 GiB RAM per vCPU), and Azure CPU nodes
are Standard_DNas_v5 (also 4 GiB RAM per vCPU), so capacity math is identical
across clouds.
"""

import re


SETTINGS_MACHINE_TYPES = {
    "gcp": (
        "n4-standard-2",
        "n4-standard-4",
        "n4-standard-8",
        "n4-standard-16",
        "n4-standard-32",
        "n4-standard-64",
        "n4-standard-80",
        "a2-highgpu-1g",
        "a2-highgpu-2g",
        "a2-highgpu-4g",
        "a2-highgpu-8g",
        "a2-ultragpu-1g",
        "a2-ultragpu-2g",
        "a2-ultragpu-4g",
        "a2-ultragpu-8g",
        "a3-highgpu-1g",
        "a3-highgpu-2g",
        "a3-highgpu-4g",
        "a3-highgpu-8g",
    ),
    "aws": (
        "m7i.large",
        "m7i.xlarge",
        "m7i.2xlarge",
        "m7i.4xlarge",
        "m7i.8xlarge",
        "m7i.16xlarge",
        "p4d.24xlarge",
        "p4de.24xlarge",
        "p5.4xlarge",
        "p5.48xlarge",
    ),
    "azure": (
        "Standard_D2as_v5",
        "Standard_D4as_v5",
        "Standard_D8as_v5",
        "Standard_D16as_v5",
        "Standard_D32as_v5",
        "Standard_D64as_v5",
    ),
}

CPU_ONLY_IMAGE_REPOSITORIES = ("python", "ubuntu", "debian", "alpine")

SETTINGS_REGIONS = {
    "gcp": (
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
        "europe-southwest1",
        "europe-north2",
        "asia-northeast1",
        "asia-northeast3",
        "asia-south1",
        "asia-southeast1",
        "australia-southeast1",
        "me-west1",
    ),
    "aws": (
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
    ),
    "azure": (
        "eastus",
        "eastus2",
        "centralus",
        "southcentralus",
        "westus2",
        "westus3",
        "canadacentral",
        "brazilsouth",
        "northeurope",
        "westeurope",
        "uksouth",
        "francecentral",
        "germanywestcentral",
        "swedencentral",
        "centralindia",
        "southeastasia",
        "eastasia",
        "japaneast",
        "koreacentral",
        "australiaeast",
    ),
}

_GPU_REGIONS = {
    "gcp": {
        "a2-highgpu": (
            "us-central1",
            "us-west3",
            "us-east1",
            "us-west4",
            "us-west1",
            "europe-west4",
            "asia-northeast1",
            "asia-northeast3",
            "me-west1",
            "asia-southeast1",
        ),
        "a2-ultragpu": (
            "us-central1",
            "us-east5",
            "us-east4",
            "europe-west4",
            "asia-southeast1",
        ),
        "a3-highgpu": (
            "us-central1",
            "us-east5",
            "us-east4",
            "us-west4",
            "us-west1",
            "europe-west1",
            "asia-northeast1",
            "asia-southeast1",
        ),
    },
    "aws": {
        "p4de.": (
            "us-east-1",
            "us-west-2",
            "eu-central-1",
            "ap-northeast-1",
            "ap-southeast-1",
        ),
        "p4d.": (
            "us-east-1",
            "us-east-2",
            "us-west-2",
            "ca-central-1",
            "sa-east-1",
            "eu-west-1",
            "eu-west-2",
            "eu-central-1",
            "eu-north-1",
            "ap-south-1",
            "ap-northeast-1",
            "ap-northeast-2",
            "ap-southeast-2",
        ),
        "p5.4xlarge": (
            "us-east-1",
            "us-east-2",
            "us-west-2",
            "sa-east-1",
            "eu-west-2",
            "ap-south-1",
            "ap-northeast-1",
            "ap-southeast-2",
        ),
        "p5.48xlarge": (
            "us-east-1",
            "us-east-2",
            "us-west-1",
            "us-west-2",
            "ca-central-1",
            "sa-east-1",
            "eu-west-2",
            "eu-north-1",
            "ap-south-1",
            "ap-northeast-1",
            "ap-northeast-2",
            "ap-southeast-2",
        ),
    },
}

ON_DEMAND_HOURLY_USD = {
    "a2-highgpu-1g": 3.673385,
    "a2-highgpu-2g": 7.34677,
    "a2-highgpu-4g": 14.69354,
    "a2-highgpu-8g": 29.38708,
    "a2-ultragpu-1g": 5.06879789,
    "a2-ultragpu-2g": 10.137595781,
    "a2-ultragpu-4g": 20.275191562,
    "a2-ultragpu-8g": 40.550383123,
    "a3-highgpu-1g": 11.0612,
    "a3-highgpu-2g": 22.1225,
    "a3-highgpu-4g": 44.245,
    "a3-highgpu-8g": 88.490000119,
    "n4-standard-2": 0.0907,
    "n4-standard-4": 0.1814,
    "n4-standard-8": 0.3628,
    "n4-standard-16": 0.7256,
    "n4-standard-32": 1.4512,
    "n4-standard-64": 2.9024,
    "n4-standard-80": 3.628,
    "p4d.24xlarge": 32.7726,
    "p4de.24xlarge": 40.9657,
    "p5.4xlarge": 6.88,
    "p5.48xlarge": 98.32,
    "m7i.large": 0.1008,
    "m7i.xlarge": 0.2016,
    "m7i.2xlarge": 0.4032,
    "m7i.4xlarge": 0.8064,
    "m7i.8xlarge": 1.6128,
    "m7i.16xlarge": 3.2256,
    "Standard_D2as_v5": 0.086,
    "Standard_D4as_v5": 0.172,
    "Standard_D8as_v5": 0.344,
    "Standard_D16as_v5": 0.688,
    "Standard_D32as_v5": 1.376,
    "Standard_D64as_v5": 2.752,
}

_M7I_SIZE_TO_CPUS = {
    "large": 2,
    "xlarge": 4,
    "2xlarge": 8,
    "4xlarge": 16,
    "8xlarge": 32,
    "12xlarge": 48,
    "16xlarge": 64,
    "24xlarge": 96,
}

_DS_CPU_SIZES = (2, 4, 8, 16, 32, 48, 64, 96, 128, 192)
_DS_PATTERN = re.compile(r"^Standard_D(\d+)(?:as_v5|s_v[67])$")

# Largest-first sizes the grow path may provision. n4-standard-48 is
# intentionally omitted to match main_service/frontend/src/types/constants.ts
# (pricing isn't defined for it). m7i.12xlarge and Standard_D48as_v5 omitted
# for symmetry.
GCP_PACK_SIZES = (80, 64, 32, 16, 8, 4, 2)
AWS_PACK_SIZES = ("16xlarge", "8xlarge", "4xlarge", "2xlarge", "xlarge", "large")
AZURE_PACK_SIZES = (64, 32, 16, 8, 4, 2)

# One entry per GPU machine type family: cpus/ram for the smallest variant,
# gpus parsed per-type below.
_GPU_MACHINE_SPECS = {
    # GCP: a2-highgpu-Ng (A100 40G), a2-ultragpu-Ng (A100 80G), a3-highgpu-Ng (H100)
    "a2-highgpu": {"cpus_per_gpu": 12, "ram_per_gpu": 85},
    "a2-ultragpu": {"cpus_per_gpu": 12, "ram_per_gpu": 170},
    "a3-highgpu": {"cpus_per_gpu": 26, "ram_per_gpu": 234},
    # AWS single-GPU-per-call machines
    "g4dn.xlarge": {"cpus": 4, "ram_gb": 16, "gpus": 1},
    "p4d.24xlarge": {"cpus": 96, "ram_gb": 1152, "gpus": 8},
    "p4de.24xlarge": {"cpus": 96, "ram_gb": 1152, "gpus": 8},
    "p5.4xlarge": {"cpus": 16, "ram_gb": 256, "gpus": 1},
    "p5.48xlarge": {"cpus": 192, "ram_gb": 2048, "gpus": 8},
}

# Maps the `func_gpu` strings users pass to `remote_parallel_map` to the
# smallest machine type with that GPU, per cloud. AWS sells A100s only in
# 8-GPU machines (each GPU still serves exactly one function call); H100s
# are available as single-GPU p5.4xlarge.
GPU_MACHINE_TYPES = {
    "gcp": {
        "A100": "a2-highgpu-1g",
        "A100_40G": "a2-highgpu-1g",
        "A100_80G": "a2-ultragpu-1g",
        "H100": "a3-highgpu-1g",
        "H100_80G": "a3-highgpu-1g",
    },
    "aws": {
        "T4": "g4dn.xlarge",
        "A100": "p4d.24xlarge",
        "A100_40G": "p4d.24xlarge",
        "A100_80G": "p4de.24xlarge",
        "H100": "p5.4xlarge",
        "H100_80G": "p5.4xlarge",
    },
    # Empty until the Azure node image ships NVIDIA drivers; any func_gpu
    # request then fails with a clear "must be one of []" error.
    "azure": {},
}


def gpu_display(machine_type: str) -> str | None:
    families = {
        "a2-highgpu": ("A100", "40G"),
        "a2-ultragpu": ("A100", "80G"),
        "a3-highgpu": ("H100", "80G"),
        "g4dn.": ("T4", "16G"),
        "p4d.": ("A100", "40G"),
        "p4de.": ("A100", "80G"),
        "p5.": ("H100", "80G"),
    }
    for prefix, (model, memory) in families.items():
        if machine_type.startswith(prefix):
            count = machine_spec(machine_type)["gpus"]
            return f"{count}x {model} {memory}"
    return None


def image_is_gpu_compatible(image: str) -> bool:
    repository = image.split(":", 1)[0].removeprefix("docker.io/")
    repository = repository.removeprefix("library/")
    return repository not in CPU_ONLY_IMAGE_REPOSITORIES


def machine_spec(machine_type: str) -> dict:
    """Returns {"cpus": int, "ram_gb": int, "gpus": int} for any supported
    machine type on either cloud. Raises ValueError for unknown types."""
    if (
        machine_type.startswith("n4-standard-")
        and machine_type.split("-")[-1].isdigit()
    ):
        cpus = int(machine_type.split("-")[-1])
        return {"cpus": cpus, "ram_gb": cpus * 4, "gpus": 0}

    if (
        machine_type.startswith("m7i.")
        and machine_type.split(".")[-1] in _M7I_SIZE_TO_CPUS
    ):
        cpus = _M7I_SIZE_TO_CPUS[machine_type.split(".")[-1]]
        return {"cpus": cpus, "ram_gb": cpus * 4, "gpus": 0}

    ds_match = _DS_PATTERN.match(machine_type)
    if ds_match and int(ds_match.group(1)) in _DS_CPU_SIZES:
        cpus = int(ds_match.group(1))
        return {"cpus": cpus, "ram_gb": cpus * 4, "gpus": 0}

    if machine_type in _GPU_MACHINE_SPECS:
        spec = _GPU_MACHINE_SPECS[machine_type]
        return {"cpus": spec["cpus"], "ram_gb": spec["ram_gb"], "gpus": spec["gpus"]}

    for family, spec in _GPU_MACHINE_SPECS.items():
        if "cpus_per_gpu" in spec and machine_type.startswith(family + "-"):
            suffix = machine_type.split("-")[-1]  # e.g. "1g", "8g"
            if suffix.endswith("g") and suffix[:-1].isdigit():
                gpus = int(suffix[:-1])
                return {
                    "cpus": spec["cpus_per_gpu"] * gpus,
                    "ram_gb": spec["ram_per_gpu"] * gpus,
                    "gpus": gpus,
                }

    raise ValueError(f"Unknown machine type: {machine_type}")


def parallelism_capacity(
    machine_type: str, func_cpu: int | str, func_ram: int | str
) -> int:
    """How many copies of a UDF with func_cpu/func_ram fit on one node.
    GPU machines run one function call per GPU."""
    func_cpu = 1 if func_cpu == "dynamic" else int(func_cpu)
    func_ram = 4 if func_ram == "dynamic" else int(func_ram)
    spec = machine_spec(machine_type)
    if spec["gpus"] > 0:
        return spec["gpus"]
    return min(spec["cpus"] // func_cpu, spec["ram_gb"] // func_ram)


def machine_type_cpu_count(machine_type: str) -> int:
    try:
        return machine_spec(machine_type)["cpus"]
    except ValueError:
        return 1


def pack_cpu_machines(num_cpus: int, cloud_provider: str) -> list[str]:
    """Pick machine types that cover `num_cpus`, greedily using as many of the
    largest size as possible and covering any remainder with the smallest size
    that fits. e.g. (gcp) 95 -> [n4-standard-80, n4-standard-16]."""
    if cloud_provider == "aws":
        sizes = [(f"m7i.{size}", _M7I_SIZE_TO_CPUS[size]) for size in AWS_PACK_SIZES]
    elif cloud_provider == "azure":
        sizes = [(f"Standard_D{cpus}as_v5", cpus) for cpus in AZURE_PACK_SIZES]
    else:
        sizes = [(f"n4-standard-{cpus}", cpus) for cpus in GCP_PACK_SIZES]

    machines = []
    largest_name, largest_cpus = sizes[0]
    remaining = num_cpus
    while remaining >= largest_cpus:
        machines.append(largest_name)
        remaining -= largest_cpus
    if remaining > 0:
        for name, cpus in reversed(sizes):
            if cpus >= remaining:
                machines.append(name)
                break
    return machines


def is_packable_cpu_machine(machine_type: str) -> bool:
    """True when the grow path should ignore the configured size and pack
    the required CPUs into the family's standard sizes instead."""
    return (
        machine_type.startswith("n4-standard-")
        or machine_type.startswith("m7i.")
        or bool(_DS_PATTERN.match(machine_type))
    )


def gpu_machine_type(func_gpu: str | None, cloud_provider: str) -> str | None:
    """Resolve a user-facing `func_gpu` string to its target machine type.
    Returns None when `func_gpu` is None. Raises ValueError for unknown strings."""
    if func_gpu is None:
        return None
    types = GPU_MACHINE_TYPES[cloud_provider]
    if func_gpu not in types:
        raise ValueError(f"func_gpu must be one of {sorted(types)}")
    return types[func_gpu]


def gpu_machine_prefix(func_gpu: str | None, cloud_provider: str) -> str | None:
    """Return the GPU-family prefix (e.g. `a2-highgpu-`) for a `func_gpu`
    string, used to filter ready nodes so larger variants of the same family
    can serve the request."""
    machine_type = gpu_machine_type(func_gpu, cloud_provider)
    if machine_type is None:
        return None
    if cloud_provider == "aws":
        return machine_type.split(".")[0] + "."
    return machine_type.rsplit("-", 1)[0] + "-"


def settings_options(cloud_provider: str) -> dict:
    machines = []
    for machine_type in SETTINGS_MACHINE_TYPES[cloud_provider]:
        spec = machine_spec(machine_type)
        machines.append(
            {
                "machine_type": machine_type,
                "vcpu_count": spec["cpus"],
                "memory_bytes": spec["ram_gb"] * 1024**3,
                "gpu_count": spec["gpus"],
                "gpu_display": gpu_display(machine_type),
                "regions": next(
                    (
                        list(regions)
                        for prefix, regions in _GPU_REGIONS.get(
                            cloud_provider, {}
                        ).items()
                        if machine_type.startswith(prefix)
                    ),
                    list(SETTINGS_REGIONS[cloud_provider]),
                ),
            }
        )
    return {
        "machine_types": machines,
        "regions": list(SETTINGS_REGIONS[cloud_provider]),
        "cpu_only_image_repositories": list(CPU_ONLY_IMAGE_REPOSITORIES),
        "constraints": {
            "quantity": {"minimum": 1, "maximum": 1000},
            "disk_gb": {"minimum": 10, "maximum": 2000},
            "inactivity_timeout_seconds": {"minimum": 0, "maximum": 86400},
        },
    }


def on_demand_hourly_usd(machine_type: str) -> float | None:
    return ON_DEMAND_HOURLY_USD.get(machine_type)
