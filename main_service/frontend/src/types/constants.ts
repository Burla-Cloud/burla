// constants.ts

export type VmType = "CPU" | "A100" | "H100" | "H200";

export const VM_TYPES: VmType[] = ["CPU", "A100", "H100", "H200"];

const GCP_MACHINE_MAPPING: Record<string, VmType> = {
  "a2-highgpu-1g": "A100",
  "a2-highgpu-2g": "A100",
  "a2-highgpu-4g": "A100",
  "a2-highgpu-8g": "A100",
  "a2-ultragpu-1g": "A100",
  "a2-ultragpu-2g": "A100",
  "a2-ultragpu-4g": "A100",
  "a2-ultragpu-8g": "A100",

  "a3-highgpu-1g": "H100",
  "a3-highgpu-2g": "H100",
  "a3-highgpu-4g": "H100",
  "a3-highgpu-8g": "H100",

  "a3-ultragpu-8g": "H200",

  "n4-standard-2": "CPU",
  "n4-standard-4": "CPU",
  "n4-standard-8": "CPU",
  "n4-standard-16": "CPU",
  "n4-standard-32": "CPU",
  "n4-standard-64": "CPU",
  "n4-standard-80": "CPU",

  // AWS
  "p4d.24xlarge": "A100",
  "p4de.24xlarge": "A100",
  "p5.4xlarge": "H100",
  "p5.48xlarge": "H100",

  "m7i.large": "CPU",
  "m7i.xlarge": "CPU",
  "m7i.2xlarge": "CPU",
  "m7i.4xlarge": "CPU",
  "m7i.8xlarge": "CPU",
  "m7i.16xlarge": "CPU",

  // Azure
  Standard_D2s_v6: "CPU",
  Standard_D4s_v6: "CPU",
  Standard_D8s_v6: "CPU",
  Standard_D16s_v6: "CPU",
  Standard_D32s_v6: "CPU",
  Standard_D64s_v6: "CPU",
};

const GCP_MACHINE_PRICING_MAPPING: Record<
  string,
  { type: VmType; on_demand_price: number }
> = {
  "a2-highgpu-1g": { type: "A100", on_demand_price: 3.673385 },
  "a2-highgpu-2g": { type: "A100", on_demand_price: 7.34677 },
  "a2-highgpu-4g": { type: "A100", on_demand_price: 14.69354 },
  "a2-highgpu-8g": { type: "A100", on_demand_price: 29.38708 },

  "a2-ultragpu-1g": { type: "A100", on_demand_price: 5.06879789 },
  "a2-ultragpu-2g": { type: "A100", on_demand_price: 10.137595781 },
  "a2-ultragpu-4g": { type: "A100", on_demand_price: 20.275191562 },
  "a2-ultragpu-8g": { type: "A100", on_demand_price: 40.550383123 },

  "a3-highgpu-1g": { type: "H100", on_demand_price: 11.0612 },
  "a3-highgpu-2g": { type: "H100", on_demand_price: 22.1225 },
  "a3-highgpu-4g": { type: "H100", on_demand_price: 44.245 },
  "a3-highgpu-8g": { type: "H100", on_demand_price: 88.490000119 },

  "a3-ultragpu-8g": { type: "H200", on_demand_price: 84.806908493 },

  "n4-standard-2": { type: "CPU", on_demand_price: 0.0907 },
  "n4-standard-4": { type: "CPU", on_demand_price: 0.1814 },
  "n4-standard-8": { type: "CPU", on_demand_price: 0.3628 },
  "n4-standard-16": { type: "CPU", on_demand_price: 0.7256 },
  "n4-standard-32": { type: "CPU", on_demand_price: 1.4512 },
  "n4-standard-64": { type: "CPU", on_demand_price: 2.9024 },
  "n4-standard-80": { type: "CPU", on_demand_price: 3.628 },

  // AWS on-demand (us-east-1)
  "p4d.24xlarge": { type: "A100", on_demand_price: 32.7726 },
  "p4de.24xlarge": { type: "A100", on_demand_price: 40.9657 },
  "p5.4xlarge": { type: "H100", on_demand_price: 6.88 },
  "p5.48xlarge": { type: "H100", on_demand_price: 98.32 },

  "m7i.large": { type: "CPU", on_demand_price: 0.1008 },
  "m7i.xlarge": { type: "CPU", on_demand_price: 0.2016 },
  "m7i.2xlarge": { type: "CPU", on_demand_price: 0.4032 },
  "m7i.4xlarge": { type: "CPU", on_demand_price: 0.8064 },
  "m7i.8xlarge": { type: "CPU", on_demand_price: 1.6128 },
  "m7i.16xlarge": { type: "CPU", on_demand_price: 3.2256 },

  // Azure on-demand (eastus)
  Standard_D2s_v6: { type: "CPU", on_demand_price: 0.101 },
  Standard_D4s_v6: { type: "CPU", on_demand_price: 0.202 },
  Standard_D8s_v6: { type: "CPU", on_demand_price: 0.403 },
  Standard_D16s_v6: { type: "CPU", on_demand_price: 0.806 },
  Standard_D32s_v6: { type: "CPU", on_demand_price: 1.613 },
  Standard_D64s_v6: { type: "CPU", on_demand_price: 3.226 },
};

// AWS machine specs for cluster-status displays (mirrors main_service/providers/catalog.py).
export const AWS_MACHINE_SPECS: Record<string, { cpus: number; ram: string; gpu: string | null }> =
  {
    "m7i.large": { cpus: 2, ram: "8G", gpu: null },
    "m7i.xlarge": { cpus: 4, ram: "16G", gpu: null },
    "m7i.2xlarge": { cpus: 8, ram: "32G", gpu: null },
    "m7i.4xlarge": { cpus: 16, ram: "64G", gpu: null },
    "m7i.8xlarge": { cpus: 32, ram: "128G", gpu: null },
    "m7i.12xlarge": { cpus: 48, ram: "192G", gpu: null },
    "m7i.16xlarge": { cpus: 64, ram: "256G", gpu: null },
    "m7i.24xlarge": { cpus: 96, ram: "384G", gpu: null },
    "p4d.24xlarge": { cpus: 96, ram: "1152G", gpu: "8x A100 40G" },
    "p4de.24xlarge": { cpus: 96, ram: "1152G", gpu: "8x A100 80G" },
    "p5.4xlarge": { cpus: 16, ram: "256G", gpu: "1x H100 80G" },
    "p5.48xlarge": { cpus: 192, ram: "2048G", gpu: "8x H100 80G" },
  };

export function getVmCategory(machineType: string): VmType | null {
  const mt = String(machineType || "");
  const mapped = GCP_MACHINE_MAPPING[mt];
  if (mapped) return mapped;

  // fallback: any n4 (GCP), m7i (AWS), or Dsv6 (Azure) is CPU
  if (mt.startsWith("n4-") || mt.startsWith("m7i.") || mt.startsWith("Standard_D")) return "CPU";

  return null;
}

export function getOnDemandHourlyUsdForMachine(machineType: string): number | null {
  const mt = String(machineType || "");
  const price = GCP_MACHINE_PRICING_MAPPING[mt]?.on_demand_price;
  return typeof price === "number" ? price : null;
}