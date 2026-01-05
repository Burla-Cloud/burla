// constants.ts

export type VmType = "CPU" | "A100" | "H100" | "H200";

export const VM_TYPES: VmType[] = ["CPU", "A100", "H100", "H200"];

// Keep if other UI uses it. Removed n4-standard-48 because you do not price it.
export const MACHINE_TYPES = [
  "n4-standard-2",
  "n4-standard-4",
  "n4-standard-8",
  "n4-standard-16",
  "n4-standard-32",
  "n4-standard-64",
  "n4-standard-80",
];

export const GCP_MACHINE_MAPPING: Record<string, VmType> = {
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
};

export const GCP_MACHINE_PRICING_MAPPING: Record<
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
};

export function getVmCategory(machineType: string): VmType | null {
  const mt = String(machineType || "");
  const mapped = GCP_MACHINE_MAPPING[mt];
  if (mapped) return mapped;

  // fallback: any n4 is CPU
  if (mt.startsWith("n4-")) return "CPU";

  return null;
}

export function getOnDemandHourlyUsdForMachine(machineType: string): number | null {
  const mt = String(machineType || "");
  const price = GCP_MACHINE_PRICING_MAPPING[mt]?.on_demand_price;
  return typeof price === "number" ? price : null;
}
