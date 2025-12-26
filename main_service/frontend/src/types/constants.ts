export const MACHINE_TYPES = [
    "n4-standard-2",
    "n4-standard-4",
    "n4-standard-8",
    "n4-standard-16",
    "n4-standard-32",
    "n4-standard-48",
    "n4-standard-64",
    "n4-standard-80",
];

export type GcpHourlyRate = {
  hourlyUsd: number;
  spot: boolean; // false = on-demand, true = spot
};

// Region -> machine_type -> rate (on-demand)
export const GCP_ON_DEMAND_HOURLY_USD: Record<string, Record<string, GcpHourlyRate>> = {
  "us-central1": {
    "n4-standard-2": { hourlyUsd: 0.0907, spot: false },
    "n4-standard-4": { hourlyUsd: 0.1814, spot: false },
    "n4-standard-8": { hourlyUsd: 0.3628, spot: false },
    "n4-standard-16": { hourlyUsd: 0.7256, spot: false },
    "n4-standard-32": { hourlyUsd: 1.4512, spot: false },
    "n4-standard-64": { hourlyUsd: 2.9024, spot: false },
    "n4-standard-80": { hourlyUsd: 3.628, spot: false },
  },

  "us-east1": {
    "n4-standard-2": { hourlyUsd: 0.0907, spot: false },
    "n4-standard-4": { hourlyUsd: 0.1814, spot: false },
    "n4-standard-8": { hourlyUsd: 0.3628, spot: false },
    "n4-standard-16": { hourlyUsd: 0.7256, spot: false },
    "n4-standard-32": { hourlyUsd: 1.4512, spot: false },
    "n4-standard-64": { hourlyUsd: 2.9024, spot: false },
    "n4-standard-80": { hourlyUsd: 3.628, spot: false },
  },

  "us-east4": {
    "n4-standard-2": { hourlyUsd: 0.0907, spot: false },
    "n4-standard-4": { hourlyUsd: 0.1814, spot: false },
    "n4-standard-8": { hourlyUsd: 0.3628, spot: false },
    "n4-standard-16": { hourlyUsd: 0.7256, spot: false },
    "n4-standard-32": { hourlyUsd: 1.4512, spot: false },
    "n4-standard-64": { hourlyUsd: 2.9024, spot: false },
    "n4-standard-80": { hourlyUsd: 3.628, spot: false },
  },

  "us-east5": {
    "n4-standard-2": { hourlyUsd: 0.0907, spot: false },
    "n4-standard-4": { hourlyUsd: 0.1814, spot: false },
    "n4-standard-8": { hourlyUsd: 0.3628, spot: false },
    "n4-standard-16": { hourlyUsd: 0.7256, spot: false },
    "n4-standard-32": { hourlyUsd: 1.4512, spot: false },
    "n4-standard-64": { hourlyUsd: 2.9024, spot: false },
    "n4-standard-80": { hourlyUsd: 3.628, spot: false },
  },

  "us-west1": {
    "n4-standard-2": { hourlyUsd: 0.0907, spot: false },
    "n4-standard-4": { hourlyUsd: 0.1814, spot: false },
    "n4-standard-8": { hourlyUsd: 0.3628, spot: false },
    "n4-standard-16": { hourlyUsd: 0.7256, spot: false },
    "n4-standard-32": { hourlyUsd: 1.4512, spot: false },
    "n4-standard-64": { hourlyUsd: 2.9024, spot: false },
    "n4-standard-80": { hourlyUsd: 3.628, spot: false },
  },

  "us-south1": {
    "n4-standard-2": { hourlyUsd: 0.107026, spot: false },
    "n4-standard-4": { hourlyUsd: 0.214052, spot: false },
    "n4-standard-8": { hourlyUsd: 0.428104, spot: false },
    "n4-standard-16": { hourlyUsd: 0.856208, spot: false },
    "n4-standard-32": { hourlyUsd: 1.712416, spot: false },
    "n4-standard-64": { hourlyUsd: 3.424832, spot: false },
    "n4-standard-80": { hourlyUsd: 4.28104, spot: false },
  },

  "us-west3": {
    "n4-standard-2": { hourlyUsd: 0.109030472, spot: false },
    "n4-standard-4": { hourlyUsd: 0.218060944, spot: false },
    "n4-standard-8": { hourlyUsd: 0.436121888, spot: false },
    "n4-standard-16": { hourlyUsd: 0.872243776, spot: false },
    "n4-standard-32": { hourlyUsd: 1.744487552, spot: false },
    "n4-standard-64": { hourlyUsd: 3.488975104, spot: false },
    "n4-standard-80": { hourlyUsd: 4.36121888, spot: false },
  },

  "northamerica-northeast2": {
    "n4-standard-2": { hourlyUsd: 0.099869772, spot: false },
    "n4-standard-4": { hourlyUsd: 0.199739544, spot: false },
    "n4-standard-8": { hourlyUsd: 0.399479088, spot: false },
    "n4-standard-16": { hourlyUsd: 0.798958176, spot: false },
    "n4-standard-32": { hourlyUsd: 1.597916352, spot: false },
    "n4-standard-64": { hourlyUsd: 3.195832704, spot: false },
    "n4-standard-80": { hourlyUsd: 3.99479088, spot: false },
  },

  "northamerica-south1": {
    "n4-standard-2": { hourlyUsd: 0.098863, spot: false },
    "n4-standard-4": { hourlyUsd: 0.197726, spot: false },
    "n4-standard-8": { hourlyUsd: 0.395452, spot: false },
    "n4-standard-16": { hourlyUsd: 0.790904, spot: false },
    "n4-standard-32": { hourlyUsd: 1.581808, spot: false },
    "n4-standard-64": { hourlyUsd: 3.163616, spot: false },
    "n4-standard-80": { hourlyUsd: 3.95452, spot: false },
  },

  "europe-west1": {
    "n4-standard-2": { hourlyUsd: 0.099869772, spot: false },
    "n4-standard-4": { hourlyUsd: 0.199739544, spot: false },
    "n4-standard-8": { hourlyUsd: 0.399479088, spot: false },
    "n4-standard-16": { hourlyUsd: 0.798958176, spot: false },
    "n4-standard-32": { hourlyUsd: 1.597916352, spot: false },
    "n4-standard-64": { hourlyUsd: 3.195832704, spot: false },
    "n4-standard-80": { hourlyUsd: 3.99479088, spot: false },
  },

  "europe-west2": {
    "n4-standard-2": { hourlyUsd: 0.103398, spot: false },
    "n4-standard-4": { hourlyUsd: 0.206796, spot: false },
    "n4-standard-8": { hourlyUsd: 0.413592, spot: false },
    "n4-standard-16": { hourlyUsd: 0.827184, spot: false },
    "n4-standard-32": { hourlyUsd: 1.654368, spot: false },
    "n4-standard-64": { hourlyUsd: 3.308736, spot: false },
    "n4-standard-80": { hourlyUsd: 4.13592, spot: false },
  },
};

export function getOnDemandHourlyUsd(region: string, machineType: string): number | null {
  const rate = GCP_ON_DEMAND_HOURLY_USD?.[region]?.[machineType]?.hourlyUsd;
  return typeof rate === "number" ? rate : null;
}