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
    "a2-highgpu-1g": { hourlyUsd: 3.673385, spot: false },
    "a2-highgpu-2g": { hourlyUsd: 7.34677, spot: false },
    "a2-highgpu-4g": { hourlyUsd: 14.69354, spot: false },
    "a2-highgpu-8g": { hourlyUsd: 29.38708, spot: false }, 
    "a2-ultragpu-1g": { hourlyUsd: 5.06879789, spot: false },
    "a2-ultragpu-2g": { hourlyUsd: 10.137595781, spot: false },
    "a2-ultragpu-4g": { hourlyUsd: 20.275191562, spot: false },
    "a2-ultragpu-8g": { hourlyUsd: 40.550383123, spot: false },
    "a3-highgpu-8g": { hourlyUsd: 88.490000119, spot: false },
  },

  "us-east1": {
    "n4-standard-2": { hourlyUsd: 0.0907, spot: false },
    "n4-standard-4": { hourlyUsd: 0.1814, spot: false },
    "n4-standard-8": { hourlyUsd: 0.3628, spot: false },
    "n4-standard-16": { hourlyUsd: 0.7256, spot: false },
    "n4-standard-32": { hourlyUsd: 1.4512, spot: false },
    "n4-standard-64": { hourlyUsd: 2.9024, spot: false },
    "n4-standard-80": { hourlyUsd: 3.628, spot: false },
    "a2-highgpu-1g": { hourlyUsd: 3.673385, spot: false },
    "a2-highgpu-2g": { hourlyUsd: 7.34677, spot: false },
    "a2-highgpu-4g": { hourlyUsd: 14.69354, spot: false },
    "a2-highgpu-8g": { hourlyUsd: 29.38708, spot: false },
    "a2-ultragpu-1g": { hourlyUsd: 5.06879789, spot: false },
    "a2-ultragpu-2g": { hourlyUsd: 10.137595781, spot: false },
    "a2-ultragpu-4g": { hourlyUsd: 20.275191562, spot: false },
    "a2-ultragpu-8g": { hourlyUsd: 40.550383123, spot: false },
    "a3-highgpu-8g": { hourlyUsd: 88.490000119, spot: false },
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
    "a2-highgpu-1g": { hourlyUsd: 3.673385, spot: false },
    "a2-highgpu-2g": { hourlyUsd: 7.34677, spot: false },
    "a2-highgpu-4g": { hourlyUsd: 14.69354, spot: false },
    "a2-highgpu-8g": { hourlyUsd: 29.38708, spot: false },
    "a2-ultragpu-1g": { hourlyUsd: 5.06879789, spot: false },
    "a2-ultragpu-2g": { hourlyUsd: 10.137595781, spot: false },
    "a2-ultragpu-4g": { hourlyUsd: 20.275191562, spot: false },
    "a2-ultragpu-8g": { hourlyUsd: 40.550383123, spot: false },
    "a3-highgpu-8g": { hourlyUsd: 88.490000119, spot: false },
  },

  "us-west4": {
  "a2-highgpu-1g": { hourlyUsd: 3.934354926, spot: false },
  "a2-highgpu-2g": { hourlyUsd: 7.868709852, spot: false },
  "a2-highgpu-4g": { hourlyUsd: 15.737419704, spot: false },
  "a2-highgpu-8g": { hourlyUsd: 31.474839408, spot: false },
  "a2-ultragpu-1g": { hourlyUsd: 5.707732479, spot: false },
  "a2-ultragpu-2g": { hourlyUsd: 11.415464959, spot: false },
  "a2-ultragpu-4g": { hourlyUsd: 22.830929918, spot: false },
  "a2-ultragpu-8g": { hourlyUsd: 45.661859836, spot: false },
  "a3-highgpu-8g": { hourlyUsd: 99.648954439, spot: false },
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
    "a2-highgpu-1g": { hourlyUsd: 4.325068684, spot: false },
    "a2-highgpu-2g": { hourlyUsd: 8.650137368, spot: false },
    "a2-highgpu-4g": { hourlyUsd: 17.300274736, spot: false },
    "a2-highgpu-8g": { hourlyUsd: 34.600549472, spot: false },
    "a2-ultragpu-1g": { hourlyUsd: 6.092030068, spot: false },
    "a2-ultragpu-2g": { hourlyUsd: 12.184060137, spot: false },
    "a2-ultragpu-4g": { hourlyUsd: 24.368120274, spot: false },
    "a2-ultragpu-8g": { hourlyUsd: 48.736240548, spot: false },
    "a3-highgpu-8g": { hourlyUsd: 106.372421208, spot: false },
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

  "europe-west4": {
  "a2-highgpu-1g": { hourlyUsd: 3.747972, spot: false },
  "a2-highgpu-2g": { hourlyUsd: 7.495944, spot: false },
  "a2-highgpu-4g": { hourlyUsd: 14.991888, spot: false },
  "a2-highgpu-8g": { hourlyUsd: 29.983776, spot: false },
  "a2-ultragpu-1g": { hourlyUsd: 5.580918479, spot: false },
  "a2-ultragpu-2g": { hourlyUsd: 11.161836959, spot: false },
  "a2-ultragpu-4g": { hourlyUsd: 22.323673918, spot: false },
  "a2-ultragpu-8g": { hourlyUsd: 44.647347836, spot: false },
  "a3-highgpu-8g": { hourlyUsd: 112.540479175, spot: false },
},

"asia-northeast1": {
  "a2-highgpu-1g": { hourlyUsd: 4.049590926, spot: false },
  "a2-highgpu-2g": { hourlyUsd: 8.099181852, spot: false },
  "a2-highgpu-4g": { hourlyUsd: 16.198363704, spot: false },
  "a2-highgpu-8g": { hourlyUsd: 32.396727408, spot: false },
  "a2-ultragpu-1g": { hourlyUsd: 6.506510658, spot: false },
  "a2-ultragpu-2g": { hourlyUsd: 13.013021315, spot: false },
  "a2-ultragpu-4g": { hourlyUsd: 26.02604263, spot: false },
  "a2-ultragpu-8g": { hourlyUsd: 52.05208526, spot: false },
  "a3-highgpu-8g": { hourlyUsd: 126.643390633, spot: false },
}, 

"asia-northeast3": {
  "a2-highgpu-1g": { hourlyUsd: 4.049590926, spot: false },
  "a2-highgpu-2g": { hourlyUsd: 8.099181852, spot: false },
  "a2-highgpu-4g": { hourlyUsd: 16.198363704, spot: false },
  "a2-highgpu-8g": { hourlyUsd: 32.396727408, spot: false },
  "a2-ultragpu-1g": { hourlyUsd: 6.506510658, spot: false },
  "a2-ultragpu-2g": { hourlyUsd: 13.013021315, spot: false },
  "a2-ultragpu-4g": { hourlyUsd: 26.02604263, spot: false },
  "a2-ultragpu-8g": { hourlyUsd: 52.05208526, spot: false },
  "a3-highgpu-8g": { hourlyUsd: 113.649246633, spot: false },
}, 

"me-west1": {
  "a2-highgpu-1g": { hourlyUsd: 4.0407237, spot: false },
  "a2-highgpu-2g": { hourlyUsd: 8.0814474, spot: false },
  "a2-highgpu-4g": { hourlyUsd: 16.1628948, spot: false },
  "a2-highgpu-8g": { hourlyUsd: 32.3257896, spot: false },
  "a2-ultragpu-1g": { hourlyUsd: 5.575677679, spot: false },
  "a2-ultragpu-2g": { hourlyUsd: 11.151355359, spot: false },
  "a2-ultragpu-4g": { hourlyUsd: 22.302710718, spot: false },
  "a2-ultragpu-8g": { hourlyUsd: 44.605421436, spot: false },
  "a3-highgpu-8g": { hourlyUsd: 97.339034199, spot: false },
}, 

"asia-southeast1": {
  "a2-highgpu-1g": { hourlyUsd: 4.013757926, spot: false },
  "a2-highgpu-2g": { hourlyUsd: 8.027515852, spot: false },
  "a2-highgpu-4g": { hourlyUsd: 16.055031704, spot: false },
  "a2-highgpu-8g": { hourlyUsd: 32.110063408, spot: false },
  "a2-ultragpu-1g": { hourlyUsd: 6.247685479, spot: false },
  "a2-ultragpu-2g": { hourlyUsd: 12.495370959, spot: false },
  "a2-ultragpu-4g": { hourlyUsd: 24.990741918, spot: false },
  "a2-ultragpu-8g": { hourlyUsd: 49.981483836, spot: false },
  "a3-highgpu-8g": { hourlyUsd: 114.278292967, spot: false },
}
};

export function getOnDemandHourlyUsd(region: string, machineType: string): number | null {
  const rate = GCP_ON_DEMAND_HOURLY_USD?.[region]?.[machineType]?.hourlyUsd;
  return typeof rate === "number" ? rate : null;
}