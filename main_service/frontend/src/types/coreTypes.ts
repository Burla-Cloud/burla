export type ClusterStatus = "ON" | "OFF" | "BOOTING" | "REBOOTING" | "STOPPING";

export type NodeStatus =
  | "READY"
  | "RUNNING"
  | "BOOTING"
  | "STOPPING"
  | "FAILED"
  | "DELETED";

export type JobsStatus = "PENDING" | "RUNNING" | "FAILED" | "COMPLETED" | "CANCELED";

// If we simply name it `Node`, it will conflict with the Node type in React.
export interface BurlaNode {
  id: string;
  name: string;
  status: NodeStatus | null;
  type: string;
  cpus?: number;
  gpus?: number;
  memory?: string;
  age?: string;
  logs?: string[];
  // set for anything that came from /v1/cluster/deleted_recent or SSE delete
  deletedAt?: number; // milliseconds since epoch
}

export interface BurlaJob {
  id: string;
  status: JobsStatus | null;
  user: string;
  checked: boolean;
  n_inputs: number;
  n_results: number;
  started_at?: Date; // parsed from UNIX timestamp in jobContext & jobs_paginated
  function_name?: string;
}

export interface Settings {
  containerImage: string;
  machineType: string;
  machineQuantity: number;
  diskSize: number; // in GB
  inactivityTimeout: number; // in minutes
  users: string[];
  gcpRegion?: string;
  burlaVersion?: string;
  googleCloudProjectId?: string;
}

export interface ServiceAccount {
  id: string;
  name: string;
  token: string;
}

export interface LogEntry {
  created_at: number;
  message: string;
  id?: string;
}
