export type ClusterStatus = "ON" | "OFF" | "BOOTING" | "REBOOTING" | "STOPPING";

export type NodeStatus = "READY" | "RUNNING" | "BOOTING" | "STOPPING" | "FAILED" | "DELETED";

export type JobsStatus = "PENDING" | "RUNNING" | "FAILED" | "COMPLETED" | "CANCELED";

// If we simply name it `Node`, it will conflict with the Node type in React.
export interface BurlaNode {
    id: string;
    name: string;
    status: NodeStatus | null;
    type: string;
    cpus?: number;
    gpus?: number;
    gpuDisplay?: string;
    memory?: string;
    age?: string;
    logs?: string[];

    // milliseconds since epoch
    started_booting_at?: number;

    deletedAt?: number; // milliseconds since epoch

    current_function?: string | null;
}

export interface BurlaJob {
    id: string;
    status: JobsStatus | null;
    user: string;
    n_inputs: number;
    n_results: number;
    n_failed?: number;
    started_at?: Date; // parsed from UNIX timestamp in jobContext & jobs_paginated
    ended_at?: Date;
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
    cloudAccountName?: string;
    cloudProvider?: string;
    filesystemEnabled?: boolean;
    options?: {
        machine_types: Array<{
            machine_type: string;
            vcpu_count: number;
            memory_bytes: number;
            gpu_count: number;
            gpu_display: string | null;
            regions: string[];
        }>;
        regions: string[];
        cpu_only_image_repositories: string[];
        constraints: Record<string, { minimum: number; maximum: number }>;
    };
}
