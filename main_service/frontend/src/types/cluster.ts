export type ClusterStatus = "ON" | "OFF" | "BOOTING" | "REBOOTING" | "STOPPING";
export type NodeStatus = "READY" | "RUNNING" | "BOOTING" | "STOPPING";
export type JobsStatus = "IN_QUEUE" | "RUNNING" | "FAILED" | "COMPLETED";

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
}

export interface BurlaJob {
    id: string;
    status: JobsStatus | null;
    user: string; // Added user field
    checked: boolean | false;
    started_at?: number; // Renamed from submitted_date and ensured it's a timestamp
}