export type ClusterStatus = "ON" | "OFF" | "BOOTING" | "REBOOTING" | "STOPPING";
export type NodeStatus = "READY" | "RUNNING" | "BOOTING" | "STOPPING";
export type JobsStatus = "PENDING" | "RUNNING" | "FAILED" | "COMPLETED";

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
    n_inputs: number;
    started_at?: Date; // Renamed from submitted_date and ensured it's a timestamp
}


export interface Settings {
    containerImage: string;
    pythonExecutable: string;
    pythonVersion: string;
    machineType: string;
    machineQuantity: number;
    users: string[];
  }

  export interface ServiceAccount {
    id: string;
    name: string;
    token: string;
  }

  export interface LogEntry {
    time: string;
    message: string; 
  }
