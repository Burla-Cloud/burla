export type ClusterStatus = "ON" | "OFF" | "BOOTING" | "STOPPING";
export type NodeStatus = "READY" | "RUNNING" | "BOOTING" | "STOPPING";

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
