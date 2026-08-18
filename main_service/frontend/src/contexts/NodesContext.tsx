import { createContext, useContext, useEffect, useState } from "react";
import { BurlaNode, NodeStatus } from "@/types/coreTypes";
import { managementEvents } from "@/lib/managementApi";

interface NodesContextType {
    nodes: BurlaNode[];
    loading: boolean;
}

const NodesContext = createContext<NodesContextType>({
    nodes: [],
    loading: true,
});

export const NodesProvider = ({ children }: { children: React.ReactNode }) => {
    const [nodes, setNodes] = useState<BurlaNode[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const update = (data: any) => {
            setNodes((data.nodes ?? []).map(createNewNode));
            setLoading(false);
        };
        const source = managementEvents("/cluster/watch", {
            snapshot: update,
            update,
        });
        return () => {
            source.close();
        };
    }, []);

    return <NodesContext.Provider value={{ nodes, loading }}>{children}</NodesContext.Provider>;
};

const createNewNode = (data: any): BurlaNode => ({
    id: String(data.node_id),
    name: String(data.node_id),
    status: String(data.status || "unknown").toUpperCase() as NodeStatus,
    type: data.machine_type || "unknown",
    cpus: data.vcpu_count,
    gpus: data.gpu_count,
    gpuDisplay: data.gpu_display,
    memory: typeof data.memory_bytes === "number" ? `${Math.round(data.memory_bytes / 1024 ** 3)}G` : undefined,
    started_booting_at: typeof data.started_booting_at === "string" ? Date.parse(data.started_booting_at) : undefined,
    deletedAt: undefined,
    current_function: data.current_function,
});

export const useNodes = () => useContext(NodesContext);
