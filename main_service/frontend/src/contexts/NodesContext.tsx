import { createContext, useContext, useEffect, useState } from "react";
import { BurlaNode, NodeStatus } from "@/types/cluster";

interface NodesContextType {
    nodes: BurlaNode[];
}

const NodesContext = createContext<NodesContextType>({ nodes: [] });

export const NodesProvider = ({ children }: { children: React.ReactNode }) => {
    const [nodes, setNodes] = useState<BurlaNode[]>([]);

    const handleNodeUpdate = (data: any) => {
        setNodes((prevNodes) => {
            if (data.deleted) {
                return prevNodes.filter((node) => node.id !== data.nodeId);
            }

            console.log("Node status:", data.status);

            const existingNode = prevNodes.find((node) => node.id === data.nodeId);
            if (!existingNode) {
                return [...prevNodes, createNewNode(data)];
            }
            return prevNodes.map((node) =>
                node.id === data.nodeId ? { ...node, status: data.status as NodeStatus } : node
            );
        });
    };

    useEffect(() => {
        const eventSource = new EventSource("/v1/cluster");
        eventSource.onmessage = (event) => handleNodeUpdate(JSON.parse(event.data));
        eventSource.onerror = (error) => console.error("EventSource failed:", error);
        return () => eventSource.close();
    }, []);

    return <NodesContext.Provider value={{ nodes }}>{children}</NodesContext.Provider>;
};

const createNewNode = (data: any): BurlaNode => ({
    id: data.nodeId,
    name: data.nodeId,
    status: data.status as NodeStatus,
    type: data.type || "unknown",
    cpus: data.cpus,
    gpus: data.gpus,
    memory: data.memory,
    age: data.age,
});

export const useNodes = () => useContext(NodesContext);
