import { createContext, useContext, useEffect, useState } from "react";
import { BurlaNode, NodeStatus } from "@/types/coreTypes";

interface NodesContextType {
    nodes: BurlaNode[];
    setNodes: React.Dispatch<React.SetStateAction<BurlaNode[]>>;
}

const NodesContext = createContext<NodesContextType>({ nodes: [], setNodes: () => {} });

export const NodesProvider = ({ children }: { children: React.ReactNode }) => {
    const [nodes, setNodes] = useState<BurlaNode[]>([]);

    const handleNodeUpdate = (data: any) => {
        setNodes((prevNodes) => {
            if (data.deleted) {
                // Remove the node from the list if deleted
                return prevNodes.filter((node) => node.id !== data.nodeId);
            }
            const existingNode = prevNodes.find((node) => node.id === data.nodeId);

            // create a new node entry if one does not exist yet
            if (!existingNode) {
                return [...prevNodes, createNewNode(data)];
            }

            // merge updated fields into the existing node (status and logs)
            return prevNodes.map((node) =>
                node.id === data.nodeId
                    ? {
                          ...node,
                          status: data.status as NodeStatus,
                          // bring in new logs but keep existing logs if the backend removed them
                          logs: data.logs ?? node.logs,
                      }
                    : node
            );
        });
    };

    useEffect(() => {
        const eventSource = new EventSource("/v1/cluster");
        eventSource.onmessage = (event) => handleNodeUpdate(JSON.parse(event.data));
        eventSource.onerror = (error) => console.error("EventSource failed:", error);
        return () => eventSource.close();
    }, []);

    return <NodesContext.Provider value={{ nodes, setNodes }}>{children}</NodesContext.Provider>;
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
    logs: data.logs,
});

export const useNodes = () => useContext(NodesContext);
