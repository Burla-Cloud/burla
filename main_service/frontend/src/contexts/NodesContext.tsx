import { createContext, useContext, useEffect, useState } from "react";
import { BurlaNode, NodeStatus } from "@/types/coreTypes";

interface NodesContextType {
    nodes: BurlaNode[];
    setNodes: React.Dispatch<React.SetStateAction<BurlaNode[]>>;
    loading: boolean;
}

const NodesContext = createContext<NodesContextType>({
    nodes: [],
    setNodes: () => {},
    loading: true,
});

export const NodesProvider = ({ children }: { children: React.ReactNode }) => {
    const [nodes, setNodes] = useState<BurlaNode[]>([]);
    const [loading, setLoading] = useState(true);
    // const firstEventReceived = useState(false); // not needed anymore

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
        let first = true;
        const eventSource = new EventSource("/v1/cluster");
        eventSource.onmessage = (event) => {
            const data = JSON.parse(event.data);
            if (first) {
                setLoading(false);
                first = false;
            }
            if (data.type === "empty") {
                setNodes([]); // ensure nodes is empty
                setLoading(false);
                return;
            }
            handleNodeUpdate(data);
        };
        eventSource.onerror = (error) => console.error("EventSource failed:", error);
        return () => eventSource.close();
    }, []);

    return (
        <NodesContext.Provider value={{ nodes, setNodes, loading }}>
            {children}
        </NodesContext.Provider>
    );
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
