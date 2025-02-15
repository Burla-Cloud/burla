import { createContext, useContext, useEffect, useState } from "react";
import { BurlaNode, NodeStatus } from "@/types/cluster";

interface NodesContextType {
    nodes: BurlaNode[];
}

const NodesContext = createContext<NodesContextType>({ nodes: [] });

export const NodesProvider = ({ children }: { children: React.ReactNode }) => {
    const [nodes, setNodes] = useState<BurlaNode[]>([]);

    const handleNodeUpdate = (data: any) => {
        console.log("Received node update:", data);

        setNodes((prevNodes) => {
            let newNodes;
            if (data.deleted) {
                newNodes = prevNodes.filter((node) => node.id !== data.nodeId);
            } else {
                const existingNode = prevNodes.find((node) => node.id === data.nodeId);
                if (!existingNode) {
                    const newNode = createNewNode(data);
                    console.log("Creating new node:", newNode);
                    newNodes = [...prevNodes, newNode];
                } else {
                    console.log(
                        "Updating existing node:",
                        existingNode,
                        "with new status:",
                        data.status
                    );
                    newNodes = prevNodes.map((node) =>
                        node.id === data.nodeId
                            ? { ...node, status: data.status as NodeStatus }
                            : node
                    );
                }
            }

            return newNodes;
        });
    };

    useEffect(() => {
        const eventSource = new EventSource("/v1/cluster");

        // Add connection state handlers
        eventSource.onopen = () => {
            console.log("EventSource connected");
        };

        eventSource.onmessage = (event) => {
            console.log("Raw event received:", event.data);
            const parsedData = JSON.parse(event.data);
            console.log("Parsed event data:", parsedData);
            handleNodeUpdate(parsedData);
        };

        eventSource.onerror = (error) => {
            console.error("EventSource error:", error);
            // Optionally reconnect on error
            // eventSource.close();
        };

        // Clean up
        return () => {
            console.log("Closing EventSource connection");
            eventSource.close();
        };
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
