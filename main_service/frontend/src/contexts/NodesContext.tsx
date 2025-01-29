import { createContext, useContext, useEffect, useState } from 'react';

interface Node {
  id: string;
  name: string;
  status: "RUNNING" | "STARTING" | "STOPPING" | null;
  type: string;
  cpus?: number;
  gpus?: number;
  memory?: string;
  age?: string;
}

interface NodesContextType {
  nodes: Node[];
}

const NodesContext = createContext<NodesContextType>({ nodes: [] });

export const NodesProvider = ({ children }: { children: React.ReactNode }) => {
  const [nodes, setNodes] = useState<Node[]>([]);

  useEffect(() => {
    const eventSource = new EventSource('/v1/cluster');

    eventSource.onmessage = (event) => {
      const data = JSON.parse(event.data);
      
      setNodes(prevNodes => {
        if (data.deleted) {
          return prevNodes.filter(node => node.id !== data.nodeId);
        }
        
        const nodeIndex = prevNodes.findIndex(node => node.id === data.nodeId);
        if (nodeIndex === -1) {
          // New node
          return [...prevNodes, {
            id: data.nodeId,
            name: data.nodeId,
            status: data.status,
            type: "unknown" // You might want to adjust this based on your needs
          }];
        }
        
        // Update existing node
        const updatedNodes = [...prevNodes];
        updatedNodes[nodeIndex] = {
          ...updatedNodes[nodeIndex],
          status: data.status
        };
        return updatedNodes;
      });
    };

    eventSource.onerror = (error) => {
      console.error('EventSource failed:', error);
    };

    return () => {
      eventSource.close();
    };
  }, []);

  return (
    <NodesContext.Provider value={{ nodes }}>
      {children}
    </NodesContext.Provider>
  );
};

export const useNodes = () => useContext(NodesContext); 