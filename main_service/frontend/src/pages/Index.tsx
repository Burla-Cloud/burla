import { useState } from "react";
import { ClusterStatusCard } from "@/components/ClusterStatusCard";
import { ClusterControls } from "@/components/ClusterControls";
import { NodesList } from "@/components/NodesList";
import { useToast } from "@/components/ui/use-toast";
import clusterImage from "@/assets/burla_logo.png";


type ClusterStatus = "RUNNING" | "STOPPED" | "STARTING" | "STOPPING";
type NodeStatus = ClusterStatus;

interface Node {
  id: string;
  name: string;
  status: NodeStatus;
  type: string;
  cpus: number;
  gpus: number;
  memory: string;
  age: string;
}

const MOCK_NODES: Node[] = [
  { 
    id: "1", 
    name: "burla-node-14fd50f2", 
    status: "STOPPED", 
    type: "n2-standard-16",
    cpus: 16,
    gpus: 0,
    memory: "64Gi",
    age: "2 days ago"
  },
  { 
    id: "2", 
    name: "burla-node-1d5f5e28", 
    status: "STOPPED", 
    type: "n2-standard-16",
    cpus: 16,
    gpus: 0,
    memory: "64Gi",
    age: "2 days ago"
  },
  { 
    id: "3", 
    name: "burla-node-60acd984", 
    status: "STOPPED", 
    type: "n2-standard-16",
    cpus: 16,
    gpus: 0,
    memory: "64Gi",
    age: "2 days ago"
  },
];

const Index = () => {
  const [nodes, setNodes] = useState<Node[]>(MOCK_NODES);
  const { toast } = useToast();

  const calculateClusterStatus = (nodes: Node[]): ClusterStatus => {
    if (nodes.length === 0 || nodes.every(node => node.status === "STOPPED")) {
      return "STOPPED";
    }
    if (nodes.some(node => node.status === "STARTING")) {
      return "STARTING";
    }
    if (nodes.some(node => node.status === "STOPPING")) {
      return "STOPPING";
    }
    if (nodes.every(node => node.status === "RUNNING")) {
      return "RUNNING";
    }
    return "STOPPED";
  };

  const clusterStatus = calculateClusterStatus(nodes);

  const startCluster = () => {
    setNodes(nodes.map(node => ({ ...node, status: "STARTING" as const })));
    
    // Simulate cluster startup
    setTimeout(() => {
      setNodes(nodes.map(node => ({ ...node, status: "RUNNING" as const })));
      toast({
        title: "Cluster Started",
        description: "The cluster has been successfully started.",
      });
    }, 3000);
  };

  const stopCluster = () => {
    setNodes(nodes.map(node => ({ ...node, status: "STOPPING" as const })));
    
    // Simulate cluster shutdown
    setTimeout(() => {
      setNodes(nodes.map(node => ({ ...node, status: "STOPPED" as const })));
      toast({
        title: "Cluster Stopped",
        description: "The cluster has been successfully stopped.",
      });
    }, 3000);
  };

  const deleteNode = (nodeId: string) => {
    setNodes(nodes.filter(node => node.id !== nodeId));
    toast({
      title: "Node Deleted",
      description: `Node ${nodeId} has been deleted.`,
    });
  };

  return (
    <div className="min-h-screen bg-gray-50 py-20">
      <div className="container max-w-4xl mx-auto px-4">
        {/* Cluster Image */}
        <div className="mb-8">
          <img 
            src={clusterImage} 
            alt="Cluster Management" 
            className="w-32 h-auto" // Smaller size
          />
        </div>
        
        {/* Cluster Status and Controls */}
        <div className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <ClusterStatusCard status={clusterStatus} />
            <div className="flex items-center justify-center">
              <ClusterControls
                status={clusterStatus}
                onStart={startCluster}
                onStop={stopCluster}
              />
            </div>
          </div>
          
          <NodesList nodes={nodes} onDeleteNode={deleteNode} />
          
          <div className="text-center text-sm text-gray-500 mt-8">
            Need help? Contact support at{" "}
            <a href="mailto:jake@burla.dev" className="text-blue-500 hover:underline">
              jake@burla.dev
            </a>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Index;