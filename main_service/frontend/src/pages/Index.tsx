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
];

const Index = () => {
  const [nodes, setNodes] = useState<Node[]>(MOCK_NODES);
  const { toast } = useToast();

  const [status, setStatus] = useState<ClusterStatus>("STOPPED");

  const startCluster = async (newStatus: "STARTING" | "RUNNING" | "OFF") => {
    setStatus(newStatus);

    if (newStatus === "STARTING") {
      try {
        const response = await fetch("http://localhost:5001/v1/cluster/restart", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
        });

        if (response.ok) {
          setStatus("RUNNING");
          setNodes(nodes.map((node) => ({ ...node, status: "RUNNING" })));
          toast({
            title: "Cluster Started",
            description: "The cluster has been successfully started.",
          });
        } else {
          throw new Error("Failed to restart the cluster");
        }
      } catch (error) {
        console.error("Error restarting the cluster:", error);
        setStatus("OFF");
        toast({
          title: "Error",
          description: "Failed to restart the cluster. Please try again.",
          variant: "destructive",
        });
      }
    }
  };

  const stopCluster = async (newStatus: "STOPPING" | "OFF") => {
    setStatus(newStatus);

    if (newStatus === "STOPPING") {
      try {
        const response = await fetch("http://localhost:5001/v1/cluster/delete", {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
        });

        if (response.ok) {
          setStatus("OFF");
          setNodes(nodes.map((node) => ({ ...node, status: "STOPPED" })));
          toast({
            title: "Cluster Stopped",
            description: "The cluster has been successfully stopped.",
          });
        } else {
          throw new Error("Failed to stop the cluster");
        }
      } catch (error) {
        console.error("Error stopping the cluster:", error);
        setStatus("RUNNING");
        toast({
          title: "Error",
          description: "Failed to stop the cluster. Please try again.",
          variant: "destructive",
        });
      }
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 py-20">
      <div className="container max-w-4xl mx-auto px-4">
        {/* Cluster Image */}
        <div className="mb-8">
          <img
            src={clusterImage}
            alt="Cluster Management"
            className="w-32 h-auto"
          />
        </div>

        {/* Cluster Status and Controls */}
        <div className="space-y-6">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <ClusterStatusCard status={status} />
            <div className="flex items-center justify-center">
              <ClusterControls
                status={status}
                onStart={startCluster}
                onStop={stopCluster}
              />
            </div>
          </div>

          <NodesList nodes={nodes} onDeleteNode={(nodeId) => {
            setNodes(nodes.filter((node) => node.id !== nodeId));
          }} />

          <div className="text-center text-sm text-gray-500 mt-8">
            Need help? Contact support at{" "}
            <a
              href="mailto:jake@burla.dev"
              className="text-blue-500 hover:underline"
            >
              jake@burla.dev
            </a>
          </div>
        </div>
      </div>
    </div>
  );
};

export default Index;