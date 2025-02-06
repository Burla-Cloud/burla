import { useState } from "react";
import { ClusterStatusCard } from "@/components/ClusterStatusCard";
import { ClusterControls } from "@/components/ClusterControls";
import { NodesList } from "@/components/NodesList";
import clusterImage from "@/assets/burla_logo.png";
import { useClusterControl } from "@/hooks/useClusterControl";

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

const Index = () => {
    const [nodes, setNodes] = useState<Node[]>([]);
    const { status, startCluster, stopCluster } = useClusterControl();

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

                    <NodesList
                        nodes={nodes}
                        onDeleteNode={(nodeId) => {
                            setNodes(
                                nodes.filter((node) => node.id !== nodeId)
                            );
                        }}
                    />

                    <div className="text-center text-sm text-gray-500 mt-8">
                        Need help? Email me!{" "}
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
