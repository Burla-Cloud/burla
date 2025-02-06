import { ClusterStatusCard } from "@/components/ClusterStatusCard";
import { ClusterControls } from "@/components/ClusterControls";
import { NodesList } from "@/components/NodesList";
import clusterImage from "@/assets/burla_logo.png";
import { useClusterControl } from "@/hooks/useClusterControl";
import { useNodes } from "@/contexts/NodesContext";
import { useCluster } from "@/contexts/ClusterContext";

const Index = () => {
    const { startCluster, stopCluster } = useClusterControl();
    const { nodes } = useNodes();
    const { clusterStatus } = useCluster();

    return (
        <div className="min-h-screen bg-gray-50 py-20">
            <div className="container max-w-4xl mx-auto px-4">
                <div className="mb-8">
                    <img src={clusterImage} className="w-32 h-auto" />
                </div>

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

                    <NodesList nodes={nodes} />

                    <div className="text-center text-sm text-gray-500 mt-8">
                        Need help? Email me!{" "}
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
