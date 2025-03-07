import { ClusterStatusCard } from "@/components/ClusterStatusCard";
import { ClusterControls } from "@/components/ClusterControls";
import { NodesList } from "@/components/NodesList";
import { useClusterControl } from "@/hooks/useClusterControl";
import { useNodes } from "@/contexts/NodesContext";
import { useCluster } from "@/contexts/ClusterContext";


const Dashboard = () => {
    const { rebootCluster, stopCluster } = useClusterControl();
    const { nodes } = useNodes();
    const { clusterStatus } = useCluster();

    return (
        <div className="flex-1 flex flex-col justify-start px-12 pt-0">
            <div className="max-w-6xl mx-auto w-full">
                {/* Move Dashboard Heading Up Precisely */}
                <h1 className="text-3xl font-bold mt-[-4px] mb-4" style={{ color: "#3b5a64" }}>
                    Dashboard
                </h1>

                <div className="space-y-6">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        <ClusterStatusCard status={clusterStatus} />
                        <div className="flex items-center justify-center">
                            <ClusterControls
                                status={clusterStatus}
                                onReboot={rebootCluster}
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

export default Dashboard;

