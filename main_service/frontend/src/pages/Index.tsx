import { ClusterStatusCard } from "@/components/ClusterStatusCard";
import { ClusterControls } from "@/components/ClusterControls";
import { NodesList } from "@/components/NodesList";
import { useClusterControl } from "@/hooks/useClusterControl";
import { useNodes } from "@/contexts/NodesContext";
import { useCluster } from "@/contexts/ClusterContext";
import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

const Dashboard = () => {
    const { rebootCluster, stopCluster } = useClusterControl();
    const { nodes, loading } = useNodes();
    const { clusterStatus } = useCluster();

    // Add local state for disabling buttons
    const [disableStartButton, setDisableStartButton] = useState(false);
    const [disableStopButton, setDisableStopButton] = useState(false);

    const extractCpuCount = (type: string): number | null => {
        const customMatch = type.match(/^custom-(\d+)-/);
        if (customMatch) return parseInt(customMatch[1], 10);

        const standardMatch = type.match(/-(\d+)$/);
        return standardMatch ? parseInt(standardMatch[1], 10) : null;
    };

    const parallelism = nodes.reduce((sum, node) => {
        const cpus = node.cpus ?? extractCpuCount(node.type) ?? 0;
        return sum + cpus;
    }, 0);

    // Handler wrappers for temporary disabling
    const handleReboot = async () => {
        setDisableStartButton(true);
        setTimeout(() => setDisableStartButton(false), 4000);
        await rebootCluster();
    };
    const handleStop = async () => {
        setDisableStopButton(true);
        setTimeout(() => setDisableStopButton(false), 4000);
        await stopCluster();
    };

    return (
        <div className="flex-1 flex flex-col justify-start px-12 pt-0">
            <div className="max-w-6xl mx-auto w-full">
                {/* Move Dashboard Heading Up Precisely */}
                <h1 className="text-3xl font-bold mt-[-4px] mb-4 text-primary">Dashboard</h1>

                <div className="space-y-6">
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {loading ? (
                            <Card className="w-full animate-pulse">
                                <CardHeader className="pb-2">
                                    <CardTitle className="text-xl font-semibold text-primary">
                                        <Skeleton className="h-6 w-40 mb-2" />
                                    </CardTitle>
                                </CardHeader>
                                <CardContent className="mt-6">
                                    <div className="flex items-center gap-4">
                                        <Skeleton className="w-6 h-6 rounded-full" />
                                        <Skeleton className="h-5 w-24" />
                                        <Skeleton className="h-5 w-16 ml-8" />
                                    </div>
                                    <div className="mt-6 flex gap-2">
                                        <Skeleton className="h-4 w-20" />
                                        <Skeleton className="h-4 w-10" />
                                    </div>
                                </CardContent>
                            </Card>
                        ) : (
                            <ClusterStatusCard status={clusterStatus} parallelism={parallelism} />
                        )}
                        <div className="flex items-center justify-center">
                            <ClusterControls
                                status={clusterStatus}
                                onReboot={handleReboot}
                                onStop={handleStop}
                                disableStartButton={disableStartButton || loading}
                                disableStopButton={disableStopButton || loading}
                            />
                        </div>
                    </div>

                    {loading ? (
                        <Card className="w-full animate-pulse">
                            <CardHeader className="flex flex-row items-center justify-between">
                                <CardTitle className="text-xl font-semibold text-primary">
                                    <Skeleton className="h-6 w-32" />
                                </CardTitle>
                            </CardHeader>
                            <CardContent>
                                <div className="space-y-2">
                                    {[...Array(3)].map((_, i) => (
                                        <div key={i} className="flex items-center gap-4 py-2">
                                            <Skeleton className="w-4 h-4 rounded-full" />
                                            <Skeleton className="h-4 w-24" />
                                            <Skeleton className="h-4 w-16" />
                                            <Skeleton className="h-4 w-16" />
                                            <Skeleton className="h-4 w-16" />
                                            <Skeleton className="h-4 w-8" />
                                        </div>
                                    ))}
                                </div>
                            </CardContent>
                        </Card>
                    ) : (
                        <NodesList nodes={nodes} />
                    )}

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
