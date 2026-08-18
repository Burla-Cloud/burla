import { ClusterControls } from "@/components/ClusterControls";
import { NodesList } from "@/components/NodesList";
import { QuickstartCard } from "@/components/QuickstartCard";
import { PageHeader } from "@/components/PageHeader";
import { StatusBadge, clusterStatusBadge } from "@/components/StatusBadge";
import { useClusterControl } from "@/hooks/useClusterControl";
import { useNodes } from "@/contexts/NodesContext";
import { useCluster } from "@/contexts/ClusterContext";
import { useEffect, useMemo, useState } from "react";
import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

const ACTIVE_STATUSES = new Set(["BOOTING", "READY", "RUNNING"]);

const parseRamGB = (ram: string): number => {
    if (!ram) return 0;
    const match = ram.match(/(\d+)(G|g)/);
    return match ? parseInt(match[1], 10) : 0;
};

const Stat = ({ label, value, loading }: { label: string; value: string; loading: boolean }) => (
    <div className="min-w-0 px-5 py-4">
        <div className="text-[13px] text-muted-foreground">{label}</div>
        {loading ? (
            <Skeleton className="mt-1.5 h-7 w-16" />
        ) : (
            <div className="mt-0.5 truncate text-2xl font-semibold tabular-nums tracking-tight text-foreground">
                {value}
            </div>
        )}
    </div>
);

const Dashboard = () => {
    const { rebootCluster, stopCluster } = useClusterControl();
    const { nodes, loading } = useNodes();
    const { clusterStatus } = useCluster();

    const [disableStartButton, setDisableStartButton] = useState(false);
    const [disableStopButton, setDisableStopButton] = useState(false);
    const SHOW_DELETED_STORAGE_KEY = "nodesShowDeleted";

    const [showDeleted, setShowDeleted] = useState(() => {
        if (typeof window === "undefined") return false;
        return localStorage.getItem(SHOW_DELETED_STORAGE_KEY) === "true";
    });

    const [welcomeVisible, setWelcomeVisible] = useState(() => localStorage.getItem("welcomeMessageHidden") !== "true");

    const dismissWelcome = () => {
        setWelcomeVisible(false);
        try {
            localStorage.setItem("welcomeMessageHidden", "true");
        } catch {
            // ignore
        }
    };

    useEffect(() => {
        if (typeof window === "undefined") return;
        localStorage.setItem(SHOW_DELETED_STORAGE_KEY, showDeleted ? "true" : "false");
    }, [showDeleted]);

    const countedNodes = useMemo(() => nodes.filter((n) => ACTIVE_STATUSES.has(n.status)), [nodes]);

    const parallelism = useMemo(
        () =>
            countedNodes.reduce((sum, node) => {
                return sum + (node.cpus ?? 0);
            }, 0),
        [countedNodes],
    );

    const totalRamGB = useMemo(
        () =>
            countedNodes.reduce((sum, node) => {
                return sum + parseRamGB(node.memory ?? "");
            }, 0),
        [countedNodes],
    );

    const gpuTotalCount = useMemo(() => countedNodes.reduce((sum, node) => sum + (node.gpus ?? 0), 0), [countedNodes]);

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

    const badge = clusterStatusBadge(clusterStatus);

    return (
        <div className="flex flex-1 flex-col min-w-0">
            <div className="mx-auto flex w-full max-w-6xl flex-1 flex-col">
                <PageHeader
                    title="Cluster"
                    titleAccessory={
                        loading ? (
                            <Skeleton className="h-5 w-14 rounded-md" />
                        ) : (
                            <StatusBadge tone={badge.tone} label={badge.label} pulse={badge.pulse} />
                        )
                    }
                    actions={
                        <ClusterControls
                            status={clusterStatus}
                            onReboot={handleReboot}
                            onStop={handleStop}
                            disableStartButton={disableStartButton || loading}
                            disableStopButton={disableStopButton || loading}
                        />
                    }
                />

                <div className="flex-1 space-y-5">
                    {welcomeVisible && <QuickstartCard onDismiss={dismissWelcome} />}

                    <Card className="grid grid-cols-2 divide-y divide-border/70 sm:grid-cols-4 sm:divide-x sm:divide-y-0">
                        <Stat label="Nodes" value={countedNodes.length.toLocaleString()} loading={loading} />
                        <Stat label="vCPUs" value={parallelism.toLocaleString()} loading={loading} />
                        <Stat label="RAM" value={totalRamGB > 0 ? `${totalRamGB}G` : "—"} loading={loading} />
                        <Stat
                            label="GPUs"
                            value={gpuTotalCount > 0 ? gpuTotalCount.toLocaleString() : "—"}
                            loading={loading}
                        />
                    </Card>

                    <NodesList
                        nodes={nodes}
                        loading={loading}
                        showDeleted={showDeleted}
                        onShowDeletedChange={setShowDeleted}
                    />
                </div>
            </div>
        </div>
    );
};

export default Dashboard;
