import { ClusterStatusCard } from "@/components/ClusterStatusCard";
import { ClusterControls } from "@/components/ClusterControls";
import { NodesList } from "@/components/NodesList";
import { useClusterControl } from "@/hooks/useClusterControl";
import { useNodes } from "@/contexts/NodesContext";
import { useCluster } from "@/contexts/ClusterContext";
import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

const ACTIVE_STATUSES = new Set(["BOOTING", "READY", "RUNNING"]);

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

    const [welcomeVisible, setWelcomeVisible] = useState(
        () => localStorage.getItem("welcomeMessageHidden") !== "true" 
    );

    useEffect(() => {
        const handler = (evt: Event) => {
            const custom = evt as CustomEvent<boolean>;
            setWelcomeVisible(Boolean(custom.detail));
        };
        window.addEventListener("welcomeVisibilityChanged", handler as EventListener);
        return () =>
            window.removeEventListener("welcomeVisibilityChanged", handler as EventListener);
    }, []);

    useEffect(() => {
        if (typeof window === "undefined") return;
        localStorage.setItem(SHOW_DELETED_STORAGE_KEY, showDeleted ? "true" : "false");
    }, [showDeleted]);

    const countedNodes = useMemo(() => nodes.filter((n) => ACTIVE_STATUSES.has(n.status)), [nodes]);

    const extractCpuCount = (type: string): number | null => {
        const customMatch = type.match(/^custom-(\d+)-/);
        if (customMatch) return parseInt(customMatch[1], 10);

        const standardMatch = type.match(/-(\d+)$/);
        if (standardMatch) return parseInt(standardMatch[1], 10);

        const gpuMatch = type.match(/^(a\d-(highgpu|ultragpu|megagpu|edgegpu))-(\d+)g$/);
        if (gpuMatch) {
            const family = gpuMatch[1];
            const gpus = parseInt(gpuMatch[3], 10);

            const cpuLookup: Record<string, Record<number, number>> = {
                "a2-highgpu": { 1: 12, 2: 24, 4: 48, 8: 96 },
                "a2-ultragpu": { 1: 12, 2: 24, 4: 48, 8: 96 },
                "a2-megagpu": { 16: 96 },
                "a3-highgpu": { 1: 26, 2: 52, 4: 104, 8: 208 },
                "a3-ultragpu": { 8: 224 },
                "a3-edgegpu": { 8: 208 },
            };

            return cpuLookup[family]?.[gpus] ?? null;
        }

        return null;
    };

    const parallelism = useMemo(
        () =>
            countedNodes.reduce((sum, node) => {
                const cpus = node.cpus ?? extractCpuCount(node.type) ?? 0;
                return sum + cpus;
            }, 0),
        [countedNodes]
    );

    const parseRamGB = (ram: string): number => {
        if (!ram) return 0;
        const match = ram.match(/(\d+)(G|g)/);
        return match ? parseInt(match[1], 10) : 0;
    };

    const parseRamDisplay = (type: string): string => {
        const lower = type.toLowerCase();
        if (lower.startsWith("n4-standard-")) {
            const cpu = extractCpuCount(type);
            return cpu !== null ? `${cpu * 4}G` : "-";
        }

        const ramTable: Record<string, Record<number, string>> = {
            "a2-highgpu": { 1: "85G", 2: "170G", 4: "340G", 8: "680G", 16: "1360G" },
            "a2-ultragpu": { 1: "170G", 2: "340G", 4: "680G", 8: "1360G" },
            "a2-megagpu": { 16: "1360G" },
            "a3-highgpu": { 1: "234G", 2: "468G", 4: "936G", 8: "1872G" },
            "a3-ultragpu": { 8: "2952G" },
        };

        const match = lower.match(/^(a\d-(highgpu|ultragpu|megagpu|edgegpu))-(\d+)g$/);
        if (!match) return "-";

        const family = match[1];
        const count = parseInt(match[3], 10);
        return ramTable[family]?.[count] ?? "-";
    };

    const totalRamGB = useMemo(
        () =>
            countedNodes.reduce((sum, node) => {
                const ramStr = node.memory || parseRamDisplay(node.type);
                return sum + parseRamGB(ramStr);
            }, 0),
        [countedNodes]
    );

    const totalRam = totalRamGB > 0 ? `${totalRamGB}G` : "-";

    const parseGpuDisplay = (type: string): string => {
        const lower = type.toLowerCase();
        const gpuDefs = [
            { prefix: "a2-highgpu-", model: "A100", vram: "40G" },
            { prefix: "a2-ultragpu-", model: "A100", vram: "80G" },
            { prefix: "a2-megagpu-", model: "A100", vram: "40G" },
            { prefix: "a3-highgpu-", model: "H100", vram: "80G" },
            { prefix: "a3-ultragpu-", model: "H200", vram: "141G" },
        ];

        for (const def of gpuDefs) {
            if (lower.startsWith(def.prefix)) {
                const match = lower.match(/-(\d+)g$/);
                if (!match) return "-";
                const count = parseInt(match[1], 10);
                return `${count}x ${def.model} ${def.vram}`;
            }
        }

        return "-";
    };

    const { gpuSummary, gpuTotalCount } = useMemo(() => {
        const gpuCount: Record<string, number> = {};

        countedNodes.forEach((node) => {
            const gpuStr = parseGpuDisplay(node.type);
            if (gpuStr !== "-") {
                const match = gpuStr.match(/^(\d+)x (.+)$/);
                if (match) {
                    const count = parseInt(match[1], 10);
                    const key = match[2];
                    gpuCount[key] = (gpuCount[key] || 0) + count;
                }
            }
        });

        return {
            gpuSummary:
                Object.entries(gpuCount)
                    .map(([k, v]) => `${v} ${k}`)
                    .join(", ") || "-",
            gpuTotalCount: Object.values(gpuCount).reduce((a, b) => a + b, 0),
        };
    }, [countedNodes]);

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
        <div className="flex-1 flex flex-col justify-start px-12 pt-6">
            <div className="max-w-6xl mx-auto w-full flex-1 flex flex-col">
                <h1 className="text-2xl font-bold mt-2 mb-6 text-primary">Cluster Status</h1>

                <div className="space-y-8 flex-1">
                    {/* status + controls */}
                    <div className="grid grid-cols-1 gap-8 md:grid-cols-[minmax(0,1fr)_auto] md:items-center">
                        <div className="min-w-0">
                            {loading ? (
                                <Card className="inline-block animate-pulse">
                                    <CardContent className="p-0 px-7 py-3">
                                        <div className="flex items-center gap-4">
                                            <div className="flex items-center gap-2">
                                                <Skeleton className="w-3 h-3 rounded-full" />
                                                <Skeleton className="h-5 w-10" />
                                            </div>
                                        </div>
                                    </CardContent>
                                </Card>
                            ) : (
                                <ClusterStatusCard
                                    status={clusterStatus}
                                    parallelism={parallelism}
                                    totalRam={totalRam}
                                    gpuSummary={gpuSummary}
                                    gpuCount={gpuTotalCount}
                                    hasResources={countedNodes.length > 0}
                                />
                            )}
                        </div>

                        <div className="flex items-center justify-end">
                            <ClusterControls
                                status={clusterStatus}
                                onReboot={handleReboot}
                                onStop={handleStop}
                                disableStartButton={disableStartButton || loading}
                                disableStopButton={disableStopButton || loading}
                                highlightStart={welcomeVisible}
                            />
                        </div>
                    </div>

                    {loading ? (
                        <Card className="w-full animate-pulse">
                            <CardHeader className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                                <Skeleton className="h-6 w-16" />
                                <div className="flex items-center gap-2 sm:ml-auto">
                                    <Skeleton className="h-4 w-32" />
                                    <Skeleton className="h-5 w-9 rounded-full" />
                                </div>
                            </CardHeader>
                            <CardContent>
                                <table className="table-auto w-full">
                                    <thead>
                                        <tr>
                                            <th className="w-8 pl-6 pr-4 py-2" />
                                            <th className="w-24 pl-6 pr-4 py-2 text-left"><Skeleton className="h-3 w-12" /></th>
                                            <th className="w-48 pl-6 pr-4 py-2 text-left"><Skeleton className="h-3 w-10" /></th>
                                            <th className="w-24 pl-6 pr-4 py-2 text-left"><Skeleton className="h-3 w-12" /></th>
                                            <th className="w-24 pl-6 pr-4 py-2 text-left"><Skeleton className="h-3 w-10" /></th>
                                            <th className="w-24 pl-6 pr-4 py-2 text-left"><Skeleton className="h-3 w-12" /></th>
                                            <th className="w-8 pl-6 pr-2 py-2" />
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {[...Array(3)].map((_, i) => (
                                            <tr key={i}>
                                                <td className="w-8 pl-6 pr-4 py-2"><Skeleton className="h-4 w-4" /></td>
                                                <td className="w-24 pl-6 pr-4 py-2">
                                                    <div className="flex items-center gap-2">
                                                        <Skeleton className="w-2 h-2 rounded-full" />
                                                        <Skeleton className="h-4 w-14" />
                                                    </div>
                                                </td>
                                                <td className="w-48 pl-6 pr-4 py-2"><Skeleton className="h-4 w-36" /></td>
                                                <td className="w-24 pl-6 pr-4 py-2"><Skeleton className="h-4 w-8" /></td>
                                                <td className="w-24 pl-6 pr-4 py-2"><Skeleton className="h-4 w-10" /></td>
                                                <td className="w-24 pl-6 pr-4 py-2"><Skeleton className="h-4 w-20" /></td>
                                                <td className="w-8 pl-6 pr-2 py-2" />
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </CardContent>
                        </Card>
                    ) : (
                        // ******** FIXED BLOCK ********
                        <div className="mt-1">
                            <NodesList
                                nodes={nodes}
                                showDeleted={showDeleted}
                                onShowDeletedChange={setShowDeleted}
                            />
                        </div>
                        // ******** END FIXED BLOCK ********
                    )}
                </div>

                <div className="text-center text-sm text-gray-500 dark:text-gray-400 mt-auto pt-8">
                    Need help? Email me{" "}
                    <a
                        href="mailto:jake@burla.dev"
                        className="text-blue-500 dark:text-blue-400 hover:underline"
                    >
                        jake@burla.dev
                    </a>
                </div>
            </div>
        </div>
    );
};

export default Dashboard;
