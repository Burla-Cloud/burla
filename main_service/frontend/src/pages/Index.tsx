// import { ClusterStatusCard } from "@/components/ClusterStatusCard";
// import { ClusterControls } from "@/components/ClusterControls";
// import { NodesList } from "@/components/NodesList";
// import { useClusterControl } from "@/hooks/useClusterControl";
// import { useNodes } from "@/contexts/NodesContext";
// import { useCluster } from "@/contexts/ClusterContext";
// import { useEffect, useState } from "react";
// import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
// import { Skeleton } from "@/components/ui/skeleton";

// const Dashboard = () => {
//     const { rebootCluster, stopCluster } = useClusterControl();
//     const { nodes, loading } = useNodes();
//     const { clusterStatus } = useCluster();

//     // Add local state for disabling buttons
//     const [disableStartButton, setDisableStartButton] = useState(false);
//     const [disableStopButton, setDisableStopButton] = useState(false);
//     const [welcomeVisible, setWelcomeVisible] = useState(
//         () => localStorage.getItem("welcomeMessageHidden") !== "true"
//     );

//     useEffect(() => {
//         const handleWelcomeVisibility = (event: Event) => {
//             const custom = event as CustomEvent<boolean>;
//             setWelcomeVisible(Boolean(custom.detail));
//         };
//         window.addEventListener(
//             "welcomeVisibilityChanged",
//             handleWelcomeVisibility as EventListener
//         );
//         return () => {
//             window.removeEventListener(
//                 "welcomeVisibilityChanged",
//                 handleWelcomeVisibility as EventListener
//             );
//         };
//     }, []);

//     const extractCpuCount = (type: string): number | null => {
//         const customMatch = type.match(/^custom-(\d+)-/);
//         if (customMatch) return parseInt(customMatch[1], 10);

//         // n4-standard-16 -> captures 16
//         const standardMatch = type.match(/-(\d+)$/);
//         if (standardMatch) return parseInt(standardMatch[1], 10);

//         // GPU machine types like a2-highgpu-4g, a3-highgpu-1g, etc.
//         const gpuMatch = type.match(/^(a\d-(highgpu|ultragpu|megagpu|edgegpu))-(\d+)g$/);
//         if (gpuMatch) {
//             const family = gpuMatch[1];
//             const gpus = parseInt(gpuMatch[3], 10);

//             const cpuTable: Record<string, Record<number, number>> = {
//                 "a2-highgpu": { 1: 12, 2: 24, 4: 48, 8: 96 },
//                 "a2-ultragpu": { 1: 12, 2: 24, 4: 48, 8: 96 },
//                 "a2-megagpu": { 16: 96 },
//                 "a3-highgpu": { 1: 26, 2: 52, 4: 104, 8: 208 },
//                 "a3-ultragpu": { 8: 224 },
//                 "a3-edgegpu": { 8: 208 },
//             };

//             const cpus = cpuTable[family]?.[gpus];
//             if (cpus) return cpus;
//         }

//         return null;
//     };

//     const parallelism = nodes.reduce((sum, node) => {
//         const cpus = node.cpus ?? extractCpuCount(node.type) ?? 0;
//         return sum + cpus;
//     }, 0);

//     // Helper to parse RAM string like '16G' or '340G' to number of GB
//     const parseRamGB = (ram: string): number => {
//         if (!ram) return 0;
//         const match = ram.match(/(\d+)(G|g)/);
//         if (match) return parseInt(match[1], 10);
//         return 0;
//     };

//     // Copy of parseRamDisplay from NodesList
//     const parseRamDisplay = (type: string): string => {
//         const lower = type.toLowerCase();
//         if (lower.startsWith("n4-standard-")) {
//             const cpu = extractCpuCount(type);
//             if (cpu !== null) return `${cpu * 4}G`;
//         }
//         const ramTable: Record<string, Record<number, string>> = {
//             "a2-highgpu": { 1: "85G", 2: "170G", 4: "340G", 8: "680G", 16: "1360G" },
//             "a2-ultragpu": { 1: "170G", 2: "340G", 4: "680G", 8: "1360G" },
//             "a2-megagpu": { 16: "1360G" },
//             "a3-highgpu": { 1: "234G", 2: "468G", 4: "936G", 8: "1872G" },
//             "a3-ultragpu": { 8: "2952G" },
//         };
//         const match = lower.match(/^(a\d-(highgpu|ultragpu|megagpu|edgegpu))-(\d+)g$/);
//         if (match) {
//             const family = match[1];
//             const count = parseInt(match[3], 10);
//             const sizes = ramTable[family];
//             if (sizes && sizes[count]) return sizes[count];
//         }
//         return "-";
//     };

//     // Copy of parseGpuDisplay from NodesList
//     const parseGpuDisplay = (type: string): string => {
//         const lower = type.toLowerCase();
//         const gpuPatterns: { prefix: string; model: string; vram: string }[] = [
//             { prefix: "a2-highgpu-", model: "A100", vram: "40G" },
//             { prefix: "a2-ultragpu-", model: "A100", vram: "80G" },
//             { prefix: "a2-megagpu-", model: "A100", vram: "40G" },
//             { prefix: "a3-highgpu-", model: "H100", vram: "80G" },
//             { prefix: "a3-ultragpu-", model: "H200", vram: "141G" },
//         ];
//         for (const { prefix, model, vram } of gpuPatterns) {
//             if (lower.startsWith(prefix)) {
//                 const countMatch = lower.match(/-(\d+)g$/);
//                 if (countMatch) {
//                     const count = parseInt(countMatch[1], 10);
//                     return `${count}x ${model} ${vram}`;
//                 }
//             }
//         }
//         return "-";
//     };

//     // Aggregate total RAM (in GB)
//     const totalRamGB = nodes.reduce((sum, node) => {
//         const ramStr = node.memory || parseRamDisplay(node.type);
//         return sum + parseRamGB(ramStr);
//     }, 0);
//     const totalRam = totalRamGB > 0 ? `${totalRamGB}G` : "-";

//     // Aggregate GPU summary: group by GPU type (model + vram), count total GPUs per type
//     const gpuCountMap: Record<string, number> = {};
//     nodes.forEach((node) => {
//         const gpuStr = parseGpuDisplay(node.type);
//         if (gpuStr !== "-") {
//             // Parse the count and type (e.g., '2x H100 80G' -> 2, 'H100 80G')
//             const match = gpuStr.match(/^(\d+)x (.+)$/);
//             if (match) {
//                 const count = parseInt(match[1], 10);
//                 const key = match[2];
//                 gpuCountMap[key] = (gpuCountMap[key] || 0) + count;
//             }
//         }
//     });
//     const gpuSummary =
//         Object.entries(gpuCountMap)
//             .map(([type, count]) => `${count} ${type}`)
//             .join(", ") || "-";

//     // Handler wrappers for temporary disabling
//     const handleReboot = async () => {
//         setDisableStartButton(true);
//         setTimeout(() => setDisableStartButton(false), 4000);
//         await rebootCluster();
//     };
//     const handleStop = async () => {
//         setDisableStopButton(true);
//         setTimeout(() => setDisableStopButton(false), 4000);
//         await stopCluster();
//     };

//     return (
//         <div className="flex-1 flex flex-col justify-start px-12 pt-6">
//             <div className="max-w-6xl mx-auto w-full flex-1 flex flex-col">
//                 {/* Move Dashboard Heading Up Precisely */}
//                 <h1 className="text-3xl font-bold mt-2 mb-6 text-primary">Cluster Status</h1>

//                 <div className="space-y-8 flex-1">
//                     <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
//                         <div className="md:col-span-3 justify-self-start">
//                             {loading ? (
//                                 <Card className="w-full animate-pulse">
//                                     <CardHeader className="pb-2">
//                                         <CardTitle className="text-xl font-semibold text-primary">
//                                             <Skeleton className="h-6 w-40 mb-2" />
//                                         </CardTitle>
//                                     </CardHeader>
//                                     <CardContent className="mt-6">
//                                         <div className="flex items-center gap-4">
//                                             <Skeleton className="w-6 h-6 rounded-full" />
//                                             <Skeleton className="h-5 w-24" />
//                                             <Skeleton className="h-5 w-16 ml-8" />
//                                         </div>
//                                         <div className="mt-6 flex gap-2">
//                                             <Skeleton className="h-4 w-20" />
//                                             <Skeleton className="h-4 w-10" />
//                                         </div>
//                                     </CardContent>
//                                 </Card>
//                             ) : (
//                                 <ClusterStatusCard
//                                     status={clusterStatus}
//                                     parallelism={parallelism}
//                                     totalRam={totalRam}
//                                     gpuSummary={gpuSummary}
//                                     gpuCount={Object.values(gpuCountMap).reduce((a, b) => a + b, 0)}
//                                     hasResources={nodes.length > 0}
//                                 />
//                             )}
//                         </div>
//                         <div className="flex items-center justify-center md:col-span-1">
//                             <ClusterControls
//                                 status={clusterStatus}
//                                 onReboot={handleReboot}
//                                 onStop={handleStop}
//                                 disableStartButton={disableStartButton || loading}
//                                 disableStopButton={disableStopButton || loading}
//                                 highlightStart={welcomeVisible}
//                             />
//                         </div>
//                     </div>

//                     {loading ? (
//                         <Card className="w-full animate-pulse">
//                             <CardHeader className="flex flex-row items-center justify-between">
//                                 <CardTitle className="text-xl font-semibold text-primary">
//                                     <Skeleton className="h-6 w-32" />
//                                 </CardTitle>
//                             </CardHeader>
//                             <CardContent>
//                                 <div className="space-y-2">
//                                     {[...Array(3)].map((_, i) => (
//                                         <div key={i} className="flex items-center gap-4 py-2">
//                                             <Skeleton className="w-4 h-4 rounded-full" />
//                                             <Skeleton className="h-4 w-24" />
//                                             <Skeleton className="h-4 w-16" />
//                                             <Skeleton className="h-4 w-16" />
//                                             <Skeleton className="h-4 w-16" />
//                                             <Skeleton className="h-4 w-8" />
//                                         </div>
//                                     ))}
//                                 </div>
//                             </CardContent>
//                         </Card>
//                     ) : (
//                         <NodesList nodes={nodes} />
//                     )}
//                 </div>
//                 <div className="text-center text-sm text-gray-500 mt-auto pt-8">
//                     Need help? Email me!{" "}
//                     <a href="mailto:jake@burla.dev" className="text-blue-500 hover:underline">
//                         jake@burla.dev
//                     </a>
//                 </div>
//             </div>
//         </div>
//     );
// };

// export default Dashboard;


import { ClusterStatusCard } from "@/components/ClusterStatusCard";
import { ClusterControls } from "@/components/ClusterControls";
import { NodesList } from "@/components/NodesList";
import { useClusterControl } from "@/hooks/useClusterControl";
import { useNodes } from "@/contexts/NodesContext";
import { useCluster } from "@/contexts/ClusterContext";
import { useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

const ACTIVE_STATUSES = new Set(["BOOTING", "READY", "RUNNING"]);

const Dashboard = () => {
  const { rebootCluster, stopCluster } = useClusterControl();
  const { nodes, loading } = useNodes();
  const { clusterStatus } = useCluster();

  // Disable buttons briefly after click to avoid spam
  const [disableStartButton, setDisableStartButton] = useState(false);
  const [disableStopButton, setDisableStopButton] = useState(false);
  const [welcomeVisible, setWelcomeVisible] = useState(
    () => localStorage.getItem("welcomeMessageHidden") !== "true"
  );

  useEffect(() => {
    const handleWelcomeVisibility = (event: Event) => {
      const custom = event as CustomEvent<boolean>;
      setWelcomeVisible(Boolean(custom.detail));
    };
    window.addEventListener("welcomeVisibilityChanged", handleWelcomeVisibility as EventListener);
    return () => {
      window.removeEventListener("welcomeVisibilityChanged", handleWelcomeVisibility as EventListener);
    };
  }, []);

  // Only count resources from nodes that are BOOTING, READY, or RUNNING
  const countedNodes = useMemo(
    () => nodes.filter((n) => ACTIVE_STATUSES.has(n.status)),
    [nodes]
  );

  const extractCpuCount = (type: string): number | null => {
    const customMatch = type.match(/^custom-(\d+)-/);
    if (customMatch) return parseInt(customMatch[1], 10);

    // n4-standard-16 -> captures 16
    const standardMatch = type.match(/-(\d+)$/);
    if (standardMatch) return parseInt(standardMatch[1], 10);

    // GPU machine types like a2-highgpu-4g, a3-highgpu-1g, etc.
    const gpuMatch = type.match(/^(a\d-(highgpu|ultragpu|megagpu|edgegpu))-(\d+)g$/);
    if (gpuMatch) {
      const family = gpuMatch[1];
      const gpus = parseInt(gpuMatch[3], 10);

      const cpuTable: Record<string, Record<number, number>> = {
        "a2-highgpu": { 1: 12, 2: 24, 4: 48, 8: 96 },
        "a2-ultragpu": { 1: 12, 2: 24, 4: 48, 8: 96 },
        "a2-megagpu": { 16: 96 },
        "a3-highgpu": { 1: 26, 2: 52, 4: 104, 8: 208 },
        "a3-ultragpu": { 8: 224 },
        "a3-edgegpu": { 8: 208 },
      };

      const cpus = cpuTable[family]?.[gpus];
      if (cpus) return cpus;
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

  // Parse RAM like '16G' or '340G' => number of GB
  const parseRamGB = (ram: string): number => {
    if (!ram) return 0;
    const match = ram.match(/(\d+)(G|g)/);
    if (match) return parseInt(match[1], 10);
    return 0;
  };

  // Derive RAM from type when node.memory is missing
  const parseRamDisplay = (type: string): string => {
    const lower = type.toLowerCase();
    if (lower.startsWith("n4-standard-")) {
      const cpu = extractCpuCount(type);
      if (cpu !== null) return `${cpu * 4}G`;
    }
    const ramTable: Record<string, Record<number, string>> = {
      "a2-highgpu":  { 1: "85G",  2: "170G", 4: "340G", 8: "680G", 16: "1360G" },
      "a2-ultragpu": { 1: "170G", 2: "340G", 4: "680G", 8: "1360G" },
      "a2-megagpu":  { 16: "1360G" },
      "a3-highgpu":  { 1: "234G", 2: "468G", 4: "936G", 8: "1872G" },
      "a3-ultragpu": { 8: "2952G" },
    };
    const match = lower.match(/^(a\d-(highgpu|ultragpu|megagpu|edgegpu))-(\d+)g$/);
    if (match) {
      const family = match[1];
      const count = parseInt(match[3], 10);
      const sizes = ramTable[family];
      if (sizes && sizes[count]) return sizes[count];
    }
    return "-";
  };

  // GPU text per type
  const parseGpuDisplay = (type: string): string => {
    const lower = type.toLowerCase();
    const gpuPatterns: { prefix: string; model: string; vram: string }[] = [
      { prefix: "a2-highgpu-",  model: "A100", vram: "40G" },
      { prefix: "a2-ultragpu-", model: "A100", vram: "80G" },
      { prefix: "a2-megagpu-",  model: "A100", vram: "40G" },
      { prefix: "a3-highgpu-",  model: "H100", vram: "80G" },
      { prefix: "a3-ultragpu-", model: "H200", vram: "141G" },
    ];
    for (const { prefix, model, vram } of gpuPatterns) {
      if (lower.startsWith(prefix)) {
        const countMatch = lower.match(/-(\d+)g$/);
        if (countMatch) {
          const count = parseInt(countMatch[1], 10);
          return `${count}x ${model} ${vram}`;
        }
      }
    }
    return "-";
  };

  // Totals computed only over countedNodes
  const totalRamGB = useMemo(
    () =>
      countedNodes.reduce((sum, node) => {
        const ramStr = node.memory || parseRamDisplay(node.type);
        return sum + parseRamGB(ramStr);
      }, 0),
    [countedNodes]
  );

  const totalRam = totalRamGB > 0 ? `${totalRamGB}G` : "-";

  const { gpuSummary, gpuTotalCount } = useMemo(() => {
    const gpuCountMap: Record<string, number> = {};
    countedNodes.forEach((node) => {
      const gpuStr = parseGpuDisplay(node.type);
      if (gpuStr !== "-") {
        const match = gpuStr.match(/^(\d+)x (.+)$/);
        if (match) {
          const count = parseInt(match[1], 10);
          const key = match[2]; // e.g., 'H100 80G'
          gpuCountMap[key] = (gpuCountMap[key] || 0) + count;
        }
      }
    });

    const gpuSummaryStr =
      Object.entries(gpuCountMap)
        .map(([type, count]) => `${count} ${type}`)
        .join(", ") || "-";

    const totalCount = Object.values(gpuCountMap).reduce((a, b) => a + b, 0);

    return { gpuSummary: gpuSummaryStr, gpuTotalCount: totalCount };
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
        <h1 className="text-3xl font-bold mt-2 mb-6 text-primary">Cluster Status</h1>

        <div className="space-y-8 flex-1">
          <div className="grid grid-cols-1 md:grid-cols-4 gap-8">
            <div className="md:col-span-3 justify-self-start">
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

            <div className="flex items-center justify-center md:col-span-1">
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
        </div>

        <div className="text-center text-sm text-gray-500 mt-auto pt-8">
          Need help? Email me!{" "}
          <a href="mailto:jake@burla.dev" className="text-blue-500 hover:underline">
            jake@burla.dev
          </a>
        </div>
      </div>
    </div>
  );
};

export default Dashboard;
