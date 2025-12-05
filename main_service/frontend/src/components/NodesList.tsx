import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Link } from "react-router-dom";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { Skeleton } from "@/components/ui/skeleton";
import { Cpu, X, ChevronRight, Copy } from "lucide-react";
import { NodeStatus, BurlaNode } from "@/types/coreTypes";
import React, { useEffect, useState, useRef } from "react";
import { useNodes } from "@/contexts/NodesContext";

interface NodesListProps {
    nodes: BurlaNode[];
}

export const NodesList = ({ nodes }: NodesListProps) => {
    const { setNodes } = useNodes();
    const [showWelcome, setShowWelcome] = useState(true);
    const [copied, setCopied] = useState(false);
    const pythonExampleCode = `from burla import remote_parallel_map

def my_function(x):
    print(f"Running on a remote computer in the cloud! #{x}")

remote_parallel_map(my_function, list(range(1000)))`;

    useEffect(() => {
        const isWelcomeHidden = localStorage.getItem("welcomeMessageHidden") === "true";
        setShowWelcome(!isWelcomeHidden);
    }, []);

    const handleDismissWelcome = () => {
        setShowWelcome(false);
        localStorage.setItem("welcomeMessageHidden", "true");
        window.dispatchEvent(new CustomEvent("welcomeVisibilityChanged", { detail: false }));
    };

    const getStatusClass = (nodeStatus: NodeStatus | string | null) => {
        const statusClasses = {
            READY: "bg-green-500",
            RUNNING: "bg-green-500 animate-pulse",
            BOOTING: "bg-yellow-500 animate-pulse",
            STOPPING: "bg-gray-300 animate-pulse",
            FAILED: "bg-red-500",
            DELETED: "bg-red-500", // use same as FAILED
        };
        return cn(
            "w-2 h-2 rounded-full",
            nodeStatus && typeof nodeStatus === "string"
                ? statusClasses[nodeStatus] || "bg-gray-300"
                : "bg-gray-300"
        );
    };

    const extractCpuCount = (type: string): number | null => {
        const customMatch = type.match(/^custom-(\d+)-/);
        if (customMatch) return parseInt(customMatch[1], 10);

        // n4-standard-16 -> captures 16
        const standardMatch = type.match(/-(\d+)$/);
        if (standardMatch) return parseInt(standardMatch[1], 10);

        // GPU machine types like a2-highgpu-4g
        const gpuMatch = type.match(/^(a\d-(highgpu|ultragpu|megagpu|edgegpu))-([\d]+)g$/);
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

    const parseGpuDisplay = (type: string): string => {
        const lower = type.toLowerCase();

        const gpuPatterns: { prefix: string; model: string; vram: string }[] = [
            { prefix: "a2-highgpu-", model: "A100", vram: "40G" },
            { prefix: "a2-ultragpu-", model: "A100", vram: "80G" },
            { prefix: "a2-megagpu-", model: "A100", vram: "40G" },
            { prefix: "a3-highgpu-", model: "H100", vram: "80G" },
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

        return "-"; // CPU-only machine
    };

    const parseRamDisplay = (type: string): string => {
        const lower = type.toLowerCase();

        if (lower.startsWith("n4-standard-")) {
            const cpu = extractCpuCount(type);
            if (cpu !== null) return `${cpu * 4}G`;
        }

        const ramTable: Record<string, Record<number, string>> = {
            "a2-highgpu": { 1: "85G", 2: "170G", 4: "340G", 8: "680G", 16: "1360G" },
            "a2-ultragpu": { 1: "170G", 2: "340G", 4: "680G", 8: "1360G" },
            "a2-megagpu": { 16: "1360G" },
            "a3-highgpu": { 1: "234G", 2: "468G", 4: "936G", 8: "1872G" },
            "a3-ultragpu": { 8: "2952G" },
        };

        const match = lower.match(/^(a\d-(highgpu|ultragpu|megagpu|edgegpu))-([\d]+)g$/);
        if (match) {
            const family = match[1];
            const count = parseInt(match[3], 10);
            const sizes = ramTable[family];
            if (sizes && sizes[count]) return sizes[count];
        }

        return "-";
    };

    // track which node row is currently expanded to show the error message
    const [expandedNodeId, setExpandedNodeId] = useState<string | null>(null);
    const [nodeLogs, setNodeLogs] = useState<Record<string, string[]>>({});
    const [logsLoading, setLogsLoading] = useState<Record<string, boolean>>({});
    const logSourceRef = useRef<EventSource | null>(null);

    useEffect(() => {
        if (!expandedNodeId) return;

        setNodeLogs((prev) => ({ ...prev, [expandedNodeId]: [] }));
        setLogsLoading((prev) => ({ ...prev, [expandedNodeId]: true }));

        let source: EventSource | null = null;
        let rotateTimeoutId: number | undefined;
        let closingForRotate = false;
        let stopped = false;
        const ROTATE_MS = 55_000;

        const armRotationTimer = () => {
            if (rotateTimeoutId) window.clearTimeout(rotateTimeoutId);
            rotateTimeoutId = window.setTimeout(() => {
                if (stopped) return;
                closingForRotate = true;
                if (source) source.close();
                window.setTimeout(() => {
                    closingForRotate = false;
                    open();
                }, 0);
            }, ROTATE_MS);
        };

        const open = () => {
            if (stopped) return;
            if (source) source.close();
            let clearedOnThisConnection = false;
            source = new EventSource(`/v1/cluster/${expandedNodeId}/logs`);
            logSourceRef.current = source;

            source.onopen = () => {
                armRotationTimer();
            };

            source.onmessage = (event) => {
                const data = JSON.parse(event.data);
                if (!clearedOnThisConnection) {
                    setNodeLogs((prev) => ({ ...prev, [expandedNodeId]: [] }));
                    setLogsLoading((prev) => ({ ...prev, [expandedNodeId]: false }));
                    clearedOnThisConnection = true;
                }
                setNodeLogs((prev) => {
                    const existing = prev[expandedNodeId] || [];
                    return { ...prev, [expandedNodeId]: [...existing, data.message] };
                });
                setLogsLoading((prev) => ({ ...prev, [expandedNodeId]: false }));
            };

            source.onerror = (error) => {
                if (closingForRotate) return; // intentional rotation
                if (rotateTimeoutId) window.clearTimeout(rotateTimeoutId);
                console.error("Node logs stream error; retry in 5s:", error);
                setLogsLoading((prev) => ({ ...prev, [expandedNodeId]: false }));
            };
        };

        open();

        return () => {
            stopped = true;
            if (rotateTimeoutId) window.clearTimeout(rotateTimeoutId);
            if (source) source.close();
        };
    }, [expandedNodeId]);

    const toggleExpanded = (nodeId: string) => {
        setExpandedNodeId((prev) => (prev === nodeId ? null : nodeId));
    };

    const deleteNode = async (nodeId: string) => {
        // Immediately remove node from UI
        setNodes((prev) => prev.filter((node) => node.id !== nodeId));
        try {
            await fetch(`/v1/cluster/${nodeId}`, { method: "DELETE" });
        } catch (error) {
            console.error("Failed to delete node", error);
        }
    };

    return (
        <div className="space-y-6">
            {showWelcome && (
                <div className="spotlight-surface rounded-xl my-8">
                    <Card className="w-full relative rounded-xl shadow-lg shadow-black/5 bg-white/90 backdrop-blur">
                        <button
                            onClick={handleDismissWelcome}
                            className="absolute top-2 right-2 p-1 hover:bg-gray-100 rounded-full"
                            aria-label="Dismiss welcome message"
                        >
                            <X className="h-6 w-6" />
                        </button>
                        <CardHeader className="pb-4">
                            <CardTitle className="text-[1.45rem] font-semibold text-primary">
                                Welcome to Burla!
                            </CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-6">
                            <div className="grid grid-cols-1 gap-4">
                                <div className="space-y-4">
                                    <ol className="list-none space-y-3">
                                        <li>
                                            🔌 &nbsp;Hit{" "}
                                            <span className="font-semibold">⏻ Start</span> to boot
                                            some machines (1-2 min)
                                        </li>
                                        <li>
                                            📦 &nbsp;Run{" "}
                                            <code className="bg-gray-100 px-1 py-0.5 rounded">
                                                pip install burla
                                            </code>
                                        </li>
                                        <li>
                                            🔑 &nbsp;Run{" "}
                                            <code className="bg-gray-100 px-1 py-0.5 rounded">
                                                burla login
                                            </code>
                                        </li>
                                        <li>
                                            🚀 &nbsp;Run some code:
                                            <br />
                                            <div className="relative mt-3 inline-block w-fit max-w-full">
                                                <button
                                                    type="button"
                                                    aria-label="Copy code"
                                                    onClick={async () => {
                                                        try {
                                                            await navigator.clipboard.writeText(
                                                                pythonExampleCode
                                                            );
                                                            setCopied(true);
                                                            window.setTimeout(
                                                                () => setCopied(false),
                                                                1400
                                                            );
                                                        } catch (e) {
                                                            console.error("Failed to copy", e);
                                                        }
                                                    }}
                                                    className="absolute top-2 right-2 z-10 px-2 py-1 text-xs bg-white/90 hover:bg-white border rounded shadow-sm text-gray-700"
                                                >
                                                    <span className="inline-flex items-center gap-1">
                                                        <Copy className="h-3 w-3" />
                                                        {copied ? "Copied!" : "Copy"}
                                                    </span>
                                                </button>
                                                <pre className="bg-gray-50 border rounded p-3 overflow-x-auto text-sm font-mono pr-14 w-fit max-w-full">
                                                    <code>
                                                        <span className="text-blue-700">from</span>{" "}
                                                        burla{" "}
                                                        <span className="text-blue-700">
                                                            import
                                                        </span>{" "}
                                                        remote_parallel_map
                                                        <br />
                                                        <br />
                                                        <span className="text-blue-700">
                                                            def
                                                        </span>{" "}
                                                        <span className="text-amber-800">
                                                            my_function
                                                        </span>
                                                        (x):
                                                        <br />
                                                        {"    "}print(
                                                        <span className="text-red-700">f</span>
                                                        <span className="text-red-700">
                                                            "Running on a remote computer in the
                                                            cloud! #
                                                        </span>
                                                        {"{"}x{"}"}
                                                        <span className="text-red-700">"</span>)
                                                        <br />
                                                        <br />
                                                        remote_parallel_map(
                                                        <span className="text-amber-800">
                                                            my_function
                                                        </span>
                                                        ,{" "}
                                                        <span className="text-blue-700">list</span>(
                                                        <span className="text-blue-700">range</span>
                                                        (
                                                        <span className="text-purple-700">
                                                            1000
                                                        </span>
                                                        )))
                                                    </code>
                                                </pre>
                                            </div>
                                        </li>
                                    </ol>
                                </div>
                            </div>
                        </CardContent>
                    </Card>
                </div>
            )}

            <Card className="w-full">
                <CardHeader className="flex flex-row items-center justify-between">
                    <CardTitle className="text-xl font-semibold text-primary">Nodes</CardTitle>
                </CardHeader>
                <CardContent>
                    {nodes.length === 0 ? (
                        <div className="border-2 border-dashed rounded-lg p-8 text-center text-muted-foreground">
                            <div className="text-sm">
                                Zero nodes running, hit{" "}
                                <span className="font-semibold">⏻ Start</span> to launch some!
                            </div>
                            <div className="mt-6 space-y-2">
                                {[...Array(3)].map((_, i) => (
                                    <div
                                        key={i}
                                        className="flex items-center gap-4 py-2 justify-center"
                                    >
                                        <span className="w-4 h-4 rounded-full bg-muted/60" />
                                        <Skeleton className="h-4 w-24" />
                                        <Skeleton className="h-4 w-16" />
                                        <Skeleton className="h-4 w-16" />
                                        <Skeleton className="h-4 w-24" />
                                    </div>
                                ))}
                            </div>
                        </div>
                    ) : (
                        <Table className="table-auto w-full">
                            <TableHeader>
                                <TableRow>
                                    <TableHead className="w-8 pl-6 pr-4 py-2" />
                                    <TableHead className="w-24 pl-6 pr-4 py-2">Status</TableHead>
                                    <TableHead className="w-48 pl-6 pr-4 py-2">Name</TableHead>
                                    <TableHead className="w-24 pl-6 pr-4 py-2">CPUs</TableHead>
                                    <TableHead className="w-24 pl-6 pr-4 py-2">RAM</TableHead>
                                    <TableHead className="w-24 pl-6 pr-4 py-2">GPUs</TableHead>
                                    <TableHead className="w-8 pl-6 pr-2 py-2" />
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {nodes.map((node, idx) => (
                                    <React.Fragment key={node.id}>
                                        <TableRow
                                            onClick={() => toggleExpanded(node.id)}
                                            className={cn("cursor-pointer animate-row-in")}
                                            style={{ animationDelay: `${idx * 50}ms` }}
                                        >
                                            <TableCell className="w-8 pl-6 pr-4 py-2">
                                                <ChevronRight
                                                    className={cn(
                                                        "h-4 w-4 transition-transform duration-200",
                                                        { "rotate-90": expandedNodeId === node.id }
                                                    )}
                                                />
                                            </TableCell>
                                            <TableCell className="w-24 pl-6 pr-4 py-2">
                                                <div className="flex items-center space-x-2">
                                                    <div className={getStatusClass(node.status)} />
                                                    <span
                                                        className={cn(
                                                            "text-sm capitalize",
                                                            node.status
                                                        )}
                                                    >
                                                        {node.status}
                                                    </span>
                                                </div>
                                            </TableCell>
                                            <TableCell className="w-48 pl-6 pr-4 py-2 whitespace-nowrap">
                                                {node.name}
                                            </TableCell>
                                            <TableCell className="w-24 pl-6 pr-4 py-2">
                                                <div className="inline-flex items-center space-x-1 justify-center">
                                                    <Cpu className="h-4 w-4" />
                                                    <span>
                                                        {node.cpus ??
                                                            extractCpuCount(node.type) ??
                                                            "?"}
                                                    </span>
                                                </div>
                                            </TableCell>
                                            <TableCell className="w-24 pl-6 pr-4 py-2">
                                                {parseRamDisplay(node.type)}
                                            </TableCell>
                                            <TableCell className="w-24 pl-6 pr-4 py-2">
                                                {parseGpuDisplay(node.type)}
                                            </TableCell>
                                            <TableCell className="w-8 pl-6 pr-2 py-2 text-center">
                                                {(String(node.status) === "FAILED" ||
                                                    String(node.status) === "DELETED") && (
                                                    <button
                                                        onClick={(e) => {
                                                            e.stopPropagation();
                                                            deleteNode(node.id);
                                                        }}
                                                        className="text-gray-400 hover:text-red-600"
                                                        aria-label="Dismiss node"
                                                    >
                                                        <X className="h-4 w-4" />
                                                    </button>
                                                )}
                                            </TableCell>
                                        </TableRow>

                                        {expandedNodeId === node.id && (
                                            <TableRow
                                                key={`${node.id}-error`}
                                                className={cn("transition-all duration-300", {
                                                    "bg-gray-50": expandedNodeId === node.id,
                                                })}
                                            >
                                                <TableCell colSpan={7} className="p-0">
                                                    <div
                                                        className={cn(
                                                            "overflow-y-auto transition-all duration-300",
                                                            {
                                                                "max-h-0":
                                                                    expandedNodeId !== node.id,
                                                                "h-[400px] resize-y py-2 px-4":
                                                                    expandedNodeId === node.id,
                                                            }
                                                        )}
                                                    >
                                                        {logsLoading[node.id] ? (
                                                            <div className="flex flex-col items-center justify-center h-40 w-full text-gray-500">
                                                                <svg
                                                                    className="animate-spin h-8 w-8 text-primary mb-2"
                                                                    viewBox="0 0 24 24"
                                                                    fill="none"
                                                                    xmlns="http://www.w3.org/2000/svg"
                                                                >
                                                                    <circle
                                                                        cx="12"
                                                                        cy="12"
                                                                        r="10"
                                                                        stroke="currentColor"
                                                                        strokeWidth="4"
                                                                        opacity="0.2"
                                                                    />
                                                                    <path
                                                                        d="M22 12a10 10 0 0 1-10 10"
                                                                        stroke="currentColor"
                                                                        strokeWidth="4"
                                                                        strokeLinecap="round"
                                                                    />
                                                                </svg>
                                                            </div>
                                                        ) : (
                                                            <pre className="whitespace-pre-wrap text-gray-600 text-sm">
                                                                {nodeLogs[node.id]?.join("\n")}
                                                            </pre>
                                                        )}
                                                    </div>
                                                </TableCell>
                                            </TableRow>
                                        )}
                                    </React.Fragment>
                                ))}
                            </TableBody>
                        </Table>
                    )}
                </CardContent>
            </Card>
        </div>
    );
};
