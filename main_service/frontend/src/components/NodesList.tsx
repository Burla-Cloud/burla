import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import { Cpu, X, ChevronRight, Copy } from "lucide-react";
import { cn } from "@/lib/utils";
import { BurlaNode, NodeStatus } from "@/types/coreTypes";

interface NodesListProps {
    nodes: BurlaNode[];
    showDeleted: boolean;
    onShowDeletedChange: (show: boolean) => void;
}

type NodeStatusLike = NodeStatus | string | null | undefined;

const DELETED_STATUSES = new Set<NodeStatusLike>(["FAILED", "DELETED"]);
const PAGE_SIZE = 15;

export const NodesList: React.FC<NodesListProps> = ({
    nodes,
    showDeleted,
    onShowDeletedChange,
}) => {
    const [showWelcome, setShowWelcome] = useState(true);
    const [copied, setCopied] = useState(false);

    const [expandedNodeId, setExpandedNodeId] = useState<string | null>(null);
    const [nodeLogs, setNodeLogs] = useState<Record<string, string[]>>({});
    const [logsLoading, setLogsLoading] = useState<Record<string, boolean>>({});
    const logSourceRef = useRef<EventSource | null>(null);

    const didMountRef = useRef(false);

    // active nodes pagination (client side)
    const [activePage, setActivePage] = useState(0);

    // deleted nodes pagination (server side)
    const [deletedPage, setDeletedPage] = useState(0);
    const [deletedTotalPages, setDeletedTotalPages] = useState(1);
    const [deletedNodes, setDeletedNodes] = useState<BurlaNode[]>([]);
    const [deletedLoading, setDeletedLoading] = useState(false);
    const [deletedError, setDeletedError] = useState<string | null>(null);
    const deletedRequestIdRef = useRef(0);

    const pythonExampleCode = `from burla import remote_parallel_map

def my_function(x):
    print(f"Running on a remote computer in the cloud! #{x}")

remote_parallel_map(my_function, list(range(1000)))`;

    useEffect(() => {
        const isWelcomeHidden =
            typeof window !== "undefined" &&
            localStorage.getItem("welcomeMessageHidden") === "true";
        setShowWelcome(!isWelcomeHidden);
    }, []);

    const handleDismissWelcome = () => {
        setShowWelcome(false);
        try {
            localStorage.setItem("welcomeMessageHidden", "true");
        } catch {
            // ignore
        }
        window.dispatchEvent(new CustomEvent("welcomeVisibilityChanged", { detail: false }));
    };

    const getStatusClass = (nodeStatus: NodeStatusLike) => {
        const statusClasses: Record<string, string> = {
            READY: "bg-green-500",
            RUNNING: "bg-green-500 animate-pulse",
            BOOTING: "bg-yellow-500 animate-pulse",
            STOPPING: "bg-gray-300 animate-pulse",
            FAILED: "bg-red-500",
            DELETED: "bg-red-500",
        };

        const key = typeof nodeStatus === "string" ? nodeStatus : "";
        return cn(
            "w-2 h-2 rounded-full",
            key ? statusClasses[key] ?? "bg-gray-300" : "bg-gray-300"
        );
    };

    const extractCpuCount = (type: string): number | null => {
        const customMatch = type.match(/^custom-(\d+)-/);
        if (customMatch) return parseInt(customMatch[1], 10);

        const standardMatch = type.match(/-(\d+)$/);
        if (standardMatch) return parseInt(standardMatch[1], 10);

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

        return "-";
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

    // logs SSE
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
                if (closingForRotate) return;
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

    const STATUS_RANK: Record<string, number> = {
        DELETED: 0,
        FAILED: 1,
        STOPPING: 2,
        BOOTING: 3,
        RUNNING: 4,
        READY: 5,
    };

    const isDeletedOrFailed = (n: BurlaNode) => {
        const status = String(n.status || "").toUpperCase();
        return status === "FAILED" || status === "DELETED";
    };

    const toMs = (ts?: number | null) => {
        if (!ts) return 0;
        // safety: handle both seconds and ms if anything slips through
        return ts < 2_000_000_000 ? Math.floor(ts * 1000) : Math.floor(ts);
    };

    const nodeStartedMs = (n: BurlaNode) =>
        toMs(n.started_booting_at ?? n.deletedAt ?? 0);

    const sortNodes = useCallback(
        (a: BurlaNode, b: BurlaNode) => {
            // group: active nodes first, then FAILED/DELETED
            const aGroup = isDeletedOrFailed(a) ? 0 : 1;
            const bGroup = isDeletedOrFailed(b) ? 0 : 1;
            if (aGroup !== bGroup) return bGroup - aGroup;

            // within group: most recent started_booting_at first
            const aTs = nodeStartedMs(a);
            const bTs = nodeStartedMs(b);
            if (aTs !== bTs) return bTs - aTs;

            // tie-breaker: status rank (minor)
            const aStatus = STATUS_RANK[String(a.status || "").toUpperCase()] ?? 0;
            const bStatus = STATUS_RANK[String(b.status || "").toUpperCase()] ?? 0;
            if (aStatus !== bStatus) return bStatus - aStatus;

            return String(a.name).localeCompare(String(b.name));
        },
        []
    );

    const sortedNodes = useMemo(() => {
        const arr = [...nodes];
        arr.sort(sortNodes);
        return arr;
    }, [nodes, sortNodes]);

    const activeNodes = useMemo(() => {
        return sortedNodes.filter((n) => !isDeletedOrFailed(n));
    }, [sortedNodes]);

    const activeTotalPages = Math.max(1, Math.ceil(activeNodes.length / PAGE_SIZE));

    useEffect(() => {
        if (activePage > 0 && activePage >= activeTotalPages) {
            setActivePage(activeTotalPages - 1);
        }
    }, [activeTotalPages, activePage]);

    const activePagedNodes = useMemo(() => {
        const start = activePage * PAGE_SIZE;
        const end = start + PAGE_SIZE;
        return activeNodes.slice(start, end);
    }, [activeNodes, activePage]);

    // clear or reset deleted pagination when toggling
    useEffect(() => {
        if (showDeleted) {
            setDeletedPage(0);
        } else {
            // leaving deleted view, clear cache so we always refetch
            setDeletedNodes([]);
            setDeletedTotalPages(1);
            setDeletedPage(0);
            setDeletedLoading(false);
            setDeletedError(null);
            deletedRequestIdRef.current += 1;
        }
    }, [showDeleted]);

    // fetch deleted nodes
    useEffect(() => {
        if (!showDeleted) return;

        setExpandedNodeId(null);

        const controller = new AbortController();
        const requestId = ++deletedRequestIdRef.current;

        const load = async () => {
            try {
                setDeletedLoading(true);
                setDeletedError(null);

                const res = await fetch(
                    `/v1/cluster/deleted_recent_paginated?page=${deletedPage}&page_size=${PAGE_SIZE}`,
                    { signal: controller.signal }
                );

                if (!res.ok) {
                    throw new Error(`status ${res.status}`);
                }

                const json = await res.json();

                if (requestId !== deletedRequestIdRef.current) return;

                const rawNodes: any[] = Array.isArray(json.nodes) ? json.nodes : [];

                const mapped: BurlaNode[] = rawNodes.map((raw) => ({
                    id: raw.id,
                    name: raw.name ?? raw.id,
                    status: (raw.status || "DELETED") as NodeStatus,
                    type: raw.type || "unknown",
                    cpus: raw.cpus ?? undefined,
                    gpus: raw.gpus ?? undefined,
                    memory: raw.memory ?? undefined,
                    age: undefined,
                    logs: undefined,
                    started_booting_at:
                        typeof raw.started_booting_at === "number" ? raw.started_booting_at : undefined,
                    deletedAt: typeof raw.deletedAt === "number" ? raw.deletedAt : undefined,
                }));

                setDeletedNodes(mapped);

                const total: number = typeof json.total === "number" ? json.total : mapped.length;
                const pages = Math.max(1, Math.ceil(total / PAGE_SIZE));
                setDeletedTotalPages(pages);
            } catch (err: any) {
                if (err.name === "AbortError") return;
                console.error("error fetching deleted nodes", err);
                setDeletedError(err?.message || "Failed to load deleted nodes");
            } finally {
                if (requestId === deletedRequestIdRef.current) {
                    setDeletedLoading(false);
                }
            }
        };

        load();

        return () => controller.abort();
    }, [showDeleted, deletedPage]);

    useEffect(() => {
        didMountRef.current = true;
    }, []);

    const displayNodes = useMemo(() => {
        if (!showDeleted) return activePagedNodes;

        // show active first (paged), then deleted list appended, then sort with grouping rules
        const combined = [...activePagedNodes];
        const seen = new Set(combined.map((node) => node.id));

        deletedNodes.forEach((node) => {
            if (!seen.has(node.id)) combined.push(node);
        });

        combined.sort(sortNodes);
        return combined;
    }, [showDeleted, activePagedNodes, deletedNodes, sortNodes]);

    const noActiveNodes = !showDeleted && activeNodes.length === 0;
    const noDeletedNodes = showDeleted && !deletedLoading && displayNodes.length === 0;

    const handleShowDeletedChange = (value: boolean) => {
        onShowDeletedChange(value);
    };

    return (
        <div className="space-y-6 [scrollbar-gutter:stable_both-edges]">
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
                <CardHeader className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                    <CardTitle className="text-xl font-semibold text-primary">Nodes</CardTitle>
                    <div className="flex items-center gap-2 text-sm text-muted-foreground sm:ml-auto">
                        <span>Show deleted nodes</span>
                        <Switch
                            checked={showDeleted}
                            onCheckedChange={handleShowDeletedChange}
                            className="scale-90"
                        />
                    </div>
                </CardHeader>

                <CardContent>
                    {showDeleted && deletedLoading ? (
                        <div className="flex justify-center py-8">
                            <div className="w-5 h-5 border-2 border-primary border-t-transparent rounded-full animate-spin" />
                        </div>
                    ) : (
                        <>
                            {noActiveNodes && !showDeleted && (
                                <div className="border-2 border-dashed rounded-lg p-8 text-center text-muted-foreground">
                                    <div className="text-sm">
                                        Zero nodes running, hit{" "}
                                        <span className="font-semibold">⏻ Start</span> to launch
                                        some.
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
                            )}

                            {noDeletedNodes && showDeleted && (
                                <div className="border-2 border-dashed rounded-lg p-8 text-center text-muted-foreground">
                                    <div className="text-sm">No nodes to display.</div>
                                </div>
                            )}

                            {deletedError && showDeleted && (
                                <div className="border border-red-300 rounded-lg p-4 mb-4 text-sm text-red-700 bg-red-50">
                                    {deletedError}
                                </div>
                            )}

                            {displayNodes.length > 0 && (
                                <>
                                    <Table className="table-auto w-full">
                                        <TableHeader>
                                            <TableRow>
                                                <TableHead className="w-8 pl-6 pr-4 py-2" />
                                                <TableHead className="w-24 pl-6 pr-4 py-2">
                                                    Status
                                                </TableHead>
                                                <TableHead className="w-48 pl-6 pr-4 py-2">
                                                    Name
                                                </TableHead>
                                                <TableHead className="w-24 pl-6 pr-4 py-2">
                                                    vCPUs
                                                </TableHead>
                                                <TableHead className="w-24 pl-6 pr-4 py-2">
                                                    RAM
                                                </TableHead>
                                                <TableHead className="w-24 pl-6 pr-4 py-2">
                                                    GPUs
                                                </TableHead>
                                                <TableHead className="w-8 pl-6 pr-2 py-2 text-right" />
                                            </TableRow>
                                        </TableHeader>
                                        <TableBody>
                                            {displayNodes.map((node, idx) => (
                                                <React.Fragment key={node.id}>
                                                    <TableRow
                                                        onClick={() => toggleExpanded(node.id)}
                                                        className={cn(
                                                            "cursor-pointer",
                                                            didMountRef.current
                                                                ? ""
                                                                : "animate-row-in"
                                                        )}
                                                        style={{ animationDelay: `${idx * 50}ms` }}
                                                    >
                                                        <TableCell className="w-8 pl-6 pr-4 py-2">
                                                            <ChevronRight
                                                                className={cn(
                                                                    "h-4 w-4 transition-transform duration-200",
                                                                    {
                                                                        "rotate-90":
                                                                            expandedNodeId ===
                                                                            node.id,
                                                                    }
                                                                )}
                                                            />
                                                        </TableCell>
                                                        <TableCell className="w-24 pl-6 pr-4 py-2">
                                                            <div className="flex items-center space-x-2">
                                                                <div
                                                                    className={getStatusClass(
                                                                        node.status
                                                                    )}
                                                                />
                                                                <span className="text-sm capitalize">
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
                                                        <TableCell className="w-8 pl-6 pr-2 py-2 text-center" />
                                                    </TableRow>

                                                    {expandedNodeId === node.id && (
                                                        <TableRow
                                                            key={`${node.id}-logs`}
                                                            className={cn(
                                                                "transition-all duration-300",
                                                                {
                                                                    "bg-gray-50":
                                                                        expandedNodeId === node.id,
                                                                }
                                                            )}
                                                        >
                                                            <TableCell colSpan={7} className="p-0">
                                                                <div
                                                                    className={cn(
                                                                        "overflow-y-auto transition-all duration-300",
                                                                        {
                                                                            "max-h-0":
                                                                                expandedNodeId !==
                                                                                node.id,
                                                                            "h-[400px] resize-y py-2 px-4":
                                                                                expandedNodeId ===
                                                                                node.id,
                                                                        }
                                                                    )}
                                                                >
                                                                    {logsLoading[node.id] ? (
                                                                        <div className="flex flex-col items-center justify-center h-40 w-full text-gray-500">
                                                                            <div className="w-5 h-5 border-2 border-primary border-t-transparent rounded-full animate-spin mb-2" />
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

                                    <div className="flex justify-center mt-6 space-x-2 items-center">
                                        {showDeleted ? (
                                            <>
                                                {deletedPage > 0 && (
                                                    <button
                                                        onClick={() => setDeletedPage(deletedPage - 1)}
                                                        className="px-3 py-1 text-sm text-primary hover:underline disabled:text-gray-400 disabled:cursor-not-allowed"
                                                        disabled={deletedLoading}
                                                    >
                                                        ‹ Prev
                                                    </button>
                                                )}

                                                <button
                                                    onClick={() => setDeletedPage(0)}
                                                    className={`px-3 py-1 rounded text-sm border ${
                                                        deletedPage === 0
                                                            ? "bg-primary text-primary-foreground"
                                                            : "bg-white text-gray-700 hover:bg-gray-100"
                                                    }`}
                                                    disabled={deletedLoading}
                                                >
                                                    1
                                                </button>

                                                {deletedPage > 3 && <span className="px-1">...</span>}

                                                {Array.from({ length: deletedTotalPages }, (_, i) => i)
                                                    .filter(
                                                        (i) =>
                                                            i !== 0 &&
                                                            i !== deletedTotalPages - 1 &&
                                                            Math.abs(i - deletedPage) <= 2
                                                    )
                                                    .map((i) => (
                                                        <button
                                                            key={i}
                                                            onClick={() => setDeletedPage(i)}
                                                            className={`px-3 py-1 rounded text-sm border ${
                                                                deletedPage === i
                                                                    ? "bg-primary text-primary-foreground"
                                                                    : "bg-white text-gray-700 hover:bg-gray-100"
                                                            }`}
                                                            disabled={deletedLoading}
                                                        >
                                                            {i + 1}
                                                        </button>
                                                    ))}

                                                {deletedPage < deletedTotalPages - 4 && (
                                                    <span className="px-1">...</span>
                                                )}

                                                {deletedTotalPages > 1 && (
                                                    <button
                                                        onClick={() => setDeletedPage(deletedTotalPages - 1)}
                                                        className={`px-3 py-1 rounded text-sm border ${
                                                            deletedPage === deletedTotalPages - 1
                                                                ? "bg-primary text-primary-foreground"
                                                                : "bg-white text-gray-700 hover:bg-gray-100"
                                                        }`}
                                                        disabled={deletedLoading}
                                                    >
                                                        {deletedTotalPages}
                                                    </button>
                                                )}

                                                {deletedPage < deletedTotalPages - 1 && (
                                                    <button
                                                        onClick={() => setDeletedPage(deletedPage + 1)}
                                                        className="px-3 py-1 text-sm text-primary hover:underline disabled:text-gray-400 disabled:cursor-not-allowed"
                                                        disabled={deletedLoading}
                                                    >
                                                        Next ›
                                                    </button>
                                                )}
                                            </>
                                        ) : (
                                            <>
                                                {activePage > 0 && (
                                                    <button
                                                        onClick={() => setActivePage(activePage - 1)}
                                                        className="px-3 py-1 text-sm text-primary hover:underline"
                                                    >
                                                        ‹ Prev
                                                    </button>
                                                )}

                                                <button
                                                    onClick={() => setActivePage(0)}
                                                    className={`px-3 py-1 rounded text-sm border ${
                                                        activePage === 0
                                                            ? "bg-primary text-primary-foreground"
                                                            : "bg-white text-gray-700 hover:bg-gray-100"
                                                    }`}
                                                >
                                                    1
                                                </button>

                                                {activePage > 3 && <span className="px-1">...</span>}

                                                {Array.from({ length: activeTotalPages }, (_, i) => i)
                                                    .filter(
                                                        (i) =>
                                                            i !== 0 &&
                                                            i !== activeTotalPages - 1 &&
                                                            Math.abs(i - activePage) <= 2
                                                    )
                                                    .map((i) => (
                                                        <button
                                                            key={i}
                                                            onClick={() => setActivePage(i)}
                                                            className={`px-3 py-1 rounded text-sm border ${
                                                                activePage === i
                                                                    ? "bg-primary text-primary-foreground"
                                                                    : "bg-white text-gray-700 hover:bg-gray-100"
                                                            }`}
                                                        >
                                                            {i + 1}
                                                        </button>
                                                    ))}

                                                {activePage < activeTotalPages - 4 && (
                                                    <span className="px-1">...</span>
                                                )}

                                                {activeTotalPages > 1 && (
                                                    <button
                                                        onClick={() => setActivePage(activeTotalPages - 1)}
                                                        className={`px-3 py-1 rounded text-sm border ${
                                                            activePage === activeTotalPages - 1
                                                                ? "bg-primary text-primary-foreground"
                                                                : "bg-white text-gray-700 hover:bg-gray-100"
                                                        }`}
                                                    >
                                                        {activeTotalPages}
                                                    </button>
                                                )}

                                                {activePage < activeTotalPages - 1 && (
                                                    <button
                                                        onClick={() => setActivePage(activePage + 1)}
                                                        className="px-3 py-1 text-sm text-primary hover:underline"
                                                    >
                                                        Next ›
                                                    </button>
                                                )}
                                            </>
                                        )}
                                    </div>
                                </>
                            )}
                        </>
                    )}
                </CardContent>
            </Card>
        </div>
    );
};
