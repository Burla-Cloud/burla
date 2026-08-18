import React, { useEffect, useMemo, useRef, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import { ChevronRight, Server } from "lucide-react";
import { cn } from "@/lib/utils";
import { BurlaNode, NodeStatus } from "@/types/coreTypes";
import { StatusBadge, nodeStatusBadge } from "@/components/StatusBadge";
import { TablePagination } from "@/components/TablePagination";
import { managementEvents, managementJson } from "@/lib/managementApi";

interface NodesListProps {
    nodes: BurlaNode[];
    loading: boolean;
    showDeleted: boolean;
    onShowDeletedChange: (show: boolean) => void;
}

const PAGE_SIZE = 15;

const ACTIVE_STATUSES = new Set<string>(["RUNNING", "READY", "BOOTING"]);

export const NodesList: React.FC<NodesListProps> = ({ nodes, loading, showDeleted, onShowDeletedChange }) => {
    const [expandedNodeId, setExpandedNodeId] = useState<string | null>(null);
    const [nodeLogs, setNodeLogs] = useState<Record<string, string[]>>({});
    const [logsLoading, setLogsLoading] = useState<Record<string, boolean>>({});

    const [page, setPage] = useState(0);

    // deleted fetch state (only used when showDeleted is true)
    const [deletedSlice, setDeletedSlice] = useState<BurlaNode[]>([]);
    const [deletedTotal, setDeletedTotal] = useState(0);
    const [deletedLoading, setDeletedLoading] = useState(false);
    const [deletedError, setDeletedError] = useState<string | null>(null);
    const deletedRequestIdRef = useRef(0);
    const deletedCursorsRef = useRef<Record<number, string | null>>({ 0: null });

    // UX: when switching showDeleted on, show loader until first deleted page returns
    const [showDeletedHydrating, setShowDeletedHydrating] = useState(false);

    // logs SSE
    useEffect(() => {
        if (!expandedNodeId) return;

        setNodeLogs((prev) => ({ ...prev, [expandedNodeId]: [] }));
        setLogsLoading((prev) => ({ ...prev, [expandedNodeId]: true }));

        const source = managementEvents(`/nodes/${expandedNodeId}/logs/stream`, {
            log: (data) => {
                setNodeLogs((prev) => {
                    const existing = prev[expandedNodeId] || [];
                    return { ...prev, [expandedNodeId]: [...existing, data.message] };
                });
                setLogsLoading((prev) => ({ ...prev, [expandedNodeId]: false }));
            },
        });
        source.onerror = (error) => {
            console.error("Node logs stream error", error);
            setLogsLoading((prev) => ({ ...prev, [expandedNodeId]: false }));
        };

        return () => {
            source.close();
        };
    }, [expandedNodeId]);

    const toggleExpanded = (nodeId: string) => {
        setExpandedNodeId((prev) => (prev === nodeId ? null : nodeId));
    };

    const toMs = (ts?: number | null) => {
        if (!ts) return 0;
        return ts < 2_000_000_000 ? Math.floor(ts * 1000) : Math.floor(ts);
    };

    const activeNodes = useMemo(() => {
        const actives = nodes.filter((n) => ACTIVE_STATUSES.has(String(n.status || "").toUpperCase()));
        actives.sort((a, b) => toMs(b.started_booting_at) - toMs(a.started_booting_at));
        return actives;
    }, [nodes]);

    // Combined pagination math when showDeleted is true:
    // pages are over (activeNodes + deletedTotal)
    const totalCount = showDeleted ? activeNodes.length + deletedTotal : activeNodes.length;
    const totalPages = Math.max(1, Math.ceil(totalCount / PAGE_SIZE));

    useEffect(() => {
        if (page > 0 && page >= totalPages) setPage(totalPages - 1);
    }, [page, totalPages]);

    // Compute what this page needs from deleted
    const pageStart = page * PAGE_SIZE;
    const pageEnd = pageStart + PAGE_SIZE;

    const activeSlice = useMemo(() => {
        const start = Math.min(pageStart, activeNodes.length);
        const end = Math.min(pageEnd, activeNodes.length);
        return activeNodes.slice(start, end);
    }, [activeNodes, pageStart, pageEnd]);

    const deletedOffset = useMemo(() => {
        if (!showDeleted) return 0;
        if (pageStart < activeNodes.length) return 0;
        return pageStart - activeNodes.length;
    }, [showDeleted, pageStart, activeNodes.length]);

    const deletedLimit = useMemo(() => {
        if (!showDeleted) return 0;
        if (pageStart < activeNodes.length) {
            const needed = Math.max(0, pageEnd - activeNodes.length);
            return Math.min(PAGE_SIZE, needed);
        }
        return PAGE_SIZE;
    }, [showDeleted, pageStart, pageEnd, activeNodes.length]);

    // Handle toggle transitions
    useEffect(() => {
        setExpandedNodeId(null);
        setPage(0);

        if (showDeleted) {
            setShowDeletedHydrating(true);
            setDeletedSlice([]);
            setDeletedTotal(0);
            setDeletedError(null);
            deletedCursorsRef.current = { 0: null };
        } else {
            setShowDeletedHydrating(false);
            setDeletedSlice([]);
            setDeletedTotal(0);
            setDeletedError(null);
            deletedRequestIdRef.current += 1;
        }
    }, [showDeleted]);

    // Fetch deleted slice for this page
    useEffect(() => {
        if (!showDeleted) return;

        // If this page is fully within active nodes, we still want deletedTotal for page count
        // So request limit=1 just to get total. But we can also request limit=deletedLimit (0 allowed),
        // and backend returns total either way. We will request limit=max(1,deletedLimit) if deletedLimit is 0.
        const needsSlice = deletedLimit > 0;
        const reqLimit = needsSlice ? deletedLimit : 1;
        const reqOffset = needsSlice ? deletedOffset : 0;

        const controller = new AbortController();
        const requestId = ++deletedRequestIdRef.current;

        const load = async () => {
            try {
                setDeletedLoading(true);
                setDeletedError(null);

                const query = new URLSearchParams({
                    status: "all",
                    ended_after: "1970-01-01T00:00:00Z",
                    sort: "ended_at",
                    order: "desc",
                    limit: String(reqLimit),
                });
                const cursor = deletedCursorsRef.current[reqOffset];
                if (cursor) query.set("cursor", cursor);
                const json = await managementJson<any>(`/nodes?${query}`, {
                    signal: controller.signal,
                });
                if (requestId !== deletedRequestIdRef.current) return;

                const rawNodes = json.items ?? [];
                setDeletedTotal(json.total_count ?? rawNodes.length);
                deletedCursorsRef.current[reqOffset + rawNodes.length] = json.next_cursor;

                if (needsSlice) {
                    const mapped: BurlaNode[] = rawNodes.map((raw) => ({
                        id: raw.node_id,
                        name: raw.node_id,
                        status: String(raw.status || "deleted").toUpperCase() as NodeStatus,
                        type: raw.machine_type || "unknown",
                        cpus: raw.vcpu_count ?? undefined,
                        gpus: raw.gpu_count ?? undefined,
                        gpuDisplay: raw.gpu_display ?? undefined,
                        memory:
                            typeof raw.memory_bytes === "number"
                                ? `${Math.round(raw.memory_bytes / 1024 ** 3)}G`
                                : undefined,
                        age: undefined,
                        logs: undefined,
                        started_booting_at:
                            typeof raw.started_booting_at === "string" ? Date.parse(raw.started_booting_at) : undefined,
                        deletedAt: typeof raw.ended_at === "string" ? Date.parse(raw.ended_at) : undefined,
                    }));
                    setDeletedSlice(mapped);
                } else {
                    setDeletedSlice([]);
                }
            } catch (err: any) {
                if (err.name === "AbortError") return;
                console.error("error fetching deleted nodes", err);
                setDeletedError(err?.message || "Failed to load deleted nodes");
                setDeletedTotal(0);
                setDeletedSlice([]);
            } finally {
                if (requestId === deletedRequestIdRef.current) {
                    setDeletedLoading(false);
                    setShowDeletedHydrating(false);
                }
            }
        };

        load();
        return () => controller.abort();
    }, [showDeleted, deletedOffset, deletedLimit]);

    const displayNodes = useMemo(() => {
        if (!showDeleted) return activeSlice;
        return [...activeSlice, ...deletedSlice];
    }, [showDeleted, activeSlice, deletedSlice]);

    const noActiveNodes = !showDeleted && activeNodes.length === 0;
    const noCombinedNodes = showDeleted && !showDeletedHydrating && !deletedLoading && displayNodes.length === 0;

    const isBusy = showDeletedHydrating || (showDeleted && deletedLoading);

    return (
        <Card>
            <CardHeader className="flex-row items-center justify-between space-y-0 border-b border-border/70 px-5 py-4">
                <CardTitle>Nodes</CardTitle>
                <label className="flex cursor-pointer select-none items-center gap-2 text-[13px] text-muted-foreground">
                    Show deleted
                    <Switch checked={showDeleted} onCheckedChange={onShowDeletedChange} />
                </label>
            </CardHeader>

            <CardContent className="p-0">
                {loading ? (
                    <div className="space-y-3 px-5 py-4">
                        {[...Array(3)].map((_, i) => (
                            <div key={i} className="flex items-center gap-6">
                                <Skeleton className="h-5 w-16 rounded-md" />
                                <Skeleton className="h-4 w-40" />
                                <Skeleton className="h-4 w-24" />
                                <Skeleton className="h-4 w-10" />
                                <Skeleton className="h-4 w-10" />
                            </div>
                        ))}
                    </div>
                ) : isBusy ? (
                    <div className="flex justify-center py-12">
                        <div className="h-5 w-5 animate-spin rounded-full border-2 border-border border-t-primary" />
                    </div>
                ) : (
                    <>
                        {deletedError && showDeleted && (
                            <div className="mx-5 my-4 rounded-lg border border-destructive/30 bg-destructive/10 px-4 py-3 text-sm text-destructive">
                                {deletedError}
                            </div>
                        )}

                        {(noActiveNodes || noCombinedNodes) && !deletedError ? (
                            <div className="flex flex-col items-center justify-center px-6 py-14 text-center">
                                <div className="flex h-10 w-10 items-center justify-center rounded-full bg-muted">
                                    <Server className="h-[18px] w-[18px] text-muted-foreground" />
                                </div>
                                <p className="mt-3 text-sm font-medium text-foreground">
                                    {noActiveNodes ? "No nodes running" : "No nodes to display"}
                                </p>
                                {noActiveNodes && (
                                    <p className="mt-1 text-[13px] text-muted-foreground">
                                        Hit <span className="font-medium">Start</span> to boot machines.
                                    </p>
                                )}
                            </div>
                        ) : (
                            displayNodes.length > 0 && (
                                <>
                                    <Table>
                                        <TableHeader>
                                            <TableRow className="hover:bg-transparent">
                                                <TableHead className="w-10 pl-5 pr-0" />
                                                <TableHead>Status</TableHead>
                                                <TableHead>Name</TableHead>
                                                <TableHead>Function</TableHead>
                                                <TableHead className="text-right">vCPUs</TableHead>
                                                <TableHead className="text-right">RAM</TableHead>
                                                <TableHead className="pr-5">GPUs</TableHead>
                                            </TableRow>
                                        </TableHeader>
                                        <TableBody>
                                            {displayNodes.map((node) => (
                                                <React.Fragment key={node.id}>
                                                    <TableRow
                                                        onClick={() => toggleExpanded(node.id)}
                                                        className="cursor-pointer"
                                                    >
                                                        <TableCell className="w-10 pl-5 pr-0">
                                                            <ChevronRight
                                                                className={cn(
                                                                    "h-4 w-4 text-muted-foreground transition-transform duration-200",
                                                                    expandedNodeId === node.id && "rotate-90",
                                                                )}
                                                            />
                                                        </TableCell>
                                                        <TableCell>
                                                            <StatusBadge {...nodeStatusBadge(node.status)} />
                                                        </TableCell>
                                                        <TableCell className="whitespace-nowrap font-mono text-[13px] text-foreground">
                                                            {node.name}
                                                        </TableCell>
                                                        <TableCell>
                                                            <div
                                                                className="max-w-[220px] truncate font-mono text-[13px]"
                                                                title={node.current_function ?? ""}
                                                            >
                                                                {node.current_function ?? (
                                                                    <span className="text-muted-foreground">—</span>
                                                                )}
                                                            </div>
                                                        </TableCell>
                                                        <TableCell className="text-right tabular-nums">
                                                            {node.cpus ?? "—"}
                                                        </TableCell>
                                                        <TableCell className="text-right tabular-nums">
                                                            {node.memory ? (
                                                                node.memory
                                                            ) : (
                                                                <span className="text-muted-foreground">—</span>
                                                            )}
                                                        </TableCell>
                                                        <TableCell className="whitespace-nowrap pr-5">
                                                            {node.gpuDisplay ? (
                                                                node.gpuDisplay
                                                            ) : (
                                                                <span className="text-muted-foreground">—</span>
                                                            )}
                                                        </TableCell>
                                                    </TableRow>

                                                    {expandedNodeId === node.id && (
                                                        <TableRow
                                                            key={`${node.id}-logs`}
                                                            className="bg-muted/30 hover:bg-muted/30"
                                                        >
                                                            <TableCell colSpan={7} className="p-0">
                                                                <div className="h-[400px] resize-y overflow-y-auto px-5 py-3">
                                                                    {logsLoading[node.id] ? (
                                                                        <div className="flex h-40 w-full items-center justify-center">
                                                                            <div className="h-5 w-5 animate-spin rounded-full border-2 border-border border-t-primary" />
                                                                        </div>
                                                                    ) : (
                                                                        <pre className="whitespace-pre-wrap font-mono text-xs leading-5 text-muted-foreground">
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

                                    {totalPages > 1 && (
                                        <div className="px-5 pb-4">
                                            <TablePagination
                                                page={page}
                                                totalPages={totalPages}
                                                onPageChange={setPage}
                                            />
                                        </div>
                                    )}
                                </>
                            )
                        )}
                    </>
                )}
            </CardContent>
        </Card>
    );
};
