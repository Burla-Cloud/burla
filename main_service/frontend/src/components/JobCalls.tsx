import { useCallback, useEffect, useMemo, useState } from "react";
import { ArrowDown, ArrowUp, ChevronDown, ChevronLeft, ChevronRight } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { TablePagination } from "@/components/TablePagination";
import { StatusBadge } from "@/components/StatusBadge";
import {
    MetricChart,
    PRIMARY,
    SECONDARY,
    formatBytes,
    formatDuration,
    formatRate,
} from "@/components/JobMetricChart";
import { cn } from "@/lib/utils";

type TaskPoint = {
    t: number;
    cpus: number;
    mem: number;
    net_rx: number;
    net_tx: number;
    disk_read: number;
    disk_write: number;
    gpu: number | null;
    gpu_mem: number | null;
};

type TaskSeries = {
    has_metrics: boolean;
    has_gpu: boolean;
    prev_index: number | null;
    next_index: number | null;
    n_attempts: number;
    bucket_sec: number;
    points: TaskPoint[];
};

type TaskSummary = {
    index: number;
    duration_sec: number | null;
    attempts: number | null;
    peak_cpus: number | null;
    peak_mem_bytes: number | null;
    failed: boolean;
};

type TaskSummaryPage = {
    total: number;
    tasks: TaskSummary[];
};

type CallLogEntry = {
    message: string;
    log_timestamp: number;
    is_error: boolean;
};

type CallLogs = {
    entries: CallLogEntry[];
    truncated: boolean;
};

type SortKey = "index" | "duration" | "attempts" | "peak_cpus" | "peak_mem";

const CALLS_PER_PAGE = 50;

const formatLogTime = (epochSec: number) =>
    new Date(epochSec * 1000).toLocaleTimeString("en-US", {
        hour: "numeric",
        minute: "2-digit",
        second: "2-digit",
        hour12: true,
    });

const iconBtnClass = (disabled: boolean) =>
    disabled
        ? "flex h-7 w-7 items-center justify-center rounded-md border border-border bg-card text-muted-foreground opacity-50 cursor-default"
        : "flex h-7 w-7 items-center justify-center rounded-md border border-border bg-card text-muted-foreground shadow-sm transition-colors duration-150 hover:bg-muted/60 hover:text-foreground";

const SortableHead = ({
    label,
    column,
    sort,
    descending,
    onSort,
    align = "left",
    className,
}: {
    label: string;
    column: SortKey;
    sort: SortKey;
    descending: boolean;
    onSort: (column: SortKey) => void;
    align?: "left" | "right";
    className?: string;
}) => (
    <TableHead className={className}>
        <button
            type="button"
            onClick={() => onSort(column)}
            className={cn(
                "inline-flex items-center gap-1 transition-colors duration-150 hover:text-foreground focus-visible:outline-none",
                align === "right" && "flex-row-reverse",
                sort === column && "text-foreground"
            )}
        >
            {label}
            {sort === column &&
                (descending ? <ArrowDown className="h-3 w-3" /> : <ArrowUp className="h-3 w-3" />)}
        </button>
    </TableHead>
);

const CallDetail = ({
    jobId,
    taskIndex,
    isLive,
    isJobTerminal,
    onSelectTask,
    onClearTask,
}: {
    jobId: string;
    taskIndex: number;
    isLive: boolean;
    isJobTerminal: boolean;
    onSelectTask: (index: number) => void;
    onClearTask: () => void;
}) => {
    const [series, setSeries] = useState<TaskSeries | null>(null);
    const [summary, setSummary] = useState<TaskSummary | null>(null);
    const [logs, setLogs] = useState<CallLogs | null>(null);
    const [isLoading, setIsLoading] = useState(true);
    // Charts are secondary to status/logs, so they start collapsed.
    const [isChartsOpen, setIsChartsOpen] = useState(false);

    const load = useCallback(async () => {
        const [seriesRes, summaryRes, logsRes] = await Promise.all([
            fetch(`/v1/jobs/${jobId}/metrics/tasks/${taskIndex}`),
            fetch(`/v1/jobs/${jobId}/metrics/task-summaries?index=${taskIndex}&limit=1`),
            fetch(`/v1/jobs/${jobId}/logs?index=${taskIndex}`),
        ]);
        if (seriesRes.ok) setSeries(await seriesRes.json());
        if (summaryRes.ok) {
            const page: TaskSummaryPage = await summaryRes.json();
            setSummary(page.tasks[0] ?? null);
        }
        if (logsRes.ok) {
            const payload = await logsRes.json();
            setLogs({
                entries: payload.logs ?? [],
                truncated: Boolean(payload.has_more_older),
            });
        }
    }, [jobId, taskIndex]);

    useEffect(() => {
        let cancelled = false;
        setSeries(null);
        setSummary(null);
        setLogs(null);
        setIsLoading(true);
        setIsChartsOpen(false);
        (async () => {
            await load().catch(() => {});
            if (!cancelled) setIsLoading(false);
        })();
        if (!isLive) {
            return () => {
                cancelled = true;
            };
        }
        const id = window.setInterval(() => void load().catch(() => {}), 5000);
        return () => {
            cancelled = true;
            window.clearInterval(id);
        };
    }, [load, isLive]);

    const taskData = useMemo(() => series?.points ?? [], [series]);
    const taskStartAt = taskData.length ? taskData[0].t : 0;

    const facts: { label: string; value: React.ReactNode }[] = summary
        ? [
              {
                  label: "Duration",
                  value:
                      summary.duration_sec != null ? (
                          formatDuration(summary.duration_sec)
                      ) : (
                          <span className="text-muted-foreground">No samples</span>
                      ),
              },
              {
                  label: "Attempts",
                  value:
                      summary.attempts != null ? (
                          summary.attempts
                      ) : (
                          <span className="text-muted-foreground">unknown</span>
                      ),
              },
              {
                  label: "Peak CPU",
                  value:
                      summary.peak_cpus != null ? (
                          `${summary.peak_cpus.toFixed(2)} vCPU`
                      ) : (
                          <span className="text-muted-foreground">unknown</span>
                      ),
              },
              {
                  label: "Peak memory",
                  value:
                      summary.peak_mem_bytes != null ? (
                          formatBytes(summary.peak_mem_bytes)
                      ) : (
                          <span className="text-muted-foreground">unknown</span>
                      ),
              },
          ]
        : [];

    return (
        <div className="rounded-xl border border-border bg-card shadow-sm">
            {/* List / stepping bar */}
            <div className="flex items-center justify-between border-b border-border/70 px-5 py-3">
                <button
                    type="button"
                    onClick={onClearTask}
                    className="inline-flex items-center gap-1 text-[13px] font-medium text-muted-foreground transition-colors duration-150 hover:text-foreground focus-visible:outline-none"
                >
                    <ChevronLeft className="h-3.5 w-3.5" />
                    All function calls
                </button>
                <div className="flex items-center gap-1">
                    <button
                        type="button"
                        onClick={() =>
                            series?.prev_index != null && onSelectTask(series.prev_index)
                        }
                        disabled={series?.prev_index == null}
                        className={iconBtnClass(series?.prev_index == null)}
                        aria-label="Previous call with samples"
                        title="Previous call with samples"
                    >
                        <ChevronLeft className="h-3.5 w-3.5" />
                    </button>
                    <button
                        type="button"
                        onClick={() =>
                            series?.next_index != null && onSelectTask(series.next_index)
                        }
                        disabled={series?.next_index == null}
                        className={iconBtnClass(series?.next_index == null)}
                        aria-label="Next call with samples"
                        title="Next call with samples"
                    >
                        <ChevronRight className="h-3.5 w-3.5" />
                    </button>
                </div>
            </div>

            <div className="px-5 py-4">
                {/* Title + status */}
                <div className="flex items-center gap-3">
                    <h2 className="text-base font-semibold tabular-nums text-foreground">
                        Input {taskIndex.toLocaleString()}
                    </h2>
                    {summary?.failed ? (
                        <StatusBadge tone="danger" label="Failed" />
                    ) : summary != null && isJobTerminal ? (
                        <StatusBadge tone="success" label="Succeeded" />
                    ) : null}
                </div>

                {/* Facts */}
                {isLoading ? (
                    <Skeleton className="mt-4 h-10 w-2/3" />
                ) : facts.length > 0 ? (
                    <div className="mt-4 flex flex-wrap gap-y-4">
                        {facts.map((fact, i) => (
                            <div
                                key={fact.label}
                                className={cn(
                                    "min-w-0 pr-7",
                                    i > 0 && "border-l border-border/70 pl-7"
                                )}
                            >
                                <div className="eyebrow">{fact.label}</div>
                                <div className="mt-1 text-sm leading-snug tabular-nums text-foreground">
                                    {fact.value}
                                </div>
                            </div>
                        ))}
                    </div>
                ) : (
                    <p className="mt-3 text-[13px] text-muted-foreground">
                        No recorded data for this call.
                    </p>
                )}

                {/* Logs */}
                <div className="mt-6">
                    <div className="eyebrow">Logs</div>
                    {isLoading ? (
                        <Skeleton className="mt-2 h-24 w-full" />
                    ) : logs == null || logs.entries.length === 0 ? (
                        <p className="mt-2 text-[13px] text-muted-foreground">
                            No logs for this call.
                        </p>
                    ) : (
                        <div className="mt-2 overflow-hidden rounded-lg border border-border/70 bg-muted/30">
                            {logs.truncated && (
                                <div className="border-b border-border/50 px-4 py-1.5 text-[12px] text-muted-foreground">
                                    Showing the latest {logs.entries.length.toLocaleString()} log
                                    lines.
                                </div>
                            )}
                            <div className="max-h-96 overflow-y-auto font-mono text-xs leading-5">
                                {logs.entries.map((entry, i) => (
                                    <div
                                        key={`${entry.log_timestamp}-${i}`}
                                        className="grid grid-cols-[6.5rem,1fr] gap-3 px-4 py-1.5 [&+&]:border-t [&+&]:border-border/40"
                                    >
                                        <span className="select-none tabular-nums text-muted-foreground">
                                            {formatLogTime(entry.log_timestamp)}
                                        </span>
                                        <span
                                            className={cn(
                                                "whitespace-pre-wrap break-words",
                                                entry.is_error
                                                    ? "text-destructive"
                                                    : "text-foreground/90"
                                            )}
                                        >
                                            {entry.message}
                                        </span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}
                </div>

                {/* Utilization charts, collapsed by default */}
                <div className="mt-6">
                    {isLoading ? (
                        <>
                            <div className="eyebrow">Utilization</div>
                            <Skeleton className="mt-2 h-5 w-48" />
                        </>
                    ) : series == null || !series.has_metrics ? (
                        <>
                            <div className="eyebrow">Utilization</div>
                            <p className="mt-2 text-[13px] text-muted-foreground">
                                No samples for this call. Calls shorter than about two seconds are
                                not sampled.
                            </p>
                        </>
                    ) : (
                        <>
                            <button
                                type="button"
                                onClick={() => setIsChartsOpen((open) => !open)}
                                aria-expanded={isChartsOpen}
                                className="group flex items-center gap-2 focus-visible:outline-none"
                            >
                                <span className="eyebrow transition-colors duration-150 group-hover:text-foreground">
                                    Utilization
                                </span>
                                {!isChartsOpen && (
                                    <span className="text-[13px] text-muted-foreground">
                                        CPU, memory, I/O per second
                                    </span>
                                )}
                                <ChevronDown
                                    className={cn(
                                        "h-3.5 w-3.5 text-muted-foreground transition-transform duration-150",
                                        isChartsOpen && "rotate-180"
                                    )}
                                />
                            </button>
                            {isChartsOpen && (
                                <div className="mt-3 grid gap-x-8 gap-y-5 lg:grid-cols-2">
                                    <MetricChart
                                        title="CPU (vCPUs)"
                                        data={taskData}
                                        series={[{ key: "cpus", label: "vCPUs", color: PRIMARY }]}
                                        startAt={taskStartAt}
                                        format={(v) => v.toFixed(2)}
                                        compact
                                    />
                                    <MetricChart
                                        title="Memory"
                                        data={taskData}
                                        series={[{ key: "mem", label: "Memory", color: PRIMARY }]}
                                        startAt={taskStartAt}
                                        format={formatBytes}
                                        compact
                                    />
                                    <MetricChart
                                        title="Network I/O"
                                        data={taskData}
                                        series={[
                                            { key: "net_rx", label: "In", color: PRIMARY },
                                            { key: "net_tx", label: "Out", color: SECONDARY },
                                        ]}
                                        startAt={taskStartAt}
                                        format={formatRate}
                                        compact
                                    />
                                    <MetricChart
                                        title="Disk I/O"
                                        data={taskData}
                                        series={[
                                            { key: "disk_read", label: "Read", color: PRIMARY },
                                            { key: "disk_write", label: "Write", color: SECONDARY },
                                        ]}
                                        startAt={taskStartAt}
                                        format={formatRate}
                                        compact
                                    />
                                    {series.has_gpu && (
                                        <>
                                            <MetricChart
                                                title="GPU"
                                                data={taskData}
                                                series={[
                                                    { key: "gpu", label: "GPU", color: PRIMARY },
                                                ]}
                                                startAt={taskStartAt}
                                                format={(v) => `${Math.round(v)}%`}
                                                domainMax={100}
                                                compact
                                            />
                                            <MetricChart
                                                title="GPU memory"
                                                data={taskData}
                                                series={[
                                                    {
                                                        key: "gpu_mem",
                                                        label: "GPU memory",
                                                        color: PRIMARY,
                                                    },
                                                ]}
                                                startAt={taskStartAt}
                                                format={formatBytes}
                                                compact
                                            />
                                        </>
                                    )}
                                </div>
                            )}
                        </>
                    )}
                </div>
            </div>
        </div>
    );
};

const JobCalls = ({
    jobId,
    jobStatus,
    taskIndex,
    onSelectTask,
    onClearTask,
}: {
    jobId: string;
    jobStatus: string | null;
    taskIndex: number | null;
    onSelectTask: (index: number) => void;
    onClearTask: () => void;
}) => {
    const [taskPage, setTaskPage] = useState<TaskSummaryPage | null>(null);
    const [sort, setSort] = useState<SortKey>("duration");
    const [descending, setDescending] = useState(true);
    const [failedOnly, setFailedOnly] = useState(false);
    const [page, setPage] = useState(0);
    const [searchValue, setSearchValue] = useState("");

    const isLive = jobStatus === "RUNNING" || jobStatus === "PENDING";
    const isJobTerminal =
        jobStatus === "COMPLETED" || jobStatus === "FAILED" || jobStatus === "CANCELED";

    const searchIndex = /^\d+$/.test(searchValue) ? Number(searchValue) : null;

    const loadTaskPage = useCallback(async () => {
        const params = new URLSearchParams({
            sort,
            dir: descending ? "desc" : "asc",
            failed_only: String(failedOnly),
            offset: String(page * CALLS_PER_PAGE),
            limit: String(CALLS_PER_PAGE),
        });
        if (searchIndex != null) params.set("index", String(searchIndex));
        try {
            const res = await fetch(`/v1/jobs/${jobId}/metrics/task-summaries?${params}`);
            if (!res.ok) throw new Error();
            setTaskPage(await res.json());
        } catch {
            setTaskPage(null);
        }
    }, [jobId, sort, descending, failedOnly, page, searchIndex]);

    useEffect(() => {
        setTaskPage(null);
        setPage(0);
        setSearchValue("");
    }, [jobId]);

    useEffect(() => {
        void loadTaskPage();
        if (!isLive) return;
        const id = window.setInterval(() => void loadTaskPage(), 5000);
        return () => window.clearInterval(id);
    }, [isLive, loadTaskPage]);

    const onSort = (column: SortKey) => {
        if (sort === column) {
            setDescending((d) => !d);
        } else {
            setSort(column);
            setDescending(column !== "index");
        }
        setPage(0);
    };

    const totalTasks = taskPage?.total ?? 0;
    const totalPages = Math.max(1, Math.ceil(totalTasks / CALLS_PER_PAGE));

    if (taskIndex != null) {
        return (
            <CallDetail
                jobId={jobId}
                taskIndex={taskIndex}
                isLive={isLive}
                isJobTerminal={isJobTerminal}
                onSelectTask={onSelectTask}
                onClearTask={onClearTask}
            />
        );
    }

    return (
        <div className="rounded-xl border border-border bg-card shadow-sm">
            <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-2 px-5 py-3.5">
                <div className="flex items-baseline gap-3">
                    <span className="text-sm font-semibold text-foreground">Function calls</span>
                    <span className="text-[13px] text-muted-foreground">
                        Calls with recorded samples or logs
                    </span>
                </div>
                <div className="flex flex-wrap items-center gap-5">
                    <input
                        type="text"
                        inputMode="numeric"
                        value={searchValue}
                        onChange={(event) => {
                            setSearchValue(event.target.value.replace(/\D/g, ""));
                            setPage(0);
                        }}
                        placeholder="Filter by input index"
                        className="h-7 w-44 rounded-md border border-border bg-background px-2.5 text-[13px] tabular-nums text-foreground placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
                    />
                    <label className="flex cursor-pointer items-center gap-2 text-[13px] text-muted-foreground">
                        <Switch
                            checked={failedOnly}
                            onCheckedChange={(checked) => {
                                setFailedOnly(checked);
                                setPage(0);
                            }}
                        />
                        <span className="whitespace-nowrap">Failed only</span>
                    </label>
                </div>
            </div>

            {taskPage == null ? (
                <div className="space-y-2 border-t border-border/70 px-5 py-4">
                    {[0, 1, 2, 3].map((i) => (
                        <Skeleton key={i} className="h-6 w-full" />
                    ))}
                </div>
            ) : taskPage.tasks.length === 0 ? (
                <div className="border-t border-border/70 px-5 py-6">
                    <p className="text-sm font-medium text-foreground">
                        {failedOnly
                            ? "No failed calls"
                            : searchIndex != null
                            ? `No call with input index ${searchIndex.toLocaleString()}`
                            : "No call data"}
                    </p>
                    {!failedOnly && searchIndex == null && (
                        <p className="mt-1 text-[13px] text-muted-foreground">
                            {isLive
                                ? "Calls appear here once their first samples or logs arrive."
                                : "Calls shorter than about two seconds leave no samples, and this job logged nothing."}
                        </p>
                    )}
                </div>
            ) : (
                <>
                    <div className="w-full min-w-0 overflow-x-auto border-t border-border/70">
                        <Table className="w-full min-w-[640px]">
                            <TableHeader>
                                <TableRow className="hover:bg-transparent">
                                    <SortableHead
                                        label="Input"
                                        column="index"
                                        sort={sort}
                                        descending={descending}
                                        onSort={onSort}
                                        className="pl-5"
                                    />
                                    <SortableHead
                                        label="Duration"
                                        column="duration"
                                        sort={sort}
                                        descending={descending}
                                        onSort={onSort}
                                        align="right"
                                        className="text-right"
                                    />
                                    <SortableHead
                                        label="Attempts"
                                        column="attempts"
                                        sort={sort}
                                        descending={descending}
                                        onSort={onSort}
                                        align="right"
                                        className="text-right"
                                    />
                                    <SortableHead
                                        label="Peak CPU"
                                        column="peak_cpus"
                                        sort={sort}
                                        descending={descending}
                                        onSort={onSort}
                                        align="right"
                                        className="text-right"
                                    />
                                    <SortableHead
                                        label="Peak memory"
                                        column="peak_mem"
                                        sort={sort}
                                        descending={descending}
                                        onSort={onSort}
                                        align="right"
                                        className="text-right"
                                    />
                                    <TableHead className="w-20 pr-5" />
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {taskPage.tasks.map((row) => (
                                    <TableRow
                                        key={row.index}
                                        className="cursor-pointer"
                                        onClick={() => onSelectTask(row.index)}
                                        onKeyDown={(event) => {
                                            if (event.key !== "Enter" && event.key !== " ") return;
                                            event.preventDefault();
                                            onSelectTask(row.index);
                                        }}
                                        tabIndex={0}
                                    >
                                        <TableCell className="pl-5 text-[13px] font-medium tabular-nums text-foreground">
                                            {row.index.toLocaleString()}
                                        </TableCell>
                                        <TableCell className="text-right text-[13px] tabular-nums text-foreground">
                                            {row.duration_sec != null ? (
                                                formatDuration(row.duration_sec)
                                            ) : (
                                                <span className="text-muted-foreground">
                                                    No samples
                                                </span>
                                            )}
                                        </TableCell>
                                        <TableCell className="text-right text-[13px] tabular-nums text-muted-foreground">
                                            {row.attempts != null ? row.attempts : ""}
                                        </TableCell>
                                        <TableCell className="text-right text-[13px] tabular-nums text-muted-foreground">
                                            {row.peak_cpus != null
                                                ? `${row.peak_cpus.toFixed(2)} vCPU`
                                                : ""}
                                        </TableCell>
                                        <TableCell className="text-right text-[13px] tabular-nums text-muted-foreground">
                                            {row.peak_mem_bytes != null
                                                ? formatBytes(row.peak_mem_bytes)
                                                : ""}
                                        </TableCell>
                                        <TableCell className="w-20 pr-5 text-right">
                                            {row.failed && (
                                                <span className="text-[13px] font-medium text-destructive">
                                                    Failed
                                                </span>
                                            )}
                                        </TableCell>
                                    </TableRow>
                                ))}
                            </TableBody>
                        </Table>
                    </div>
                    <div className="px-5 pb-4">
                        <TablePagination
                            page={page}
                            totalPages={totalPages}
                            onPageChange={setPage}
                            resultsLabel={`${totalTasks.toLocaleString()} ${
                                totalTasks === 1 ? "call" : "calls"
                            }`}
                        />
                    </div>
                </>
            )}
        </div>
    );
};

export default JobCalls;
