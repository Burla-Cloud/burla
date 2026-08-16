import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ArrowDown, ArrowUp, ChevronDown, ChevronLeft, ChevronRight, X } from "lucide-react";
import {
    Area,
    AreaChart,
    CartesianGrid,
    ResponsiveContainer,
    Tooltip,
    XAxis,
    YAxis,
} from "recharts";
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
import { cn } from "@/lib/utils";

type JobPoint = {
    t: number;
    nodes: number;
    cpu: number;
    mem: number;
    net_rx: number;
    net_tx: number;
    disk_read: number;
    disk_write: number;
    gpu: number | null;
    gpu_mem: number | null;
};

type JobSeries = {
    has_metrics: boolean;
    has_gpu: boolean;
    bucket_sec: number;
    points: JobPoint[];
};

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

type SortKey = "index" | "duration" | "attempts" | "peak_cpus" | "peak_mem";

const TASKS_PER_PAGE = 50;

const PRIMARY = "hsl(var(--primary))";
const SECONDARY = "hsl(215 14% 55%)";

// Non-breaking space so axis tick labels stay on one line.
const formatBytes = (value: number): string => {
    if (value < 1024) return `${Math.round(value)}\u00A0B`;
    const units = ["KB", "MB", "GB", "TB"];
    let v = value / 1024;
    let unit = 0;
    while (v >= 1024 && unit < units.length - 1) {
        v /= 1024;
        unit += 1;
    }
    return `${v >= 10 ? Math.round(v) : v.toFixed(1)}\u00A0${units[unit]}`;
};

const formatRate = (value: number) => `${formatBytes(value)}/s`;

const formatElapsed = (seconds: number): string => {
    if (seconds < 60) return `${Math.round(seconds)}s`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m`;
    const hours = Math.floor(minutes / 60);
    return `${hours}h ${minutes % 60}m`;
};

const formatDuration = (seconds: number): string => {
    const s = Math.max(0, Math.round(seconds));
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m ${s % 60}s`;
    const h = Math.floor(m / 60);
    return `${h}h ${m % 60}m`;
};

const formatClock = (epochSec: number) =>
    new Date(epochSec * 1000).toLocaleTimeString("en-US", {
        hour: "numeric",
        minute: "2-digit",
        second: "2-digit",
        hour12: true,
    });

type SeriesSpec = {
    key: string;
    label: string;
    color: string;
};

const MetricChart = ({
    title,
    data,
    series,
    startAt,
    format,
    domainMax,
    integerTicks,
}: {
    title: string;
    data: Record<string, number | null>[];
    series: SeriesSpec[];
    startAt: number;
    format: (v: number) => string;
    domainMax?: number;
    integerTicks?: boolean;
}) => (
    <div className="min-w-0">
        <div className="flex items-center justify-between">
            <span className="eyebrow">{title}</span>
            {series.length > 1 && (
                <span className="flex items-center gap-3 text-[11px] text-muted-foreground">
                    {series.map((s) => (
                        <span key={s.key} className="inline-flex items-center gap-1.5">
                            <span
                                className="h-1.5 w-1.5 rounded-full"
                                style={{ backgroundColor: s.color }}
                            />
                            {s.label}
                        </span>
                    ))}
                </span>
            )}
        </div>
        <div className="mt-2 h-56">
            <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={data} margin={{ top: 8, right: 4, left: 0, bottom: 0 }}>
                    <CartesianGrid stroke="hsl(var(--border) / 0.5)" vertical={false} />
                    <XAxis
                        dataKey="t"
                        type="number"
                        domain={["dataMin", "dataMax"]}
                        tickLine={false}
                        axisLine={false}
                        tick={{ fill: "hsl(var(--muted-foreground))", fontSize: 11 }}
                        tickFormatter={(t) => formatElapsed(Number(t) - startAt)}
                        minTickGap={40}
                    />
                    <YAxis
                        width={64}
                        allowDecimals={!integerTicks}
                        domain={[0, domainMax ?? "auto"]}
                        tickLine={false}
                        axisLine={false}
                        tick={{ fill: "hsl(var(--muted-foreground))", fontSize: 11 }}
                        tickFormatter={(v) => format(Number(v))}
                    />
                    <Tooltip
                        cursor={{ stroke: "hsl(var(--border))" }}
                        content={({ active, payload, label }) => {
                            if (!active || !payload?.length) return null;
                            return (
                                <div className="rounded-lg border border-border bg-popover px-3 py-2 shadow-md">
                                    <div className="text-xs font-medium text-muted-foreground">
                                        {formatClock(Number(label))} (+
                                        {formatElapsed(Number(label) - startAt)})
                                    </div>
                                    {payload.map((entry) => (
                                        <div
                                            key={String(entry.dataKey)}
                                            className="mt-0.5 flex items-center gap-2 text-sm tabular-nums text-foreground"
                                        >
                                            <span
                                                className="h-1.5 w-1.5 rounded-full"
                                                style={{ backgroundColor: entry.color }}
                                            />
                                            {series.length > 1 && (
                                                <span className="text-xs text-muted-foreground">
                                                    {
                                                        series.find(
                                                            (s) => s.key === entry.dataKey
                                                        )?.label
                                                    }
                                                </span>
                                            )}
                                            <span className="font-medium">
                                                {format(Number(entry.value))}
                                            </span>
                                        </div>
                                    ))}
                                </div>
                            );
                        }}
                    />
                    {series.map((s) => (
                        <Area
                            key={s.key}
                            dataKey={s.key}
                            type="monotone"
                            stroke={s.color}
                            strokeWidth={1.5}
                            fill={s.color}
                            fillOpacity={0.08}
                            isAnimationActive={false}
                            connectNulls
                        />
                    ))}
                </AreaChart>
            </ResponsiveContainer>
        </div>
    </div>
);

const ChartSkeletons = () => (
    <div className="space-y-7">
        {[0, 1, 2].map((i) => (
            <div key={i}>
                <Skeleton className="h-3 w-24" />
                <Skeleton className="mt-2 h-56 w-full" />
            </div>
        ))}
    </div>
);

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

const JobUtilization = ({
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
    const [jobSeries, setJobSeries] = useState<JobSeries | null>(null);
    const [jobLoadFailed, setJobLoadFailed] = useState(false);
    const [task, setTask] = useState<TaskSeries | null>(null);
    const [isTaskLoading, setIsTaskLoading] = useState(false);
    // Arriving with a task selected (deep link) keeps the focus on that task:
    // the job-level section starts collapsed.
    const [isAggregateOpen, setIsAggregateOpen] = useState(taskIndex == null);

    const [taskPage, setTaskPage] = useState<TaskSummaryPage | null>(null);
    const [sort, setSort] = useState<SortKey>("duration");
    const [descending, setDescending] = useState(true);
    const [failedOnly, setFailedOnly] = useState(false);
    const [page, setPage] = useState(0);
    const [searchValue, setSearchValue] = useState("");

    const detailRef = useRef<HTMLDivElement | null>(null);
    const previousTaskIndexRef = useRef(taskIndex);

    const isLive = jobStatus === "RUNNING" || jobStatus === "PENDING";

    const loadJobSeries = useCallback(async () => {
        try {
            const res = await fetch(`/v1/jobs/${jobId}/metrics`);
            if (!res.ok) throw new Error();
            setJobSeries(await res.json());
            setJobLoadFailed(false);
        } catch {
            setJobLoadFailed(true);
        }
    }, [jobId]);

    const searchIndex = /^\d+$/.test(searchValue) ? Number(searchValue) : null;

    const loadTaskPage = useCallback(async () => {
        const params = new URLSearchParams({
            sort,
            dir: descending ? "desc" : "asc",
            failed_only: String(failedOnly),
            offset: String(page * TASKS_PER_PAGE),
            limit: String(TASKS_PER_PAGE),
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
        setJobSeries(null);
        setJobLoadFailed(false);
        setTask(null);
        setTaskPage(null);
        setPage(0);
        setSearchValue("");
    }, [jobId]);

    useEffect(() => {
        void loadJobSeries();
        void loadTaskPage();
        if (!isLive) return;
        const id = window.setInterval(() => {
            void loadJobSeries();
            void loadTaskPage();
        }, 5000);
        return () => window.clearInterval(id);
    }, [isLive, loadJobSeries, loadTaskPage]);

    useEffect(() => {
        if (taskIndex == null) {
            setTask(null);
            return;
        }
        let cancelled = false;
        setIsTaskLoading(true);
        (async () => {
            try {
                const res = await fetch(`/v1/jobs/${jobId}/metrics/tasks/${taskIndex}`);
                if (!res.ok) throw new Error();
                const series: TaskSeries = await res.json();
                if (!cancelled) setTask(series);
            } catch {
                if (!cancelled) setTask(null);
            } finally {
                if (!cancelled) setIsTaskLoading(false);
            }
        })();
        return () => {
            cancelled = true;
        };
    }, [jobId, taskIndex]);

    // Picking a task (table row click) collapses the aggregate so the task's
    // charts are immediately visible; stepping between tasks leaves it alone.
    useEffect(() => {
        if (taskIndex != null && previousTaskIndexRef.current == null) {
            setIsAggregateOpen(false);
        }
        if (taskIndex != null && taskIndex !== previousTaskIndexRef.current) {
            detailRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
        }
        previousTaskIndexRef.current = taskIndex;
    }, [taskIndex]);

    const onSort = (column: SortKey) => {
        if (sort === column) {
            setDescending((d) => !d);
        } else {
            setSort(column);
            setDescending(column !== "index");
        }
        setPage(0);
    };

    const jobData = useMemo(() => jobSeries?.points ?? [], [jobSeries]);
    const jobStartAt = jobData.length ? jobData[0].t : 0;
    const taskData = useMemo(() => task?.points ?? [], [task]);
    const taskStartAt = taskData.length ? taskData[0].t : 0;

    const taskDurationSec = taskData.length
        ? taskData[taskData.length - 1].t - taskData[0].t + (task?.bucket_sec ?? 0)
        : 0;
    const taskPeakVcpus = taskData.length ? Math.max(...taskData.map((p) => p.cpus)) : 0;

    const totalTasks = taskPage?.total ?? 0;
    const totalPages = Math.max(1, Math.ceil(totalTasks / TASKS_PER_PAGE));

    const jobCharts = jobSeries?.has_metrics ? (
        <div className="space-y-7">
            <MetricChart
                title="Nodes"
                data={jobData}
                series={[{ key: "nodes", label: "Nodes", color: PRIMARY }]}
                startAt={jobStartAt}
                format={(v) => String(Math.round(v))}
                integerTicks
            />
            <MetricChart
                title="CPU"
                data={jobData}
                series={[{ key: "cpu", label: "CPU", color: PRIMARY }]}
                startAt={jobStartAt}
                format={(v) => `${Math.round(v)}%`}
                domainMax={100}
            />
            <MetricChart
                title="Memory"
                data={jobData}
                series={[{ key: "mem", label: "Memory", color: PRIMARY }]}
                startAt={jobStartAt}
                format={(v) => `${Math.round(v)}%`}
                domainMax={100}
            />
            <MetricChart
                title="Network I/O"
                data={jobData}
                series={[
                    { key: "net_rx", label: "In", color: PRIMARY },
                    { key: "net_tx", label: "Out", color: SECONDARY },
                ]}
                startAt={jobStartAt}
                format={formatRate}
            />
            <MetricChart
                title="Disk I/O"
                data={jobData}
                series={[
                    { key: "disk_read", label: "Read", color: PRIMARY },
                    { key: "disk_write", label: "Write", color: SECONDARY },
                ]}
                startAt={jobStartAt}
                format={formatRate}
            />
            {jobSeries.has_gpu && (
                <>
                    <MetricChart
                        title="GPU"
                        data={jobData}
                        series={[{ key: "gpu", label: "GPU", color: PRIMARY }]}
                        startAt={jobStartAt}
                        format={(v) => `${Math.round(v)}%`}
                        domainMax={100}
                    />
                    <MetricChart
                        title="GPU memory"
                        data={jobData}
                        series={[{ key: "gpu_mem", label: "GPU memory", color: PRIMARY }]}
                        startAt={jobStartAt}
                        format={(v) => `${Math.round(v)}%`}
                        domainMax={100}
                    />
                </>
            )}
        </div>
    ) : null;

    return (
        <div className="space-y-4 pb-2">
            {/* Job-level aggregate */}
            <div className="rounded-xl border border-border bg-card shadow-sm">
                <button
                    type="button"
                    onClick={() => setIsAggregateOpen((open) => !open)}
                    aria-expanded={isAggregateOpen}
                    className="flex w-full items-center justify-between px-5 py-3.5 text-left"
                >
                    <span className="text-sm font-semibold text-foreground">All nodes</span>
                    <span className="flex items-center gap-2 text-[13px] text-muted-foreground">
                        {!isAggregateOpen && <span>Nodes, CPU, memory, I/O for the whole job</span>}
                        <ChevronDown
                            className={`h-4 w-4 transition-transform duration-150 ${
                                isAggregateOpen ? "rotate-180" : ""
                            }`}
                        />
                    </span>
                </button>

                {isAggregateOpen && (
                    <div className="border-t border-border/70">
                        {jobLoadFailed ? (
                            <div className="px-5 py-6 text-sm text-muted-foreground">
                                Failed to load utilization data.{" "}
                                <button
                                    onClick={() => void loadJobSeries()}
                                    className="font-medium text-primary hover:underline"
                                >
                                    Retry
                                </button>
                            </div>
                        ) : jobSeries == null ? (
                            <div className="px-5 py-5">
                                <ChartSkeletons />
                            </div>
                        ) : !jobSeries.has_metrics ? (
                            <div className="px-5 py-6">
                                <p className="text-sm font-medium text-foreground">
                                    No utilization data
                                </p>
                                <p className="mt-1 text-[13px] text-muted-foreground">
                                    {isLive
                                        ? "Samples appear a few seconds after work starts."
                                        : "This job ran before per-second resource metrics were collected."}
                                </p>
                            </div>
                        ) : (
                            <div className="px-5 py-5">{jobCharts}</div>
                        )}
                    </div>
                )}
            </div>

            {/* Per-task drill-down */}
            {taskIndex != null && (
                <div
                    ref={detailRef}
                    className="scroll-mt-4 rounded-xl border border-border bg-card shadow-sm"
                >
                    <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-2 border-b border-border/70 px-5 py-3">
                        <div className="flex items-center gap-3">
                            <span className="text-sm font-semibold tabular-nums text-foreground">
                                Input {taskIndex.toLocaleString()}
                            </span>
                            <div className="flex items-center gap-1">
                                <button
                                    type="button"
                                    onClick={() =>
                                        task?.prev_index != null && onSelectTask(task.prev_index)
                                    }
                                    disabled={task?.prev_index == null}
                                    className={iconBtnClass(task?.prev_index == null)}
                                    aria-label="Previous input with samples"
                                    title="Previous input with samples"
                                >
                                    <ChevronLeft className="h-3.5 w-3.5" />
                                </button>
                                <button
                                    type="button"
                                    onClick={() =>
                                        task?.next_index != null && onSelectTask(task.next_index)
                                    }
                                    disabled={task?.next_index == null}
                                    className={iconBtnClass(task?.next_index == null)}
                                    aria-label="Next input with samples"
                                    title="Next input with samples"
                                >
                                    <ChevronRight className="h-3.5 w-3.5" />
                                </button>
                            </div>
                        </div>
                        <div className="flex items-center gap-4">
                            {task != null && task.has_metrics && !isTaskLoading && (
                                <span className="text-[13px] tabular-nums text-muted-foreground">
                                    {formatDuration(taskDurationSec)}
                                    <span className="mx-1.5">·</span>
                                    {task.n_attempts}{" "}
                                    {task.n_attempts === 1 ? "attempt" : "attempts"}
                                    <span className="mx-1.5">·</span>
                                    peak {taskPeakVcpus.toFixed(2)} vCPU
                                </span>
                            )}
                            <button
                                type="button"
                                onClick={onClearTask}
                                className="flex h-6 w-6 items-center justify-center rounded-md text-muted-foreground transition-colors duration-150 hover:bg-muted/60 hover:text-foreground"
                                aria-label="Close task detail"
                                title="Close"
                            >
                                <X className="h-3.5 w-3.5" />
                            </button>
                        </div>
                    </div>

                    <div className="px-5 py-5">
                        {isTaskLoading || task == null ? (
                            <ChartSkeletons />
                        ) : !task.has_metrics ? (
                            <p className="text-[13px] text-muted-foreground">
                                No samples for input {taskIndex}. Tasks shorter than about two
                                seconds are not sampled.
                            </p>
                        ) : (
                            <div className="space-y-7">
                                <MetricChart
                                    title="CPU (vCPUs)"
                                    data={taskData}
                                    series={[{ key: "cpus", label: "vCPUs", color: PRIMARY }]}
                                    startAt={taskStartAt}
                                    format={(v) => v.toFixed(2)}
                                />
                                <MetricChart
                                    title="Memory"
                                    data={taskData}
                                    series={[{ key: "mem", label: "Memory", color: PRIMARY }]}
                                    startAt={taskStartAt}
                                    format={formatBytes}
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
                                />
                                {task.has_gpu && (
                                    <>
                                        <MetricChart
                                            title="GPU"
                                            data={taskData}
                                            series={[{ key: "gpu", label: "GPU", color: PRIMARY }]}
                                            startAt={taskStartAt}
                                            format={(v) => `${Math.round(v)}%`}
                                            domainMax={100}
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
                                        />
                                    </>
                                )}
                            </div>
                        )}
                    </div>
                </div>
            )}

            {/* Task table */}
            <div className="rounded-xl border border-border bg-card shadow-sm">
                <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-2 px-5 py-3.5">
                    <span className="text-sm font-semibold text-foreground">Tasks</span>
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
                                ? "No failed tasks"
                                : searchIndex != null
                                ? `No task with input index ${searchIndex.toLocaleString()}`
                                : "No sampled tasks"}
                        </p>
                        {!failedOnly && searchIndex == null && (
                            <p className="mt-1 text-[13px] text-muted-foreground">
                                {isLive
                                    ? "Tasks appear here once their first samples arrive."
                                    : "Tasks shorter than about two seconds are not sampled."}
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
                                            className={cn(
                                                "cursor-pointer",
                                                taskIndex === row.index && "bg-muted/50"
                                            )}
                                            onClick={() => onSelectTask(row.index)}
                                            onKeyDown={(event) => {
                                                if (event.key !== "Enter" && event.key !== " ")
                                                    return;
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
                                    totalTasks === 1 ? "task" : "tasks"
                                }`}
                            />
                        </div>
                    </>
                )}
            </div>
        </div>
    );
};

export default JobUtilization;
