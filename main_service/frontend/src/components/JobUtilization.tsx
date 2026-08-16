import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { ChevronDown, ChevronLeft, ChevronRight } from "lucide-react";
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

const PRIMARY = "hsl(var(--primary))";
const SECONDARY = "hsl(215 14% 55%)";

const formatBytes = (value: number): string => {
    if (value < 1024) return `${Math.round(value)} B`;
    const units = ["KB", "MB", "GB", "TB"];
    let v = value / 1024;
    let unit = 0;
    while (v >= 1024 && unit < units.length - 1) {
        v /= 1024;
        unit += 1;
    }
    return `${v >= 10 ? Math.round(v) : v.toFixed(1)} ${units[unit]}`;
};

const formatRate = (value: number) => `${formatBytes(value)}/s`;

const formatElapsed = (seconds: number): string => {
    if (seconds < 60) return `${Math.round(seconds)}s`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m`;
    const hours = Math.floor(minutes / 60);
    return `${hours}h ${minutes % 60}m`;
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
}: {
    title: string;
    data: Record<string, number | null>[];
    series: SeriesSpec[];
    startAt: number;
    format: (v: number) => string;
    domainMax?: number;
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
        <div className="mt-2 h-32">
            <ResponsiveContainer width="100%" height="100%">
                <AreaChart data={data} margin={{ top: 4, right: 4, left: 0, bottom: 0 }}>
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
                        width={44}
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
    <div className="grid grid-cols-1 gap-x-8 gap-y-6 sm:grid-cols-2 lg:grid-cols-3">
        {[0, 1, 2].map((i) => (
            <div key={i}>
                <Skeleton className="h-3 w-20" />
                <Skeleton className="mt-2 h-32 w-full" />
            </div>
        ))}
    </div>
);

const iconBtnClass = (disabled: boolean) =>
    disabled
        ? "flex h-7 w-7 items-center justify-center rounded-md border border-border bg-card text-muted-foreground opacity-50 cursor-default"
        : "flex h-7 w-7 items-center justify-center rounded-md border border-border bg-card text-muted-foreground shadow-sm transition-colors duration-150 hover:bg-muted/60 hover:text-foreground";

const JobUtilization = ({ jobId, jobStatus }: { jobId: string; jobStatus: string | null }) => {
    const [isOpen, setIsOpen] = useState(false);
    const [job, setJob] = useState<JobSeries | null>(null);
    const [jobLoadFailed, setJobLoadFailed] = useState(false);
    const [task, setTask] = useState<TaskSeries | null>(null);
    const [isTaskLoading, setIsTaskLoading] = useState(false);
    const [taskIndex, setTaskIndex] = useState(0);
    const [indexInputValue, setIndexInputValue] = useState("0");
    const hasAutoJumpedRef = useRef(false);

    const isLive = jobStatus === "RUNNING" || jobStatus === "PENDING";

    const loadJobSeries = useCallback(async () => {
        try {
            const res = await fetch(`/v1/jobs/${jobId}/metrics`);
            if (!res.ok) throw new Error();
            setJob(await res.json());
            setJobLoadFailed(false);
        } catch {
            setJobLoadFailed(true);
        }
    }, [jobId]);

    const loadTaskSeries = useCallback(
        async (index: number) => {
            setIsTaskLoading(true);
            try {
                const res = await fetch(`/v1/jobs/${jobId}/metrics/tasks/${index}`);
                if (!res.ok) throw new Error();
                const series: TaskSeries = await res.json();
                // First open lands on input 0, which may have no samples;
                // jump forward once to the first input that does.
                if (
                    !hasAutoJumpedRef.current &&
                    !series.has_metrics &&
                    series.next_index != null
                ) {
                    hasAutoJumpedRef.current = true;
                    setTaskIndex(series.next_index);
                    return;
                }
                hasAutoJumpedRef.current = true;
                setTask(series);
            } catch {
                setTask(null);
            } finally {
                setIsTaskLoading(false);
            }
        },
        [jobId]
    );

    useEffect(() => {
        setIsOpen(false);
        setJob(null);
        setTask(null);
        setTaskIndex(0);
        setIndexInputValue("0");
        hasAutoJumpedRef.current = false;
    }, [jobId]);

    useEffect(() => {
        if (!isOpen) return;
        void loadJobSeries();
        if (!isLive) return;
        const id = window.setInterval(() => void loadJobSeries(), 5000);
        return () => window.clearInterval(id);
    }, [isOpen, isLive, loadJobSeries]);

    useEffect(() => {
        if (!isOpen) return;
        void loadTaskSeries(taskIndex);
    }, [isOpen, taskIndex, loadTaskSeries]);

    useEffect(() => {
        setIndexInputValue(String(taskIndex));
    }, [taskIndex]);

    const jobData = useMemo(() => job?.points ?? [], [job]);
    const jobStartAt = jobData.length ? jobData[0].t : 0;
    const taskData = useMemo(() => task?.points ?? [], [task]);
    const taskStartAt = taskData.length ? taskData[0].t : 0;

    const goToTypedIndex = () => {
        const parsed = Number(indexInputValue.trim());
        if (!Number.isInteger(parsed) || parsed < 0) {
            setIndexInputValue(String(taskIndex));
            return;
        }
        setTaskIndex(parsed);
    };

    return (
        <div className="mb-4 rounded-xl border border-border bg-card shadow-sm">
            <button
                type="button"
                onClick={() => setIsOpen((open) => !open)}
                aria-expanded={isOpen}
                className="flex w-full items-center justify-between px-5 py-3.5 text-left"
            >
                <span className="text-sm font-semibold text-foreground">Utilization</span>
                <span className="flex items-center gap-2 text-[13px] text-muted-foreground">
                    {!isOpen && <span>CPU, memory, I/O per node and task</span>}
                    <ChevronDown
                        className={`h-4 w-4 transition-transform duration-150 ${
                            isOpen ? "rotate-180" : ""
                        }`}
                    />
                </span>
            </button>

            {isOpen && (
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
                    ) : job == null ? (
                        <div className="px-5 py-5">
                            <ChartSkeletons />
                        </div>
                    ) : !job.has_metrics ? (
                        <div className="px-5 py-6">
                            <p className="text-sm font-medium text-foreground">
                                No utilization data
                            </p>
                            <p className="mt-1 text-[13px] text-muted-foreground">
                                This job ran before per-second resource metrics were
                                collected.
                            </p>
                        </div>
                    ) : (
                        <>
                            <div className="px-5 py-5">
                                <div className="grid grid-cols-1 gap-x-8 gap-y-6 sm:grid-cols-2 lg:grid-cols-3">
                                    <MetricChart
                                        title="Nodes"
                                        data={jobData}
                                        series={[{ key: "nodes", label: "Nodes", color: PRIMARY }]}
                                        startAt={jobStartAt}
                                        format={(v) => String(Math.round(v))}
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
                                    {job.has_gpu && (
                                        <>
                                            <MetricChart
                                                title="GPU"
                                                data={jobData}
                                                series={[
                                                    { key: "gpu", label: "GPU", color: PRIMARY },
                                                ]}
                                                startAt={jobStartAt}
                                                format={(v) => `${Math.round(v)}%`}
                                                domainMax={100}
                                            />
                                            <MetricChart
                                                title="GPU memory"
                                                data={jobData}
                                                series={[
                                                    {
                                                        key: "gpu_mem",
                                                        label: "GPU memory",
                                                        color: PRIMARY,
                                                    },
                                                ]}
                                                startAt={jobStartAt}
                                                format={(v) => `${Math.round(v)}%`}
                                                domainMax={100}
                                            />
                                        </>
                                    )}
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
                                </div>
                            </div>

                            {/* Task drill-down */}
                            <div className="border-t border-border/70">
                                <div className="flex items-center gap-3 bg-muted/40 px-5 py-2.5">
                                    <span className="text-[13px] font-medium text-foreground">
                                        Task drill-down
                                    </span>
                                    <div className="h-4 w-px bg-border" aria-hidden="true" />
                                    <label className="flex items-center gap-1.5 whitespace-nowrap text-[13px] tabular-nums text-muted-foreground">
                                        <span>Input</span>
                                        <input
                                            type="number"
                                            min={0}
                                            value={indexInputValue}
                                            onChange={(e) => setIndexInputValue(e.target.value)}
                                            onBlur={goToTypedIndex}
                                            onKeyDown={(e) => {
                                                if (e.key !== "Enter") return;
                                                e.preventDefault();
                                                goToTypedIndex();
                                            }}
                                            className="h-7 w-24 rounded-md border border-input bg-card px-1.5 text-center text-[13px] tabular-nums text-foreground shadow-sm [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none focus-visible:border-primary/70 focus-visible:outline-none focus-visible:ring-[3px] focus-visible:ring-primary/15"
                                            aria-label="Task input index"
                                        />
                                    </label>
                                    <div className="flex items-center gap-1">
                                        <button
                                            type="button"
                                            onClick={() =>
                                                task?.prev_index != null &&
                                                setTaskIndex(task.prev_index)
                                            }
                                            disabled={task?.prev_index == null}
                                            className={iconBtnClass(task?.prev_index == null)}
                                            aria-label="Previous task with samples"
                                            title="Previous task with samples"
                                        >
                                            <ChevronLeft className="h-3.5 w-3.5" />
                                        </button>
                                        <button
                                            type="button"
                                            onClick={() =>
                                                task?.next_index != null &&
                                                setTaskIndex(task.next_index)
                                            }
                                            disabled={task?.next_index == null}
                                            className={iconBtnClass(task?.next_index == null)}
                                            aria-label="Next task with samples"
                                            title="Next task with samples"
                                        >
                                            <ChevronRight className="h-3.5 w-3.5" />
                                        </button>
                                    </div>
                                    {task != null && task.n_attempts > 1 && (
                                        <span className="text-[13px] text-muted-foreground">
                                            {task.n_attempts} attempts
                                        </span>
                                    )}
                                </div>

                                <div className="px-5 py-5">
                                    {isTaskLoading || task == null ? (
                                        <ChartSkeletons />
                                    ) : !task.has_metrics ? (
                                        <p className="text-[13px] text-muted-foreground">
                                            No samples for input {taskIndex}. Tasks shorter
                                            than about two seconds are not sampled.
                                        </p>
                                    ) : (
                                        <div className="grid grid-cols-1 gap-x-8 gap-y-6 sm:grid-cols-2 lg:grid-cols-3">
                                            <MetricChart
                                                title="CPU (vCPUs)"
                                                data={taskData}
                                                series={[
                                                    { key: "cpus", label: "vCPUs", color: PRIMARY },
                                                ]}
                                                startAt={taskStartAt}
                                                format={(v) => v.toFixed(2)}
                                            />
                                            <MetricChart
                                                title="Memory"
                                                data={taskData}
                                                series={[
                                                    { key: "mem", label: "Memory", color: PRIMARY },
                                                ]}
                                                startAt={taskStartAt}
                                                format={formatBytes}
                                            />
                                            {task.has_gpu && (
                                                <>
                                                    <MetricChart
                                                        title="GPU"
                                                        data={taskData}
                                                        series={[
                                                            {
                                                                key: "gpu",
                                                                label: "GPU",
                                                                color: PRIMARY,
                                                            },
                                                        ]}
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
                                            <MetricChart
                                                title="Network I/O"
                                                data={taskData}
                                                series={[
                                                    { key: "net_rx", label: "In", color: PRIMARY },
                                                    {
                                                        key: "net_tx",
                                                        label: "Out",
                                                        color: SECONDARY,
                                                    },
                                                ]}
                                                startAt={taskStartAt}
                                                format={formatRate}
                                            />
                                            <MetricChart
                                                title="Disk I/O"
                                                data={taskData}
                                                series={[
                                                    {
                                                        key: "disk_read",
                                                        label: "Read",
                                                        color: PRIMARY,
                                                    },
                                                    {
                                                        key: "disk_write",
                                                        label: "Write",
                                                        color: SECONDARY,
                                                    },
                                                ]}
                                                startAt={taskStartAt}
                                                format={formatRate}
                                            />
                                        </div>
                                    )}
                                </div>
                            </div>
                        </>
                    )}
                </div>
            )}
        </div>
    );
};

export default JobUtilization;
