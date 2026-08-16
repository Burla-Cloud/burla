import { Link, useParams, useSearchParams } from "react-router-dom";
import { useEffect, useRef, useState } from "react";
import { useJobs } from "@/contexts/JobsContext";
import { BurlaJob, JobsStatus } from "@/types/coreTypes";
import JobLogs from "@/components/JobLogs";
import JobUtilization from "@/components/JobUtilization";
import { Button } from "@/components/ui/button";
import { Check, ChevronRight, Copy, PowerOff } from "lucide-react";
import { useToast } from "@/components/ui/use-toast";
import { StatusBadge, jobStatusBadge } from "@/components/StatusBadge";
import { cn } from "@/lib/utils";

type JobResultStats = {
    n_inputs: number;
    n_results: number;
    n_failed: number;
};

type JobDoc = {
    image?: string | null;
    max_parallelism?: number | null;
    func_cpu?: number | string | null;
    func_ram?: number | string | null;
    func_gpu?: string | null;
};

const formatDuration = (seconds: number): string => {
    const s = Math.max(0, Math.round(seconds));
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    if (m < 60) return `${m}m ${s % 60}s`;
    const h = Math.floor(m / 60);
    return `${h}h ${m % 60}m`;
};

const Unknown = () => <span className="text-muted-foreground">unknown</span>;

const CopyButton = ({ value, label }: { value: string; label: string }) => {
    const [copied, setCopied] = useState(false);
    return (
        <button
            type="button"
            onClick={() => {
                void navigator.clipboard?.writeText(value);
                setCopied(true);
                window.setTimeout(() => setCopied(false), 1500);
            }}
            className="flex h-6 w-6 shrink-0 items-center justify-center rounded-md text-muted-foreground transition-colors duration-150 hover:bg-muted/60 hover:text-foreground"
            aria-label={label}
            title={copied ? "Copied" : "Copy"}
        >
            {copied ? (
                <Check className="h-3.5 w-3.5 text-emerald-600 dark:text-emerald-400" />
            ) : (
                <Copy className="h-3.5 w-3.5" />
            )}
        </button>
    );
};

const tabClass = (active: boolean) =>
    cn(
        "relative -mb-px border-b-2 px-1 pb-2.5 text-sm font-medium transition-colors duration-150 focus-visible:outline-none",
        active
            ? "border-primary text-foreground"
            : "border-transparent text-muted-foreground hover:text-foreground"
    );

const JobDetails = () => {
    const jobId = useParams<{ jobId: string }>().jobId!;
    const { jobs } = useJobs();
    const { toast } = useToast();
    const [searchParams, setSearchParams] = useSearchParams();
    const [isStopping, setIsStopping] = useState(false);
    const [stats, setStats] = useState<JobResultStats | null>(null);
    const [isStatsLoading, setIsStatsLoading] = useState(true);
    const [statsLoadError, setStatsLoadError] = useState(false);
    const [jobDoc, setJobDoc] = useState<JobDoc | null>(null);
    const [isImageOpen, setIsImageOpen] = useState(false);
    // Deep links: jobs outside the SSE-streamed first page never appear in
    // the jobs context, so the page falls back to the job summary that
    // result-stats returns.
    const [fetchedJob, setFetchedJob] = useState<BurlaJob | null>(null);
    const hasCompletedInitialStatsLoadRef = useRef(false);
    const [userTimeZone, setUserTimeZone] = useState<string>(() => {
        const stored = typeof window !== "undefined" ? localStorage.getItem("userTimezone") : null;
        if (stored) return stored;
        const cookieTz =
            typeof document !== "undefined"
                ? document.cookie
                      .split("; ")
                      .find((row) => row.startsWith("timezone="))
                      ?.split("=")[1]
                : null;
        return cookieTz || Intl.DateTimeFormat().resolvedOptions().timeZone;
    });

    const activeTab = searchParams.get("tab") === "utilization" ? "utilization" : "overview";
    const taskParam = searchParams.get("task");
    const selectedTaskIndex =
        taskParam !== null && /^\d+$/.test(taskParam) ? Number(taskParam) : null;

    const openTab = (tab: "overview" | "utilization") => {
        const sp = new URLSearchParams(searchParams);
        if (tab === "utilization") sp.set("tab", "utilization");
        else sp.delete("tab");
        setSearchParams(sp);
    };

    const openTaskUtilization = (index: number) => {
        const sp = new URLSearchParams(searchParams);
        sp.set("tab", "utilization");
        sp.set("task", String(index));
        setSearchParams(sp);
    };

    // Stepping between tasks replaces the history entry so the back button
    // returns to the logs, not through every visited task.
    const stepToTask = (index: number) => {
        const sp = new URLSearchParams(searchParams);
        sp.set("task", String(index));
        setSearchParams(sp, { replace: true });
    };

    useEffect(() => {
        let cancelled = false;
        const loadTimezone = async () => {
            try {
                const res = await fetch("/api/user");
                if (res.ok) {
                    const data = await res.json();
                    const tz = data?.timezone || data?.time_zone || data?.tz || null;
                    if (tz && !cancelled) {
                        setUserTimeZone(tz);
                        try {
                            localStorage.setItem("userTimezone", tz);
                        } catch {}
                        return;
                    }
                }
            } catch {}
            if (!cancelled) {
                const cookieTz = document.cookie
                    .split("; ")
                    .find((row) => row.startsWith("timezone="))
                    ?.split("=")[1];
                setUserTimeZone(cookieTz || Intl.DateTimeFormat().resolvedOptions().timeZone);
            }
        };
        loadTimezone();
        return () => {
            cancelled = true;
        };
    }, []);

    const formatDateTime = (date?: Date): React.ReactNode => {
        if (!date) return <Unknown />;
        const tz = userTimeZone || Intl.DateTimeFormat().resolvedOptions().timeZone;
        const monthDay = date.toLocaleDateString("en-US", {
            timeZone: tz,
            month: "short",
            day: "numeric",
        });
        const time = date.toLocaleTimeString("en-US", {
            timeZone: tz,
            hour: "numeric",
            minute: "2-digit",
            hour12: true,
        });
        return `${monthDay}, ${time}`;
    };

    const stopJob = async () => {
        try {
            setIsStopping(true);
            const res = await fetch(`/v1/jobs/${jobId}/stop`, { method: "POST" });
            if (!res.ok) throw new Error("Failed to stop job");
            toast({ title: "Stopping job", description: `Job ${jobId} is stopping.` });
        } catch (err) {
            toast({
                variant: "destructive",
                title: "Error",
                description: "Failed to stop job. Please try again.",
            });
        } finally {
            setIsStopping(false);
        }
    };

    const job = jobs.find((j) => j.id === jobId) ?? fetchedJob ?? undefined;
    const isLiveJob = job?.status === "RUNNING" || job?.status === "PENDING";

    // Live duration ticks once per second while the job runs.
    const [nowMs, setNowMs] = useState(() => Date.now());
    useEffect(() => {
        if (!isLiveJob) return;
        const id = window.setInterval(() => setNowMs(Date.now()), 1000);
        return () => window.clearInterval(id);
    }, [isLiveJob]);

    useEffect(() => {
        setStats(null);
        setStatsLoadError(false);
        setIsStatsLoading(true);
        setFetchedJob(null);
        setIsImageOpen(false);
        hasCompletedInitialStatsLoadRef.current = false;
    }, [jobId]);

    useEffect(() => {
        setJobDoc(null);
        const controller = new AbortController();
        (async () => {
            const res = await fetch(`/v1/jobs/${jobId}`, { signal: controller.signal });
            if (!res.ok) return;
            setJobDoc(await res.json());
        })().catch(() => {});
        return () => controller.abort();
    }, [jobId]);

    useEffect(() => {
        const controller = new AbortController();
        let cancelled = false;
        let refreshIntervalId: number | undefined;
        let failedSyncTimeoutIdShort: number | undefined;
        let failedSyncTimeoutIdLong: number | undefined;

        const isTerminalStatus =
            job?.status === "FAILED" || job?.status === "COMPLETED" || job?.status === "CANCELED";

        const loadStats = async (forceLoadingSpinner: boolean) => {
            if (forceLoadingSpinner) setIsStatsLoading(true);
            try {
                const response = await fetch(`/v1/jobs/${jobId}/result-stats`, {
                    signal: controller.signal,
                });
                if (!response.ok) throw new Error("Failed to load job result stats");
                const payload = await response.json();
                if (cancelled) return;
                setStats({
                    n_inputs: Number(payload?.n_inputs ?? 0),
                    n_results: Number(payload?.n_results ?? 0),
                    n_failed: Number(payload?.n_failed ?? 0),
                });
                setFetchedJob({
                    id: jobId,
                    status: (payload?.status as JobsStatus) ?? null,
                    user: payload?.user || "Unknown",
                    n_inputs: Number(payload?.n_inputs ?? 0),
                    n_results: Number(payload?.n_results ?? 0),
                    n_failed: Number(payload?.n_failed ?? 0),
                    function_name:
                        typeof payload?.function_name === "string"
                            ? payload.function_name
                            : "Unknown",
                    started_at:
                        typeof payload?.started_at === "number"
                            ? new Date(payload.started_at * 1000)
                            : undefined,
                    ended_at:
                        typeof payload?.ended_at === "number"
                            ? new Date(payload.ended_at * 1000)
                            : undefined,
                });
                setStatsLoadError(false);
                hasCompletedInitialStatsLoadRef.current = true;
            } catch {
                if (cancelled) return;
                if (!hasCompletedInitialStatsLoadRef.current) {
                    setStats(null);
                    setStatsLoadError(true);
                }
            } finally {
                if (!cancelled && forceLoadingSpinner) setIsStatsLoading(false);
            }
        };

        void loadStats(!hasCompletedInitialStatsLoadRef.current);

        if (!isTerminalStatus) {
            refreshIntervalId = window.setInterval(() => {
                void loadStats(false);
            }, 2500);
        } else {
            // Final sync when status changes to a terminal state.
            void loadStats(false);
            // Some failed writes may land shortly after terminal status flips.
            if (job?.status === "FAILED") {
                failedSyncTimeoutIdShort = window.setTimeout(() => {
                    void loadStats(false);
                }, 7500);
                failedSyncTimeoutIdLong = window.setTimeout(() => {
                    void loadStats(false);
                }, 20000);
            }
        }

        return () => {
            cancelled = true;
            if (refreshIntervalId) window.clearInterval(refreshIntervalId);
            if (failedSyncTimeoutIdShort) window.clearTimeout(failedSyncTimeoutIdShort);
            if (failedSyncTimeoutIdLong) window.clearTimeout(failedSyncTimeoutIdLong);
            controller.abort();
        };
    }, [jobId, job?.status]);

    if (statsLoadError) {
        return (
            <div className="flex flex-1 flex-col items-center justify-center text-center">
                <p className="text-sm font-medium text-foreground">
                    Failed to load job result stats
                </p>
                <button
                    onClick={() => window.location.reload()}
                    className="mt-2 text-[13px] font-medium text-primary hover:underline"
                >
                    Retry
                </button>
            </div>
        );
    }

    if (!job || isStatsLoading || !stats) {
        return (
            <div className="flex flex-1 flex-col items-center justify-center">
                <div className="inline-flex items-center gap-3 text-muted-foreground">
                    <div
                        className="h-5 w-5 animate-spin rounded-full border-2 border-border border-t-primary"
                        role="status"
                        aria-label="Loading job details"
                    />
                    <span className="text-sm">Loading job…</span>
                </div>
            </div>
        );
    }

    const safeFailedCount = Math.max(0, stats.n_failed);
    // n_results counts every finished call, including failed ones.
    const finishedCount = Math.max(0, stats.n_results);
    const succeededCount = Math.max(0, finishedCount - safeFailedCount);
    const remainingCount = Math.max(0, stats.n_inputs - succeededCount - safeFailedCount);
    const succeededPct = stats.n_inputs ? (succeededCount / stats.n_inputs) * 100 : 0;
    const failedPct = stats.n_inputs ? (safeFailedCount / stats.n_inputs) * 100 : 0;
    const remainingPct = stats.n_inputs ? (remainingCount / stats.n_inputs) * 100 : 0;

    const badge = jobStatusBadge(job.status);
    const canStop = job.status === "RUNNING" || job.status === "PENDING";

    const startedAtMs = job.started_at?.getTime();
    const endedAtMs = job.ended_at?.getTime();
    let durationValue: React.ReactNode = <Unknown />;
    if (startedAtMs != null && isLiveJob) {
        durationValue = formatDuration((nowMs - startedAtMs) / 1000);
    } else if (startedAtMs != null && endedAtMs != null) {
        durationValue = formatDuration((endedAtMs - startedAtMs) / 1000);
    }

    const loadingValue = <span className="text-muted-foreground">…</span>;
    const resourceValue = (
        value: number | string | null | undefined,
        unit: string
    ): React.ReactNode => {
        if (jobDoc == null) return loadingValue;
        if (value == null) return <Unknown />;
        if (value === "dynamic") return "Dynamic";
        return `${value} ${unit}`;
    };

    const facts: { label: string; value: React.ReactNode }[] = [
        {
            label: "Started",
            value: <span className="tabular-nums">{formatDateTime(job.started_at)}</span>,
        },
        ...(isLiveJob
            ? []
            : [
                  {
                      label: "Ended",
                      value: <span className="tabular-nums">{formatDateTime(job.ended_at)}</span>,
                  },
              ]),
        { label: "Duration", value: <span className="tabular-nums">{durationValue}</span> },
        {
            label: "Max parallelism",
            value: (
                <span className="tabular-nums">
                    {jobDoc == null ? (
                        loadingValue
                    ) : jobDoc.max_parallelism != null ? (
                        jobDoc.max_parallelism.toLocaleString()
                    ) : (
                        <Unknown />
                    )}
                </span>
            ),
        },
        {
            label: "CPU / call",
            value: <span className="tabular-nums">{resourceValue(jobDoc?.func_cpu, "vCPU")}</span>,
        },
        {
            label: "RAM / call",
            value: <span className="tabular-nums">{resourceValue(jobDoc?.func_ram, "GB")}</span>,
        },
        {
            label: "GPU / call",
            value: jobDoc == null ? loadingValue : jobDoc.func_gpu ?? "None",
        },
    ];

    const imageValue = jobDoc?.image ?? null;

    return (
        <div className="flex flex-1 flex-col min-h-0 min-w-0">
            <div className="mx-auto flex w-full max-w-6xl flex-1 flex-col min-h-0">
                {/* Breadcrumb */}
                <nav className="flex items-center gap-1 text-[13px] text-muted-foreground">
                    <Link
                        to="/jobs"
                        className="rounded font-medium transition-colors duration-150 hover:text-foreground"
                    >
                        Jobs
                    </Link>
                    <ChevronRight className="h-3.5 w-3.5" />
                    <span className="truncate font-mono text-xs">{job.id}</span>
                </nav>

                {/* Title row */}
                <div className="mt-3 flex flex-wrap items-start justify-between gap-x-6 gap-y-3 pb-4">
                    <div className="min-w-0">
                        <div className="flex items-center gap-3">
                            <h1 className="truncate font-mono text-xl font-semibold tracking-tight text-foreground">
                                {job.function_name ?? "Unknown"}
                            </h1>
                            <StatusBadge tone={badge.tone} label={badge.label} pulse={badge.pulse} />
                        </div>
                    </div>
                    <Button
                        variant="outline-destructive"
                        onClick={stopJob}
                        disabled={isStopping || !canStop}
                        className="shrink-0"
                    >
                        <PowerOff />
                        Stop job
                    </Button>
                </div>

                {/* Tabs (same pattern as the settings page) */}
                <div className="border-b border-border">
                    <nav className="flex items-center gap-5" aria-label="Job sections">
                        <button
                            type="button"
                            onClick={() => openTab("overview")}
                            className={tabClass(activeTab === "overview")}
                            aria-pressed={activeTab === "overview"}
                        >
                            Overview
                        </button>
                        <button
                            type="button"
                            onClick={() => openTab("utilization")}
                            className={tabClass(activeTab === "utilization")}
                            aria-pressed={activeTab === "utilization"}
                        >
                            Utilization
                        </button>
                    </nav>
                </div>

                {activeTab === "overview" ? (
                    <div className="mt-5 flex flex-1 flex-col min-h-0">
                        {/* Progress */}
                        <div className="mb-4 rounded-xl border border-border bg-card px-5 py-4 shadow-sm">
                            <div className="flex flex-wrap items-baseline justify-between gap-x-6 gap-y-2">
                                <span className="tabular-nums text-foreground">
                                    <span className="text-xl font-semibold">
                                        {finishedCount.toLocaleString()}
                                    </span>
                                    <span className="text-sm text-muted-foreground">
                                        {" "}
                                        / {stats.n_inputs.toLocaleString()} function calls complete
                                    </span>
                                </span>

                                <div className="flex flex-wrap items-center gap-4 text-[13px] text-muted-foreground">
                                    <span className="inline-flex items-center gap-1.5">
                                        <span className="h-1.5 w-1.5 rounded-full bg-emerald-500 dark:bg-emerald-400" />
                                        Succeeded
                                        <span className="tabular-nums text-foreground">
                                            {succeededCount.toLocaleString()}
                                        </span>
                                    </span>
                                    <span className="inline-flex items-center gap-1.5">
                                        <span className="h-1.5 w-1.5 rounded-full bg-destructive" />
                                        Failed
                                        <span className="tabular-nums text-foreground">
                                            {safeFailedCount.toLocaleString()}
                                        </span>
                                    </span>
                                    <span className="inline-flex items-center gap-1.5">
                                        <span className="h-1.5 w-1.5 rounded-full bg-amber-400" />
                                        Remaining
                                        <span className="tabular-nums text-foreground">
                                            {remainingCount.toLocaleString()}
                                        </span>
                                    </span>
                                </div>
                            </div>

                            <div className="mt-3 h-2 w-full overflow-hidden rounded-full bg-secondary">
                                <div className="flex h-full w-full">
                                    <div
                                        className="h-full bg-emerald-500 transition-all dark:bg-emerald-400"
                                        style={{ width: `${succeededPct}%` }}
                                        aria-hidden="true"
                                    />
                                    <div
                                        className="h-full bg-destructive transition-all"
                                        style={{ width: `${failedPct}%` }}
                                        aria-hidden="true"
                                    />
                                    <div
                                        className="h-full bg-amber-400 transition-all"
                                        style={{ width: `${remainingPct}%` }}
                                        aria-hidden="true"
                                    />
                                </div>
                            </div>
                        </div>

                        {/* Details */}
                        <div className="mb-4 rounded-xl border border-border bg-card shadow-sm">
                            <div className="flex flex-wrap gap-y-4 px-5 py-4">
                                {facts.map((fact, i) => (
                                    <div
                                        key={fact.label}
                                        className={cn(
                                            "min-w-0 pr-7",
                                            i > 0 && "border-l border-border/70 pl-7"
                                        )}
                                    >
                                        <div className="eyebrow">{fact.label}</div>
                                        <div className="mt-1 text-sm leading-snug text-foreground">
                                            {fact.value}
                                        </div>
                                    </div>
                                ))}
                            </div>

                            <div className="border-t border-border/70">
                                <button
                                    type="button"
                                    onClick={() => setIsImageOpen((open) => !open)}
                                    aria-expanded={isImageOpen}
                                    className="flex w-full items-center gap-1.5 px-5 py-2.5 text-left text-[13px] font-medium text-muted-foreground transition-colors duration-150 hover:text-foreground"
                                >
                                    <ChevronRight
                                        className={cn(
                                            "h-3.5 w-3.5 transition-transform duration-150",
                                            isImageOpen && "rotate-90"
                                        )}
                                    />
                                    Image
                                </button>
                                {isImageOpen && (
                                    <div className="flex min-w-0 items-center gap-2 px-5 pb-3 pl-10">
                                        {jobDoc == null ? (
                                            loadingValue
                                        ) : imageValue ? (
                                            <>
                                                <code
                                                    className="min-w-0 truncate font-mono text-[13px] text-foreground"
                                                    title={imageValue}
                                                >
                                                    {imageValue}
                                                </code>
                                                <CopyButton
                                                    value={imageValue}
                                                    label="Copy image reference"
                                                />
                                            </>
                                        ) : (
                                            <span className="text-[13px] text-muted-foreground">
                                                Default image
                                            </span>
                                        )}
                                    </div>
                                )}
                            </div>
                        </div>

                        {/* Logs */}
                        <div className="flex flex-1 flex-col min-h-0">
                            <JobLogs
                                jobId={job.id}
                                jobStatus={job.status}
                                nInputs={stats.n_inputs}
                                failedCount={safeFailedCount}
                                onViewUtilization={openTaskUtilization}
                            />
                        </div>
                    </div>
                ) : (
                    <div className="mt-5">
                        <JobUtilization
                            jobId={job.id}
                            jobStatus={job.status}
                            taskIndex={selectedTaskIndex}
                            onSelectTask={stepToTask}
                        />
                    </div>
                )}
            </div>
        </div>
    );
};

export default JobDetails;
