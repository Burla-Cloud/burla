import { Link, useParams } from "react-router-dom";
import { useEffect, useRef, useState } from "react";
import { useJobs } from "@/contexts/JobsContext";
import { BurlaJob, JobsStatus } from "@/types/coreTypes";
import JobLogs from "@/components/JobLogs";
import JobUtilization from "@/components/JobUtilization";
import { Button } from "@/components/ui/button";
import { ChevronRight, PowerOff } from "lucide-react";
import { useToast } from "@/components/ui/use-toast";
import { StatusBadge, jobStatusBadge } from "@/components/StatusBadge";

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

const Fact = ({ label, value }: { label: string; value: React.ReactNode }) => (
    <div className="min-w-0">
        <div className="eyebrow">{label}</div>
        <div className="mt-1 text-sm leading-snug text-foreground">{value}</div>
    </div>
);

const JobDetails = () => {
    const jobId = useParams<{ jobId: string }>().jobId!;
    const { jobs } = useJobs();
    const { toast } = useToast();
    const [isStopping, setIsStopping] = useState(false);
    const [stats, setStats] = useState<JobResultStats | null>(null);
    const [isStatsLoading, setIsStatsLoading] = useState(true);
    const [statsLoadError, setStatsLoadError] = useState(false);
    const [jobDoc, setJobDoc] = useState<JobDoc | null>(null);
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

    const formatStartedAt = (date?: Date): string => {
        if (!date) return "—";
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

    useEffect(() => {
        setStats(null);
        setStatsLoadError(false);
        setIsStatsLoading(true);
        setFetchedJob(null);
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
                <div className="mt-3 flex flex-wrap items-start justify-between gap-x-6 gap-y-3 pb-5">
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

                {/* Details + progress */}
                <div className="mb-4 rounded-xl border border-border bg-card shadow-sm">
                    <div className="grid grid-cols-2 gap-x-8 gap-y-4 px-5 py-4 sm:grid-cols-3 lg:grid-cols-6">
                        <Fact
                            label="Started"
                            value={
                                <span className="tabular-nums">
                                    {formatStartedAt(job.started_at)}
                                </span>
                            }
                        />
                        <Fact
                            label="Image"
                            value={
                                <span className="break-all font-mono text-[13px]">
                                    {jobDoc?.image ?? (jobDoc ? "default" : "—")}
                                </span>
                            }
                        />
                        <Fact
                            label="Max parallelism"
                            value={
                                <span className="tabular-nums">
                                    {jobDoc?.max_parallelism ?? "—"}
                                </span>
                            }
                        />
                        <Fact
                            label="CPU / call"
                            value={
                                <span className="tabular-nums">
                                    {jobDoc?.func_cpu != null ? `${jobDoc.func_cpu} vCPU` : "—"}
                                </span>
                            }
                        />
                        <Fact
                            label="RAM / call"
                            value={
                                <span className="tabular-nums">
                                    {jobDoc?.func_ram != null ? `${jobDoc.func_ram} GB` : "—"}
                                </span>
                            }
                        />
                        <Fact label="GPU / call" value={jobDoc?.func_gpu ?? (jobDoc ? "None" : "—")} />
                    </div>

                    <div className="border-t border-border/70 px-5 py-4">
                        <div className="flex flex-wrap items-center justify-between gap-x-6 gap-y-2">
                            <span className="text-sm tabular-nums text-foreground">
                                <span className="font-semibold">
                                    {finishedCount.toLocaleString()}
                                </span>
                                <span className="text-muted-foreground">
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

                        <div className="mt-2.5 h-1.5 w-full overflow-hidden rounded-full bg-secondary">
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
                </div>

                <JobUtilization jobId={job.id} jobStatus={job.status} />

                {/* Logs */}
                <div className="flex flex-1 flex-col min-h-0">
                    <JobLogs
                        jobId={job.id}
                        jobStatus={job.status}
                        nInputs={stats.n_inputs}
                        failedCount={safeFailedCount}
                    />
                </div>
            </div>
        </div>
    );
};

export default JobDetails;
