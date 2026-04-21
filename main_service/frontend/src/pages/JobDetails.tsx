import { useParams, useNavigate } from "react-router-dom";
import { useEffect, useRef, useState } from "react";
import { useJobs } from "@/contexts/JobsContext";
import JobLogs from "@/components/JobLogs";
import { Button } from "@/components/ui/button";
import { PowerOff } from "lucide-react";
import { useToast } from "@/components/ui/use-toast";

type JobResultStats = {
    n_inputs: number;
    n_results: number;
    n_failed: number;
};

type JobDoc = {
    image?: string | null;
    max_parallelism?: number | null;
    func_cpu?: number | null;
    func_ram?: number | null;
    func_gpu?: string | null;
};

const Fact = ({
    label,
    value,
    span = 1,
}: {
    label: string;
    value: React.ReactNode;
    span?: number;
}) => (
    <div className="min-w-0" style={{ gridColumn: `span ${span} / span ${span}` }}>
        <div className="text-[11px] uppercase tracking-[0.08em] font-medium text-gray-500">
            {label}
        </div>
        <div className="mt-1.5 text-[14.5px] leading-snug text-gray-900">{value}</div>
    </div>
);

const JobDetails = () => {
    const { jobId } = useParams<{ jobId: string }>();
    const { jobs } = useJobs();
    const navigate = useNavigate();
    const { toast } = useToast();
    const [isStopping, setIsStopping] = useState(false);
    const [stats, setStats] = useState<JobResultStats | null>(null);
    const [isStatsLoading, setIsStatsLoading] = useState(true);
    const [statsLoadError, setStatsLoadError] = useState(false);
    const [jobDoc, setJobDoc] = useState<JobDoc | null>(null);
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

    const getTimeZoneAbbr = (tz: string, at: Date): string => {
        const parts = new Intl.DateTimeFormat("en-US", {
            timeZone: tz,
            timeZoneName: "short",
            hour: "numeric",
        }).formatToParts(at);
        return parts.find((p) => p.type === "timeZoneName")?.value || "";
    };

    const formatStartedAtTime = (date?: Date): string => {
        if (!date) return "";
        const tz = userTimeZone || Intl.DateTimeFormat().resolvedOptions().timeZone;
        const t = date.toLocaleTimeString("en-US", {
            timeZone: tz,
            hour: "numeric",
            minute: "2-digit",
            hour12: true,
        });
        const abbr = getTimeZoneAbbr(tz, date);
        return `${t} ${abbr},`;
    };

    const formatStartedAtWeekday = (date?: Date): string => {
        if (!date) return "";
        const tz = userTimeZone || Intl.DateTimeFormat().resolvedOptions().timeZone;
        const wd = date.toLocaleDateString("en-US", {
            timeZone: tz,
            weekday: "long",
        });
        return `${wd},`;
    };

    const formatStartedAtMonthDay = (date?: Date): string => {
        if (!date) return "";
        const tz = userTimeZone || Intl.DateTimeFormat().resolvedOptions().timeZone;
        return date.toLocaleDateString("en-US", {
            timeZone: tz,
            month: "short",
            day: "numeric",
        });
    };

    const formatStartedAt = (date?: Date): string => {
        if (!date) return "—";
        const tz = userTimeZone || Intl.DateTimeFormat().resolvedOptions().timeZone;
        const time = date.toLocaleTimeString("en-US", {
            timeZone: tz,
            hour: "numeric",
            minute: "2-digit",
            hour12: true,
        });
        const monthDay = date.toLocaleDateString("en-US", {
            timeZone: tz,
            month: "short",
            day: "numeric",
        });
        return `${time}, ${monthDay}`;
    };
    const getStatusBadgeClass = (status: string | null) => {
        const statusClasses: Record<string, string> = {
            PENDING: "border-slate-300 bg-slate-50 text-slate-700",
            RUNNING: "border-amber-200 bg-amber-50 text-amber-700",
            FAILED: "border-rose-200 bg-rose-50 text-rose-600",
            CANCELED: "border-rose-200 bg-rose-50 text-rose-600",
            COMPLETED: "border-emerald-200 bg-emerald-50 text-emerald-700",
        };
        return status ? statusClasses[status] || "border-slate-300 bg-slate-50 text-slate-700" : "border-slate-300 bg-slate-50 text-slate-700";
    };

    const getStatusDotClass = (status: string | null) => {
        const statusDotClasses: Record<string, string> = {
            PENDING: "bg-slate-500",
            RUNNING: "bg-amber-500",
            FAILED: "bg-rose-500",
            CANCELED: "bg-rose-500",
            COMPLETED: "bg-emerald-500",
        };
        return status ? statusDotClasses[status] || "bg-slate-500" : "bg-slate-500";
    };

    const stopJob = async () => {
        if (!jobId) return;
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

    if (!jobId) {
        return (
            <div className="flex-1 flex flex-col items-center justify-center px-12 pt-10">
                <h1 className="text-2xl font-semibold text-red-600">Missing job ID</h1>
                <button
                    onClick={() => navigate("/jobs")}
                    className="mt-4 text-primary underline underline-offset-2"
                >
                    Back to Jobs
                </button>
            </div>
        );
    }

    const job = jobs.find((j) => j.id === jobId);

    useEffect(() => {
        if (!jobId) return;

        setStats(null);
        setStatsLoadError(false);
        setIsStatsLoading(true);
        hasCompletedInitialStatsLoadRef.current = false;
    }, [jobId]);

    useEffect(() => {
        if (!jobId) return;
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
        if (!jobId) return;

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

    if (!job) {
        return (
            <div className="flex-1 flex flex-col items-center justify-center px-12 pt-10">
                <h1 className="text-2xl font-semibold text-gray-500">Loading job...</h1>
            </div>
        );
    }

    if (isStatsLoading) {
        return (
            <div className="flex-1 flex flex-col items-center justify-center px-12 pt-10">
                <div className="inline-flex items-center gap-3 text-gray-600">
                    <div
                        className="h-6 w-6 rounded-full border-2 border-gray-300 border-t-primary animate-spin"
                        role="status"
                        aria-label="Loading job result stats"
                    />
                    <h1 className="text-2xl font-semibold text-gray-500">Loading job details...</h1>
                </div>
            </div>
        );
    }

    if (statsLoadError || !stats) {
        return (
            <div className="flex-1 flex flex-col items-center justify-center px-12 pt-10">
                <h1 className="text-2xl font-semibold text-red-600">
                    Failed to load job result stats
                </h1>
                <button
                    onClick={() => window.location.reload()}
                    className="mt-4 text-primary underline underline-offset-2"
                >
                    Retry
                </button>
            </div>
        );
    }

    const safeFailedCount = Math.max(0, stats.n_failed);
    const succeededCount = Math.max(0, stats.n_results);
    const remainingCount = Math.max(0, stats.n_inputs - succeededCount - safeFailedCount);
    const succeededPct = stats.n_inputs ? (succeededCount / stats.n_inputs) * 100 : 0;
    const failedPct = stats.n_inputs ? (safeFailedCount / stats.n_inputs) * 100 : 0;
    const remainingPct = stats.n_inputs ? (remainingCount / stats.n_inputs) * 100 : 0;

    return (
        <div className="flex flex-col flex-1 min-h-0 px-12 pt-0">
            <div className="max-w-6xl mx-auto w-full flex flex-col flex-1 min-h-0">
                <div className="mb-3 rounded-lg border border-gray-200 bg-white px-6 py-5">
                    <div className="flex flex-row items-start justify-between gap-4">
                        <div className="min-w-0">
                            <div className="flex items-center gap-3 flex-wrap">
                                <h1 className="text-[24px] font-semibold tracking-tight text-gray-900 truncate">
                                    {job.function_name ?? "Unknown"}
                                </h1>
                                <span
                                    className={`inline-flex items-center gap-2 rounded-full border px-3 py-1 text-[13.5px] font-medium ${getStatusBadgeClass(job.status)}`}
                                >
                                    <span className={`h-2 w-2 rounded-full ${getStatusDotClass(job.status)}`} />
                                    <span>{(job.status ?? "UNKNOWN").toLowerCase().replace(/^./, c => c.toUpperCase())}</span>
                                </span>
                            </div>
                            <div className="mt-1.5 font-mono text-[13px] text-gray-500 truncate">
                                {job.id}
                            </div>
                        </div>
                        {(() => {
                            const canStop = job?.status === "RUNNING" || job?.status === "PENDING";
                            return (
                                <Button
                                    variant={canStop ? "destructive" : "outline"}
                                    size="sm"
                                    className="h-9 rounded-md shrink-0"
                                    onClick={stopJob}
                                    disabled={isStopping || !canStop}
                                >
                                    <PowerOff className="mr-2 h-3.5 w-3.5" />
                                    Stop
                                </Button>
                            );
                        })()}
                    </div>

                    <div className="mt-5 pt-5 border-t border-gray-100 grid grid-cols-2 sm:grid-cols-3 gap-x-8 gap-y-5">
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
                                <span className="font-mono text-[13px] break-all">
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
                            label="Function CPU"
                            value={
                                <span className="tabular-nums">
                                    {jobDoc?.func_cpu != null ? `${jobDoc.func_cpu} vCPU` : "—"}
                                </span>
                            }
                        />
                        <Fact
                            label="Function RAM"
                            value={
                                <span className="tabular-nums">
                                    {jobDoc?.func_ram != null ? `${jobDoc.func_ram} GB` : "—"}
                                </span>
                            }
                        />
                        <Fact
                            label="Function GPU"
                            value={jobDoc?.func_gpu ?? (jobDoc ? "None" : "—")}
                        />
                    </div>
                </div>

                <div className="mb-3 rounded-lg border border-gray-200 bg-white px-4 py-3">
                    <div className="flex items-end justify-between gap-6">
                        <div>
                            <div className="mt-0.5 text-[14.5px] tabular-nums text-gray-800">
                                {succeededCount.toLocaleString()}
                                <span className="mx-1.5">/</span>
                                <span>
                                    {stats.n_inputs.toLocaleString()}
                                </span>
                                <span className="ml-2">
                                    Function calls complete.
                                </span>
                            </div>
                        </div>

                        <div className="flex flex-wrap items-center justify-end gap-5 text-[14.5px] text-gray-800">
                            <span className="inline-flex items-center gap-1.5">
                                <span className="h-2 w-2 rounded-full bg-emerald-500" />
                                <span>Success</span>
                                <span className="tabular-nums">
                                    {succeededCount.toLocaleString()}
                                </span>
                            </span>
                            <span className="inline-flex items-center gap-1.5">
                                <span className="h-2 w-2 rounded-full bg-rose-500" />
                                <span>Failed</span>
                                <span className="tabular-nums">
                                    {safeFailedCount.toLocaleString()}
                                </span>
                            </span>
                            <span className="inline-flex items-center gap-1.5">
                                <span className="h-2 w-2 rounded-full bg-amber-400" />
                                <span>Remaining</span>
                                <span className="tabular-nums">
                                    {remainingCount.toLocaleString()}
                                </span>
                            </span>
                        </div>
                    </div>

                    <div className="mt-2 h-2.5 w-full overflow-hidden rounded-full bg-slate-200">
                        <div className="flex h-full w-full">
                            <div
                                className="h-full bg-emerald-500 transition-all"
                                style={{ width: `${succeededPct}%` }}
                                aria-hidden="true"
                            />
                            <div
                                className="h-full bg-rose-500 transition-all"
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

                {/* Logs Section */}
                <div className="flex-1 min-h-0 flex flex-col">
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
