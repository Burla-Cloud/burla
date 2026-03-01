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

const JobDetails = () => {
    const { jobId } = useParams<{ jobId: string }>();
    const { jobs } = useJobs();
    const navigate = useNavigate();
    const { toast } = useToast();
    const [isStopping, setIsStopping] = useState(false);
    const [stats, setStats] = useState<JobResultStats | null>(null);
    const [isStatsLoading, setIsStatsLoading] = useState(true);
    const [statsLoadError, setStatsLoadError] = useState(false);
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
    const getStatusClass = (status: string | null) => {
        const statusClasses: Record<string, string> = {
            PENDING: "bg-gray-400",
            RUNNING: "bg-yellow-500 animate-pulse",
            FAILED: "bg-red-500",
            CANCELED: "bg-red-500",
            COMPLETED: "bg-green-500",
        };
        return `w-2 h-2 rounded-full ${status ? statusClasses[status] || "" : ""}`;
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
                        className="h-7 w-7 rounded-full border-2 border-gray-300 border-t-primary animate-spin"
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
                <h1 className="text-2xl font-semibold text-red-600">Failed to load job result stats</h1>
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
                {/* Breadcrumb */}
                <h1 className="text-3xl font-bold mt-[-4px] mb-3 text-primary">
                    <button
                        onClick={() => navigate("/jobs")}
                        className="hover:underline underline-offset-2 decoration-[0.5px] transition text-inherit"
                    >
                        Jobs
                    </button>
                    <span className="mx-2 text-inherit">›</span>
                    <span className="text-inherit">{job.id}</span>
                </h1>

                {/* Metadata row with Stop button on the same row */}
                <div className="flex flex-row items-center justify-between text-sm text-gray-600 mb-3">
                    <div className="flex items-center space-x-6">
                        <div className="flex items-center space-x-2">
                            <div className={getStatusClass(job.status)} />
                            <span className="text-sm capitalize">
                                {job.status?.toUpperCase() || "UNKNOWN"}
                            </span>
                        </div>
                        <div className="flex items-baseline">
                            <strong>Function:</strong>
                            <span className="ml-2">{job.function_name ?? "Unknown"}</span>
                        </div>
                        <div className="flex items-baseline">
                            <strong>Started At:</strong>
                            <span className="ml-2 flex items-baseline">
                                <span className="tabular-nums">
                                    {formatStartedAtTime(job.started_at)}
                                </span>
                                <span className="ml-1">
                                    {formatStartedAtWeekday(job.started_at)}
                                </span>
                                <span className="ml-1">
                                    {formatStartedAtMonthDay(job.started_at)}
                                </span>
                            </span>
                        </div>
                    </div>
                    <Button
                        variant="destructive"
                        size="lg"
                        className="w-32 -mt-2"
                        onClick={stopJob}
                        disabled={
                            isStopping || (job?.status !== "RUNNING" && job?.status !== "PENDING")
                        }
                    >
                        <PowerOff className="mr-2 h-4 w-4" />
                        Stop
                    </Button>
                </div>

                <div className="mt-4 mb-3 px-1 py-1">
                    <div className="flex items-end justify-between gap-6">
                        <div>
                            <div className="text-[11px] font-semibold tracking-[0.08em] uppercase text-slate-500">
                                Results
                            </div>
                            <div className="mt-0.5 text-lg font-semibold tabular-nums text-slate-700">
                                {succeededCount.toLocaleString()}
                                <span className="mx-1.5 text-slate-300">/</span>
                                <span className="text-slate-500">{stats.n_inputs.toLocaleString()}</span>
                            </div>
                        </div>

                        <div className="flex flex-wrap items-center justify-end gap-5 text-sm">
                            <span className="inline-flex items-center gap-1.5 text-slate-700">
                                <span className="h-2 w-2 rounded-full bg-emerald-500" />
                                <span className="text-slate-500">Success</span>
                                <strong className="tabular-nums">{succeededCount.toLocaleString()}</strong>
                            </span>
                            <span className="inline-flex items-center gap-1.5 text-slate-700">
                                <span className="h-2 w-2 rounded-full bg-rose-500" />
                                <span className="text-slate-500">Failed</span>
                                <strong className="tabular-nums">{safeFailedCount.toLocaleString()}</strong>
                            </span>
                            <span className="inline-flex items-center gap-1.5 text-slate-700">
                                <span className="h-2 w-2 rounded-full bg-amber-400" />
                                <span className="text-slate-500">Remaining</span>
                                <strong className="tabular-nums">{remainingCount.toLocaleString()}</strong>
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
