import { useParams, useNavigate } from "react-router-dom";
import { useEffect, useCallback, useState } from "react";
import { useJobs } from "@/contexts/JobsContext";
import { useLogsContext } from "@/contexts/LogsContext";
import JobLogs from "@/components/JobLogs";
import { Button } from "@/components/ui/button";
import { PowerOff } from "lucide-react";
import { useToast } from "@/components/ui/use-toast";

type JobLogsSummary = {
    failed_indexes: number[];
    seen_indexes?: number[];
    indexes_with_logs?: number[];
};

const JobDetails = () => {
    const { jobId } = useParams<{ jobId: string }>();
    const { jobs } = useJobs();
    const { clearSummaryCache, closeLiveStream } = useLogsContext();
    const navigate = useNavigate(); 
    const { toast } = useToast();
    const [isStopping, setIsStopping] = useState(false);
    const [initialLogsSummary, setInitialLogsSummary] = useState<JobLogsSummary | null>(null);
    const [failedCount, setFailedCount] = useState<number>(0);
    const [isStatsLoading, setIsStatsLoading] = useState(true);
    const job = jobId ? jobs.find((j) => j.id === jobId) : undefined;
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

    useEffect(() => {
        if (!jobId) return;
        return () => {
            closeLiveStream(jobId);
            clearSummaryCache(jobId);
        };
    }, [jobId, closeLiveStream, clearSummaryCache]);

    const fetchLogsSummary = async (
        activeJobId: string,
        signal?: AbortSignal
    ): Promise<JobLogsSummary | null> => {
        try {
            const res = await fetch(`/v1/jobs/${activeJobId}/logs?summary=true`, { signal });
            if (!res.ok) return null;
            const data = await res.json();
            return {
                failed_indexes: Array.isArray(data?.failed_indexes) ? data.failed_indexes : [],
                seen_indexes: Array.isArray(data?.seen_indexes) ? data.seen_indexes : [],
                indexes_with_logs: Array.isArray(data?.indexes_with_logs)
                    ? data.indexes_with_logs
                    : [],
            };
        } catch {
            return null;
        }
    };

    const handleFailedCountChange = useCallback((nextFailedCount: number) => {
        setFailedCount((prev) => (prev === nextFailedCount ? prev : nextFailedCount));
    }, []);

    useEffect(() => {
        if (!jobId) {
            setInitialLogsSummary(null);
            setFailedCount(0);
            setIsStatsLoading(false);
            return;
        }

        let cancelled = false;
        const initialController = new AbortController();
        setIsStatsLoading(true);
        setInitialLogsSummary(null);
        setFailedCount(0);

        const loadInitialSummary = async () => {
            const summary = await fetchLogsSummary(jobId, initialController.signal);
            if (cancelled) return;
            setInitialLogsSummary(summary);
            setFailedCount(summary?.failed_indexes?.length ?? 0);
            setIsStatsLoading(false);
        };
        loadInitialSummary();

        return () => {
            cancelled = true;
            initialController.abort();
        };
    }, [jobId]);

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

    const isPageLoading = !job || isStatsLoading;

    if (isPageLoading) {
        return (
            <div className="flex-1 flex flex-col items-center justify-center px-12 pt-10">
                <div className="inline-flex items-center gap-3 text-gray-600">
                    <div
                        className="h-7 w-7 rounded-full border-2 border-gray-300 border-t-primary animate-spin"
                        role="status"
                        aria-label="Loading job details"
                    />
                    <h1 className="text-2xl font-semibold text-gray-500">Loading job details...</h1>
                </div>
            </div>
        );
    }

    const safeFailedCount = failedCount;
    const succeededCount = Math.max(0, job.n_results - safeFailedCount);
    const remainingCount = Math.max(0, job.n_inputs - job.n_results);
    const succeededPct = job.n_inputs ? (succeededCount / job.n_inputs) * 100 : 0;
    const failedPct = job.n_inputs ? (safeFailedCount / job.n_inputs) * 100 : 0;
    const remainingPct = job.n_inputs ? (remainingCount / job.n_inputs) * 100 : 0;

    return (
        <div className="flex min-h-0 flex-1 flex-col px-12 pt-0 pb-[25px] -mb-12">
            <div className="mx-auto flex min-h-0 w-full max-w-6xl flex-1 flex-col">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between lg:gap-8">
                    <div className="min-w-0">
                        <h1 className="mb-2 text-3xl font-bold text-primary">
                            <button
                                onClick={() => navigate("/jobs")}
                                className="hover:underline underline-offset-2 decoration-[0.5px] transition text-inherit"
                            >
                                Jobs
                            </button>
                            <span className="mx-2 text-inherit">›</span>
                            <span className="text-inherit">{job.id}</span>
                        </h1>

                        <div className="flex flex-wrap items-center gap-x-6 gap-y-2 text-sm text-gray-600">
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
                    </div>

                    <div className="flex w-full justify-end lg:w-auto lg:pt-2">
                        <Button
                            variant="destructive"
                            size="lg"
                            className="w-32"
                            onClick={stopJob}
                            disabled={
                                isStopping || (job?.status !== "RUNNING" && job?.status !== "PENDING")
                            }
                        >
                            <PowerOff className="mr-2 h-4 w-4" />
                            Stop
                        </Button>
                    </div>
                </div>

                <div className="mt-4 mb-3 px-1 py-1">
                    <div className="flex items-end justify-between gap-6">
                        <div>
                            <div className="text-[11px] font-semibold tracking-[0.08em] uppercase text-slate-500">
                                Results
                            </div>
                            <div className="mt-0.5 text-lg font-semibold tabular-nums text-slate-700">
                                {job.n_results.toLocaleString()}
                                <span className="mx-1.5 text-slate-300">/</span>
                                <span className="text-slate-500">{job.n_inputs.toLocaleString()}</span>
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
                <div className="mt-0 flex-1 min-h-0 flex flex-col">
                    <JobLogs
                        jobId={job.id}
                        jobStatus={job.status}
                        nResults={job.n_results}
                        initialSummary={initialLogsSummary}
                        onFailedCountChange={handleFailedCountChange}
                    />
                </div>
            </div>
        </div>
    );
};

export default JobDetails;   
