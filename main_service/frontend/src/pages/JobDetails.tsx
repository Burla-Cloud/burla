import { useParams, useNavigate } from "react-router-dom";
import { useEffect, useState } from "react";
import { useJobs } from "@/contexts/JobsContext";
import JobLogs from "@/components/JobLogs";
import { Button } from "@/components/ui/button";
import { PowerOff } from "lucide-react";
import { useToast } from "@/components/ui/use-toast";

const JobDetails = () => {
    const { jobId } = useParams<{ jobId: string }>();
    const { jobs } = useJobs();
    const navigate = useNavigate();
    const { toast } = useToast();
    const [isStopping, setIsStopping] = useState(false);
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

    if (!job) {
        return (
            <div className="flex-1 flex flex-col items-center justify-center px-12 pt-10">
                <h1 className="text-2xl font-semibold text-gray-500">Loading job...</h1>
            </div>
        );
    }

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

                <div className="mb-4">
                    <div className="flex flex-col space-y-0.5 min-w-[100px]">
                        <div>
                            <strong>Results:</strong> {job.n_results.toLocaleString()} /{" "}
                            {job.n_inputs.toLocaleString()}
                        </div>
                        <div className="w-full bg-gray-200 rounded h-1.5 overflow-hidden">
                            <div
                                className="bg-primary h-1.5 transition-all"
                                style={{
                                    width: `${
                                        job.n_inputs
                                            ? Math.min(100, (job.n_results / job.n_inputs) * 100)
                                            : 0
                                    }%`,
                                }}
                            />
                        </div>
                    </div>
                </div>

                {/* Logs Section */}
                <div className="flex-1 min-h-0 flex flex-col">
                    <JobLogs jobId={job.id} jobStatus={job.status} nInputs={job.n_inputs} />
                </div>
            </div>
        </div>
    );
};

export default JobDetails;
