import { useJobs } from "@/contexts/JobsContext";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { useEffect, useState } from "react";
import { Link } from "react-router-dom";

export const JobsList = () => {
    const { jobs, setJobs, page, setPage, totalPages, isLoading } = useJobs();
    const anySelected = jobs.some((job) => job.checked);

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

    const formatStartedAt = (date: Date): string => {
        const tz = userTimeZone || Intl.DateTimeFormat().resolvedOptions().timeZone;
        const time = date.toLocaleTimeString("en-US", {
            timeZone: tz,
            hour: "numeric",
            minute: "2-digit",
            hour12: true,
        });
        const dateStr = date.toLocaleDateString("en-US", {
            timeZone: tz,
            weekday: "long",
            month: "short",
            day: "numeric",
        });
        return `${time}, ${dateStr}`;
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
        return `${t},`;
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

    const handleCheckboxChange = (id: string) => {
        setJobs((prev) =>
            prev.map((job) => (job.id === id ? { ...job, checked: !job.checked } : job))
        );
    };

    const handleSelectAllChange = () => {
        const selectAll = !jobs.every((job) => job.checked);
        setJobs((prev) => prev.map((job) => ({ ...job, checked: selectAll })));
    };

    const getStatusClass = (status: string | null) => {
        const statusClasses = {
            PENDING: "bg-gray-400",
            RUNNING: "bg-yellow-500 animate-pulse",
            FAILED: "bg-red-500",
            COMPLETED: "bg-green-500",
        };
        return cn("w-2 h-2 rounded-full", status ? statusClasses[status] : "");
    };

    return (
        <div className="space-y-6 overflow-hidden">
            <Card>
                <CardHeader className="flex items-center justify-between py-4" />

                <CardContent>
                    {isLoading ? (
                        <div className="flex justify-center py-8">
                            <div className="w-5 h-5 border-2 border-primary border-t-transparent rounded-full animate-spin" />
                        </div>
                    ) : jobs.length === 0 ? (
                        <div className="text-center text-gray-500 py-4">No jobs</div>
                    ) : (
                        <>
                            <Table className="w-full">
                                <TableHeader>
                                    <TableRow>
                                        <TableHead className="w-10">
                                            <input
                                                type="checkbox"
                                                checked={jobs.every((j) => j.checked)}
                                                onChange={handleSelectAllChange}
                                                className="w-4 h-4 border-2 border-gray-400 rounded-none appearance-none checked:bg-primary checked:border-primary cursor-pointer"
                                            />
                                        </TableHead>
                                        <TableHead>Status</TableHead>
                                        <TableHead>Function</TableHead>
                                        <TableHead>Results</TableHead>
                                        <TableHead>User</TableHead>
                                        <TableHead colSpan={3}>
                                            {(() => {
                                                const tz =
                                                    userTimeZone ||
                                                    Intl.DateTimeFormat().resolvedOptions()
                                                        .timeZone;
                                                const abbr = getTimeZoneAbbr(tz, new Date());
                                                return (
                                                    <>
                                                        <span>Started At </span>{" "}
                                                        <span className="text-s text-gray-500 font-normal">
                                                            ({abbr})
                                                        </span>
                                                    </>
                                                );
                                            })()}
                                        </TableHead>
                                        <TableHead className="w-[5px] text-right" />
                                    </TableRow>
                                </TableHeader>

                                <TableBody>
                                    {jobs.map((job) => (
                                        <TableRow key={job.id}>
                                            <TableCell>
                                                <input
                                                    type="checkbox"
                                                    checked={job.checked}
                                                    onChange={() => handleCheckboxChange(job.id)}
                                                    className="w-4 h-4 border-2 border-gray-400 rounded-none appearance-none checked:bg-primary checked:border-primary cursor-pointer"
                                                />
                                            </TableCell>
                                            <TableCell>
                                                <div className="flex items-center space-x-2">
                                                    <div className={getStatusClass(job.status)} />
                                                    <span className="text-sm capitalize">
                                                        {job.status?.toUpperCase()}
                                                    </span>
                                                </div>
                                            </TableCell>
                                            <TableCell>
                                                <Link
                                                    to={`/jobs/${job.id}`}
                                                    className="text-black underline underline-offset-2 hover:text-[#1a1a1a] transition-all"
                                                >
                                                    {job.function_name ?? "Unknown"}
                                                </Link>
                                            </TableCell>
                                            <TableCell>
                                                <div className="flex flex-col space-y-1 min-w-[100px]">
                                                    <div>
                                                        {job.n_results.toLocaleString()} /{" "}
                                                        {job.n_inputs.toLocaleString()}
                                                    </div>
                                                    <div className="w-full bg-gray-200 rounded h-1.5 overflow-hidden">
                                                        <div
                                                            className="bg-primary h-1.5 transition-all"
                                                            style={{
                                                                width: `${
                                                                    job.n_inputs
                                                                        ? Math.min(
                                                                              100,
                                                                              (job.n_results /
                                                                                  job.n_inputs) *
                                                                                  100
                                                                          )
                                                                        : 0
                                                                }%`,
                                                            }}
                                                        />
                                                    </div>
                                                </div>
                                            </TableCell>
                                            <TableCell>{job.user}</TableCell>
                                            <TableCell className="whitespace-nowrap">
                                                <span className="flex items-baseline">
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
                                            </TableCell>
                                            <TableCell className="text-right" />
                                        </TableRow>
                                    ))}
                                </TableBody>
                            </Table>

                            {/* Pagination */}
                            <div className="flex justify-center mt-6 space-x-2 items-center">
                                {page > 0 && (
                                    <button
                                        onClick={() => setPage(page - 1)}
                                        className="px-3 py-1 text-sm text-primary hover:underline"
                                    >
                                        ‹ Prev
                                    </button>
                                )}

                                <button
                                    onClick={() => setPage(0)}
                                    className={`px-3 py-1 rounded text-sm border ${
                                        page === 0
                                            ? "bg-primary text-primary-foreground"
                                            : "bg-white text-gray-700 hover:bg-gray-100"
                                    }`}
                                >
                                    1
                                </button>

                                {page > 3 && <span className="px-1">...</span>}

                                {Array.from({ length: totalPages }, (_, i) => i)
                                    .filter(
                                        (i) =>
                                            i !== 0 &&
                                            i !== totalPages - 1 &&
                                            Math.abs(i - page) <= 2
                                    )
                                    .map((i) => (
                                        <button
                                            key={i}
                                            onClick={() => setPage(i)}
                                            className={`px-3 py-1 rounded text-sm border ${
                                                page === i
                                                    ? "bg-primary text-primary-foreground"
                                                    : "bg-white text-gray-700 hover:bg-gray-100"
                                            }`}
                                        >
                                            {i + 1}
                                        </button>
                                    ))}

                                {page < totalPages - 4 && <span className="px-1">...</span>}

                                {totalPages > 1 && (
                                    <button
                                        onClick={() => setPage(totalPages - 1)}
                                        className={`px-3 py-1 rounded text-sm border ${
                                            page === totalPages - 1
                                                ? "bg-primary text-primary-foreground"
                                                : "bg-white text-gray-700 hover:bg-gray-100"
                                        }`}
                                    >
                                        {totalPages}
                                    </button>
                                )}

                                {page < totalPages - 1 && (
                                    <button
                                        onClick={() => setPage(page + 1)}
                                        className="px-3 py-1 text-sm text-primary hover:underline"
                                    >
                                        Next ›
                                    </button>
                                )}
                            </div>
                        </>
                    )}
                </CardContent>
            </Card>
        </div>
    );
};
