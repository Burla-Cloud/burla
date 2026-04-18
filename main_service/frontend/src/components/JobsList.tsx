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
import { useNavigate } from "react-router-dom";

export const JobsList = () => {
  const { jobs, page, setPage, totalPages, isLoading } = useJobs();
  const navigate = useNavigate();

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

  const getStatusDotClass = (status: string | null) => {
    const statusDotClasses: Record<string, string> = {
      PENDING: "bg-slate-400",
      RUNNING: "bg-amber-300",
      FAILED: "bg-rose-300",
      CANCELED: "bg-rose-300",
      COMPLETED: "bg-emerald-300",
    };
    return status ? statusDotClasses[status] || "bg-slate-400" : "bg-slate-400";
  };

  const getStatusTextClass = (status: string | null) => {
    const statusTextClasses: Record<string, string> = {
      PENDING: "text-slate-600 dark:text-slate-300",
      RUNNING: "text-amber-700 dark:text-amber-400",
      FAILED: "text-rose-600 dark:text-rose-400",
      CANCELED: "text-rose-600 dark:text-rose-400",
      COMPLETED: "text-emerald-700 dark:text-emerald-400",
    };
    return status
      ? statusTextClasses[status] || "text-slate-600 dark:text-slate-300"
      : "text-slate-600 dark:text-slate-300";
  };

  return (
    <div className="space-y-6 min-w-0">
      <Card className="min-w-0">
        <CardHeader className="flex items-center justify-between py-4" />

        <CardContent className="min-w-0">
          {isLoading ? (
            <div className="flex justify-center py-8">
              <div className="w-5 h-5 border-2 border-primary border-t-transparent rounded-full animate-spin" />
            </div>
          ) : jobs.length === 0 ? (
            <div className="text-center text-gray-500 dark:text-gray-400 py-4">No jobs</div>
          ) : (
            <>
              {/* CONTAIN OVERFLOW HERE so the PAGE doesn't get a horizontal scrollbar */}
              <div className="w-full min-w-0 overflow-x-auto">
                <Table className="w-full min-w-[920px]">
                  <TableHeader>
                    <TableRow>
                      <TableHead>Status</TableHead>
                      <TableHead>Function</TableHead>
                      <TableHead className="w-[360px]">Results</TableHead>
                      <TableHead>User</TableHead>
                      <TableHead colSpan={3}>
                        {(() => {
                          const tz = userTimeZone || Intl.DateTimeFormat().resolvedOptions().timeZone;
                          const abbr = getTimeZoneAbbr(tz, new Date());
                          return (
                            <>
                              <span>Started At </span>
                              <span className="text-s text-gray-500 dark:text-gray-400 font-normal">
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
                    {jobs.map((job) => {
                      const failedCount = Math.max(0, job.n_failed ?? 0);
                      const successfulCount = Math.max(0, job.n_results - failedCount);
                      return (
                        <TableRow
                          key={job.id}
                          className="cursor-pointer hover:bg-slate-50/60 dark:hover:bg-slate-800/40"
                          onClick={() => navigate(`/jobs/${job.id}`)}
                          onKeyDown={(event) => {
                            if (event.key !== "Enter" && event.key !== " ") return;
                            event.preventDefault();
                            navigate(`/jobs/${job.id}`);
                          }}
                          tabIndex={0}
                        >
                        <TableCell>
                          <span
                            className={cn("inline-flex items-center gap-2 text-[14px] font-normal", getStatusTextClass(job.status))}
                          >
                            <span className={cn("h-2.5 w-2.5 rounded-full", getStatusDotClass(job.status))} />
                            <span className="capitalize">{job.status?.toLowerCase() || "unknown"}</span>
                          </span>
                        </TableCell>

                        {/* BIGGEST CULPRIT: long function names. Truncate them. */}
                        <TableCell>
                          <div className="max-w-[360px] truncate">
                            <span
                              title={job.function_name ?? "Unknown"}
                              className="text-foreground underline underline-offset-2"
                            >
                              {job.function_name ?? "Unknown"}
                            </span>
                          </div>
                        </TableCell>

                        <TableCell className="w-[360px]">
                          <div className="flex flex-col space-y-1 min-w-[320px]">
                            <div>
                              {successfulCount.toLocaleString()} / {job.n_inputs.toLocaleString()}
                            </div>
                            <div className="w-full bg-gray-200 dark:bg-gray-700 rounded h-1.5 overflow-hidden">
                              <div
                                className="bg-primary h-1.5 transition-all"
                                style={{
                                  width: `${
                                    job.n_inputs
                                      ? Math.min(100, (successfulCount / job.n_inputs) * 100)
                                      : 0
                                  }%`,
                                }}
                              />
                            </div>
                          </div>
                        </TableCell>

                        {/* Second culprit: long emails. Truncate them. */}
                        <TableCell>
                          <div className="max-w-[220px] truncate" title={job.user}>
                            {job.user}
                          </div>
                        </TableCell>

                        <TableCell className="whitespace-nowrap">
                          <span className="flex items-baseline">
                            <span className="tabular-nums">{formatStartedAtTime(job.started_at)}</span>
                            <span className="ml-1">{formatStartedAtWeekday(job.started_at)}</span>
                            <span className="ml-1">{formatStartedAtMonthDay(job.started_at)}</span>
                          </span>
                        </TableCell>

                        <TableCell className="text-right" />
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </div>

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
                      ? "bg-primary text-primary-foreground border-primary"
                      : "bg-card text-foreground hover:bg-muted"
                  }`}
                >
                  1
                </button>

                {page > 3 && <span className="px-1">...</span>}

                {Array.from({ length: totalPages }, (_, i) => i)
                  .filter((i) => i !== 0 && i !== totalPages - 1 && Math.abs(i - page) <= 2)
                  .map((i) => (
                    <button
                      key={i}
                      onClick={() => setPage(i)}
                      className={`px-3 py-1 rounded text-sm border ${
                        page === i
                          ? "bg-primary text-primary-foreground border-primary"
                          : "bg-card text-foreground hover:bg-muted"
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
                        ? "bg-primary text-primary-foreground border-primary"
                        : "bg-card text-foreground hover:bg-muted"
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
