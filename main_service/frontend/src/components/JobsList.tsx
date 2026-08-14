import { useJobs } from "@/contexts/JobsContext";
import { Card, CardContent } from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { ListChecks } from "lucide-react";
import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { StatusBadge, jobStatusBadge } from "@/components/StatusBadge";
import { TablePagination } from "@/components/TablePagination";

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

  const tz = userTimeZone || Intl.DateTimeFormat().resolvedOptions().timeZone;
  const tzAbbr = getTimeZoneAbbr(tz, new Date());

  return (
    <Card className="min-w-0">
      <CardContent className="p-0">
        {isLoading ? (
          <div className="flex justify-center py-12">
            <div className="h-5 w-5 animate-spin rounded-full border-2 border-border border-t-primary" />
          </div>
        ) : jobs.length === 0 ? (
          <div className="flex flex-col items-center justify-center px-6 py-14 text-center">
            <div className="flex h-10 w-10 items-center justify-center rounded-full bg-muted">
              <ListChecks className="h-[18px] w-[18px] text-muted-foreground" />
            </div>
            <p className="mt-3 text-sm font-medium text-foreground">No jobs yet</p>
            <p className="mt-1 text-[13px] text-muted-foreground">
              Jobs appear here when you call{" "}
              <code className="font-mono text-xs">remote_parallel_map</code>.
            </p>
          </div>
        ) : (
          <>
            {/* CONTAIN OVERFLOW HERE so the PAGE doesn't get a horizontal scrollbar */}
            <div className="w-full min-w-0 overflow-x-auto">
              <Table className="w-full min-w-[860px]">
                <TableHeader>
                  <TableRow className="hover:bg-transparent">
                    <TableHead className="pl-5">Status</TableHead>
                    <TableHead>Function</TableHead>
                    <TableHead className="w-[280px]">Results</TableHead>
                    <TableHead>User</TableHead>
                    <TableHead className="pr-5 text-right">
                      Started{" "}
                      <span className="font-normal text-muted-foreground/80">({tzAbbr})</span>
                    </TableHead>
                  </TableRow>
                </TableHeader>

                <TableBody>
                  {jobs.map((job) => {
                    const failedCount = Math.max(0, job.n_failed ?? 0);
                    const successfulCount = Math.max(0, job.n_results - failedCount);
                    const pct = job.n_inputs
                      ? Math.min(100, (successfulCount / job.n_inputs) * 100)
                      : 0;
                    return (
                      <TableRow
                        key={job.id}
                        className="cursor-pointer"
                        onClick={() => navigate(`/jobs/${job.id}`)}
                        onKeyDown={(event) => {
                          if (event.key !== "Enter" && event.key !== " ") return;
                          event.preventDefault();
                          navigate(`/jobs/${job.id}`);
                        }}
                        tabIndex={0}
                      >
                        <TableCell className="pl-5">
                          <StatusBadge {...jobStatusBadge(job.status)} />
                        </TableCell>

                        <TableCell>
                          <div className="max-w-[320px] truncate">
                            <span
                              title={job.function_name ?? "Unknown"}
                              className="font-mono text-[13px] font-medium text-foreground"
                            >
                              {job.function_name ?? "Unknown"}
                            </span>
                          </div>
                        </TableCell>

                        <TableCell className="w-[280px]">
                          <div className="flex items-center gap-3">
                            <span className="whitespace-nowrap text-[13px] tabular-nums text-foreground">
                              {successfulCount.toLocaleString()}
                              <span className="text-muted-foreground">
                                {" "}
                                / {job.n_inputs.toLocaleString()}
                              </span>
                            </span>
                            <div className="h-1 w-24 shrink-0 overflow-hidden rounded-full bg-secondary">
                              <div
                                className="h-full rounded-full bg-primary transition-all"
                                style={{ width: `${pct}%` }}
                              />
                            </div>
                          </div>
                        </TableCell>

                        <TableCell>
                          <div
                            className="max-w-[200px] truncate text-[13px] text-muted-foreground"
                            title={job.user}
                          >
                            {job.user}
                          </div>
                        </TableCell>

                        <TableCell className="whitespace-nowrap pr-5 text-right text-[13px] tabular-nums text-muted-foreground">
                          {formatStartedAt(job.started_at)}
                        </TableCell>
                      </TableRow>
                    );
                  })}
                </TableBody>
              </Table>
            </div>

            <div className="px-5 pb-4">
              <TablePagination page={page} totalPages={totalPages} onPageChange={setPage} />
            </div>
          </>
        )}
      </CardContent>
    </Card>
  );
};
