import {
  createContext,
  useContext,
  useEffect,
  useState,
  useCallback,
} from "react";
import { BurlaJob, JobsStatus } from "@/types/coreTypes";

interface JobsContextType {
  jobs: BurlaJob[];
  setJobs: React.Dispatch<React.SetStateAction<BurlaJob[]>>;
  page: number;
  setPage: React.Dispatch<React.SetStateAction<number>>;
  totalPages: number;
}

const JobsContext = createContext<JobsContextType>({
  jobs: [],
  setJobs: () => {},
  page: 0,
  setPage: () => {},
  totalPages: 1,
});

export const JobsProvider = ({ children }: { children: React.ReactNode }) => {
  const [jobs, setJobs] = useState<BurlaJob[]>([]);
  const [page, setPage] = useState(0);
  const [totalPages, setTotalPages] = useState(1);

  const fetchJobs = useCallback(async () => {
    try {
      const response = await fetch(`/v1/jobs_paginated?page=${page}`);
      const json = await response.json();

      const jobList = (json.jobs ?? []).map(createNewJob);

      setJobs(
        jobList.sort(
          (a, b) =>
            (b.started_at?.getTime() || 0) - (a.started_at?.getTime() || 0)
        )
      );

      if (json.total && json.limit) {
        setTotalPages(Math.max(1, Math.ceil(json.total / json.limit)));
      } else {
        setTotalPages(1);
      }
    } catch (err) {
      console.error("❌ Error fetching paginated jobs:", err);
    }
  }, [page]);

  useEffect(() => {
    fetchJobs();

    const eventSource = new EventSource("/v1/jobs_paginated?stream=true");

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);

        if (data.deleted) return;

        const newJob: BurlaJob = {
          id: data.jobId,
          status: data.status as JobsStatus,
          user: data.user || "Unknown",
          checked: false,
          n_inputs: typeof data.n_inputs === "number" ? data.n_inputs : 0,
          n_results: typeof data.n_results === "number" ? data.n_results : 0,
          started_at:
            typeof data.started_at === "number"
              ? new Date(data.started_at * 1000)
              : undefined,
        };

        setJobs((prev) => {
          const idx = prev.findIndex((j) => j.id === newJob.id);

          // If it's already in the list, update it
          if (idx !== -1) {
            const copy = [...prev];
            copy[idx] = { ...copy[idx], ...newJob };
            return copy;
          }

          // If you're on page 0, insert it at top and trim to 15
          if (page === 0) {
            const next = [newJob, ...prev].sort(
              (a, b) =>
                (b.started_at?.getTime() || 0) -
                (a.started_at?.getTime() || 0)
            );
            return next.slice(0, 15);
          }

          // Otherwise, ignore new jobs
          return prev;
        });
      } catch (err) {
        console.error("❌ Failed to parse SSE job update:", err);
      }
    };

    eventSource.onerror = (err) => {
      console.error("❌ SSE failed:", err);
      eventSource.close();
    };

    return () => eventSource.close();
  }, [page, fetchJobs]);

  return (
    <JobsContext.Provider value={{ jobs, setJobs, page, setPage, totalPages }}>
      {children}
    </JobsContext.Provider>
  );
};

const createNewJob = (data: any): BurlaJob => ({
  id: data.jobId,
  status: data.status as JobsStatus,
  user: data.user || "Unknown",
  checked: false,
  n_inputs: typeof data.n_inputs === "number" ? data.n_inputs : 0,
  n_results: typeof data.n_results === "number" ? data.n_results : 0,
  started_at:
    typeof data.started_at === "number"
      ? new Date(data.started_at * 1000)
      : undefined,
});

export const useJobs = () => useContext(JobsContext);