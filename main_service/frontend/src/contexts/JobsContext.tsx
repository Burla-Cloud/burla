import { createContext, useContext, useEffect, useState, useCallback } from "react";
import { BurlaJob, JobsStatus } from "@/types/coreTypes";

interface JobsContextType {
    jobs: BurlaJob[];
    setJobs: React.Dispatch<React.SetStateAction<BurlaJob[]>>;
    page: number;
    setPage: React.Dispatch<React.SetStateAction<number>>;
    totalPages: number;
    isLoading: boolean;
}

const JobsContext = createContext<JobsContextType>({
    jobs: [],
    setJobs: () => {},
    page: 0,
    setPage: () => {},
    totalPages: 1,
    isLoading: false,
});

export const JobsProvider = ({ children }: { children: React.ReactNode }) => {
    const [jobs, setJobs] = useState<BurlaJob[]>([]);
    const [page, setPage] = useState(0);
    const [totalPages, setTotalPages] = useState(1);
    const [isLoading, setIsLoading] = useState(false);

    const fetchJobs = useCallback(async () => {
        setIsLoading(true);
        try {
            const response = await fetch(`/v1/jobs_paginated?page=${page}`);
            const json = await response.json();
            const jobList = (json.jobs ?? []).map(createNewJob);

            setJobs((prev) => {
                if (page !== 0) {
                    return jobList;
                }

                const existingIds = new Set(jobList.map((j) => j.id));
                const preservedFromSSE = prev.filter((j) => !existingIds.has(j.id));

                return [...jobList, ...preservedFromSSE]
                    .filter((job, index, arr) => arr.findIndex((j) => j.id === job.id) === index)
                    .sort((a, b) => (b.started_at?.getTime() || 0) - (a.started_at?.getTime() || 0))
                    .slice(0, 15);
            });

            if (json.total && json.limit) {
                setTotalPages(Math.max(1, Math.ceil(json.total / json.limit)));
            } else {
                setTotalPages(1);
            }
        } catch (err) {
            console.error("❌ Error fetching paginated jobs:", err);
        } finally {
            setIsLoading(false);
        }
    }, [page]);

    useEffect(() => {
        fetchJobs();
    }, [page, fetchJobs]);

    useEffect(() => {
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
                    function_name:
                        typeof data.function_name === "string" ? data.function_name : "Unknown",
                    started_at:
                        typeof data.started_at === "number"
                            ? new Date(data.started_at * 1000)
                            : undefined,
                };

                setJobs((prevJobs) => {
                    const idx = prevJobs.findIndex((j) => j.id === newJob.id);
                    if (idx !== -1) {
                        const updated = [...prevJobs];
                        updated[idx] = { ...updated[idx], ...newJob };
                        return updated;
                    }

                    if (page === 0) {
                        return [newJob, ...prevJobs]
                            .filter(
                                (job, index, arr) => arr.findIndex((j) => j.id === job.id) === index
                            )
                            .sort(
                                (a, b) =>
                                    (b.started_at?.getTime() || 0) - (a.started_at?.getTime() || 0)
                            )
                            .slice(0, 15);
                    }

                    return prevJobs;
                });
            } catch (err) {
                console.error("❌ Failed to parse SSE job update:", err);
            }
        };

        eventSource.onerror = (err) => {
            // Safari and proxies may emit transient errors; let EventSource auto-reconnect
            console.warn("SSE transient error (jobs_paginated), will auto-reconnect:", err);
        };

        return () => eventSource.close();
    }, [page]);

    return (
        <JobsContext.Provider value={{ jobs, setJobs, page, setPage, totalPages, isLoading }}>
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
    function_name: typeof data.function_name === "string" ? data.function_name : "Unknown",
    started_at: typeof data.started_at === "number" ? new Date(data.started_at * 1000) : undefined,
});

export const useJobs = () => useContext(JobsContext);
