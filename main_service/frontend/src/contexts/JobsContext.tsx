import { createContext, useContext, useEffect, useState, useCallback, useRef } from "react";
import { BurlaJob, JobsStatus } from "@/types/coreTypes";
import { managementEvents, managementJson } from "@/lib/managementApi";

interface JobsContextType {
    jobs: BurlaJob[];
    page: number;
    setPage: React.Dispatch<React.SetStateAction<number>>;
    totalPages: number;
    isLoading: boolean;
}

const JobsContext = createContext<JobsContextType>({
    jobs: [],
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
    const pageCursors = useRef<Record<number, string | null>>({ 0: null });

    const fetchJobs = useCallback(async () => { 
        setIsLoading(true);
        try {
            const cursor = pageCursors.current[page];
            const query = new URLSearchParams({ limit: "15", sort: "started_at", order: "desc" });
            if (cursor) query.set("cursor", cursor);
            const json = await managementJson<any>(`/jobs?${query}`);
            const jobList = (json.items ?? []).map(createNewJob);
            setJobs(jobList);
            pageCursors.current[page + 1] = json.next_cursor;
            setTotalPages(Math.max(1, Math.ceil((json.total_count ?? jobList.length) / 15)));
        } catch (err) {
            console.error("Error fetching jobs:", err);
        } finally {
            setIsLoading(false);
        }
    }, [page]);

    useEffect(() => {
        fetchJobs();
    }, [page, fetchJobs]);

    useEffect(() => {
        const update = (data: any) => {
            if (page !== 0) return;
            const newJob = createNewJob(data);
            setJobs((previous) => {
                const without = previous.filter((job) => job.id !== newJob.id);
                return [newJob, ...without]
                    .sort(
                        (a, b) =>
                            (b.started_at?.getTime() || 0) -
                            (a.started_at?.getTime() || 0)
                    )
                    .slice(0, 15);
            });
        };
        const source = managementEvents("/jobs/watch", {
            snapshot: (data) => {
                if (page === 0) setJobs((data.items ?? []).slice(0, 15).map(createNewJob));
            },
            update,
        });
        return () => {
            source.close();
        };
    }, [page]);

    return (
        <JobsContext.Provider value={{ jobs, page, setPage, totalPages, isLoading }}>
            {children}
        </JobsContext.Provider>
    );
};

const createNewJob = (data: any): BurlaJob => ({
    id: data.job_id,
    status: String(data.status || "unknown").toUpperCase() as JobsStatus,
    user: data.user || "Unknown",
    n_inputs: typeof data.input_count === "number" ? data.input_count : 0,
    n_results: typeof data.result_count === "number" ? data.result_count : 0,
    n_failed: typeof data.failed_count === "number" ? data.failed_count : 0,
    function_name: typeof data.function_name === "string" ? data.function_name : "Unknown",
    started_at: data.started_at ? new Date(data.started_at) : undefined,
    ended_at: data.ended_at ? new Date(data.ended_at) : undefined,
});

export const useJobs = () => useContext(JobsContext);
