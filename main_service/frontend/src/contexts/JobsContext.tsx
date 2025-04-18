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
  
        console.log("✅ Raw response from /v1/jobs_paginated:", json);
  
        const jobList = Array.isArray(json)
          ? json.map(createNewJob)
          : json.jobs?.map(createNewJob) || [];
  
        setJobs(
          jobList.sort(
            (a, b) =>
              (b.started_at?.getTime() || 0) -
              (a.started_at?.getTime() || 0)
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
      console.log("📡 Connected to /v1/jobs_paginated live stream");
  
      eventSource.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);
  
          setJobs((prevJobs) => {
            const index = prevJobs.findIndex((job) => job.id === data.jobId);
            if (index === -1) return prevJobs;
  
            const updated = [...prevJobs];
            updated[index] = {
              ...updated[index],
              status: data.status || updated[index].status,
              user: data.user || updated[index].user,
              started_at:
                typeof data.started_at === "number"
                  ? new Date(data.started_at * 1000)
                  : updated[index].started_at,
            };
            return updated;
          });
        } catch (err) {
          console.error("❌ Error parsing job update SSE:", err);
        }
      };
  
      eventSource.onerror = (err) => {
        console.error("❌ SSE connection failed:", err);
        eventSource.close();
      };
  
      return () => {
        console.log("🛑 Closing job updates SSE");
        eventSource.close();
      };
    }, [page]); // only reconnect when page changes
  
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
    started_at:
      typeof data.started_at === "number"
        ? new Date(data.started_at * 1000)
        : undefined,
  });
  
  export const useJobs = () => useContext(JobsContext);