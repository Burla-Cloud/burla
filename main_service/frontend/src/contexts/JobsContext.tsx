// import { createContext, useContext, useEffect, useState, useCallback } from "react";
// import { BurlaJob, JobsStatus } from "@/types/coreTypes";

// interface JobsContextType {
//     jobs: BurlaJob[];
//     setJobs: React.Dispatch<React.SetStateAction<BurlaJob[]>>;
// }

// const JobsContext = createContext<JobsContextType>({
//     jobs: [],
//     setJobs: () => {},
// });

// export const JobsProvider = ({ children }: { children: React.ReactNode }) => {
//     const [jobs, setJobs] = useState<BurlaJob[]>([]);

//     /** ✅ Memoized Fetch Jobs Function */
//     const fetchJobs = useCallback(async () => {
//         try {
//             console.log("✅ Fetching initial job data...");
//             const response = await fetch("/v1/job_context");
//             if (!response.ok) throw new Error("Failed to fetch jobs");
//             const data = await response.json();
//             console.log("🔥 Fetched jobs:", data);

//             if (Array.isArray(data)) {
//                 setJobs(
//                     data.map(createNewJob).sort((a, b) => (b.started_at?.getTime() || 0) - (a.started_at?.getTime() || 0))
//                 );
//             } else {
//                 console.warn("⚠ Unexpected data format received:", data);
//                 setJobs([]);
//             }
//         } catch (error) {
//             console.error("🔥 Failed to fetch jobs:", error);
//         }
//     }, []);

//     useEffect(() => {
//         fetchJobs(); // Initial fetch on mount

//         const eventSource = new EventSource("/v1/job_context");
//         console.log("✅ SSE Connection opened...");

//         eventSource.onmessage = (event) => {
//             try {
//                 const jobData = JSON.parse(event.data);
//                 console.log("🔥 Received job update:", jobData);

//                 if (!jobData || typeof jobData !== "object") {
//                     console.error("❌ Invalid job data received:", jobData);
//                     return;
//                 }

//                 const parsedJobData = Array.isArray(jobData) ? jobData : [jobData];

//                 setJobs((prevJobs) => {
//                     let updatedJobs = [...prevJobs];

//                     parsedJobData.forEach((jobData) => {
//                         const jobId = jobData.jobId;

//                         if (jobData.deleted) {
//                             console.log(`🗑 Removing job ${jobId} (deleted by API)`);
//                             updatedJobs = updatedJobs.filter((job) => job.id !== jobId);
//                         } else {
//                             const existingIndex = updatedJobs.findIndex((job) => job.id === jobId);
//                             if (existingIndex === -1) {
//                                 console.log("🆕 Adding new job:", jobData);
//                                 updatedJobs.push(createNewJob(jobData));
//                             } else {
//                                 console.log("🔄 Updating job:", jobData);
//                                 updatedJobs[existingIndex] = {
//                                     ...updatedJobs[existingIndex],
//                                     status: jobData.status as JobsStatus,
//                                     user: jobData.user || "Unknown",
//                                     started_at: typeof jobData.started_at === "number"
//                                         ? new Date(jobData.started_at * 1000) // ✅ Convert UNIX timestamp
//                                         : updatedJobs[existingIndex].started_at,
//                                 };
//                             }
//                         }
//                     });

//                     // **🔥 Sort jobs by `started_at` (newest first)**
//                     return updatedJobs.sort((a, b) => (b.started_at?.getTime() || 0) - (a.started_at?.getTime() || 0));
//                 });
//             } catch (error) {
//                 console.error("🔥 Error parsing SSE message:", error);
//             }
//         };

//         eventSource.onerror = (error) => {
//             console.error("🔥 SSE Error:", error);
//             eventSource.close();
//         };

//         return () => {
//             console.log("🛑 Closing SSE connection...");
//             eventSource.close();
//         };
//     }, [fetchJobs]); // ✅ Depend on `fetchJobs` to avoid unnecessary re-renders

//     return <JobsContext.Provider value={{ jobs, setJobs }}>{children}</JobsContext.Provider>;
// };

// const createNewJob = (data: any): BurlaJob => ({
//     id: data.jobId,
//     status: data.status as JobsStatus,
//     user: data.user || "Unknown",
//     checked: data.checked ?? false,
//     n_inputs: typeof data.n_inputs === "number" ? data.n_inputs : 0, // ✅ add this
//     started_at: typeof data.started_at === "number"
//       ? new Date(data.started_at * 1000)
//       : undefined,
//   });

// export const useJobs = () => useContext(JobsContext);



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