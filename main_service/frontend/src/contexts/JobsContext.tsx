// import { createContext, useContext, useEffect, useState } from "react";
// import { BurlaJob, JobsStatus } from "@/types/cluster";

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

//     const fetchJobs = async () => {
//         try {
//             console.log("✅ Fetching initial job data...");
//             const response = await fetch("/v1/job_context");
//             if (!response.ok) throw new Error("Failed to fetch jobs");
//             const data = await response.json();
//             console.log("🔥 Fetched jobs:", data);

//             if (Array.isArray(data)) {
//                 setJobs(data.map(createNewJob)); // Properly format job data
//             } else {
//                 console.warn("⚠ Unexpected data format received:", data);
//                 setJobs([]);
//             }
//         } catch (error) {
//             console.error("🔥 Failed to fetch jobs:", error);
//         }
//     };

//     useEffect(() => {
//         fetchJobs(); // Initial fetch on mount

//         const eventSource = new EventSource("/v1/job_context");
//         console.log("✅ SSE Connection opened...");

//         eventSource.onmessage = (event) => {
//             try {
//                 const jobData = JSON.parse(event.data);
//                 console.log("🔥 Received job update:", jobData);

//                 // Ensure jobData is an array
//                 const parsedJobData = Array.isArray(jobData) ? jobData : [jobData];

//                 setJobs((prevJobs) => {
//                     let updatedJobs = [...prevJobs];

//                     parsedJobData.forEach((jobData) => {
//                         const jobId = jobData.jobId;

//                         // **Handle deleted jobs**
//                         if (jobData.deleted) {
//                             console.log(`🗑 Removing job ${jobId} (deleted by API)`);
//                             updatedJobs = updatedJobs.filter((job) => job.id !== jobId);
//                         } else {
//                             // **Check if the job exists, update or add it**
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
//                                     ? new Date(jobData.started_at * 1000) // ✅ Convert UNIX timestamp to Date
//                                     : updatedJobs[existingIndex].started_at, // Preserve old value if undefined
//                                 };
//                             }
//                         }
//                     });

//                     return updatedJobs;
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
//     }, []);

//     return <JobsContext.Provider value={{ jobs, setJobs }}>{children}</JobsContext.Provider>;
// };

// const createNewJob = (data: any): BurlaJob => ({
//     id: data.jobId,
//     status: data.status as JobsStatus,
//     user: data.user || "Unknown", // ✅ Add user
//     checked: data.checked ?? false,
//     started_at: typeof data.started_at === "number" 
//         ? new Date(data.started_at * 1000) // ✅ Convert UNIX timestamp (seconds) to Date
//         : undefined,

// });

// export const useJobs = () => useContext(JobsContext);







// import { createContext, useContext, useEffect, useState, useCallback } from "react";
// import { BurlaJob, JobsStatus } from "@/types/cluster";

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

// /** ✅ Helper function to format job data */
// const createNewJob = (data: any): BurlaJob => ({
//     id: data.jobId,
//     status: data.status as JobsStatus,
//     user: data.user || "Unknown", // ✅ Add user field
//     checked: data.checked ?? false,
//     started_at: typeof data.started_at === "number"
//         ? new Date(data.started_at * 1000) // ✅ Convert UNIX timestamp (seconds) to Date
//         : undefined,
// });

// export const useJobs = () => useContext(JobsContext);




import { createContext, useContext, useEffect, useState, useCallback } from "react";
import { BurlaJob, JobsStatus } from "@/types/cluster";

interface JobsContextType {
    jobs: BurlaJob[];
    setJobs: React.Dispatch<React.SetStateAction<BurlaJob[]>>;
    selectedFilter: "24h" | "7d" | "90d";
    setSelectedFilter: React.Dispatch<React.SetStateAction<"24h" | "7d" | "90d">>;
}

const JobsContext = createContext<JobsContextType>({
    jobs: [],
    setJobs: () => {},
    selectedFilter: "24h",
    setSelectedFilter: () => {},
});

export const JobsProvider = ({ children }: { children: React.ReactNode }) => {
    const [jobs, setJobs] = useState<BurlaJob[]>([]);
    const [selectedFilter, setSelectedFilter] = useState<"24h" | "7d" | "90d">("24h");

    /** ✅ Memoized Fetch Jobs Function */
    const fetchJobs = useCallback(async () => {
        try {
            console.log("✅ Fetching initial job data...");
            const response = await fetch("/v1/job_context");
            if (!response.ok) throw new Error("Failed to fetch jobs");
            const data = await response.json();
            console.log("🔥 Fetched jobs:", data);

            if (Array.isArray(data)) {
                setJobs(
                    data
                        .map(createNewJob)
                        .sort((a, b) => (b.started_at?.getTime() || 0) - (a.started_at?.getTime() || 0))
                );
            } else {
                console.warn("⚠ Unexpected data format received:", data);
                setJobs([]);
            }
        } catch (error) {
            console.error("🔥 Failed to fetch jobs:", error);
        }
    }, []);

    useEffect(() => {
        fetchJobs(); // Initial fetch on mount

        const eventSource = new EventSource("/v1/job_context");
        console.log("✅ SSE Connection opened...");

        eventSource.onmessage = (event) => {
            try {
                const jobData = JSON.parse(event.data);
                console.log("🔥 Received job update:", jobData);

                if (!jobData || typeof jobData !== "object") {
                    console.error("❌ Invalid job data received:", jobData);
                    return;
                }

                const parsedJobData = Array.isArray(jobData) ? jobData : [jobData];

                setJobs((prevJobs) => {
                    let updatedJobs = [...prevJobs];

                    parsedJobData.forEach((jobData) => {
                        const jobId = jobData.jobId;

                        if (jobData.deleted) {
                            console.log(`🗑 Removing job ${jobId} (deleted by API)`);
                            updatedJobs = updatedJobs.filter((job) => job.id !== jobId);
                        } else {
                            const existingIndex = updatedJobs.findIndex((job) => job.id === jobId);
                            if (existingIndex === -1) {
                                console.log("🆕 Adding new job:", jobData);
                                updatedJobs.push(createNewJob(jobData));
                            } else {
                                console.log("🔄 Updating job:", jobData);
                                updatedJobs[existingIndex] = {
                                    ...updatedJobs[existingIndex],
                                    status: jobData.status as JobsStatus,
                                    user: jobData.user || "Unknown",
                                    started_at: typeof jobData.started_at === "number"
                                        ? new Date(jobData.started_at * 1000) // ✅ Convert UNIX timestamp
                                        : updatedJobs[existingIndex].started_at,
                                };
                            }
                        }
                    });

                    return updatedJobs.sort((a, b) => (b.started_at?.getTime() || 0) - (a.started_at?.getTime() || 0));
                });
            } catch (error) {
                console.error("🔥 Error parsing SSE message:", error);
            }
        };

        eventSource.onerror = (error) => {
            console.error("🔥 SSE Error:", error);
            eventSource.close();
        };

        return () => {
            console.log("🛑 Closing SSE connection...");
            eventSource.close();
        };
    }, [fetchJobs]);

    return (
        <JobsContext.Provider value={{ jobs, setJobs, selectedFilter, setSelectedFilter }}>
            {children}
        </JobsContext.Provider>
    );
};

/** ✅ Helper function to format job data */
const createNewJob = (data: any): BurlaJob => ({
    id: data.jobId,
    status: data.status as JobsStatus,
    user: data.user || "Unknown",
    checked: data.checked ?? false,
    started_at: typeof data.started_at === "number"
        ? new Date(data.started_at * 1000) // ✅ Convert UNIX timestamp (seconds) to Date
        : undefined,
});

export const useJobs = () => useContext(JobsContext);


