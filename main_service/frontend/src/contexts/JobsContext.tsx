import { createContext, useContext, useEffect, useState } from "react";
import { BurlaJob, JobsStatus } from "@/types/cluster";

interface JobsContextType {
    jobs: BurlaJob[];
    setJobs: React.Dispatch<React.SetStateAction<BurlaJob[]>>;
}

const JobsContext = createContext<JobsContextType>({
    jobs: [],
    setJobs: () => {},
});

export const JobsProvider = ({ children }: { children: React.ReactNode }) => {
    const [jobs, setJobs] = useState<BurlaJob[]>([]);

    const fetchJobs = async () => {
        try {
            console.log("✅ Fetching initial job data...");
            const response = await fetch("/v1/job_context");
            if (!response.ok) throw new Error("Failed to fetch jobs");
            const data = await response.json();
            console.log("🔥 Fetched jobs:", data);

            if (Array.isArray(data)) {
                setJobs(data.map(createNewJob)); // Properly format job data
            } else {
                console.warn("⚠ Unexpected data format received:", data);
                setJobs([]);
            }
        } catch (error) {
            console.error("🔥 Failed to fetch jobs:", error);
        }
    };

    useEffect(() => {
        fetchJobs(); // Initial fetch on mount

        const eventSource = new EventSource("/v1/job_context");
        console.log("✅ SSE Connection opened...");

        eventSource.onmessage = (event) => {
            try {
                const jobData = JSON.parse(event.data);
                console.log("🔥 Received job update:", jobData);

                // Ensure jobData is an array
                const parsedJobData = Array.isArray(jobData) ? jobData : [jobData];

                setJobs((prevJobs) => {
                    let updatedJobs = [...prevJobs];

                    parsedJobData.forEach((jobData) => {
                        const jobId = jobData.jobId;

                        // **Handle deleted jobs**
                        if (jobData.deleted) {
                            console.log(`🗑 Removing job ${jobId} (deleted by API)`);
                            updatedJobs = updatedJobs.filter((job) => job.id !== jobId);
                        } else {
                            // **Check if the job exists, update or add it**
                            const existingIndex = updatedJobs.findIndex((job) => job.id === jobId);
                            if (existingIndex === -1) {
                                console.log("🆕 Adding new job:", jobData);
                                updatedJobs.push(createNewJob(jobData));
                            } else {
                                console.log("🔄 Updating job:", jobData);
                                updatedJobs[existingIndex] = {
                                    ...updatedJobs[existingIndex],
                                    status: jobData.status as JobsStatus,
                                    machine: jobData.machine || "Unknown",
                                };
                            }
                        }
                    });

                    return updatedJobs;
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
    }, []);

    return <JobsContext.Provider value={{ jobs, setJobs }}>{children}</JobsContext.Provider>;
};

const createNewJob = (data: any): BurlaJob => ({
    id: data.jobId,
    status: data.status as JobsStatus,
    machine: data.machine || "Unknown",
    checked: data.checked ?? false,
    submitted_date: data.submitted_date ? new Date(data.submitted_date) : undefined,
});

export const useJobs = () => useContext(JobsContext);

