import { JobsList } from "@/components/JobsList";
import { useJobs } from "@/contexts/JobsContext";

const Jobs = () => {
    const { jobs, setJobs } = useJobs(); // Get jobs & setJobs from context

    return (
        <div className="flex-1 flex flex-col justify-start px-12 pt-0">
            <div className="max-w-6xl mx-auto w-full">
                <h1 className="text-3xl font-bold mt-[-4px] mb-4" style={{ color: "#3b5a64" }}>
                    Jobs
                </h1>
                <div className="space-y-6">
                    <JobsList jobs={jobs} setJobs={setJobs} /> {/* Pass setJobs */}
                </div>
            </div>
        </div>
    );
};

export default Jobs;