import { JobsList } from "@/components/JobsList";
import { useJobs } from "@/contexts/JobsContext";

const Jobs = () => {
    const { jobs, setJobs } = useJobs(); // Get jobs & setJobs from context

    return (
        <div className="flex-1 flex flex-col justify-start px-12 pt-6">
            <div className="max-w-6xl mx-auto w-full">
                <h1 className="text-3xl font-bold mt-2 mb-6 text-primary">Jobs</h1>
                <div className="space-y-8">
                    <JobsList /> {/* Pass setJobs */}
                </div>
            </div>
        </div>
    );
};

export default Jobs;
