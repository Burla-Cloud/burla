import { JobsList } from "@/components/JobsList";
import { useJobs } from "@/contexts/JobsContext";

const Jobs = () => {
  useJobs(); // keep the hook call so context loads; you don't need to destructure here

  return (
    <div className="flex-1 flex flex-col justify-start px-12 pt-6 min-w-0">
      <div className="max-w-6xl mx-auto w-full min-w-0">
        <h1 className="text-2xl font-bold mt-2 mb-6 text-primary">Jobs</h1>
        <div className="space-y-8 min-w-0">
          <JobsList />
        </div>
      </div>
    </div>
  );
};

export default Jobs;
