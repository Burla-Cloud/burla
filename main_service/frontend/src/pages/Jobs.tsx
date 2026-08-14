import { JobsList } from "@/components/JobsList";
import { PageHeader } from "@/components/PageHeader";

const Jobs = () => {
  return (
    <div className="flex flex-1 flex-col min-w-0">
      <div className="mx-auto w-full max-w-6xl min-w-0">
        <PageHeader title="Jobs" />
        <JobsList />
      </div>
    </div>
  );
};

export default Jobs;
