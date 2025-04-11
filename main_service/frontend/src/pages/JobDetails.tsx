import { useParams, useNavigate } from "react-router-dom";
import { useJobs } from "@/contexts/JobsContext";
import JobLogs from "@/components/JobLogs";

const JobDetails = () => {
  const { jobId } = useParams<{ jobId: string }>();
  const { jobs } = useJobs();
  const navigate = useNavigate();

  const job = jobs.find((j) => j.id === jobId);

  if (!job) {
    return (
      <div className="flex-1 flex flex-col justify-start px-12 pt-0">
        <div className="max-w-6xl mx-auto w-full">
          <h1 className="text-3xl font-bold mt-[-4px] mb-4 text-red-600">
            Job not found
          </h1>
          <button
            onClick={() => navigate("/jobs")}
            className="text-[#3b5a64] hover:underline decoration-[0.5px] underline-offset-2 transition"
          >
            Back to Jobs
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 flex flex-col justify-start px-12 pt-0">
      <div className="max-w-6xl mx-auto w-full">
        {/* Header with breadcrumb + job ID */}
        <h1 className="text-3xl font-bold mt-[-4px] mb-6" style={{ color: "#3b5a64" }}>
          <button
            onClick={() => navigate("/jobs")}
            className="hover:underline decoration-[0.5px] underline-offset-2 transition text-inherit"
          >
            Jobs
          </button>
          <span className="mx-2 text-inherit">›</span>
          <span className="text-inherit">{job.id}</span>
        </h1>

        {/* Status and Started At side-by-side */}
        <div className="flex flex-row items-center text-sm text-gray-600 mb-6 space-x-8">
          <div>
            <strong>Status:</strong> {job.status}
          </div>
          <div>
            <strong>Started At:</strong>{" "}
            {job.started_at?.toLocaleString("en-US", {
              year: "numeric",
              month: "short",
              day: "2-digit",
              hour: "2-digit",
              minute: "2-digit",
              second: "2-digit",
              hour12: true,
            }) || "N/A"}
          </div>
        </div>
        <JobLogs jobId={job.id} />
      </div>
    </div>
  );
};

export default JobDetails;