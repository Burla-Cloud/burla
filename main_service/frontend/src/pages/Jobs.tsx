// import { JobsList } from "@/components/JobsList";
// import { useJobs } from "@/contexts/JobsContext";

// const Jobs = () => {
//     const { jobs, setJobs } = useJobs(); // Get jobs & setJobs from context

//     return (
//         <div className="flex-1 flex flex-col justify-start px-12 pt-0">
//             <div className="max-w-6xl mx-auto w-full">
//                 <h1 className="text-3xl font-bold mt-[-4px] mb-4" style={{ color: "#3b5a64" }}>
//                     Jobs
//                 </h1>
//                 <div className="space-y-6">
//                     <JobsList jobs={jobs} setJobs={setJobs} /> {/* Pass setJobs */}
//                 </div>
//             </div>
//         </div>
//     );
// };

// export default Jobs;


import { JobsList } from "@/components/JobsList";
import { useJobs } from "@/contexts/JobsContext";

const Jobs = () => {
    const { jobs, setJobs, selectedFilter, setSelectedFilter } = useJobs(); // ✅ Get filter state from context

    return (
        <div className="flex-1 flex flex-col justify-start px-12 pt-0">
            <div className="max-w-6xl mx-auto w-full">
                {/* ✅ Title & Filters in One Row */}
                <div className="flex justify-between items-center">
                    <h1 className="text-3xl font-bold mt-[-4px] mb-4" style={{ color: "#3b5a64" }}>
                        Jobs
                    </h1>

                    {/* ✅ Filter Buttons */}
                    <div className="flex space-x-2">
                        {["24h", "7d", "90d"].map((filter) => (
                            <button
                                key={filter}
                                className={`px-3 py-1 rounded-md text-sm ${
                                    selectedFilter === filter ? "text-white" : "bg-gray-200 text-gray-700"
                                  }`}
                                  style={{ backgroundColor: selectedFilter === filter ? "#3b5a64" : "transparent" }}
                                  
                                onClick={() => setSelectedFilter(filter as "24h" | "7d" | "90d")}
                            >
                                {filter === "24h" ? "Last 24h" : filter === "7d" ? "Past Week" : "Past 90 Days"}
                            </button>
                        ))}
                    </div>
                </div>

                <div className="space-y-6">
                <JobsList />
                </div>
            </div>
        </div>
    );
};

export default Jobs;


