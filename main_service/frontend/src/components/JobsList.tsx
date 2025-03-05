// import { Card, CardContent, CardHeader } from "@/components/ui/card";
// import {
//     Table,
//     TableBody,
//     TableCell,
//     TableHead,
//     TableHeader,
//     TableRow,
// } from "@/components/ui/table";
// import { cn } from "@/lib/utils";

// interface Job {
//     id: string;
//     status: "IN_QUEUE" | "RUNNING" | "FAILED" | "COMPLETED";
//     machine: string;
//     submitted: string;
// }

// // Job data
// const jobs: Job[] = [
//     { id: "job-001", status: "IN_QUEUE", machine: "Type A", submitted: "2025-03-04" },
//     { id: "job-002", status: "RUNNING", machine: "Type B", submitted: "2025-03-03" },
//     { id: "job-003", status: "FAILED", machine: "Type C", submitted: "2025-03-02" },
//     { id: "job-004", status: "COMPLETED", machine: "Type A", submitted: "2025-03-01" },
// ];

// // Status dot color mapping
// const getStatusClass = (status: Job["status"]) => {
//     const statusClasses = {
//         IN_QUEUE: "bg-gray-400",
//         RUNNING: "bg-yellow-500 animate-pulse",
//         FAILED: "bg-red-500",
//         COMPLETED: "bg-green-500",
//     };
//     return cn("w-2 h-2 rounded-full", statusClasses[status]);
// };

// export const JobsList = () => {
//     return (
//         <div className="space-y-6">
//             <Card>
//                 <CardHeader className="flex flex-row items-center justify-between" />
//                 <CardContent className="mt-[-30px]">
//                     <Table>
//                         <TableHeader>
//                             <TableRow>
//                                 <TableHead>Status</TableHead>
//                                 <TableHead>Job</TableHead>
//                                 <TableHead>Machines</TableHead>
//                                 <TableHead>Submitted Date</TableHead>
//                             </TableRow>
//                         </TableHeader>
//                         <TableBody>
//                             {jobs.map((job) => (
//                                 <TableRow key={job.id}>
//                                     <TableCell>
//                                         <div className="flex items-center space-x-2">
//                                             <div className={getStatusClass(job.status)} />
//                                             <span className="text-sm capitalize">{job.status.replace("_", " ")}</span>
//                                         </div>
//                                     </TableCell>
//                                     <TableCell>{job.id}</TableCell>
//                                     <TableCell>{job.machine}</TableCell>
//                                     <TableCell>{job.submitted}</TableCell>
//                                 </TableRow>
//                             ))}
//                         </TableBody>
//                     </Table>
//                 </CardContent>
//             </Card>
//         </div>
//     );
// };

import { useState } from "react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { Play, Pause, Square, Trash } from "lucide-react"; // Import icons

interface Job {
    id: string;
    status: "IN_QUEUE" | "RUNNING" | "FAILED" | "COMPLETED";
    machine: string;
    submitted: string;
    checked: boolean;
}

// Job data with selection state
const initialJobs: Job[] = [
    { id: "job-001", status: "IN_QUEUE", machine: "Type A", submitted: "2025-03-04", checked: false },
    { id: "job-002", status: "RUNNING", machine: "Type B", submitted: "2025-03-03", checked: false },
    { id: "job-003", status: "FAILED", machine: "Type C", submitted: "2025-03-02", checked: false },
    { id: "job-004", status: "COMPLETED", machine: "Type A", submitted: "2025-03-01", checked: false },
];

// Status dot color mapping
const getStatusClass = (status: Job["status"]) => {
    const statusClasses = {
        IN_QUEUE: "bg-gray-400",
        RUNNING: "bg-yellow-500 animate-pulse",
        FAILED: "bg-red-500",
        COMPLETED: "bg-green-500",
    };
    return cn("w-2 h-2 rounded-full", statusClasses[status]);
};

export const JobsList = () => {
    const [jobs, setJobs] = useState(initialJobs);

    // Check if all jobs are selected
    const allSelected = jobs.every((job) => job.checked);

    // Toggle individual checkbox
    const handleCheckboxChange = (id: string) => {
        setJobs((prevJobs) =>
            prevJobs.map((job) =>
                job.id === id ? { ...job, checked: !job.checked } : job
            )
        );
    };

    // Toggle "Select All" checkbox
    const handleSelectAllChange = () => {
        const newSelectAll = !allSelected;
        setJobs((prevJobs) =>
            prevJobs.map((job) => ({ ...job, checked: newSelectAll }))
        );
    };

    return (
        <div className="space-y-6 overflow-hidden"> {/* Prevents Scrollbar */}
            <Card>
                <CardHeader className="flex flex-row items-center justify-between" />
                <CardContent className="mt-[-30px] pb-6"> {/* Fixes scrollbar issue */}
                    <div className="overflow-hidden">
                        <Table className="w-full">
                            <TableHeader>
                                <TableRow>
                                    {/* Select All Checkbox */}
                                    <TableHead className="w-10">
                                        <input
                                            type="checkbox"
                                            checked={allSelected}
                                            onChange={handleSelectAllChange}
                                            className="w-4 h-4 border-2 border-gray-400 rounded-none appearance-none checked:bg-[#3b5a64] checked:border-[#3b5a64] cursor-pointer"
                                        />
                                    </TableHead>
                                    <TableHead>Status</TableHead>
                                    <TableHead>Job</TableHead>
                                    <TableHead>Machines</TableHead>
                                    <TableHead>Submitted Date</TableHead>

                                    {/* Reserve space for header buttons (Fix shifting issue) */}
                                    <TableHead className="w-[170px]">
                                        <div className="flex justify-start space-x-3 ml-2 min-h-[32px]">
                                            {/* Buttons always present but hidden unless selected */}
                                            {["Run", "Pause", "Stop", "Delete"].map((action, i) => (
                                                <button key={i} className={`text-[#3b5a64] hover:opacity-75 relative group ${allSelected ? "opacity-100" : "opacity-0 pointer-events-none"}`}>
                                                    {i === 0 && <Play size={21} />}
                                                    {i === 1 && <Pause size={21} />}
                                                    {i === 2 && <Square size={21} />}
                                                    {i === 3 && <Trash size={21} />}
                                                    <span className="absolute top-full left-1/2 transform -translate-x-1/2 mt-1 text-[12px] text-white bg-[#3b5a64] px-1 py-0.5 rounded opacity-0 group-hover:opacity-100 transition">
                                                        {action}
                                                    </span>
                                                </button>
                                            ))}
                                        </div>
                                    </TableHead>
                                </TableRow>
                            </TableHeader>
                            <TableBody>
                                {jobs.map((job, index) => (
                                    <TableRow 
                                        key={job.id} 
                                        className={`min-h-[50px] ${index === jobs.length - 1 ? "mb-[25px]" : ""}`} // Adds space to last row
                                    >
                                        <TableCell>
                                            <input
                                                type="checkbox"
                                                checked={job.checked}
                                                onChange={() => handleCheckboxChange(job.id)}
                                                className="w-4 h-4 border-2 border-gray-400 rounded-none appearance-none checked:bg-[#3b5a64] checked:border-[#3b5a64] cursor-pointer"
                                            />
                                        </TableCell>
                                        <TableCell>
                                            <div className="flex items-center space-x-2">
                                                <div className={getStatusClass(job.status)} />
                                                <span className="text-sm capitalize">{job.status.replace("_", " ")}</span>
                                            </div>
                                        </TableCell>
                                        <TableCell>{job.id}</TableCell>
                                        <TableCell>{job.machine}</TableCell>
                                        <TableCell>{job.submitted}</TableCell>
                                        <TableCell className="w-[170px] min-h-[32px]">
                                            <div className="flex justify-start space-x-3 ml-2">
                                                {["Run", "Pause", "Stop", "Delete"].map((action, i) => (
                                                    <button key={i} className="text-[#3b5a64] hover:opacity-75 relative group">
                                                        {i === 0 && <Play size={21} />}
                                                        {i === 1 && <Pause size={21} />}
                                                        {i === 2 && <Square size={21} />}
                                                        {i === 3 && <Trash size={21} />}
                                                        <span className="absolute top-full left-1/2 transform -translate-x-1/2 mt-1 text-[12px] text-white bg-[#3b5a64] px-1 py-0.5 rounded opacity-0 group-hover:opacity-100 transition">
                                                            {action}
                                                        </span>
                                                    </button>
                                                ))}
                                            </div>
                                        </TableCell>
                                    </TableRow>
                                ))}
                                {/* Spacer row to push last row up by 25px */}
                                <TableRow className="h-[15px]"></TableRow>
                            </TableBody>
                        </Table>
                    </div>
                </CardContent>
            </Card>
        </div>
    );
};
