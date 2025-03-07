// import { useState } from "react";
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
// import { Play, Square } from "lucide-react"; // Import icons

// interface Job {
//     id: string;
//     status: "IN_QUEUE" | "RUNNING" | "FAILED" | "COMPLETED";
//     machine: string;
//     submitted: string;
//     checked: boolean;
// }

// // Job data with selection state
// const initialJobs: Job[] = [
//     { id: "job-001", status: "IN_QUEUE", machine: "Type A", submitted: "2025-03-04", checked: false },
//     { id: "job-002", status: "RUNNING", machine: "Type B", submitted: "2025-03-03", checked: false },
//     { id: "job-003", status: "FAILED", machine: "Type C", submitted: "2025-03-02", checked: false },
//     { id: "job-004", status: "COMPLETED", machine: "Type A", submitted: "2025-03-01", checked: false },
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
//     const [jobs, setJobs] = useState(initialJobs);
//     const anySelected = jobs.some((job) => job.checked);

//     const handleCheckboxChange = (id: string) => {
//         setJobs((prevJobs) =>
//             prevJobs.map((job) =>
//                 job.id === id ? { ...job, checked: !job.checked } : job
//             )
//         );
//     };

//     const handleSelectAllChange = () => {
//         const newSelectAll = !jobs.every((job) => job.checked);
//         setJobs((prevJobs) =>
//             prevJobs.map((job) => ({ ...job, checked: newSelectAll }))
//         );
//     };

//     return (
//         <div className="space-y-6 overflow-hidden">
//             <Card>
//                 <CardHeader className="flex items-center justify-between py-4">
//                     <span className="h-0" />
//                 </CardHeader>
//                 <CardContent>
//                     <Table className="w-full">
//                         <TableHeader>
//                             <TableRow className="align-middle">
//                                 <TableHead className="w-10">
//                                     <input
//                                         type="checkbox"
//                                         checked={jobs.every((job) => job.checked)}
//                                         onChange={handleSelectAllChange}
//                                         className="w-4 h-4 border-2 border-gray-400 rounded-none appearance-none checked:bg-[#3b5a64] checked:border-[#3b5a64] cursor-pointer"
//                                     />
//                                 </TableHead>
//                                 <TableHead>Status</TableHead>
//                                 <TableHead>Job</TableHead>
//                                 <TableHead>Machines</TableHead>
//                                 <TableHead>Submitted Date</TableHead>
//                                 <TableHead>
//                                 <div className="flex space-x-4 items-center">
//                                     <div className="relative group">
//                                         <button 
//                                             className={`text-[#3b5a64] hover:opacity-75 transition-opacity ${anySelected ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
//                                         >
//                                             <Play size={23} />
//                                         </button>
//                                         <span className="absolute top-full left-1/2 transform -translate-x-1/2 mt-2 px-2 py-1 text-xs text-white bg-gray-700 rounded opacity-0 group-hover:opacity-100 transition">
//                                             Run
//                                         </span>
//                                     </div>

//                                     <div className="relative group">
//                                         <button 
//                                             className={`text-[#3b5a64] hover:opacity-75 transition-opacity ${anySelected ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
//                                         >
//                                             <Square size={23} />
//                                         </button>
//                                         <span className="absolute top-full left-1/2 transform -translate-x-1/2 mt-2 px-2 py-1 text-xs text-white bg-gray-700 rounded opacity-0 group-hover:opacity-100 transition">
//                                             Stop
//                                         </span>
//                                     </div>
//                                 </div>
//                                 </TableHead>
//                                 <TableHead className="w-[5px] text-right" />
//                             </TableRow>
//                         </TableHeader>
//                         <TableBody>
//                             {jobs.map((job) => (
//                                 <TableRow key={job.id}>
//                                     <TableCell>
//                                         <input
//                                             type="checkbox"
//                                             checked={job.checked}
//                                             onChange={() => handleCheckboxChange(job.id)}
//                                             className="w-4 h-4 border-2 border-gray-400 rounded-none appearance-none checked:bg-[#3b5a64] checked:border-[#3b5a64] cursor-pointer"
//                                         />
//                                     </TableCell>
//                                     <TableCell>
//                                         <div className="flex items-center space-x-2">
//                                             <div className={getStatusClass(job.status)} />
//                                             <span className="text-sm capitalize">{job.status.replace("_", " ")}</span>
//                                         </div>
//                                     </TableCell>
//                                     <TableCell>{job.id}</TableCell>
//                                     <TableCell>{job.machine}</TableCell>
//                                     <TableCell>{job.submitted}</TableCell>
//                                     <TableCell className="w-[100px] text-right" />
//                                 </TableRow>
//                             ))}
//                         </TableBody>
//                     </Table>
//                 </CardContent>
//             </Card>
//         </div>
//     );
// };

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
import { Play, Square } from "lucide-react"; // Import icons
import { BurlaJob } from "@/types/cluster";

interface JobsListProps {
    jobs: BurlaJob[];
    setJobs: React.Dispatch<React.SetStateAction<BurlaJob[]>>;
}

export const JobsList = ({ jobs, setJobs }: JobsListProps) => {
    const anySelected = jobs.some((job) => job.checked);

    const handleCheckboxChange = (id: string) => {
        setJobs((prevJobs) =>
            prevJobs.map((job) =>
                job.id === id ? { ...job, checked: !job.checked } : job
            )
        );
    };

    const handleSelectAllChange = () => {
        const newSelectAll = !jobs.every((job) => job.checked);
        setJobs((prevJobs) =>
            prevJobs.map((job) => ({ ...job, checked: newSelectAll }))
        );
    };


    const getStatusClass = (status: BurlaJob["status"]) => {
        const statusClasses = {
            IN_QUEUE: "bg-gray-400",
            RUNNING: "bg-yellow-500 animate-pulse",
            FAILED: "bg-red-500",
            COMPLETED: "bg-green-500",
        };
        return cn("w-2 h-2 rounded-full", statusClasses[status]);
    };

    return (
        <div className="space-y-6 overflow-hidden">
            <Card>
                <CardHeader className="flex items-center justify-between py-4">
                    <span className="h-0" />
                </CardHeader>
                <CardContent>
                    <Table className="w-full">
                        <TableHeader>
                            <TableRow className="align-middle">
                                <TableHead className="w-10">
                                    <input
                                        type="checkbox"
                                        checked={jobs.every((job) => job.checked)}
                                        onChange={handleSelectAllChange}
                                        className="w-4 h-4 border-2 border-gray-400 rounded-none appearance-none checked:bg-[#3b5a64] checked:border-[#3b5a64] cursor-pointer"
                                    />
                                </TableHead>
                                <TableHead>Status</TableHead>
                                <TableHead>Job</TableHead>
                                <TableHead>Machines</TableHead>
                                <TableHead>Submitted At</TableHead>
                                <TableHead>
                                    <div className="flex space-x-4 items-center">
                                        <div className="relative group">
                                            <button 
                                                className={`text-[#3b5a64] hover:opacity-75 transition-opacity ${anySelected ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
                                            >
                                                <Play size={23} />
                                            </button>
                                            <span className="absolute top-full left-1/2 transform -translate-x-1/2 mt-2 px-2 py-1 text-xs text-white bg-gray-700 rounded opacity-0 group-hover:opacity-100 transition">
                                                Run
                                            </span>
                                        </div>

                                        <div className="relative group">
                                            <button 
                                                className={`text-[#3b5a64] hover:opacity-75 transition-opacity ${anySelected ? 'opacity-100' : 'opacity-0 pointer-events-none'}`}
                                            >
                                                <Square size={23} />
                                            </button>
                                            <span className="absolute top-full left-1/2 transform -translate-x-1/2 mt-2 px-2 py-1 text-xs text-white bg-gray-700 rounded opacity-0 group-hover:opacity-100 transition">
                                                Stop
                                            </span>
                                        </div>
                                    </div>
                                </TableHead>
                                <TableHead className="w-[5px] text-right" />
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {jobs.map((job) => (
                                <TableRow key={job.id}>
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
                                    <TableCell>{job.submitted_date?.toLocaleDateString() || "N/A"}</TableCell>
                                    <TableCell className="w-[100px] text-right" />
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </CardContent>
            </Card>
        </div>
    );
};


