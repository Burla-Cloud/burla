import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { Cpu } from "lucide-react";
import { NodeStatus, BurlaNode } from "@/types/coreTypes";

interface NodesListProps {
    nodes: BurlaNode[];
}

export const NodesList = ({ nodes }: NodesListProps) => {
    const getStatusClass = (nodeStatus: NodeStatus | null) => {
        const statusClasses = {
            READY: "bg-green-500",
            RUNNING: "bg-green-500 animate-pulse",
            BOOTING: "bg-yellow-500 animate-pulse",
            STOPPING: "bg-gray-300 animate-pulse",
        };
        return cn("w-2 h-2 rounded-full", nodeStatus ? statusClasses[nodeStatus] : "bg-gray-300");
    };

    const extractCpuCount = (type: string): number | null => {
        const customMatch = type.match(/^custom-(\d+)-/);
        if (customMatch) return parseInt(customMatch[1], 10);

        const standardMatch = type.match(/-(\d+)$/);
        return standardMatch ? parseInt(standardMatch[1], 10) : null;
    };

    return (
        <div className="space-y-6">
            <Card className="w-full">
                <CardHeader>
                    <CardTitle className="text-xl font-semibold text-[#3b5a64]">
                        Welcome to Burla!
                    </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                    <div className="grid grid-cols-1 gap-4">
                        <div>
                            This is our demo cluster (Burla is built to be self-hosted). Use it for
                            free — just not for anything critical.
                            <br />
                            Click “Start” to boot eight 32-CPU machines (takes ~1–2 minutes), and
                            “Stop” to shut them down.
                            <br />
                            Machines auto-terminate after 10 minutes of inactivity.{" "}
                            <a
                                href="mailto:jake@burla.dev"
                                className="text-blue-500 hover:underline"
                            >
                                Email me
                            </a>{" "}
                            to adjust this or any other setting.
                            <br />
                            <br />
                            Need help? Check out our{" "}
                            <a
                                href="https://colab.research.google.com/drive/17MWiQFyFKxTmNBaq7POGL0juByWIMA3w?usp=sharing"
                                className="text-blue-500 hover:underline"
                            >
                                quickstart
                            </a>
                            ,{" "}
                            <a
                                href="https://docs.burla.dev"
                                className="text-blue-500 hover:underline"
                            >
                                docs
                            </a>
                            , or{" "}
                            <a
                                href="mailto:jake@burla.dev"
                                className="text-blue-500 hover:underline"
                            >
                                shoot me an email
                            </a>
                            .
                            <br />
                            Thanks for trying Burla!
                        </div>
                    </div>
                </CardContent>
            </Card>

            <Card className="w-full">
                <CardHeader className="flex flex-row items-center justify-between">
                    <CardTitle className="text-xl font-semibold text-[#3b5a64]">
                        Nodes
                    </CardTitle>
                </CardHeader>
                <CardContent>
                    <Table>
                        <TableHeader>
                            <TableRow>
                                <TableHead>Status</TableHead>
                                <TableHead>Name</TableHead>
                                <TableHead>Type</TableHead>
                                <TableHead>CPUs</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {nodes.map((node) => (
                                <TableRow key={node.id}>
                                    <TableCell>
                                        <div className="flex items-center space-x-2">
                                            <div className={getStatusClass(node.status)} />
                                            <span className={cn("text-sm capitalize", node.status)}>
                                                {node.status}
                                            </span>
                                        </div>
                                    </TableCell>
                                    <TableCell>{node.name}</TableCell>
                                    <TableCell>{node.type}</TableCell>
                                    <TableCell>
                                        <div className="flex items-center space-x-1">
                                            <Cpu className="h-4 w-4" />
                                            <span>
                                                {node.cpus ?? extractCpuCount(node.type) ?? "?"}
                                            </span>
                                        </div>
                                    </TableCell>
                                </TableRow>
                            ))}
                        </TableBody>
                    </Table>
                </CardContent>
            </Card>
        </div>
    );
};