import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Link } from "react-router-dom";
import {
    Table,
    TableBody,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import { Cpu, X } from "lucide-react";
import { NodeStatus, BurlaNode } from "@/types/coreTypes";
import { useEffect, useState } from "react";

interface NodesListProps {
    nodes: BurlaNode[];
}

export const NodesList = ({ nodes }: NodesListProps) => {
    const [showWelcome, setShowWelcome] = useState(true);

    useEffect(() => {
        const isWelcomeHidden = localStorage.getItem("welcomeMessageHidden") === "true";
        setShowWelcome(!isWelcomeHidden);
    }, []);

    const handleDismissWelcome = () => {
        setShowWelcome(false);
        localStorage.setItem("welcomeMessageHidden", "true");
    };

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
            {showWelcome && (
                <Card className="w-full relative">
                    <button
                        onClick={handleDismissWelcome}
                        className="absolute top-2 right-2 p-1 hover:bg-gray-100 rounded-full"
                        aria-label="Dismiss welcome message"
                    >
                        <X className="h-4 w-4" />
                    </button>
                    <CardHeader>
                        <CardTitle className="text-xl font-semibold text-primary">
                            Welcome to Burla!
                        </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="grid grid-cols-1 gap-4">
                            <div>
                                Click "Start" to boot the cluster,
                                <br />
                                See the{" "}
                                <Link to="/settings" className="text-blue-500 hover:underline">
                                    settings tab
                                </Link>{" "}
                                to change the machine type, quantity, or container your code runs
                                inside.
                                <br />
                                <br />
                                Don't hesitate to{" "}
                                <a
                                    href="mailto:jake@burla.dev"
                                    className="text-blue-500 hover:underline"
                                >
                                    email us
                                </a>{" "}
                                with feature requests, changes, or for free 1 on 1 help!
                                <br />
                                Be sure to check out our{" "}
                                <a
                                    href="https://docs.burla.dev"
                                    className="text-blue-500 hover:underline"
                                >
                                    documentation
                                </a>
                                , and thank you for using Burla!
                            </div>
                        </div>
                    </CardContent>
                </Card>
            )}

            <Card className="w-full">
                <CardHeader className="flex flex-row items-center justify-between">
                    <CardTitle className="text-xl font-semibold text-primary">Nodes</CardTitle>
                </CardHeader>
                <CardContent>
                    <Table className="table-fixed w-full">
                        {/* Define four columns with equal widths */}
                        <colgroup>
                            <col className="w-1/4" />
                            <col className="w-1/4" />
                            <col className="w-1/4" />
                            <col className="w-1/4" />
                        </colgroup>
                        <TableHeader>
                            <TableRow>
                                <TableHead className="px-4 py-2">Status</TableHead>
                                <TableHead className="px-4 py-2">Name</TableHead>
                                <TableHead className="px-4 py-2">Type</TableHead>
                                <TableHead className="px-4 py-2 text-center">CPUs</TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {nodes.map((node) => (
                                <TableRow key={node.id}>
                                    <TableCell className="px-4 py-2">
                                        <div className="flex items-center space-x-2">
                                            <div className={getStatusClass(node.status)} />
                                            <span className={cn("text-sm capitalize", node.status)}>
                                                {node.status}
                                            </span>
                                        </div>
                                    </TableCell>
                                    <TableCell className="px-4 py-2">{node.name}</TableCell>
                                    <TableCell className="px-4 py-2">{node.type}</TableCell>
                                    <TableCell className="px-4 py-2 text-center">
                                        <div className="inline-flex items-center space-x-1 justify-center">
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
