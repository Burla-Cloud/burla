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

        // n4-standard-16 -> captures 16
        const standardMatch = type.match(/-(\d+)$/);
        if (standardMatch) return parseInt(standardMatch[1], 10);

        // GPU machine types like a2-highgpu-4g
        const gpuMatch = type.match(/^(a\d-(highgpu|ultragpu|megagpu|edgegpu))-([\d]+)g$/);
        if (gpuMatch) {
            const family = gpuMatch[1];
            const gpus = parseInt(gpuMatch[3], 10);

            const cpuTable: Record<string, Record<number, number>> = {
                "a2-highgpu": { 1: 12, 2: 24, 4: 48, 8: 96 },
                "a2-ultragpu": { 1: 12, 2: 24, 4: 48, 8: 96 },
                "a2-megagpu": { 16: 96 },
                "a3-highgpu": { 1: 26, 2: 52, 4: 104, 8: 208 },
                "a3-ultragpu": { 8: 224 },
                "a3-edgegpu": { 8: 208 },
            };

            const cpus = cpuTable[family]?.[gpus];
            if (cpus) return cpus;
        }

        return null;
    };

    const parseGpuDisplay = (type: string): string => {
        const lower = type.toLowerCase();

        const gpuPatterns: { prefix: string; model: string; vram: string }[] = [
            { prefix: "a2-highgpu-", model: "A100", vram: "40G" },
            { prefix: "a2-ultragpu-", model: "A100", vram: "80G" },
            { prefix: "a2-megagpu-", model: "A100", vram: "40G" },
            { prefix: "a3-highgpu-", model: "H100", vram: "80G" },
            { prefix: "a3-ultragpu-", model: "H200", vram: "141G" },
        ];

        for (const { prefix, model, vram } of gpuPatterns) {
            if (lower.startsWith(prefix)) {
                const countMatch = lower.match(/-(\d+)g$/);
                if (countMatch) {
                    const count = parseInt(countMatch[1], 10);
                    return `${count}x ${model} ${vram}`;
                }
            }
        }

        return "-"; // CPU-only machine
    };

    const parseRamDisplay = (type: string): string => {
        const lower = type.toLowerCase();

        if (lower.startsWith("n4-standard-")) {
            const cpu = extractCpuCount(type);
            if (cpu !== null) return `${cpu * 4}G`;
        }

        const ramTable: Record<string, Record<number, string>> = {
            "a2-highgpu": { 1: "85G", 2: "170G", 4: "340G", 8: "680G", 16: "1360G" },
            "a2-ultragpu": { 1: "170G", 2: "340G", 4: "680G", 8: "1360G" },
            "a2-megagpu": { 16: "1360G" },
            "a3-highgpu": { 1: "234G", 2: "468G", 4: "936G", 8: "1872G" },
            "a3-ultragpu": { 8: "2952G" },
        };

        const match = lower.match(/^(a\d-(highgpu|ultragpu|megagpu|edgegpu))-([\d]+)g$/);
        if (match) {
            const family = match[1];
            const count = parseInt(match[3], 10);
            const sizes = ramTable[family];
            if (sizes && sizes[count]) return sizes[count];
        }

        return "-";
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
                        <colgroup>
                            <col className="w-1/5" />
                            <col className="w-1/5" />
                            <col className="w-1/5" />
                            <col className="w-1/5" />
                            <col className="w-1/5" />
                        </colgroup>
                        <TableHeader>
                            <TableRow>
                                <TableHead className="px-4 py-2">Status</TableHead>
                                <TableHead className="px-4 py-2">Name</TableHead>
                                <TableHead className="px-4 py-2">GPUs</TableHead>
                                <TableHead className="px-4 py-2 text-center">CPUs</TableHead>
                                <TableHead className="px-4 py-2 text-center">RAM</TableHead>
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
                                    <TableCell className="px-4 py-2">
                                        {parseGpuDisplay(node.type)}
                                    </TableCell>
                                    <TableCell className="px-4 py-2 text-center">
                                        <div className="inline-flex items-center space-x-1 justify-center">
                                            <Cpu className="h-4 w-4" />
                                            <span>
                                                {node.cpus ?? extractCpuCount(node.type) ?? "?"}
                                            </span>
                                        </div>
                                    </TableCell>
                                    <TableCell className="px-4 py-2 text-center">
                                        {parseRamDisplay(node.type)}
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
