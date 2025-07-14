import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ClusterStatus } from "@/types/coreTypes";
import { cn } from "@/lib/utils";

interface ClusterStatusCardProps {
    status: ClusterStatus;
    parallelism: number;
    totalRam: string;
    gpuSummary: string;
}

const statusConfig = {
    ON: { color: "bg-green-500", text: "On", pulse: false },
    OFF: { color: "bg-gray-500", text: "Off", pulse: false },
    REBOOTING: { color: "bg-yellow-500", text: "Rebooting", pulse: true },
    BOOTING: { color: "bg-yellow-500", text: "Starting", pulse: true },
    STOPPING: { color: "bg-yellow-500", text: "Stopping", pulse: true },
};

export const ClusterStatusCard = ({
    status,
    parallelism,
    totalRam,
    gpuSummary,
}: ClusterStatusCardProps) => {
    const config = statusConfig[status] || statusConfig["OFF"];

    return (
        <Card className="w-full">
            <CardHeader className="pb-2">
                <CardTitle className="text-xl font-semibold text-primary">Cluster Status</CardTitle>
            </CardHeader>

            <CardContent className="mt-6">
                <div className="relative flex items-center">
                    {/* Status section */}
                    <div className="flex items-center gap-2">
                        <div
                            className={cn("w-3 h-3 rounded-full", config.color, {
                                "animate-pulse": config.pulse,
                            })}
                        />
                        <span className="text-lg text-gray-800 font-medium">{config.text}</span>
                    </div>

                    {/* Metrics section: CPUs, RAM, GPUs (hide if OFF) */}
                    {status !== "OFF" && (
                        <div className="absolute left-[150px] flex items-center gap-8">
                            <span className="flex items-baseline gap-1 min-w-[70px]">
                                <span className="text-base font-medium text-gray-700 leading-tight">
                                    {parallelism}
                                </span>
                                <span className="text-base font-medium text-gray-500 leading-tight">
                                    CPUs
                                </span>
                            </span>
                            <span className="flex items-baseline gap-1 min-w-[70px]">
                                <span className="text-base font-medium text-gray-700 leading-tight">
                                    {totalRam}
                                </span>
                                <span className="text-base font-medium text-gray-500 leading-tight">
                                    RAM
                                </span>
                            </span>
                            {gpuSummary !== "-" && (
                                <span className="flex items-baseline gap-1 min-w-[110px]">
                                    <span className="text-base font-medium text-gray-700 leading-tight">
                                        {gpuSummary}
                                    </span>
                                    <span className="text-base font-medium text-gray-500 leading-tight">
                                        GPUs
                                    </span>
                                </span>
                            )}
                        </div>
                    )}
                </div>
            </CardContent>
        </Card>
    );
};
