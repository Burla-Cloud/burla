import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ClusterStatus } from "@/types/coreTypes";
import { cn } from "@/lib/utils";

interface ClusterStatusCardProps {
    status: ClusterStatus;
    parallelism: number;
    totalRam: string;
    gpuSummary: string;
    gpuCount?: number;
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
    gpuCount,
}: ClusterStatusCardProps) => {
    const config = statusConfig[status] || statusConfig["OFF"];
    const showStats = status !== "OFF";
    const summaryParts = [`${parallelism} CPUs`, `${totalRam} RAM`] as string[];
    if (gpuCount && gpuCount > 0) summaryParts.push(`${gpuCount} GPUs`);
    const summary = summaryParts.join(" · ");

    return (
        <Card className="inline-block">
            <CardContent className="p-0 px-7 py-3 text-center">
                <div className="flex items-center justify-center w-full gap-4">
                    <div className="flex items-center gap-2">
                        <div
                            className={cn("w-3 h-3 rounded-full", config.color, {
                                "animate-pulse": config.pulse,
                            })}
                        />
                        <span className="text-xl text-gray-800 font-medium">{config.text}</span>
                    </div>

                    <div
                        className={cn(
                            "overflow-hidden transition-all duration-500 ease-in-out",
                            showStats ? "max-w-[1000px] opacity-100" : "max-w-0 opacity-0"
                        )}
                    >
                        <span className="text-lg font-medium text-gray-700 leading-tight whitespace-nowrap">
                            {summary}
                        </span>
                    </div>
                </div>
            </CardContent>
        </Card>
    );
};
