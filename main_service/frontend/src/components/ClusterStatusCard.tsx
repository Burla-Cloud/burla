import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ClusterStatus } from "@/types/cluster";
import { cn } from "@/lib/utils";

interface ClusterStatusCardProps {
    status: ClusterStatus;
}

const statusConfig = {
    ON: { color: "bg-green-500", text: "On", pulse: false },
    OFF: { color: "bg-gray-500", text: "Off", pulse: false },
    REBOOTING: { color: "bg-yellow-500", text: "Rebooting", pulse: true },
    BOOTING: { color: "bg-yellow-500", text: "Starting", pulse: true },
    STOPPING: { color: "bg-yellow-500", text: "Stopping", pulse: true },
};

export const ClusterStatusCard = ({ status }: ClusterStatusCardProps) => {
    const config = statusConfig[status] || statusConfig["OFF"];

    return (
        <Card className="w-full">
            <CardHeader>
                <CardTitle className="text-xl font-semibold" style={{ color: "#3b5a64" }}>
                    Cluster Status
                </CardTitle>
            </CardHeader>
            <CardContent>
                <div className="flex items-center space-x-2">
                    <div
                        className={cn("w-3 h-3 rounded-full", config.color, {
                            "animate-pulse": config.pulse,
                        })}
                    />
                    <span className="text-lg">{config.text}</span>
                </div>
            </CardContent>
        </Card>
    );
};
