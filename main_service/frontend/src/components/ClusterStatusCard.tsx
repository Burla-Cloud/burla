import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

interface ClusterStatusCardProps {
  status: "RUNNING" | "STARTING" | "STOPPING" | "OFF";
}

const statusConfig = {
  RUNNING: { color: "bg-green-500", text: "Running" },
  STARTING: { color: "bg-yellow-500", text: "Starting" },
  STOPPING: { color: "bg-yellow-500", text: "Stopping" },
  OFF: { color: "bg-gray-500", text: "Off" },
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
          <div className={cn("w-3 h-3 rounded-full animate-pulse", config.color)} />
          <span className="text-lg">{config.text}</span>
        </div>
      </CardContent>
    </Card>
  );
};