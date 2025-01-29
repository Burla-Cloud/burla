import { Button } from "@/components/ui/button";
import { Power, PowerOff } from "lucide-react";

interface ClusterControlsProps {
  status: "RUNNING" | "STARTING" | "STOPPING" | "STOPPED";
  onStart: () => void;
  onStop: () => void;
}

export const ClusterControls = ({
  status,
  onStart,
  onStop,
}: ClusterControlsProps) => {
  const isStarting = status === "STARTING";
  const isStopping = status === "STOPPING";
  const isRunning = status === "RUNNING";

  return (
    <div className="flex space-x-4">
      <Button
        size="lg"
        onClick={onStart}
        disabled={isStarting || isRunning || isStopping}
        className="w-32 text-white disabled:bg-gray-400"
        style={{
          backgroundColor: "#3b5a64",
          hover: { backgroundColor: "#2d454c" },
        }}
      >
        <Power className="mr-2 h-4 w-4" />
        Start
      </Button>
      <Button
        variant="destructive"
        size="lg"
        onClick={onStop}
        disabled={isStopping || !isRunning || isStarting}
        className="w-32"
      >
        <PowerOff className="mr-2 h-4 w-4" />
        Stop
      </Button>
    </div>
  );
};
