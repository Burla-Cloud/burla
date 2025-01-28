import { Button } from "@/components/ui/button";
import { Power, PowerOff } from "lucide-react";

interface ClusterControlsProps {
  status: "RUNNING" | "STARTING" | "STOPPING" | "OFF";
  onStart: (newStatus: "STARTING" | "RUNNING" | "OFF") => void;
  onStop: (newStatus: "STOPPING" | "OFF") => void;
}

export const ClusterControls = ({
  status,
  onStart,
  onStop,
}: ClusterControlsProps) => {
  const isStarting = status === "STARTING";
  const isStopping = status === "STOPPING";
  const isRunning = status === "RUNNING";

  const handleStart = async () => {
    onStart("STARTING");

    try {
      const response = await fetch("http://localhost:5001/v1/cluster/restart", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({}),
      });

      if (response.ok) {
        const result = await response.json();
        console.log("Cluster restarted successfully:", result);
        onStart("RUNNING");
      } else {
        console.error("Failed to restart the cluster:", response.statusText);
        onStart("OFF");
      }
    } catch (error) {
      console.error("Error while restarting the cluster:", error);
      onStart("OFF");
    }
  };

  const handleStop = async () => {
    onStop("STOPPING");

    try {
      const response = await fetch("http://localhost:5001/v1/cluster/delete", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
      });

      if (response.ok) {
        console.log("Cluster stopped successfully.");
        onStop("OFF");
      } else {
        console.error("Failed to stop the cluster:", response.statusText);
        onStop("RUNNING"); // Roll back if stop fails
      }
    } catch (error) {
      console.error("Error while stopping the cluster:", error);
      onStop("RUNNING");
    }
  };

  return (
    <div className="flex space-x-4">
      <Button
        size="lg"
        onClick={handleStart}
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
        onClick={handleStop}
        disabled={isStopping || !isRunning || isStarting}
        className="w-32"
      >
        <PowerOff className="mr-2 h-4 w-4" />
        Stop
      </Button>
    </div>
  );
};
