import { Button } from "@/components/ui/button";
import { Power, PowerOff } from "lucide-react";
import { ClusterStatus } from "@/types/cluster";

interface ClusterControlsProps {
    status: ClusterStatus;
    onStart: () => void;
    onStop: () => void;
}

export const ClusterControls = ({ status, onStart, onStop }: ClusterControlsProps) => {
    const isStarting = status === "BOOTING";
    const isStopping = status === "STOPPING";
    const isOn = status === "ON";

    return (
        <div className="flex space-x-4">
            <Button
                size="lg"
                onClick={onStart}
                disabled={isStarting || isOn || isStopping}
                className="w-32 text-white disabled:bg-gray-400"
                style={{ backgroundColor: "#3b5a64" }}
            >
                <Power className="mr-2 h-4 w-4" />
                Start
            </Button>
            <Button
                variant="destructive"
                size="lg"
                onClick={onStop}
                disabled={isStopping || !isOn || isStarting}
                className="w-32"
            >
                <PowerOff className="mr-2 h-4 w-4" />
                Stop
            </Button>
        </div>
    );
};
