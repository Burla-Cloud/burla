import { Button } from "@/components/ui/button";
import { Power, PowerOff, RefreshCw } from "lucide-react";
import { ClusterStatus } from "@/types/cluster";

interface ClusterControlsProps {
    status: ClusterStatus;
    onReboot: () => void;
    onStop: () => void;
}

export const ClusterControls = ({ status, onReboot, onStop }: ClusterControlsProps) => {
    const isRebooting = status === "REBOOTING";
    const isStarting = status === "BOOTING";
    const isStopping = status === "STOPPING";
    const isOn = status === "ON";
    const isOff = status === "OFF";

    let startButtonIcon;
    let startButtonText;
    if (isOn || isRebooting) {
        startButtonIcon = <RefreshCw className="mr-2 h-4 w-4" />;
        startButtonText = "Restart";
    } else {
        startButtonIcon = <Power className="mr-2 h-4 w-4" />;
        startButtonText = "Start";
    }

    return (
        <div className="flex space-x-4">
            <Button
                size="lg"
                onClick={onReboot}
                disabled={isStarting || isStopping || isRebooting}
                className="w-32 text-white disabled:bg-gray-400"
                style={{ backgroundColor: "#3b5a64" }}
            >
                {startButtonIcon}
                {startButtonText}
            </Button>
            <Button
                variant="destructive"
                size="lg"
                onClick={onStop}
                disabled={isStopping || isOff || isRebooting || isStarting}
                className="w-32"
            >
                <PowerOff className="mr-2 h-4 w-4" />
                Stop
            </Button>
        </div>
    );
};
