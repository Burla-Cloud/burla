import { Button } from "@/components/ui/button";
import { Power, PowerOff, RefreshCw } from "lucide-react";
import { ClusterStatus } from "@/types/coreTypes";

interface ClusterControlsProps {
    status: ClusterStatus;
    onReboot: () => void;
    onStop: () => void;
    disableStartButton?: boolean;
    disableStopButton?: boolean;
}

export const ClusterControls = ({
    status,
    onReboot,
    onStop,
    disableStartButton = false,
    disableStopButton = false,
}: ClusterControlsProps) => {
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
                disabled={isStarting || isStopping || isRebooting || disableStartButton}
                className="w-32 text-white bg-primary hover:bg-primary/90 disabled:bg-gray-400"
            >
                {startButtonIcon}
                {startButtonText}
            </Button>
            <Button
                variant="destructive"
                size="lg"
                onClick={onStop}
                disabled={isStopping || isOff || isRebooting || isStarting || disableStopButton}
                className="w-32"
            >
                <PowerOff className="mr-2 h-4 w-4" />
                Stop
            </Button>
        </div>
    );
};
