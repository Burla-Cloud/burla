import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { Power, PowerOff, RefreshCw } from "lucide-react";
import { ClusterStatus } from "@/types/coreTypes";

interface ClusterControlsProps {
    status: ClusterStatus;
    onReboot: () => void;
    onStop: () => void;
    disableStartButton?: boolean;
    disableStopButton?: boolean;
    highlightStart?: boolean;
}

export const ClusterControls = ({
    status,
    onReboot,
    onStop,
    disableStartButton = false,
    disableStopButton = false,
    highlightStart = false,
}: ClusterControlsProps) => {
    const isRebooting = status === "REBOOTING";
    const isStarting = status === "BOOTING";
    const isStopping = status === "STOPPING";
    const isOn = status === "ON";
    const isOff = status === "OFF";
    const isStartDisabled = isStarting || isStopping || isRebooting || disableStartButton;

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
        <div className="flex space-x-5">
            <Button
                size="lg"
                onClick={onReboot}
                disabled={isStartDisabled}
                className={cn(
                    "w-32 text-white bg-primary hover:bg-primary/90 disabled:bg-gray-400",
                    highlightStart &&
                        !isOn &&
                        !isStartDisabled &&
                        "bg-blue-700 hover:bg-blue-800 animate-pulse glow-pulse-blue ring-4 ring-blue-500 ring-offset-2 ring-offset-background transition-shadow transform transition-transform hover:scale-105"
                )}
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
