import { Button } from "@/components/ui/button";
import { useEffect, useState } from "react";
import { cn } from "@/lib/utils";
import { Loader2, Power, PowerOff, RefreshCw } from "lucide-react";
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
    const [pendingAction, setPendingAction] = useState<null | "start" | "restart">(null);
    const isRebooting = status === "REBOOTING";
    const isStarting = status === "BOOTING";
    const isStopping = status === "STOPPING";
    const isOn = status === "ON";
    const isOff = status === "OFF";
    const isStartDisabled = isStarting || isStopping || isRebooting || disableStartButton;

    useEffect(() => {
        if (status === "ON" || status === "OFF") {
            setPendingAction(null);
        }
    }, [status]);

    let startButtonIcon;
    let startButtonText;
    if (isStarting || isRebooting) {
        startButtonIcon = <Loader2 className="mr-2 h-4 w-4 animate-spin" />;
        if (pendingAction === "restart" || isRebooting) {
            startButtonText = "Restarting…";
        } else {
            startButtonText = "Starting…";
        }
    } else if (isOn) {
        startButtonIcon = <RefreshCw className="mr-2 h-4 w-4" />;
        startButtonText = "Restart";
    } else {
        startButtonIcon = <Power className="mr-2 h-4 w-4" />;
        startButtonText = "Start";
    }

    const handleStartOrRestart = () => {
        setPendingAction(isOn ? "restart" : "start");
        onReboot();
    };

    let stopButtonIcon;
    let stopButtonText = "Stop";
    if (isStopping) {
        stopButtonIcon = <Loader2 className="mr-2 h-4 w-4 animate-spin" />;
        stopButtonText = "Stopping…";
    } else {
        stopButtonIcon = <PowerOff className="mr-2 h-4 w-4" />;
        stopButtonText = "Stop";
    }

    return (
        <div className="flex space-x-5">
            <Button
                size="lg"
                onClick={handleStartOrRestart}
                disabled={isStartDisabled}
                aria-busy={isStarting || isRebooting}
                className={cn(
                    "w-32 text-white transition-all duration-300 ease-in-out shadow-md hover:shadow-xl active:shadow transform-gpu hover:-translate-y-0.5 active:translate-y-0 disabled:shadow-none",
                    isStarting || isRebooting
                        ? "bg-primary hover:bg-primary/90"
                        : "bg-primary hover:bg-primary/90 disabled:bg-gray-400",
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
                aria-busy={isStopping}
                className="w-32 transition-all duration-300 ease-in-out shadow-md hover:shadow-xl active:shadow transform-gpu hover:-translate-y-0.5 active:translate-y-0 disabled:shadow-none"
            >
                {stopButtonIcon}
                {stopButtonText}
            </Button>
        </div>
    );
};
