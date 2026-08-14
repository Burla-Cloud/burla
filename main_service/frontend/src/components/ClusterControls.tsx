import { Button } from "@/components/ui/button";
import { useEffect, useState } from "react";
import { Loader2, Power, PowerOff, RefreshCw } from "lucide-react";
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
    const [pendingAction, setPendingAction] = useState<null | "start" | "restart">(null);
    const isRebooting = status === "REBOOTING";
    const isStarting = status === "BOOTING";
    const isStopping = status === "STOPPING";
    const isOn = status === "ON";
    const isOff = status === "OFF";
    const isStartDisabled = isStopping || disableStartButton || pendingAction !== null;

    useEffect(() => {
        if (status === "ON" || status === "OFF") {
            setPendingAction(null);
        }
    }, [status]);

    let startButtonIcon;
    let startButtonText;
    if (isStarting || isRebooting) {
        startButtonIcon = <Loader2 className="animate-spin" />;
        if (pendingAction === "restart" || isRebooting) {
            startButtonText = "Restarting…";
        } else {
            startButtonText = "Starting…";
        }
    } else if (isOn) {
        startButtonIcon = <RefreshCw />;
        startButtonText = "Restart";
    } else {
        startButtonIcon = <Power />;
        startButtonText = "Start";
    }

    const handleStartOrRestart = () => {
        if (isStartDisabled || pendingAction !== null) return;
        setPendingAction(isOn ? "restart" : "start");
        onReboot();
    };

    return (
        <div className="flex items-center gap-2">
            <Button
                variant="outline-destructive"
                onClick={onStop}
                disabled={isStopping || isOff || disableStopButton}
                aria-busy={isStopping}
                className="min-w-20"
            >
                {isStopping ? <Loader2 className="animate-spin" /> : <PowerOff />}
                {isStopping ? "Stopping…" : "Stop"}
            </Button>
            <Button
                onClick={handleStartOrRestart}
                disabled={isStartDisabled}
                aria-busy={isStarting || isRebooting}
                className="min-w-24"
            >
                {startButtonIcon}
                {startButtonText}
            </Button>
        </div>
    );
};
