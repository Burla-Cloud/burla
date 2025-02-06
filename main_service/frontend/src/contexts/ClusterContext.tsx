import { createContext, useContext, useMemo, useState, useEffect } from "react";
import { ClusterStatus } from "@/types/cluster";
import { useNodes } from "@/contexts/NodesContext";

interface ClusterContextType {
    clusterStatus: ClusterStatus;
    setClusterStatus: (status: ClusterStatus | null) => void;
}

const ClusterContext = createContext<ClusterContextType>({
    clusterStatus: "OFF",
    setClusterStatus: () => {},
});

export const ClusterProvider = ({ children }: { children: React.ReactNode }) => {
    // statusFromButtons is set by the start/stop buttons so that they transition instantly
    // after being clicked instead of waiting for the nodes to update first.
    const [statusFromButtons, setStatusFromButtons] = useState<ClusterStatus | null>(null);
    const { nodes } = useNodes();

    const statusFromNodes = useMemo(() => {
        if (nodes.length === 0) return "OFF";
        if (nodes.some((node) => node.status === "READY" || node.status === "RUNNING")) return "ON";
        if (nodes.every((node) => node.status === "BOOTING")) return "BOOTING";
        if (nodes.every((node) => node.status === "STOPPING")) return "STOPPING";
        return "OFF";
    }, [nodes]);

    // always use statusFromButtons unless it's set to null
    const currentStatus = statusFromButtons ?? statusFromNodes;

    // Only clear statusFromButtons if we were rebooting and just finished, or not rebooting.
    useEffect(() => {
        if (statusFromButtons !== "REBOOTING") {
            setStatusFromButtons(null);
        } else if (statusFromButtons === "REBOOTING" && statusFromNodes === "ON") {
            setStatusFromButtons(null);
        }
    }, [statusFromNodes]);

    return (
        <ClusterContext.Provider
            value={{
                clusterStatus: currentStatus,
                setClusterStatus: setStatusFromButtons,
            }}
        >
            {children}
        </ClusterContext.Provider>
    );
};

export const useCluster = () => useContext(ClusterContext);
