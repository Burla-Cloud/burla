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
        if (nodes.some((node) => node.status === "READY")) return "ON";
        if (nodes.every((node) => node.status === "BOOTING")) return "BOOTING";
        if (nodes.every((node) => node.status === "STOPPING")) return "STOPPING";
        return "OFF";
    }, [nodes]);

    // always use statusFromButtons unless it's set to null
    const currentStatus = statusFromButtons ?? statusFromNodes;

    // set statusFromButtons to null as soon as the nodes are updated!
    useEffect(() => {
        setStatusFromButtons(null);
    }, [nodes]);

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
