import { createContext, useContext } from "react";
import { ClusterStatus } from "@/types/cluster";
import { useNodes } from "@/contexts/NodesContext";

interface ClusterContextType {
    clusterStatus: ClusterStatus;
}

const ClusterContext = createContext<ClusterContextType>({ clusterStatus: "OFF" });

export const ClusterProvider = ({ children }: { children: React.ReactNode }) => {
    const { nodes } = useNodes();

    const calculateClusterStatus = (): ClusterStatus => {
        nodes.forEach((node) => {
            console.log(`Node ${node.id} status: ${node.status}`);
        });

        if (nodes.length === 0) return "OFF";
        if (nodes.some((node) => node.status === "READY")) return "ON";
        if (nodes.every((node) => node.status === "BOOTING")) return "BOOTING";
        if (nodes.every((node) => node.status === "STOPPING")) return "STOPPING";
        return "OFF";
    };

    return (
        <ClusterContext.Provider value={{ clusterStatus: calculateClusterStatus() }}>
            {children}
        </ClusterContext.Provider>
    );
};

export const useCluster = () => useContext(ClusterContext);
