import { createContext, useContext, useMemo, useState, useEffect } from "react";
import { ClusterStatus } from "@/types/coreTypes";
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
  // statusFromButtons lets the UI snap immediately when user clicks start/stop
  const [statusFromButtons, setStatusFromButtons] = useState<ClusterStatus | null>(null);
  const { nodes } = useNodes();

  const statusFromNodes = useMemo<ClusterStatus>(() => {
    if (nodes.length === 0) return "OFF";
  
    const ACTIVE = new Set(["READY", "RUNNING"]);
    const OFFISH = new Set(["DELETED", "FAILED", "STOPPING"]);
  
    const anyActive   = nodes.some(n => ACTIVE.has(n.status));
    const anyBooting  = nodes.some(n => n.status === "BOOTING");
    const allBooting  = nodes.every(n => n.status === "BOOTING");
    const allStopping = nodes.every(n => n.status === "STOPPING");
    const allOffish   = nodes.every(n => OFFISH.has(n.status));
  
    if (anyActive) return "ON";
  
    // Starting if: all are BOOTING, or there is at least one BOOTING and the rest are OFF-ish
    if (allBooting) return "BOOTING";
    if (anyBooting && nodes.every(n => n.status === "BOOTING" || OFFISH.has(n.status))) {
      return "BOOTING";
    }
  
    if (allStopping) return "STOPPING";
    if (allOffish)   return "OFF";
  
    // Fallback
    return "OFF";
  }, [nodes]);
  

  const currentStatus = statusFromButtons ?? statusFromNodes;

  // Clear temporary button status once we're not forcing REBOOTING, or when we land ON.
  useEffect(() => {
    if (statusFromButtons !== "REBOOTING") {
      setStatusFromButtons(null);
    } else if (statusFromButtons === "REBOOTING" && statusFromNodes === "ON") {
      setStatusFromButtons(null);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
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
