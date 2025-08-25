import { useToast } from "@/components/ui/use-toast";
import { useCluster } from "@/contexts/ClusterContext";

export const useClusterControl = () => {
    const { toast } = useToast();
    const { clusterStatus, setClusterStatus } = useCluster();

    // There is no difference between starting and rebooting.
    // the backend will realise there is nothing to stop/turn off, then start the cluster.

    const rebootCluster = async () => {
        try {
            const response = await fetch("/v1/cluster/restart", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
            });
            return true;
        } catch (error) {
            console.error("Error restarting cluster:", error);
            return false;
        }
    };

    const stopCluster = async () => {
        try {
            setClusterStatus("STOPPING");
            const response = await fetch("/v1/cluster/shutdown", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
            });

            if (!response.ok) {
                setClusterStatus(null); // Reset to calculated status
                throw new Error("Failed to stop the cluster");
            }

            toast({
                title: "Success",
                description: "Cluster has been stopped successfully",
            });
            return true;
        } catch (error) {
            toast({
                variant: "destructive",
                title: "Error",
                description: "Failed to stop the cluster. Please try again.",
            });
            return false;
        }
    };

    return {
        rebootCluster,
        stopCluster,
    };
};
