import { useToast } from "@/components/ui/use-toast";
import { useCluster } from "@/contexts/ClusterContext";

export const useClusterControl = () => {
    const { toast } = useToast();
    const { setClusterStatus } = useCluster();

    const startCluster = async () => {
        try {
            setClusterStatus("BOOTING");
            const response = await fetch("/v1/cluster/restart", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
            });

            if (!response.ok) {
                setClusterStatus(null); // Reset to calculated status
                throw new Error("Failed to start the cluster");
            }

            toast({
                title: "Success",
                description: "Cluster has been started successfully",
            });
            return true;
        } catch (error) {
            toast({
                variant: "destructive",
                title: "Error",
                description: "Failed to start the cluster. Please try again.",
            });
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
        startCluster,
        stopCluster,
    };
};
