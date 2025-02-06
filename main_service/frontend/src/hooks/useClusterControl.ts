import { useToast } from "@/components/ui/use-toast";

export const useClusterControl = () => {
    const { toast } = useToast();

    const startCluster = async () => {
        try {
            const response = await fetch("/v1/cluster/restart", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
            });

            if (!response.ok) {
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
            const response = await fetch("/v1/cluster/shutdown", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
            });

            if (!response.ok) {
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
