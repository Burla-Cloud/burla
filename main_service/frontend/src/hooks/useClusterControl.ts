import { showErrorToast } from "@/components/ErrorToast";
import { useToast } from "@/components/ui/use-toast";
import { useCluster } from "@/contexts/ClusterContext";
import { managementJson } from "@/lib/managementApi";

export const useClusterControl = () => {
    const { toast } = useToast();
    const { clusterStatus, setClusterStatus } = useCluster();

    // There is no difference between starting and rebooting.
    // the backend will realise there is nothing to stop/turn off, then start the cluster.

    const rebootCluster = async () => {
        try {
            setClusterStatus("BOOTING");
            const action = clusterStatus === "OFF" ? "start" : "restart";
            await managementJson(`/cluster/${action}`, { method: "POST" });

            return true;
        } catch (error) {
            setClusterStatus(null);
            showErrorToast({
                title: "Couldn't start the cluster",
                message: "Failed to start the cluster. Please try again.",
            });
            return false;
        }
    };

    const stopCluster = async () => {
        try {
            setClusterStatus("STOPPING");
            await managementJson("/cluster/stop", { method: "POST" });

            toast({
                title: "Success",
                description: "Cluster has been stopped successfully",
            });
            return true;
        } catch (error) {
            showErrorToast({
                title: "Couldn't stop the cluster",
                message: "Failed to stop the cluster. Please try again.",
            });
            return false;
        }
    };

    return {
        rebootCluster,
        stopCluster,
    };
};
