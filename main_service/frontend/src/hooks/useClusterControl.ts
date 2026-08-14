import { showErrorToast, ErrorToastDetail } from "@/components/ErrorToast";
import { useToast } from "@/components/ui/use-toast";
import { useCluster } from "@/contexts/ClusterContext";

export const useClusterControl = () => {
    const { toast } = useToast();
    const { setClusterStatus } = useCluster();

    // There is no difference between starting and rebooting.
    // the backend will realise there is nothing to stop/turn off, then start the cluster.

    const rebootCluster = async () => {
        try {
            setClusterStatus("BOOTING");
            const response = await fetch("/v1/cluster/restart", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
            });

            if (!response.ok) {
                setClusterStatus(null);
                let detail: ErrorToastDetail = {
                    title: "Couldn't start the cluster",
                    message: "Failed to start the cluster. Please try again.",
                };
                try {
                    const body = await response.json();
                    const raw = body?.detail;
                    if (typeof raw === "string") {
                        detail = { ...detail, message: raw };
                    } else if (
                        typeof raw?.title === "string" &&
                        typeof raw?.message === "string"
                    ) {
                        detail = raw;
                    }
                } catch {
                    // non-JSON error body; keep the generic message
                }
                showErrorToast(detail);
                return false;
            }

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
