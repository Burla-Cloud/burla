import { useToast } from "@/components/ui/use-toast";
import { useCluster } from "@/contexts/ClusterContext";
import { QuotaWarningDetails } from "@/hooks/useSaveSettings";

export type ClusterControlResult =
    | { ok: true; warnings?: QuotaWarningDetails[] }
    | { ok: false; quota?: QuotaWarningDetails; errorMessage?: string };

interface QuotaCapPayload {
    machine_type: string;
    region: string;
    limit: number;
    used?: number;
    available?: number;
    allowed?: number;
    count_unit?: string;
    quota?: string;
    units?: string;
    requested: number;
}

const quotaFromCap = (cap: QuotaCapPayload): QuotaWarningDetails => ({
    machineType: cap.machine_type,
    region: cap.region,
    limit: cap.limit,
    used: cap.used,
    available: cap.available,
    allowed: cap.allowed,
    countUnit: cap.count_unit,
    quota: cap.quota,
    units: cap.units,
    requested: cap.requested,
});

export const useClusterControl = () => {
    const { toast } = useToast();
    const { clusterStatus, setClusterStatus } = useCluster();

    // There is no difference between starting and rebooting.
    // the backend will realise there is nothing to stop/turn off, then start the cluster.

    const rebootCluster = async (): Promise<ClusterControlResult> => {
        try {
            setClusterStatus("BOOTING");
            const response = await fetch("/v1/cluster/restart", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
            });

            const body = await response.json().catch(() => ({}));
            if (!response.ok) {
                setClusterStatus(null);
                const detail = body.detail;
                if (detail?.error_code === "quota_exceeded") {
                    const cap = detail.caps?.[0];
                    return {
                        ok: false,
                        quota: cap ? quotaFromCap(cap) : undefined,
                        errorMessage: detail.message,
                    };
                }
                toast({
                    variant: "destructive",
                    title: "Error",
                    description: "Failed to start the cluster. Please try again.",
                });
                return { ok: false };
            }

            return {
                ok: true,
                warnings: (body.warnings || []).map(quotaFromCap),
            };
        } catch (error) {
            setClusterStatus(null);
            toast({
                variant: "destructive",
                title: "Error",
                description: "Failed to start the cluster. Please try again.",
            });
            return { ok: false };
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
