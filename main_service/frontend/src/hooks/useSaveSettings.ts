// src/hooks/useSaveSettings.ts
import { Settings } from "@/types/coreTypes";
import { managementJson } from "@/lib/managementApi";

export const useSaveSettings = () => {
    const saveSettings = async (settings: Settings) => {
        try {
            await managementJson("/settings", {
                method: "PATCH",
                body: JSON.stringify({
                    image: settings.containerImage,
                    machine_type: settings.machineType,
                    quantity: settings.machineQuantity,
                    region: settings.gcpRegion,
                    disk_gb: settings.diskSize,
                    inactivity_timeout_seconds: settings.inactivityTimeout * 60,
                }),
            });
            const usersResponse = await fetch("/v1/settings", {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ users: settings.users }),
            });

            if (!usersResponse.ok) throw new Error("Failed to update users");
            return true;
        } catch (err) {
            console.error("Error updating settings:", err);
            return false;
        }
    };

    return { saveSettings };
};
