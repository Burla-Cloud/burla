// src/contexts/SettingsContext.tsx
import React, { createContext, useContext, useEffect, useState } from "react";
import { Settings as SettingsData } from "@/types/coreTypes";
import { managementJson } from "@/lib/managementApi";

interface SettingsContextType {
    settings: SettingsData;
    setSettings: React.Dispatch<React.SetStateAction<SettingsData>>;
    loading: boolean;
    error: string | null;
}

const defaultSettings: SettingsData = {
    containerImage: "if you can see this",
    machineType: "then something is broken!",
    machineQuantity: 1,
    diskSize: 20,
    inactivityTimeout: 5,
    gcpRegion: " :) ",
    users: [],
    burlaVersion: " :( ",
    googleCloudProjectId: " :) ",
    cloudAccountName: " :) ",
    cloudProvider: "gcp",
};

const SettingsContext = createContext<SettingsContextType | undefined>(undefined);

export const SettingsProvider = ({ children }: { children: React.ReactNode }) => {
    const [settings, setSettings] = useState<SettingsData>(defaultSettings);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        const fetchSettings = async () => {
            try {
                const [data, legacy] = await Promise.all([
                    managementJson<any>("/settings"),
                    fetch("/v1/settings", { credentials: "include" }).then((response) =>
                        response.json()
                    ),
                ]);
                setSettings((previous) => ({
                    ...previous,
                    containerImage: data.image,
                    machineType: data.machine_type,
                    machineQuantity: data.quantity,
                    diskSize: data.disk_gb,
                    inactivityTimeout: Math.round(data.inactivity_timeout_seconds / 60),
                    gcpRegion: data.region,
                    burlaVersion: data.burla_version,
                    googleCloudProjectId: data.project_id,
                    cloudAccountName: data.cloud_account_name,
                    cloudProvider: data.cloud_provider,
                    options: data.options,
                    users: legacy.users ?? [],
                    filesystemEnabled: legacy.filesystemEnabled,
                }));
            } catch {
                setError("Could not load settings");
            } finally {
                setLoading(false);
            }
        };

        fetchSettings();
    }, []);

    return (
        <SettingsContext.Provider value={{ settings, setSettings, loading, error }}>
            {children}
        </SettingsContext.Provider>
    );
};

export const useSettings = () => {
    const context = useContext(SettingsContext);
    if (!context) throw new Error("useSettings must be used within SettingsProvider");
    return context;
};
