// src/contexts/SettingsContext.tsx
import React, { createContext, useContext, useEffect, useState } from "react";
import { Settings as SettingsData } from "@/types/coreTypes";

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
                const response = await fetch("/v1/settings", { credentials: "include" });
                if (!response.ok) throw new Error(`HTTP ${response.status}`);
                const data = await response.json();
                setSettings((previous) => ({ ...previous, ...data }));
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
