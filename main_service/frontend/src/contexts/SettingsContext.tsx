// src/contexts/SettingsContext.tsx
import React, { createContext, useContext, useEffect, useState } from "react";
import { Settings as SettingsData } from "@/types/coreTypes";

interface SettingsContextType {
    settings: SettingsData;
    setSettings: React.Dispatch<React.SetStateAction<SettingsData>>;
}

const defaultSettings: SettingsData = {
    containerImage: "burlacloud/default-image-py3.12",
    pythonVersion: "3.12",
    machineType: "n4-standard-4",
    machineQuantity: 1,
    users: [],
};

const SettingsContext = createContext<SettingsContextType | undefined>(undefined);

export const SettingsProvider = ({ children }: { children: React.ReactNode }) => {
    const [settings, setSettings] = useState<SettingsData>(defaultSettings);

    useEffect(() => {
        const fetchSettings = async () => {
            try {
                const res = await fetch("/v1/settings", {
                    method: "GET",
                    headers: {
                        "Content-Type": "application/json",
                        Email: "joe@burla.dev",
                    },
                    credentials: "include", // 👈 This line is crucial
                });
                if (!res.ok) throw new Error("Failed to fetch settings");
                const data = await res.json();

                setSettings((prev) => ({
                    ...prev,
                    ...data, // merge in values from backend (excluding `users`)
                }));
            } catch (err) {
                console.error("Error fetching settings:", err);
            }
        };

        fetchSettings();
    }, []);

    return (
        <SettingsContext.Provider value={{ settings, setSettings }}>
            {children}
        </SettingsContext.Provider>
    );
};

export const useSettings = () => {
    const context = useContext(SettingsContext);
    if (!context) throw new Error("useSettings must be used within SettingsProvider");
    return context;
};
