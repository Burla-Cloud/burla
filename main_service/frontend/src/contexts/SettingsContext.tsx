// src/contexts/SettingsContext.tsx
import React, { createContext, useContext, useState } from "react";
import { Settings as SettingsData } from "@/types/coreTypes";

interface SettingsContextType {
    settings: SettingsData;
    setSettings: React.Dispatch<React.SetStateAction<SettingsData>>;
}

const defaultSettings: SettingsData = {
    containerImage: "if you can see this",
    pythonVersion: "",
    machineType: "then something is broken!",
    machineQuantity: 1,
    diskSize: 20,
    inactivityTimeout: 5,
    gcpRegion: " :) ",
    users: [],
    burlaVersion: " :( ",
    googleCloudProjectId: " :) ",
};

const SettingsContext = createContext<SettingsContextType | undefined>(undefined);

export const SettingsProvider = ({ children }: { children: React.ReactNode }) => {
    const [settings, setSettings] = useState<SettingsData>(defaultSettings);

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
