// src/contexts/SettingsContext.tsx
import React, { createContext, useContext, useState } from "react";
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
