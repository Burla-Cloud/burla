// src/hooks/useSaveSettings.ts
import { Settings } from "@/types/coreTypes";

export const useSaveSettings = () => {
  const saveSettings = async (settings: Settings) => {
    try {
      const res = await fetch("/v1/settings", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "Email": "joe@burla.dev",
        },
        body: JSON.stringify(settings), // Ensure the full settings including users is sent
      });

      if (!res.ok) throw new Error("Failed to update settings");
      return true;
    } catch (err) {
      console.error("Error updating settings:", err);
      return false;
    }
  };

  return { saveSettings };
};