import { useEffect } from "react";
import { useSettings } from "@/contexts/SettingsContext";
// import { useServiceAccounts } from "@/contexts/ServiceAccountContext";

const AppDataLoader = () => {
    const { setSettings } = useSettings();

    useEffect(() => {
        const load = async () => {
            try {
                // Store user timezone in session on first load
                const timezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
                await fetch("/api/timezone", {
                    method: "POST",
                    credentials: "include",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ timezone }),
                });
                const [settingsRes] = await Promise.all([
                    fetch("/v1/settings", {
                        headers: { Email: "joe@burla.dev" },
                    }),
                ]);

                const settings = await settingsRes.json();

                setSettings((prev) => ({ ...prev, ...settings }));
            } catch (err) {
                console.error("Failed to load app data", err);
            }
        };

        load();
    }, []);

    return null;
};

export default AppDataLoader;
