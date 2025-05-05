import { useEffect } from "react";
import { useSettings } from "@/contexts/SettingsContext";
import { useServiceAccounts } from "@/contexts/ServiceAccountContext";

const AppDataLoader = () => {
  const { setSettings } = useSettings();
  const { setAccounts } = useServiceAccounts();

  useEffect(() => {
    const load = async () => {
      try {
        const [settingsRes, accountsRes] = await Promise.all([
          fetch("/v1/settings", {
            headers: { Email: "joe@burla.dev" },
          }),
          fetch("/v1/service-accounts", {
            headers: { Email: "joe@burla.dev" },
          }),
        ]);

        const settings = await settingsRes.json();
        const accounts = await accountsRes.json();

        setSettings(settings);
        setAccounts(accounts.service_accounts || []);
      } catch (err) {
        console.error("Failed to load app data", err);
      }
    };

    load();
  }, []);

  return null;
};

export default AppDataLoader;
