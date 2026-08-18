import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { managementJson } from "@/lib/managementApi";

type DailyGroup = {
  machine_type: string;
  gcp_region: string;
  spot: boolean;

  total_node_hours: number; // VM-hours (cost)
  total_compute_hours: number; // compute-hours (usage)
};

type DailyHoursResponse = {
  month: string;

  total_node_hours: number;
  total_compute_hours: number;

  days: Array<{
    date: string;

    total_node_hours: number;
    total_compute_hours: number;

    groups: DailyGroup[];
    estimated_spend_usd: number;
    unpriced_node_hours: number;
  }>;
  estimated_spend_usd: number;
  unpriced_node_hours: number;
  compute_types: Array<{
    type: string;
    compute_hours: number;
    estimated_spend_usd: number;
    rate_missing: boolean;
  }>;
};

type UsageContextValue = {
  loading: boolean;
  error: string | null;
  daily: DailyHoursResponse | null;

  selectedMonth: string; // YYYY-MM
  setSelectedMonth: (m: string) => void;
};

const UsageContext = createContext<UsageContextValue | null>(null);

function currentMonthKeyUtc() {
  const now = new Date();
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  return `${y}-${m}`;
}

function qs(params: Record<string, string | number | boolean | null | undefined>) {
  const sp = new URLSearchParams();
  for (const [k, v] of Object.entries(params)) {
    if (v === null || v === undefined) continue;
    sp.set(k, String(v));
  }
  const s = sp.toString();
  return s ? `?${s}` : "";
}

export function UsageProvider({ children }: { children: React.ReactNode }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [daily, setDaily] = useState<DailyHoursResponse | null>(null);

  const [selectedMonth, setSelectedMonth] = useState<string>(() => currentMonthKeyUtc());

  const refresh = async () => {
    const month = selectedMonth;
    setLoading(true);
    setError(null);

    try {
      setDaily(await managementJson<DailyHoursResponse>(`/usage${qs({ month })}`));
    } catch (e: any) {
      setError(e?.message || "Failed to load usage");
      setDaily(null);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refresh();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedMonth]);

  const value = useMemo(
    () => ({
      loading,
      error,
      daily,
      selectedMonth,
      setSelectedMonth,
    }),
    [loading, error, daily, selectedMonth]
  );

  return <UsageContext.Provider value={value}>{children}</UsageContext.Provider>;
}

export function useUsage() {
  const ctx = useContext(UsageContext);
  if (!ctx) throw new Error("useUsage must be used within UsageProvider");
  return ctx;
}