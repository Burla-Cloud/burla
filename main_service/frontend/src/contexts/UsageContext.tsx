import { createContext, useContext, useEffect, useMemo, useState } from "react";

type DailyGroup = {
  machine_type: string;
  gcp_region: string;
  spot: boolean;
  total_node_hours: number;
};

export type DailyHoursResponse = {
  month: string; // YYYY-MM
  total_node_hours: number;
  days: Array<{
    date: string; // YYYY-MM-DD
    total_node_hours: number;
    groups: DailyGroup[];
  }>;
  meta?: any;
};

export type MonthNodesResponse = {
  month: string; // YYYY-MM
  nodes: Array<{
    id: string;
    instance_name: string;
    machine_type: string;
    gcp_region: string;
    spot: boolean;
    started_at_ms: number;
    ended_at_ms: number;
    duration_hours: number;
  }>;
  meta?: any;
};

type UsageContextValue = {
  loading: boolean;
  error: string | null;
  daily: DailyHoursResponse | null;
  nodes: MonthNodesResponse | null;

  selectedMonth: string; // YYYY-MM
  setSelectedMonth: (m: string) => void;

  refresh: (monthOverride?: string) => Promise<void>;
};

const UsageContext = createContext<UsageContextValue | null>(null);

function isLikelyHtml(contentType: string | null) {
  return !!contentType && contentType.includes("text/html");
}

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url, {
    credentials: "include",
    headers: { Accept: "application/json" },
  });

  const contentType = res.headers.get("content-type");
  if (isLikelyHtml(contentType)) throw new Error("Got HTML (login page). Request is not authenticated.");

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`HTTP ${res.status}${text ? `: ${text.slice(0, 200)}` : ""}`);
  }

  return (await res.json()) as T;
}

function currentMonthKeyUtc() {
  const now = new Date();
  const y = now.getUTCFullYear();
  const m = String(now.getUTCMonth() + 1).padStart(2, "0");
  return `${y}-${m}`;
}

export function UsageProvider({ children }: { children: React.ReactNode }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [daily, setDaily] = useState<DailyHoursResponse | null>(null);
  const [nodes, setNodes] = useState<MonthNodesResponse | null>(null);

  const [selectedMonth, setSelectedMonth] = useState<string>(() => currentMonthKeyUtc());

  const refresh = async (monthOverride?: string) => {
    const month = monthOverride ?? selectedMonth;

    setLoading(true);
    setError(null);

    try {
      const qs = month ? `?month=${encodeURIComponent(month)}` : "";
      const [d, n] = await Promise.all([
        fetchJson<DailyHoursResponse>(`/v1/nodes/daily_hours${qs}`),
        fetchJson<MonthNodesResponse>(`/v1/nodes/month_nodes${qs}&limit=200`.replace("?&", "?")),
      ]);
      setDaily(d);
      setNodes(n);
    } catch (e: any) {
      setError(e?.message || "Failed to load usage");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    refresh(selectedMonth);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedMonth]);

  const value = useMemo(
    () => ({
      loading,
      error,
      daily,
      nodes,
      selectedMonth,
      setSelectedMonth,
      refresh,
    }),
    [loading, error, daily, nodes, selectedMonth]
  );

  return <UsageContext.Provider value={value}>{children}</UsageContext.Provider>;
}

export function useUsage() {
  const ctx = useContext(UsageContext);
  if (!ctx) throw new Error("useUsage must be used within UsageProvider");
  return ctx;
}

