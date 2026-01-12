import { createContext, useContext, useEffect, useMemo, useState } from "react";

export type DailyGroup = {
  machine_type: string;
  gcp_region: string;
  spot: boolean;

  total_node_hours: number; // VM-hours (cost)
  total_compute_hours: number; // compute-hours (usage)
};

export type DailyHoursResponse = {
  month: string;

  total_node_hours: number;
  total_compute_hours: number;

  days: Array<{
    date: string;

    total_node_hours: number;
    total_compute_hours: number;

    groups: DailyGroup[];
  }>;

  meta: {
    hours_precision_decimals: number;
    scanned: number;
    max_scan: number;
  };
};

export type MonthNodesCursor =
  | {
      ended_at: number; // seconds
      id: string;
    }
  | null;

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

    duration_hours: number; // VM-hours (cost)
    duration_compute_hours: number; // compute-hours (usage)
  }>;
  nextCursor?: MonthNodesCursor;
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
  const [nodes, setNodes] = useState<MonthNodesResponse | null>(null);

  const [selectedMonth, setSelectedMonth] = useState<string>(() => currentMonthKeyUtc());

  const refresh = async (monthOverride?: string) => {
    const month = monthOverride ?? selectedMonth;

    setLoading(true);
    setError(null);

    try {
      const dailyPromise = fetchJson<DailyHoursResponse>(`/v1/nodes/daily_hours${qs({ month })}`);

      const pageLimit = 2000;
      const allNodes: MonthNodesResponse["nodes"] = [];

      let cursor: MonthNodesCursor = null;
      let lastCursorKey: string | null = null;

      for (;;) {
        const page = await fetchJson<MonthNodesResponse>(
          `/v1/nodes/month_nodes${qs({
            month,
            limit: pageLimit,
            cursor_ended_at: cursor?.ended_at ?? null,
            cursor_id: cursor?.id ?? null,
          })}`
        );

        if (Array.isArray(page.nodes)) allNodes.push(...page.nodes);

        cursor = page.nextCursor ?? null;

        if (!cursor) {
          setNodes({ month: page.month, nodes: allNodes, nextCursor: null, meta: page.meta });
          break;
        }

        const key = `${cursor.ended_at}:${cursor.id}`;
        if (key === lastCursorKey) throw new Error("Pagination cursor did not advance.");
        lastCursorKey = key;
      }

      const d = await dailyPromise;
      setDaily(d);
    } catch (e: any) {
      setError(e?.message || "Failed to load usage");
      setDaily(null);
      setNodes(null);
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
