import React, { createContext, useContext, useCallback, useMemo, useRef, useState } from "react";
import { LogEntry } from "@/types/coreTypes";

type JobLogsState = {
  global: LogEntry[];
  byIndex: Record<string, LogEntry[]>;
};

type SummaryResp = { failed_indexes: number[]; seen_indexes: number[] };

interface LogsContextType {
  getLogs: (jobId: string, index: number) => LogEntry[];
  loadSummary: (jobId: string) => Promise<SummaryResp | null>;
  loadPage: (
    jobId: string,
    indexStart: number,
    indexEnd: number,
    limitPerIndex?: number,
    includeGlobal?: boolean
  ) => Promise<void>;
  evictToWindow: (jobId: string, keepStart: number, keepEnd: number, windowPages?: number) => void;
  startLiveStream: (jobId: string, index: number, includeGlobal?: boolean) => () => void;
  closeLiveStream: (jobId: string) => void;
  logsByJobId: Record<string, JobLogsState>;
}

const LogsContext = createContext<LogsContextType>({
  logsByJobId: {},
  getLogs: () => [],
  loadSummary: async () => null,
  loadPage: async () => {},
  evictToWindow: () => {},
  startLiveStream: () => () => {},
  closeLiveStream: () => {},
});

const keyFor = (e: LogEntry) => e.id ?? `${e.created_at}-${String(e.index ?? "g")}-${e.message}`;

const mergeSortedUnique = (prev: LogEntry[], next: LogEntry[]) => {
  const m = new Map<string, LogEntry>();
  for (const e of prev) m.set(keyFor(e), e);
  for (const e of next) m.set(keyFor(e), e);
  return Array.from(m.values()).sort((a, b) => (a.created_at ?? 0) - (b.created_at ?? 0));
};

const normalizeIndexKey = (idx: any) => {
  if (idx === null || idx === undefined) return null;
  const n = Number(idx);
  if (!Number.isFinite(n)) return null;
  return String(Math.trunc(n));
};

export const LogsProvider = ({ children }: { children: React.ReactNode }) => {
  const [logsByJobId, setLogsByJobId] = useState<Record<string, JobLogsState>>({});

  // per-job metadata that should NOT trigger rerenders
  const loadedPagesRef = useRef<Record<string, Set<string>>>({});
  const inflightPagesRef = useRef<Record<string, Set<string>>>({});
  const summaryCacheRef = useRef<Record<string, SummaryResp>>({});

  const sourcesRef = useRef<Record<string, EventSource | null>>({});
  const rotateTimersRef = useRef<Record<string, number | undefined>>({});
  const closingForRotateRef = useRef<Record<string, boolean>>({});
  const streamRefCountRef = useRef<Record<string, number>>({});

  const ensureSets = (jobId: string) => {
    if (!loadedPagesRef.current[jobId]) loadedPagesRef.current[jobId] = new Set();
    if (!inflightPagesRef.current[jobId]) inflightPagesRef.current[jobId] = new Set();
  };

  const getLogs = useCallback(
    (jobId: string, index: number) => {
      const state = logsByJobId[jobId];
      if (!state) return [];
      const idxKey = String(index);
      const global = state.global || [];
      const per = state.byIndex[idxKey] || [];
      return mergeSortedUnique(global, per);
    },
    [logsByJobId]
  );

  const loadSummary = useCallback(async (jobId: string) => {
    if (summaryCacheRef.current[jobId]) return summaryCacheRef.current[jobId];

    try {
      const res = await fetch(`/v1/jobs/${jobId}/logs?summary=true`);
      if (!res.ok) return null;
      const json = (await res.json()) as SummaryResp;
      summaryCacheRef.current[jobId] = json;
      return json;
    } catch {
      return null;
    }
  }, []);

  const loadPage = useCallback(
    async (jobId: string, indexStart: number, indexEnd: number, limitPerIndex: number = 200, includeGlobal: boolean = true) => {
      ensureSets(jobId);
      const pageKey = `${indexStart}-${indexEnd}`;

      if (loadedPagesRef.current[jobId].has(pageKey)) return;
      if (inflightPagesRef.current[jobId].has(pageKey)) return;

      inflightPagesRef.current[jobId].add(pageKey);

      try {
        const qs = new URLSearchParams({
          index_start: String(indexStart),
          index_end: String(indexEnd),
          limit_per_index: String(limitPerIndex),
          include_global: includeGlobal ? "true" : "false",
        });

        const res = await fetch(`/v1/jobs/${jobId}/logs?${qs.toString()}`);
        if (!res.ok) return;

        const json = await res.json();

        const incomingGlobal: LogEntry[] = (json.global_logs || []).map((x: any) => ({
          id: x.id,
          message: x.message,
          created_at: x.created_at,
          index: x.index,
          is_error: x.is_error,
        }));

        const incomingByIndex: Record<string, LogEntry[]> = {};
        const rawByIndex = json.logs_by_index || {};
        for (const [k, arr] of Object.entries(rawByIndex)) {
          incomingByIndex[String(k)] = (arr as any[]).map((x: any) => ({
            id: x.id,
            message: x.message,
            created_at: x.created_at,
            index: x.index,
            is_error: x.is_error,
          }));
        }

        setLogsByJobId((prev) => {
          const cur = prev[jobId] || { global: [], byIndex: {} };

          const nextGlobal = includeGlobal ? mergeSortedUnique(cur.global, incomingGlobal) : cur.global;

          const nextByIndex = { ...cur.byIndex };
          for (const [idxKey, arr] of Object.entries(incomingByIndex)) {
            nextByIndex[idxKey] = mergeSortedUnique(nextByIndex[idxKey] || [], arr);
          }

          return { ...prev, [jobId]: { global: nextGlobal, byIndex: nextByIndex } };
        });

        loadedPagesRef.current[jobId].add(pageKey);
      } finally {
        inflightPagesRef.current[jobId].delete(pageKey);
      }
    },
    []
  );

  const evictToWindow = useCallback((jobId: string, keepStart: number, keepEnd: number, windowPages: number = 3) => {
    ensureSets(jobId);

    // keep current page plus neighbors
    const pageSize = keepEnd - keepStart + 1;

    const half = Math.floor(windowPages / 2);
    const allowedRanges: Array<[number, number]> = [];
    for (let i = -half; i <= half; i++) {
      const s = keepStart + i * pageSize;
      const e = keepEnd + i * pageSize;
      if (s < 0) continue;
      allowedRanges.push([s, e]);
    }

    const allowedIndex = (idx: number) => allowedRanges.some(([s, e]) => idx >= s && idx <= e);
    const allowedPageKey = (k: string) => {
      const [sRaw, eRaw] = k.split("-");
      const s = Number(sRaw);
      const e = Number(eRaw);
      return Number.isFinite(s) && Number.isFinite(e) && allowedRanges.some(([as, ae]) => s === as && e === ae);
    };

    // critical: if you evict a page, remove it from loadedPages so it can be re-fetched later
    const lp = loadedPagesRef.current[jobId];
    for (const k of Array.from(lp)) {
      if (!allowedPageKey(k)) lp.delete(k);
    }

    setLogsByJobId((prev) => {
      const cur = prev[jobId];
      if (!cur) return prev;

      const nextByIndex: Record<string, LogEntry[]> = {};
      for (const [k, arr] of Object.entries(cur.byIndex)) {
        const idx = Number(k);
        if (Number.isFinite(idx) && allowedIndex(idx)) nextByIndex[k] = arr;
      }

      return { ...prev, [jobId]: { global: cur.global, byIndex: nextByIndex } };
    });
  }, []);

  const startLiveStream = useCallback((jobId: string, index: number, includeGlobal: boolean = true) => {
    let stopped = false;

    const armRotationTimer = () => {
      if (rotateTimersRef.current[jobId]) window.clearTimeout(rotateTimersRef.current[jobId]);
      rotateTimersRef.current[jobId] = window.setTimeout(() => {
        if (stopped) return;
        closingForRotateRef.current[jobId] = true;
        sourcesRef.current[jobId]?.close();
        window.setTimeout(() => {
          closingForRotateRef.current[jobId] = false;
          open();
        }, 0);
      }, 55_000);
    };

    const open = () => {
      if (stopped) return;
      if (sourcesRef.current[jobId]) return;

      const qs = new URLSearchParams({
        stream: "true",
        index: String(index),
        include_global: includeGlobal ? "true" : "false",
      });

      const source = new EventSource(`/v1/jobs/${jobId}/logs?${qs.toString()}`);
      sourcesRef.current[jobId] = source;

      source.onopen = () => armRotationTimer();

      source.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);

          const idxKey = normalizeIndexKey(data.index);
          const entry: LogEntry = {
            id: data.id,
            message: data.message,
            created_at: data.created_at,
            index: data.index,
            is_error: data.is_error,
          };

          setLogsByJobId((prev) => {
            const cur = prev[jobId] || { global: [], byIndex: {} };

            if (idxKey === null) {
              return {
                ...prev,
                [jobId]: {
                  global: mergeSortedUnique(cur.global, [entry]),
                  byIndex: cur.byIndex,
                },
              };
            }

            return {
              ...prev,
              [jobId]: {
                global: cur.global,
                byIndex: {
                  ...cur.byIndex,
                  [idxKey]: mergeSortedUnique(cur.byIndex[idxKey] || [], [entry]),
                },
              },
            };
          });
        } catch {
          // ignore bad SSE payloads
        }
      };

      source.onerror = (err) => {
        if (closingForRotateRef.current[jobId]) return;
        if (rotateTimersRef.current[jobId]) window.clearTimeout(rotateTimersRef.current[jobId]);
        console.error("SSE error (job logs):", err);
      };
    };

    streamRefCountRef.current[jobId] = (streamRefCountRef.current[jobId] || 0) + 1;
    open();

    return () => {
      stopped = true;
      const next = (streamRefCountRef.current[jobId] || 1) - 1;
      streamRefCountRef.current[jobId] = next;

      if (next <= 0) {
        if (rotateTimersRef.current[jobId]) window.clearTimeout(rotateTimersRef.current[jobId]);
        sourcesRef.current[jobId]?.close();
        sourcesRef.current[jobId] = null;
      }
    };
  }, []);

  const closeLiveStream = useCallback((jobId: string) => {
    streamRefCountRef.current[jobId] = 0;
    if (rotateTimersRef.current[jobId]) window.clearTimeout(rotateTimersRef.current[jobId]);
    sourcesRef.current[jobId]?.close();
    sourcesRef.current[jobId] = null;
  }, []);

  const value = useMemo(
    () => ({
      logsByJobId,
      getLogs,
      loadSummary,
      loadPage,
      evictToWindow,
      startLiveStream,
      closeLiveStream,
    }),
    [logsByJobId, getLogs, loadSummary, loadPage, evictToWindow, startLiveStream, closeLiveStream]
  );

  return <LogsContext.Provider value={value}>{children}</LogsContext.Provider>;
};

export const useLogsContext = () => useContext(LogsContext);


