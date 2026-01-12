import React, { createContext, useCallback, useContext, useMemo, useRef, useState } from "react";
import { LogEntry } from "@/types/coreTypes";

type JobLogsState = {
  global: LogEntry[];
  byIndex: Record<string, LogEntry[]>;
};

type SummaryResp = {
  failed_indexes: number[];
  seen_indexes: number[];
  indexes_with_logs?: number[];
};

type SummaryOpts = { force?: boolean };

interface LogsContextType {
  logsByJobId: Record<string, JobLogsState>;

  getLogs: (jobId: string, index: number) => LogEntry[];

  loadSummary: (jobId: string, opts?: SummaryOpts) => Promise<SummaryResp | null>;
  clearSummaryCache: (jobId: string) => void;

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
}

const LogsContext = createContext<LogsContextType>({
  logsByJobId: {},
  getLogs: () => [],
  loadSummary: async () => null,
  clearSummaryCache: () => {},
  loadPage: async () => {},
  evictToWindow: () => {},
  startLiveStream: () => () => {},
  closeLiveStream: () => {},
});

const normalizeIndexKey = (inputIndex: any) => {
  if (inputIndex === null || inputIndex === undefined) return null;
  const parsed = Number(inputIndex);
  if (!Number.isFinite(parsed)) return null;
  return String(Math.trunc(parsed));
};

// "same log twice" killer, even if id differs
const sigFor = (e: LogEntry) =>
  `${Number(e.created_at ?? 0)}|${String(e.input_index ?? "g")}|${String(e.message ?? "")}`;

const keyFor = (e: LogEntry) => e.id ?? sigFor(e);

// newest first
const mergeSortedUnique = (prev: LogEntry[], next: LogEntry[]) => {
  const m = new Map<string, LogEntry>();
  for (const e of prev) m.set(keyFor(e), e);
  for (const e of next) m.set(keyFor(e), e);

  return Array.from(m.values()).sort(
    (a, b) => Number(a.created_at ?? 0) - Number(b.created_at ?? 0)
  );
};

const streamKey = (jobId: string, index: number, includeGlobal: boolean) =>
  `${jobId}::${index}::${includeGlobal ? 1 : 0}`;

export const LogsProvider = ({ children }: { children: React.ReactNode }) => {
  const [logsByJobId, setLogsByJobId] = useState<Record<string, JobLogsState>>({});

  // per-job caches (no rerenders)
  const loadedPagesRef = useRef<Record<string, Set<string>>>({});
  const inflightPagesRef = useRef<Record<string, Set<string>>>({});
  const summaryCacheRef = useRef<Record<string, SummaryResp>>({});

  const ensureSets = (jobId: string) => {
    if (!loadedPagesRef.current[jobId]) loadedPagesRef.current[jobId] = new Set();
    if (!inflightPagesRef.current[jobId]) inflightPagesRef.current[jobId] = new Set();
  };

  // SSE keyed by (jobId,index,includeGlobal)
  const sourcesRef = useRef<Record<string, EventSource | null>>({});
  const rotateTimersRef = useRef<Record<string, number | undefined>>({});
  const closingForRotateRef = useRef<Record<string, boolean>>({});
  const streamRefCountRef = useRef<Record<string, number>>({});

  const closeStreamKey = (k: string) => {
    const t = rotateTimersRef.current[k];
    if (t) window.clearTimeout(t);
    rotateTimersRef.current[k] = undefined;

    sourcesRef.current[k]?.close();
    sourcesRef.current[k] = null;
  };

  const closeLiveStream = useCallback((jobId: string) => {
    const prefix = `${jobId}::`;
    for (const k of Object.keys(sourcesRef.current)) {
      if (!k.startsWith(prefix)) continue;
      streamRefCountRef.current[k] = 0;
      closeStreamKey(k);
    }
  }, []);

  // Full reset for a job
  const clearSummaryCache = useCallback(
    (jobId: string) => {
      delete summaryCacheRef.current[jobId];
      delete loadedPagesRef.current[jobId];
      delete inflightPagesRef.current[jobId];

      closeLiveStream(jobId);

      setLogsByJobId((prev) => {
        if (!prev[jobId]) return prev;
        const next = { ...prev };
        delete next[jobId];
        return next;
      });
    },
    [closeLiveStream]
  );

  const getLogs = useCallback(
    (jobId: string, index: number) => {
      const state = logsByJobId[jobId];
      if (!state) return [];

      const idxKey = String(index);
      const global = state.global || [];
      const per = state.byIndex[idxKey] || [];

      // keep newest first in merged output
      return mergeSortedUnique(global, per);
    },
    [logsByJobId]
  );

  const loadSummary = useCallback(async (jobId: string, opts?: SummaryOpts) => {
    if (!opts?.force && summaryCacheRef.current[jobId]) return summaryCacheRef.current[jobId];

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
    async (
      jobId: string,
      indexStart: number,
      indexEnd: number,
      limitPerIndex: number = 200,
      includeGlobal: boolean = true
    ) => {
      ensureSets(jobId);

      // include params so caching never lies
      const pageKey = `${indexStart}-${indexEnd}-${limitPerIndex}-${includeGlobal ? 1 : 0}`;

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
          input_index: x.input_index,
          is_error: x.is_error,
        }));

        const incomingByIndex: Record<string, LogEntry[]> = {};
        const rawByIndex = json.logs_by_index || {};

        for (const [k, arr] of Object.entries(rawByIndex)) {
          const list = (arr as any[]).map((x: any) => ({
            id: x.id,
            message: x.message,
            created_at: x.created_at,
            input_index: x.input_index,
            is_error: x.is_error,
          }));

          // never create empty buckets
          if (list.length > 0) incomingByIndex[String(k)] = list;
        }

        setLogsByJobId((prev) => {
          const cur = prev[jobId] || { global: [], byIndex: {} };

          const nextGlobal =
            includeGlobal && incomingGlobal.length > 0 ? mergeSortedUnique(cur.global, incomingGlobal) : cur.global;

          const nextByIndex = { ...cur.byIndex };
          for (const [idxKey, arr] of Object.entries(incomingByIndex)) {
            nextByIndex[idxKey] = mergeSortedUnique(nextByIndex[idxKey] || [], arr);
          }

          return { ...prev, [jobId]: { global: nextGlobal, byIndex: nextByIndex } };
        });

        // mark loaded even if empty, prevents refetch loops
        loadedPagesRef.current[jobId].add(pageKey);
      } finally {
        inflightPagesRef.current[jobId].delete(pageKey);
      }
    },
    []
  );

  const evictToWindow = useCallback((jobId: string, keepStart: number, keepEnd: number, windowPages: number = 3) => {
    ensureSets(jobId);

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

    // loadedPages keys include params, so only match start-end prefix
    const allowedPageKeyPrefix = (k: string) => {
      const parts = k.split("-");
      const s = Number(parts[0]);
      const e = Number(parts[1]);
      if (!Number.isFinite(s) || !Number.isFinite(e)) return false;
      return allowedRanges.some(([as, ae]) => s === as && e === ae);
    };

    const lp = loadedPagesRef.current[jobId];
    for (const k of Array.from(lp)) {
      if (!allowedPageKeyPrefix(k)) lp.delete(k);
    }

    setLogsByJobId((prev) => {
      const cur = prev[jobId];
      if (!cur) return prev;

      const nextByIndex: Record<string, LogEntry[]> = {};
      for (const [k, arr] of Object.entries(cur.byIndex)) {
        const idx = Number(k);
        if (Number.isFinite(idx) && allowedIndex(idx) && arr.length > 0) nextByIndex[k] = arr;
      }

      return { ...prev, [jobId]: { global: cur.global, byIndex: nextByIndex } };
    });
  }, []);

  const startLiveStream = useCallback((jobId: string, index: number, includeGlobal: boolean = true) => {
    const k = streamKey(jobId, index, includeGlobal);
    let stopped = false;

    const open = () => {
      if (stopped) return;
      if (sourcesRef.current[k]) return;

      const qs = new URLSearchParams({
        stream: "true",
        index: String(index),
        include_global: includeGlobal ? "true" : "false",
      });

      const source = new EventSource(`/v1/jobs/${jobId}/logs?${qs.toString()}`);
      sourcesRef.current[k] = source;

      const armRotationTimer = () => {
        const t = rotateTimersRef.current[k];
        if (t) window.clearTimeout(t);

        rotateTimersRef.current[k] = window.setTimeout(() => {
          if (stopped) return;
          closingForRotateRef.current[k] = true;
          closeStreamKey(k);
          closingForRotateRef.current[k] = false;
          open();
        }, 55_000);
      };

      source.onopen = () => armRotationTimer();

      source.onmessage = (event) => {
        try {
          const data = JSON.parse(event.data);

          const rawIdx = data.input_index ?? null;
          const idxKey = normalizeIndexKey(rawIdx);

          const entry: LogEntry = {
            id: data.id,
            message: data.message,
            created_at: data.created_at,
            input_index: rawIdx,
            is_error: data.is_error,
          };

          setLogsByJobId((prev) => {
            const cur = prev[jobId] || { global: [], byIndex: {} };

            // global log
            if (idxKey === null) {
              if (!includeGlobal) return prev;

              const merged = mergeSortedUnique(cur.global, [entry]);
              return {
                ...prev,
                [jobId]: { global: merged, byIndex: cur.byIndex },
              };
            }

            const existing = cur.byIndex[idxKey] || [];
            const merged = mergeSortedUnique(existing, [entry]);

            return {
              ...prev,
              [jobId]: {
                global: cur.global,
                byIndex: { ...cur.byIndex, [idxKey]: merged },
              },
            };
          });
        } catch {
          // ignore
        }
      };

      source.onerror = (err) => {
        if (closingForRotateRef.current[k]) return;
        closeStreamKey(k);
        console.error("SSE error (job logs):", err);
      };
    };

    streamRefCountRef.current[k] = (streamRefCountRef.current[k] || 0) + 1;
    open();

    return () => {
      stopped = true;
      const next = (streamRefCountRef.current[k] || 1) - 1;
      streamRefCountRef.current[k] = next;
      if (next <= 0) closeStreamKey(k);
    };
  }, []);

  const value = useMemo(
    () => ({
      logsByJobId,
      getLogs,
      loadSummary,
      clearSummaryCache,
      loadPage,
      evictToWindow,
      startLiveStream,
      closeLiveStream,
    }),
    [logsByJobId, getLogs, loadSummary, clearSummaryCache, loadPage, evictToWindow, startLiveStream, closeLiveStream]
  );

  return <LogsContext.Provider value={value}>{children}</LogsContext.Provider>;
};

export const useLogsContext = () => useContext(LogsContext);
