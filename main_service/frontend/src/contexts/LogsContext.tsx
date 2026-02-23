import React, { createContext, useCallback, useContext, useMemo, useRef, useState } from "react";
import { LogEntry } from "@/types/coreTypes";

type JobLogsState = {
  byIndex: Record<string, LogEntry[]>;
  failedInputsCount: number;
  hasMoreOlderByIndex: Record<string, boolean>;
};

type InputLogsResponse = {
  logs: Array<{
    id?: string;
    message?: string;
    created_at?: number;
    created_at_nanos?: string;
    input_index?: number | null;
    is_error?: boolean;
  }>;
  failed_inputs_count?: number;
  has_more_older?: boolean;
};

type NextFailedInputResponse = {
  next_failed_input_index?: number | null;
};

interface LogsContextType {
  getLogs: (jobId: string, index: number) => LogEntry[];
  getFailedInputsCount: (jobId: string) => number;
  getHasMoreOlderLogs: (jobId: string, index: number) => boolean;
  getNextFailedInputIndex: (jobId: string, index: number) => Promise<number | null>;
  loadInputLogs: (jobId: string, index: number, oldestTimestampNanos?: string) => Promise<void>;
  logsByJobId: Record<string, JobLogsState>;
}

const LogsContext = createContext<LogsContextType>({
  logsByJobId: {},
  getLogs: () => [],
  getFailedInputsCount: () => 0,
  getHasMoreOlderLogs: () => false,
  getNextFailedInputIndex: async () => null,
  loadInputLogs: async () => {},
});

const timestampNanosOrZero = (entry: LogEntry) => {
  if (entry.created_at_nanos) return BigInt(entry.created_at_nanos);
  const fallbackSeconds = entry.created_at ?? 0;
  return BigInt(Math.trunc(fallbackSeconds * 1_000_000_000));
};

const compareByTimestamp = (left: LogEntry, right: LogEntry) => {
  const leftNanos = timestampNanosOrZero(left);
  const rightNanos = timestampNanosOrZero(right);
  if (leftNanos < rightNanos) return -1;
  if (leftNanos > rightNanos) return 1;
  return 0;
};

const keyFor = (e: LogEntry) =>
  e.id ?? `${e.created_at_nanos ?? e.created_at}-${String(e.input_index ?? "g")}-${e.message}`;

const mergeSortedUnique = (prev: LogEntry[], next: LogEntry[]) => {
  const m = new Map<string, LogEntry>();
  for (const e of prev) m.set(keyFor(e), e);
  for (const e of next) m.set(keyFor(e), e);
  return Array.from(m.values()).sort(compareByTimestamp);
};

export const LogsProvider = ({ children }: { children: React.ReactNode }) => {
  const [logsByJobId, setLogsByJobId] = useState<Record<string, JobLogsState>>({});

  const loadedIndexesRef = useRef<Record<string, Set<number>>>({});
  const inflightIndexesRef = useRef<Record<string, Set<number>>>({});

  const ensureIndexSets = (jobId: string) => {
    if (!loadedIndexesRef.current[jobId]) loadedIndexesRef.current[jobId] = new Set();
    if (!inflightIndexesRef.current[jobId]) inflightIndexesRef.current[jobId] = new Set();
  };

  const getLogs = useCallback(
    (jobId: string, index: number) => {
      const state = logsByJobId[jobId];
      if (!state) return [];
      const idxKey = String(index);
      const per = state.byIndex[idxKey] || [];
      return mergeSortedUnique([], per);
    },
    [logsByJobId]
  );

  const getFailedInputsCount = useCallback(
    (jobId: string) => {
      return logsByJobId[jobId]?.failedInputsCount || 0;
    },
    [logsByJobId]
  );

  const getHasMoreOlderLogs = useCallback(
    (jobId: string, index: number) => {
      const state = logsByJobId[jobId];
      if (!state) return false;
      return state.hasMoreOlderByIndex[String(index)] || false;
    },
    [logsByJobId]
  );

  const getNextFailedInputIndex = useCallback(async (jobId: string, index: number) => {
    const queryString = new URLSearchParams({ index: String(index) });
    const response = await fetch(
      `/v1/jobs/${jobId}/next-failed-input?${queryString.toString()}`
    );
    if (!response.ok) return null;
    const payload = (await response.json()) as NextFailedInputResponse;
    return payload.next_failed_input_index ?? null;
  }, []);

  const loadInputLogs = useCallback(
    async (jobId: string, index: number, oldestTimestampNanos?: string) => {
      ensureIndexSets(jobId);
      const isInitialPageLoad = oldestTimestampNanos === undefined;
      if (isInitialPageLoad && loadedIndexesRef.current[jobId].has(index)) return;
      if (inflightIndexesRef.current[jobId].has(index)) return;

      inflightIndexesRef.current[jobId].add(index);

      try {
        const qs = new URLSearchParams({ index: String(index) });
        if (oldestTimestampNanos !== undefined) {
          qs.set("oldest_timestamp", oldestTimestampNanos);
        }
        const response = await fetch(`/v1/jobs/${jobId}/logs?${qs.toString()}`);
        if (!response.ok) return;

        const payload = (await response.json()) as InputLogsResponse;
        const nextLogsForIndex = (payload.logs || []).map((entry) => ({
          id: entry.id,
          message: entry.message,
          created_at: entry.created_at,
          created_at_nanos: entry.created_at_nanos,
          input_index: entry.input_index,
          is_error: entry.is_error,
        }));

        setLogsByJobId((previousState) => {
          const currentState = previousState[jobId] || {
            byIndex: {},
            failedInputsCount: 0,
            hasMoreOlderByIndex: {},
          };
          return {
            ...previousState,
            [jobId]: {
              failedInputsCount: payload.failed_inputs_count || 0,
              hasMoreOlderByIndex: {
                ...currentState.hasMoreOlderByIndex,
                [String(index)]: Boolean(payload.has_more_older),
              },
              byIndex: {
                ...currentState.byIndex,
                [String(index)]: mergeSortedUnique(
                  currentState.byIndex[String(index)] || [],
                  nextLogsForIndex
                ),
              },
            },
          };
        });

        if (isInitialPageLoad) {
          loadedIndexesRef.current[jobId].add(index);
        }
      } finally {
        inflightIndexesRef.current[jobId].delete(index);
      }
    },
    []
  );

  const value = useMemo(
    () => ({
      logsByJobId,
      getLogs,
      getFailedInputsCount,
      getHasMoreOlderLogs,
      getNextFailedInputIndex,
      loadInputLogs,
    }),
    [logsByJobId, getLogs, getFailedInputsCount, getHasMoreOlderLogs, getNextFailedInputIndex, loadInputLogs]
  );

  return <LogsContext.Provider value={value}>{children}</LogsContext.Provider>;
};

export const useLogsContext = () => useContext(LogsContext);


