import React, { createContext, useCallback, useContext, useMemo, useRef, useState } from "react";
import { LogEntry } from "@/types/coreTypes";

type JobLogsState = {
    byIndex: Record<string, LogEntry[]>;
    failedInputsCount: number;
    hasMoreOlderByIndex: Record<string, boolean>;
    oldestLogDocumentTimestampByIndex: Record<string, number | undefined>;
};

type InputLogsResponse = {
    logs: Array<{
        message?: string;
        log_timestamp: number;
        is_error?: boolean;
    }>;
    failed_inputs_count?: number;
    has_more_older?: boolean;
    log_document_timestamp?: number;
};

type NextFailedInputResponse = {
    next_failed_input_index?: number | null;
    failed_input_indexes?: number[];
};

type LoggedInputIndexesResponse = {
    indexes_with_logs?: number[];
    failed_indexes?: number[];
    non_failed_indexes_with_logs?: number[];
};

interface LogsContextType {
    getLogs: (jobId: string, index: number) => LogEntry[];
    getFailedInputsCount: (jobId: string) => number;
    getHasMoreOlderLogs: (jobId: string, index: number) => boolean;
    getOldestLoadedLogDocumentTimestamp: (jobId: string, index: number) => number | undefined;
    getNextFailedInputIndex: (jobId: string, index: number) => Promise<number | null>;
    getFailedInputIndexes: (jobId: string) => Promise<number[]>;
    getIndexesWithLogs: (jobId: string) => Promise<number[]>;
    loadInputLogs: (jobId: string, index: number, oldestLogTimestamp?: number) => Promise<void>;
    logsByJobId: Record<string, JobLogsState>;
}

const LogsContext = createContext<LogsContextType>({
    logsByJobId: {},
    getLogs: () => [],
    getFailedInputsCount: () => 0,
    getHasMoreOlderLogs: () => false,
    getOldestLoadedLogDocumentTimestamp: () => undefined,
    getNextFailedInputIndex: async () => null,
    getFailedInputIndexes: async () => [],
    getIndexesWithLogs: async () => [],
    loadInputLogs: async () => {},
});

const compareByTimestamp = (left: LogEntry, right: LogEntry) => {
    if (left.log_timestamp < right.log_timestamp) return -1;
    if (left.log_timestamp > right.log_timestamp) return 1;
    return 0;
};

const mergeSorted = (previousLogs: LogEntry[], nextLogs: LogEntry[]) =>
    [...previousLogs, ...nextLogs].sort(compareByTimestamp);

export const LogsProvider = ({ children }: { children: React.ReactNode }) => {
    const [logsByJobId, setLogsByJobId] = useState<Record<string, JobLogsState>>({});

    const inflightIndexesRef = useRef<Record<string, Set<number>>>({});
    const lastLoadedOlderCursorByInputRef = useRef<Record<string, number>>({});

    const ensureIndexSets = (jobId: string) => {
        if (!inflightIndexesRef.current[jobId]) inflightIndexesRef.current[jobId] = new Set();
    };

    const getLogs = useCallback(
        (jobId: string, index: number) => {
            const state = logsByJobId[jobId];
            if (!state) return [];
            const idxKey = String(index);
            const per = state.byIndex[idxKey] || [];
            return [...per].sort(compareByTimestamp);
        },
        [logsByJobId],
    );

    const getFailedInputsCount = useCallback(
        (jobId: string) => {
            return logsByJobId[jobId]?.failedInputsCount || 0;
        },
        [logsByJobId],
    );

    const getHasMoreOlderLogs = useCallback(
        (jobId: string, index: number) => {
            const state = logsByJobId[jobId];
            if (!state) return false;
            return state.hasMoreOlderByIndex[String(index)] || false;
        },
        [logsByJobId],
    );

    const getOldestLoadedLogDocumentTimestamp = useCallback(
        (jobId: string, index: number) => {
            const state = logsByJobId[jobId];
            if (!state) return undefined;
            return state.oldestLogDocumentTimestampByIndex[String(index)];
        },
        [logsByJobId],
    );

    const getNextFailedInputIndex = useCallback(async (jobId: string, index: number) => {
        const queryString = new URLSearchParams({ index: String(index) });
        const response = await fetch(
            `/v1/jobs/${jobId}/next-failed-input?${queryString.toString()}`,
        );
        if (!response.ok) return null;
        const payload = (await response.json()) as NextFailedInputResponse;
        return payload.next_failed_input_index ?? null;
    }, []);

    const getFailedInputIndexes = useCallback(async (jobId: string) => {
        const queryString = new URLSearchParams({ index: "-1" });
        const response = await fetch(
            `/v1/jobs/${jobId}/next-failed-input?${queryString.toString()}`,
        );
        if (!response.ok) return [];
        const payload = (await response.json()) as NextFailedInputResponse;
        if (!Array.isArray(payload.failed_input_indexes)) return [];
        return payload.failed_input_indexes
            .map((value) => Number(value))
            .filter((value) => Number.isFinite(value))
            .sort((a, b) => a - b);
    }, []);

    const getIndexesWithLogs = useCallback(async (jobId: string) => {
        const response = await fetch(`/v1/jobs/${jobId}/logged-input-indexes`);
        if (!response.ok) return [];
        const payload = (await response.json()) as LoggedInputIndexesResponse;
        if (!Array.isArray(payload.indexes_with_logs)) return [];
        return payload.indexes_with_logs
            .map((value) => Number(value))
            .filter((value) => Number.isFinite(value))
            .sort((a, b) => a - b);
    }, []);

    const loadInputLogs = useCallback(
        async (jobId: string, index: number, oldestLogTimestamp?: number) => {
            ensureIndexSets(jobId);
            if (inflightIndexesRef.current[jobId].has(index)) return;
            if (oldestLogTimestamp !== undefined) {
                const inputKey = `${jobId}:${index}`;
                if (lastLoadedOlderCursorByInputRef.current[inputKey] === oldestLogTimestamp)
                    return;
                lastLoadedOlderCursorByInputRef.current[inputKey] = oldestLogTimestamp;
            }

            inflightIndexesRef.current[jobId].add(index);

            try {
                const isLoadingOlderLogs = oldestLogTimestamp !== undefined;
                const qs = new URLSearchParams({ index: String(index) });
                if (oldestLogTimestamp !== undefined) {
                    qs.set("oldest_timestamp", String(oldestLogTimestamp));
                }
                const response = await fetch(`/v1/jobs/${jobId}/logs?${qs.toString()}`);
                if (!response.ok) return;

                const payload = (await response.json()) as InputLogsResponse;
                const nextLogsForIndex = (payload.logs || []).map((entry) => ({
                    message: entry.message,
                    log_timestamp: entry.log_timestamp,
                    is_error: entry.is_error,
                }));

                setLogsByJobId((previousState) => {
                    const currentState = previousState[jobId] || {
                        byIndex: {},
                        failedInputsCount: 0,
                        hasMoreOlderByIndex: {},
                        oldestLogDocumentTimestampByIndex: {},
                    };
                    const indexKey = String(index);
                    const currentLogsForIndex = currentState.byIndex[indexKey] || [];
                    const logsToMerge = nextLogsForIndex;
                    return {
                        ...previousState,
                        [jobId]: {
                            failedInputsCount: payload.failed_inputs_count || 0,
                            hasMoreOlderByIndex: {
                                ...currentState.hasMoreOlderByIndex,
                                [indexKey]: Boolean(payload.has_more_older),
                            },
                            oldestLogDocumentTimestampByIndex: {
                                ...currentState.oldestLogDocumentTimestampByIndex,
                                [indexKey]:
                                    payload.log_document_timestamp ??
                                    currentState.oldestLogDocumentTimestampByIndex[indexKey],
                            },
                            byIndex: {
                                ...currentState.byIndex,
                                [indexKey]: isLoadingOlderLogs
                                    ? mergeSorted(currentLogsForIndex, logsToMerge)
                                    : [...nextLogsForIndex].sort(compareByTimestamp),
                            },
                        },
                    };
                });
            } finally {
                inflightIndexesRef.current[jobId].delete(index);
            }
        },
        [],
    );

    const value = useMemo(
        () => ({
            logsByJobId,
            getLogs,
            getFailedInputsCount,
            getHasMoreOlderLogs,
            getOldestLoadedLogDocumentTimestamp,
            getNextFailedInputIndex,
            getFailedInputIndexes,
            getIndexesWithLogs,
            loadInputLogs,
        }),
        [
            logsByJobId,
            getLogs,
            getFailedInputsCount,
            getHasMoreOlderLogs,
            getOldestLoadedLogDocumentTimestamp,
            getNextFailedInputIndex,
            getFailedInputIndexes,
            getIndexesWithLogs,
            loadInputLogs,
        ],
    );

    return <LogsContext.Provider value={value}>{children}</LogsContext.Provider>;
};

export const useLogsContext = () => useContext(LogsContext);
