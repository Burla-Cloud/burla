// import { createContext, useContext, useState, useRef } from "react";
// import { LogEntry } from "@/types/coreTypes";

// interface LogsContextType {
//     logsByJobId: Record<string, LogEntry[]>;
//     startLiveStream: (jobId: string) => () => void;
//     loadInitial: (jobId: string, page?: number, limit?: number) => Promise<void>;
//     closeLiveStream: (jobId: string) => void;
// }

// const LogsContext = createContext<LogsContextType>({
//     logsByJobId: {},
//     startLiveStream: () => () => {},
//     loadInitial: async () => {},
//     closeLiveStream: () => {},
// });

// export const LogsProvider = ({ children }: { children: React.ReactNode }) => {
//     const [logsByJobId, setLogsByJobId] = useState<Record<string, LogEntry[]>>({});
//     const logsListRef = useRef<Record<string, Record<string, LogEntry>>>({});
//     const sourcesRef = useRef<Record<string, EventSource | null>>({});
//     const rotateTimersRef = useRef<Record<string, number | undefined>>({});
//     const closingForRotateRef = useRef<Record<string, boolean>>({});
//     const initialLoadedRef = useRef<Record<string, boolean>>({});
//     const streamRefCountRef = useRef<Record<string, number>>({});

//     const sortLogs = (logMap: Record<string, LogEntry>): LogEntry[] =>
//         Object.values(logMap).sort((a, b) => a.created_at - b.created_at);

//     // Fetch a large chunk of historical logs in one request (non-streaming)
//     const loadInitial = async (jobId: string, page: number = 0, limit: number = 1000) => {
//         try {
//             if (initialLoadedRef.current[jobId]) return;
//             const res = await fetch(`/v1/jobs/${jobId}/logs?page=${page}&limit=${limit}`);
//             if (!res.ok) return;
//             const json = await res.json();
//             const newMap: Record<string, LogEntry> = {};
//             for (const item of json.logs || []) {
//                 const entry: LogEntry = {
//                     id: item.id,
//                     message: item.message,
//                     created_at: item.created_at,
//                 };
//                 newMap[entry.id || `${entry.created_at}-${entry.message}`] = entry;
//             }
//             logsListRef.current[jobId] = newMap;
//             setLogsByJobId((prev) => ({
//                 ...prev,
//                 [jobId]: sortLogs(newMap),
//             }));
//             initialLoadedRef.current[jobId] = true;
//         } catch (err) {
//             console.error("Failed to load initial logs:", err);
//         }
//     };

//     const startLiveStream = (jobId: string) => {
//         let stopped = false;

//         const armRotationTimer = () => {
//             if (rotateTimersRef.current[jobId]) window.clearTimeout(rotateTimersRef.current[jobId]);
//             rotateTimersRef.current[jobId] = window.setTimeout(() => {
//                 if (stopped) return;
//                 closingForRotateRef.current[jobId] = true;
//                 if (sourcesRef.current[jobId]) sourcesRef.current[jobId]!.close();
//                 window.setTimeout(() => {
//                     closingForRotateRef.current[jobId] = false;
//                     open();
//                 }, 0);
//             }, 55_000);
//         };

//         const open = () => {
//             if (stopped) return;
//             if (!streamRefCountRef.current[jobId]) streamRefCountRef.current[jobId] = 0;
//             // If already open, don't create another connection
//             if (sourcesRef.current[jobId]) return;
//             const source = new EventSource(`/v1/jobs/${jobId}/logs?stream=true`);
//             sourcesRef.current[jobId] = source;

//             source.onopen = () => {
//                 armRotationTimer();
//             };

//             source.onmessage = (event) => {
//                 try {
//                     const data = JSON.parse(event.data);
//                     // SSE now only sends incremental changes; initial chunk comes from loadInitial()

//                     if (!logsListRef.current[jobId]) logsListRef.current[jobId] = {};
//                     const entry: LogEntry = {
//                         id: data.id,
//                         message: data.message,
//                         created_at: data.created_at,
//                     };
//                     logsListRef.current[jobId][entry.id || `${entry.created_at}-${entry.message}`] =
//                         entry;
//                     setLogsByJobId((prev) => ({
//                         ...prev,
//                         [jobId]: sortLogs(logsListRef.current[jobId]),
//                     }));
//                 } catch (err) {
//                     console.error("Failed to parse SSE job log:", err);
//                 }
//             };

//             source.onerror = (err) => {
//                 if (closingForRotateRef.current[jobId]) return;
//                 if (rotateTimersRef.current[jobId])
//                     window.clearTimeout(rotateTimersRef.current[jobId]);
//                 console.error("SSE error (job logs), retry in 5s:", err);
//             };
//         };

//         // Increment ref count and open if not already
//         streamRefCountRef.current[jobId] = (streamRefCountRef.current[jobId] || 0) + 1;
//         open();

//         return () => {
//             stopped = true;
//             const next = (streamRefCountRef.current[jobId] || 1) - 1;
//             streamRefCountRef.current[jobId] = next;
//             if (next <= 0) {
//                 if (rotateTimersRef.current[jobId])
//                     window.clearTimeout(rotateTimersRef.current[jobId]);
//                 if (sourcesRef.current[jobId]) sourcesRef.current[jobId]!.close();
//                 sourcesRef.current[jobId] = null;
//             }
//         };
//     };

//     const closeLiveStream = (jobId: string) => {
//         streamRefCountRef.current[jobId] = 0;
//         if (rotateTimersRef.current[jobId]) window.clearTimeout(rotateTimersRef.current[jobId]);
//         if (sourcesRef.current[jobId]) sourcesRef.current[jobId]!.close();
//         sourcesRef.current[jobId] = null;
//     };

//     return (
//         <LogsContext.Provider
//             value={{
//                 logsByJobId,
//                 startLiveStream,
//                 loadInitial,
//                 closeLiveStream,
//             }}
//         >
//             {children}
//         </LogsContext.Provider>
//     );
// };

// export const useLogsContext = () => useContext(LogsContext);


import { createContext, useContext, useState, useRef } from "react";
import { LogEntry } from "@/types/coreTypes";

interface LogsContextType {
    logsByJobId: Record<string, LogEntry[]>;
    startLiveStream: (jobId: string) => () => void;
    loadInitial: (jobId: string, page?: number, limit?: number) => Promise<void>;
    closeLiveStream: (jobId: string) => void;
}

const LogsContext = createContext<LogsContextType>({
    logsByJobId: {},
    startLiveStream: () => () => {},
    loadInitial: async () => {},
    closeLiveStream: () => {},
});

export const LogsProvider = ({ children }: { children: React.ReactNode }) => {
    const [logsByJobId, setLogsByJobId] = useState<Record<string, LogEntry[]>>({});

    // Internal maps keyed by jobId, then by a per log key
    const logsListRef = useRef<Record<string, Record<string, LogEntry>>>({});
    const sourcesRef = useRef<Record<string, EventSource | null>>({});
    const rotateTimersRef = useRef<Record<string, number | undefined>>({});
    const closingForRotateRef = useRef<Record<string, boolean>>({});
    const initialLoadedRef = useRef<Record<string, boolean>>({});
    const streamRefCountRef = useRef<Record<string, number>>({});

    const sortLogs = (logMap: Record<string, LogEntry>): LogEntry[] =>
        Object.values(logMap).sort((a, b) => a.created_at - b.created_at);

    // Fetch a large chunk of historical logs in one request (non streaming)
    const loadInitial = async (jobId: string, page: number = 0, limit: number = 1000) => {
        try {
            if (initialLoadedRef.current[jobId]) return;

            const res = await fetch(`/v1/jobs/${jobId}/logs?page=${page}&limit=${limit}`);
            if (!res.ok) return;

            const json = await res.json();
            const newMap: Record<string, LogEntry> = {};

            for (const item of json.logs || []) {
                const entry: LogEntry = {
                    id: item.id,
                    message: item.message,
                    created_at: item.created_at,
                    index: item.index,
                    is_error: item.is_error,
                };
                const key = entry.id || `${entry.created_at}-${entry.message}`;
                newMap[key] = entry;
            }

            logsListRef.current[jobId] = newMap;

            setLogsByJobId((prev) => ({
                ...prev,
                [jobId]: sortLogs(newMap),
            }));

            initialLoadedRef.current[jobId] = true;
        } catch (err) {
            console.error("Failed to load initial logs:", err);
        }
    };

    const startLiveStream = (jobId: string) => {
        let stopped = false;

        const armRotationTimer = () => {
            if (rotateTimersRef.current[jobId]) {
                window.clearTimeout(rotateTimersRef.current[jobId]);
            }
            rotateTimersRef.current[jobId] = window.setTimeout(() => {
                if (stopped) return;
                closingForRotateRef.current[jobId] = true;
                if (sourcesRef.current[jobId]) {
                    sourcesRef.current[jobId]!.close();
                }
                window.setTimeout(() => {
                    closingForRotateRef.current[jobId] = false;
                    open();
                }, 0);
            }, 55_000);
        };

        const open = () => {
            if (stopped) return;

            if (!streamRefCountRef.current[jobId]) {
                streamRefCountRef.current[jobId] = 0;
            }

            // If already open, do not create another connection
            if (sourcesRef.current[jobId]) return;

            const source = new EventSource(`/v1/jobs/${jobId}/logs?stream=true`);
            sourcesRef.current[jobId] = source;

            source.onopen = () => {
                armRotationTimer();
            };

            source.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);

                    if (!logsListRef.current[jobId]) {
                        logsListRef.current[jobId] = {};
                    }

                    const entry: LogEntry = {
                        id: data.id,
                        message: data.message,
                        created_at: data.created_at,
                        index: data.index,
                        is_error: data.is_error,
                    };

                    const key = entry.id || `${entry.created_at}-${entry.message}`;
                    logsListRef.current[jobId][key] = entry;

                    setLogsByJobId((prev) => ({
                        ...prev,
                        [jobId]: sortLogs(logsListRef.current[jobId]),
                    }));
                } catch (err) {
                    console.error("Failed to parse SSE job log:", err);
                }
            };

            source.onerror = (err) => {
                if (closingForRotateRef.current[jobId]) return;
                if (rotateTimersRef.current[jobId]) {
                    window.clearTimeout(rotateTimersRef.current[jobId]);
                }
                console.error("SSE error (job logs), retry in 5s:", err);
            };
        };

        // Increment ref count and open if not already
        streamRefCountRef.current[jobId] = (streamRefCountRef.current[jobId] || 0) + 1;
        open();

        return () => {
            stopped = true;
            const next = (streamRefCountRef.current[jobId] || 1) - 1;
            streamRefCountRef.current[jobId] = next;

            if (next <= 0) {
                if (rotateTimersRef.current[jobId]) {
                    window.clearTimeout(rotateTimersRef.current[jobId]);
                }
                if (sourcesRef.current[jobId]) {
                    sourcesRef.current[jobId]!.close();
                }
                sourcesRef.current[jobId] = null;
            }
        };
    };

    const closeLiveStream = (jobId: string) => {
        streamRefCountRef.current[jobId] = 0;
        if (rotateTimersRef.current[jobId]) {
            window.clearTimeout(rotateTimersRef.current[jobId]);
        }
        if (sourcesRef.current[jobId]) {
            sourcesRef.current[jobId]!.close();
        }
        sourcesRef.current[jobId] = null;
    };

    return (
        <LogsContext.Provider
            value={{
                logsByJobId,
                startLiveStream,
                loadInitial,
                closeLiveStream,
            }}
        >
            {children}
        </LogsContext.Provider>
    );
};

export const useLogsContext = () => useContext(LogsContext);
