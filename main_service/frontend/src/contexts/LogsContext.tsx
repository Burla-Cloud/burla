// import {
//   createContext,
//   useContext,
//   useState,
//   useRef,
//   useMemo,
// } from "react";
// import { LogEntry } from "@/types/coreTypes";

// interface LogsContextType {
//   logsByJobId: Record<string, LogEntry[]>;
//   hasMoreByJobId: Record<string, boolean>;
//   fetchInitialLogs: (jobId: string) => Promise<void>;
//   fetchMoreLogs: (jobId: string) => Promise<void>;
// }

// const LogsContext = createContext<LogsContextType>({
//   logsByJobId: {},
//   hasMoreByJobId: {},
//   fetchInitialLogs: async () => {},
//   fetchMoreLogs: async () => {},
// });

// export const LogsProvider = ({ children }: { children: React.ReactNode }) => {
//   const [logsByJobId, setLogsByJobId] = useState<Record<string, LogEntry[]>>({});
//   const [hasMoreByJobId, setHasMoreByJobId] = useState<Record<string, boolean>>({});
//   const cursorRef = useRef<Record<string, { time: number; id: string } | null>>({});
//   const logsMapRef = useRef<Record<string, Record<string, LogEntry>>>({});

//   const sortLogs = (logsObj: Record<string, LogEntry>): LogEntry[] =>
//     Object.values(logsObj).sort((a, b) => b.created_at - a.created_at);

//   const fetchInitialLogs = async (jobId: string) => {
//     const res = await fetch(`/v1/job_logs/${jobId}/paginated?limit=1000`);
//     const json = await res.json();

//     const newLogs: LogEntry[] = json.logs.map((log: any) => ({
//       id: log.id,
//       message: log.msg,
//       created_at: log.time,
//     }));

//     logsMapRef.current[jobId] = {};
//     for (const log of newLogs) {
//       logsMapRef.current[jobId][log.id] = log;
//     }

//     setLogsByJobId((prev) => ({
//       ...prev,
//       [jobId]: sortLogs(logsMapRef.current[jobId]),
//     }));

//     cursorRef.current[jobId] = json.nextCursor
//       ? {
//           time: json.nextCursor.start_after_time,
//           id: json.nextCursor.start_after_id,
//         }
//       : null;

//     setHasMoreByJobId((prev) => ({ ...prev, [jobId]: !!json.nextCursor }));
//   };

//   const fetchMoreLogs = async (jobId: string) => {
//     const cursor = cursorRef.current[jobId];
//     if (!cursor) return;

//     const res = await fetch(
//       `/v1/job_logs/${jobId}/paginated?limit=1000&start_after_time=${cursor.time}&start_after_id=${cursor.id}`
//     );
//     const json = await res.json();

//     const newLogs: LogEntry[] = json.logs.map((log: any) => ({
//       id: log.id,
//       message: log.msg,
//       created_at: log.time,
//     }));

//     if (!logsMapRef.current[jobId]) logsMapRef.current[jobId] = {};

//     for (const log of newLogs) {
//       logsMapRef.current[jobId][log.id] = log;
//     }

//     setLogsByJobId((prev) => ({
//       ...prev,
//       [jobId]: sortLogs(logsMapRef.current[jobId]),
//     }));

//     cursorRef.current[jobId] = json.nextCursor
//       ? {
//           time: json.nextCursor.start_after_time,
//           id: json.nextCursor.start_after_id,
//         }
//       : null;

//     setHasMoreByJobId((prev) => ({ ...prev, [jobId]: !!json.nextCursor }));
//   };

//   return (
//     <LogsContext.Provider
//       value={{
//         logsByJobId,
//         hasMoreByJobId,
//         fetchInitialLogs,
//         fetchMoreLogs,
//       }}
//     >
//       {children}
//     </LogsContext.Provider>
//   );
// };

// export const useLogsContext = () => useContext(LogsContext);


import {
  createContext,
  useContext,
  useState,
  useRef,
} from "react";
import { LogEntry } from "@/types/coreTypes";

interface LogsContextType {
  logsByJobId: Record<string, LogEntry[]>;
  hasMoreByJobId: Record<string, boolean>;
  fetchInitialLogs: (jobId: string) => Promise<void>;
  fetchMoreLogs: (jobId: string) => Promise<void>;
}

const LogsContext = createContext<LogsContextType>({
  logsByJobId: {},
  hasMoreByJobId: {},
  fetchInitialLogs: async () => {},
  fetchMoreLogs: async () => {},
});

export const LogsProvider = ({ children }: { children: React.ReactNode }) => {
  const [logsByJobId, setLogsByJobId] = useState<Record<string, LogEntry[]>>({});
  const [hasMoreByJobId, setHasMoreByJobId] = useState<Record<string, boolean>>({});
  const cursorRef = useRef<Record<string, { time: number; id: string } | null>>({});
  const logsListRef = useRef<Record<string, Record<string, LogEntry>>>({});

  const sortLogs = (logMap: Record<string, LogEntry>): LogEntry[] =>
    Object.values(logMap).sort((a, b) => a.created_at - b.created_at);

  const fetchInitialLogs = async (jobId: string) => {
    const res = await fetch(`/v1/job_logs/${jobId}/paginated?limit=1000`);
    const json = await res.json();

    const newLogs: LogEntry[] = json.logs.map((log: any) => ({
      id: log.id,
      message: log.msg,
      created_at: log.time,
    }));

    logsListRef.current[jobId] = {};
    for (const log of newLogs.reverse()) {
      logsListRef.current[jobId][log.id] = log;
    }

    setLogsByJobId((prev) => ({
      ...prev,
      [jobId]: sortLogs(logsListRef.current[jobId]),
    }));

    cursorRef.current[jobId] = json.nextCursor
      ? {
          time: json.nextCursor.start_after_time,
          id: json.nextCursor.start_after_id,
        }
      : null;

    setHasMoreByJobId((prev) => ({ ...prev, [jobId]: !!json.nextCursor }));
  };

  const fetchMoreLogs = async (jobId: string) => {
    const cursor = cursorRef.current[jobId];
    if (!cursor) return;

    const res = await fetch(
      `/v1/job_logs/${jobId}/paginated?limit=1000&start_after_time=${cursor.time}&start_after_id=${cursor.id}`
    );
    const json = await res.json();

    const newLogs: LogEntry[] = json.logs.map((log: any) => ({
      id: log.id,
      message: log.msg,
      created_at: log.time,
    }));

    if (!logsListRef.current[jobId]) logsListRef.current[jobId] = {};
    for (const log of newLogs.reverse()) {
      logsListRef.current[jobId][log.id] = log;
    }

    setLogsByJobId((prev) => ({
      ...prev,
      [jobId]: sortLogs(logsListRef.current[jobId]),
    }));

    cursorRef.current[jobId] = json.nextCursor
      ? {
          time: json.nextCursor.start_after_time,
          id: json.nextCursor.start_after_id,
        }
      : null;

    setHasMoreByJobId((prev) => ({ ...prev, [jobId]: !!json.nextCursor }));
  };

  return (
    <LogsContext.Provider
      value={{
        logsByJobId,
        hasMoreByJobId,
        fetchInitialLogs,
        fetchMoreLogs,
      }}
    >
      {children}
    </LogsContext.Provider>
  );
};

export const useLogsContext = () => useContext(LogsContext);
