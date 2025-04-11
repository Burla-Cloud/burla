import { createContext, useContext, useEffect, useState, useRef } from "react";
import { LogEntry } from "@/types/coreTypes";

interface LogsContextType {
  logsByJobId: Record<string, LogEntry[]>;
  startListening: (jobId: string) => void;
  stopListening: (jobId: string) => void;
}

const LogsContext = createContext<LogsContextType>({
  logsByJobId: {},
  startListening: () => {},
  stopListening: () => {},
});

export const LogsProvider = ({ children }: { children: React.ReactNode }) => {
  const [logsByJobId, setLogsByJobId] = useState<Record<string, LogEntry[]>>(() => {
    const cached = localStorage.getItem("logsByJobId");
    return cached ? JSON.parse(cached) : {};
  });

  const sourcesRef = useRef<Record<string, EventSource>>({});

  // ✅ Keep localStorage in sync
  useEffect(() => {
    localStorage.setItem("logsByJobId", JSON.stringify(logsByJobId));
  }, [logsByJobId]);

  const startListening = (jobId: string) => {
    if (sourcesRef.current[jobId]) return; // Already listening

    const eventSource = new EventSource(`/v1/job_logs/${jobId}`);

    eventSource.onmessage = (event) => {
      try {
        const data: LogEntry = JSON.parse(event.data);
        setLogsByJobId((prev) => ({
          ...prev,
          [jobId]: [...(prev[jobId] || []), data],
        }));
      } catch (err) {
        console.error("Error parsing log entry:", err);
      }
    };

    eventSource.onerror = () => {
      console.error(`❌ Error streaming logs for job ${jobId}`);
      eventSource.close();
      delete sourcesRef.current[jobId];
    };

    sourcesRef.current[jobId] = eventSource;
  };

  const stopListening = (jobId: string) => {
    const source = sourcesRef.current[jobId];
    if (source) {
      source.close();
      delete sourcesRef.current[jobId];
    }
  };

  return (
    <LogsContext.Provider value={{ logsByJobId, startListening, stopListening }}>
      {children}
    </LogsContext.Provider>
  );
};

export const useLogs = (jobId: string): LogEntry[] => {
  const { logsByJobId, startListening, stopListening } = useContext(LogsContext);

  useEffect(() => {
    startListening(jobId);
    return () => stopListening(jobId);
  }, [jobId]);

  return logsByJobId[jobId] || [];
};
