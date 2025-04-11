import { useEffect, useState } from "react";
import { LogEntry } from "@/types/coreTypes";

const JobLogs = ({ jobId }: { jobId: string }) => {
  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [visibleCountInput, setVisibleCountInput] = useState<string>("");
  const [sortDescending, setSortDescending] = useState(false); // default to newest first

  useEffect(() => {
    const eventSource = new EventSource(`/v1/job_logs/${jobId}`);

    eventSource.onmessage = (event) => {
      try {
        const data: LogEntry = JSON.parse(event.data);
        setLogs((prev) => {
          const updated = [...prev, data];
          const capped = updated.length > 10000 ? updated.slice(-10000) : updated;

          // Set default visible count to total logs
          if (visibleCountInput === "") {
            setVisibleCountInput(String(capped.length));
          }

          return capped;
        });
      } catch (err) {
        console.error("Error parsing log entry:", err);
      }
    };

    eventSource.onerror = () => {
      console.error("❌ Error receiving logs");
      eventSource.close();
    };

    return () => eventSource.close();
  }, [jobId]);

  const formatLocalTimestamp = (isoString: string) => {
    const d = new Date(isoString);
    const pad = (n: number) => n.toString().padStart(2, "0");
    const ms = d.getMilliseconds().toString().padStart(3, "0");
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.${ms}`;
  };

  // Final log count, clamped to total logs
  const finalVisibleCount = Math.min(parseInt(visibleCountInput || "0", 10), logs.length);
  const sortedLogs = [...logs].sort((a, b) =>
    sortDescending
      ? new Date(b.time).getTime() - new Date(a.time).getTime()
      : new Date(a.time).getTime() - new Date(b.time).getTime()
  );

  const logsToRender = sortedLogs.slice(0, finalVisibleCount);

  return (
    <div className="mt-4 flex flex-col max-h-[calc(100vh-210px)]">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-lg font-semibold text-[#3b5a64]">Logs</h2>
        <div className="flex gap-4 items-center">
          <label className="text-sm text-gray-500 flex items-center gap-2">
            Show
            <input
              type="number"
              min={1}
              max={logs.length}
              value={visibleCountInput}
              onChange={(e) => {
                const raw = e.target.value;
                if (/^\d*$/.test(raw)) {
                  setVisibleCountInput(raw);
                }
              }}
              onBlur={() => {
                // Enforce cap on blur
                const val = parseInt(visibleCountInput, 10);
                if (!isNaN(val)) {
                  const clamped = Math.min(val, logs.length);
                  setVisibleCountInput(String(clamped));
                }
              }}
              className="w-[80px] px-2 py-1 border rounded text-sm"
            />
          </label>

          <select
            value={sortDescending ? "desc" : "asc"}
            onChange={(e) => setSortDescending(e.target.value === "desc")}
            className="px-2 py-1 border rounded text-sm"
          >
            <option value="desc">Newest First</option>
            <option value="asc">Oldest First</option>
          </select>
        </div>
      </div>

      <div className="flex-1 bg-white border border-gray-200 rounded-lg shadow-sm overflow-y-auto">
        <ul className="font-mono text-sm text-gray-800">
          {logsToRender.length === 0 ? (
            <li className="px-4 py-2 italic text-gray-400">Waiting for logs…</li>
          ) : (
            logsToRender.map((log, i) => (
              <li
                key={i}
                className="flex px-4 py-2 border-t border-gray-300 gap-5"
              >
                <span className="text-gray-600 min-w-[220px]">
                  {formatLocalTimestamp(log.time)}
                </span>
                <span>{log.message}</span>
              </li>
            ))
          )}
        </ul>
      </div>
    </div>
  );
};

export default JobLogs;

