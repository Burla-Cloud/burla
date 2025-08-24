import { useEffect, useRef, useState, useCallback } from "react";
import { useLogsContext } from "@/contexts/LogsContext";
import { VariableSizeList as List } from "react-window";

interface JobLogsProps {
    jobId: string;
    jobStatus?: string;
}

const JobLogs = ({ jobId, jobStatus }: JobLogsProps) => {
    const { logsByJobId, startLiveStream, loadInitial, closeLiveStream } = useLogsContext();
    const rawLogs = logsByJobId[jobId] || [];
    const logs = [...rawLogs];

    const [hasAutoScrolled, setHasAutoScrolled] = useState(false);
    const [windowWidth, setWindowWidth] = useState(window.innerWidth);
    const [expandedLogs, setExpandedLogs] = useState<{ [id: string]: boolean }>({});

    const listRef = useRef<any>(null);

    const listMaxHeight = window.innerHeight - 250;

    useEffect(() => {
        const updateWidth = () => setWindowWidth(window.innerWidth);
        window.addEventListener("resize", updateWidth);
        return () => window.removeEventListener("resize", updateWidth);
    }, []);

    // Load initial logs once per jobId
    useEffect(() => {
        loadInitial(jobId, 0, 2000);
    }, [jobId, loadInitial]);

    // Open/close SSE based on jobStatus
    useEffect(() => {
        if (jobStatus === "RUNNING") {
            const stop = startLiveStream(jobId);
            return () => stop();
        }
        // not running: ensure any open stream is closed
        closeLiveStream(jobId);
        return () => {};
    }, [jobId, jobStatus, startLiveStream, closeLiveStream]);

    useEffect(() => {
        if (logs.length > 0 && listRef.current && !hasAutoScrolled) {
            listRef.current.scrollToItem(logs.length, "end");
            setHasAutoScrolled(true);
        }
    }, [logs.length, hasAutoScrolled]);

    const formatTime = (ts: number) => {
        const date = new Date(ts * 1000);
        return date.toLocaleTimeString("en-US", {
            hour: "numeric",
            minute: "2-digit",
            second: "2-digit",
            hour12: true,
        });
    };

    const toggleExpand = (id: string) => {
        setExpandedLogs((prev) => {
            const updated = { ...prev, [id]: !prev[id] };
            setTimeout(() => {
                listRef.current?.resetAfterIndex(0); // recalculate row heights
            }, 0);
            return updated;
        });
    };

    const getItemSize = useCallback(
        (index: number) => {
            const log = logs[index];
            const id = log?.id ?? `${log?.created_at}-${log?.message}`;
            return expandedLogs[id] ? 72 : 36;
        },
        [expandedLogs, logs]
    );

    const handleFetchMorePreservePosition = async () => {};

    const totalItemCount = logs.length;

    if (windowWidth <= 1000) {
        return (
            <div className="mt-4 mb-4 flex flex-col">
                <div className="flex items-center justify-between mb-3">
                    <h2 className="text-lg font-semibold text-primary">Logs</h2>
                </div>
                <div className="text-gray-500 italic text-sm text-center p-4">
                    Logs are hidden on small screens.
                </div>
            </div>
        );
    }

    return (
        <div
            className="mt-4 mb-4 flex flex-col"
            style={{ maxHeight: "calc(100vh - 140px)", overflow: "hidden" }}
        >
            <div className="flex items-center justify-between mb-3">
                <h2 className="text-lg font-semibold text-primary">Logs</h2>
            </div>

            <div className="flex-1 bg-white border border-gray-200 rounded-lg shadow-sm relative">
                {logs.length === 0 ? (
                    <ul className="font-mono text-xs text-gray-800">
                        <li className="px-4 py-2 text-gray-400 text-sm text-center italic min-h-[75px] flex items-center justify-center">
                            No logs
                        </li>
                    </ul>
                ) : (
                    <div className="font-mono text-xs text-gray-800 h-full">
                        <List
                            height={
                                logs.length * 36 < listMaxHeight
                                    ? logs.length * 36 + 1
                                    : listMaxHeight
                            }
                            itemCount={totalItemCount}
                            itemSize={getItemSize}
                            width="100%"
                            ref={listRef}
                        >
                            {({ index, style }) => {
                                const log = logs[index];
                                const id = log.id ?? `${log.created_at}-${log.message}`;
                                const isExpanded = expandedLogs[id];

                                return (
                                    <div
                                        key={id}
                                        style={style}
                                        onClick={() => toggleExpand(id)}
                                        className={`grid grid-cols-[8rem,1fr] gap-2 px-4 py-2 border-t border-gray-200 cursor-pointer transition ${
                                            index % 2 === 0 ? "bg-gray-50" : ""
                                        } hover:bg-gray-100`}
                                    >
                                        <div className="text-gray-500 text-left tabular-nums">
                                            {formatTime(log.created_at)}
                                        </div>
                                        <div
                                            className={
                                                isExpanded
                                                    ? "whitespace-normal break-words"
                                                    : "truncate"
                                            }
                                        >
                                            {log.message || "No message"}
                                        </div>
                                    </div>
                                );
                            }}
                        </List>
                    </div>
                )}
            </div>
        </div>
    );
};

export default JobLogs;
