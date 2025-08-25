import { useEffect, useRef, useState, useCallback, useMemo } from "react";
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

    type RowItem =
        | { type: "divider"; key: string; label: string }
        | { type: "log"; key: string; id: string; createdAt: number; message: string };

    const formatDateLabel = (tsSeconds: number) => {
        const d = new Date(tsSeconds * 1000);
        return d.toLocaleDateString("en-US", {
            weekday: "long",
            month: "long",
            day: "numeric",
            year: "numeric",
        });
    };

    const getDateKey = (tsSeconds: number) => {
        const d = new Date(tsSeconds * 1000);
        const y = d.getFullYear();
        const m = String(d.getMonth() + 1).padStart(2, "0");
        const da = String(d.getDate()).padStart(2, "0");
        return `${y}-${m}-${da}`;
    };

    const items: RowItem[] = useMemo(() => {
        const result: RowItem[] = [];
        let lastDateKey: string | null = null;
        for (const entry of logs) {
            const dateKey = getDateKey(entry.created_at);
            if (lastDateKey !== dateKey) {
                result.push({
                    type: "divider",
                    key: `divider-${dateKey}`,
                    label: formatDateLabel(entry.created_at),
                });
                lastDateKey = dateKey;
            }
            const key = entry.id ?? `${entry.created_at}-${entry.message}`;
            result.push({
                type: "log",
                key,
                id: key,
                createdAt: entry.created_at,
                message: entry.message || "No message",
            });
        }
        return result;
    }, [logs]);

    const [hasAutoScrolled, setHasAutoScrolled] = useState(false);
    const [windowWidth, setWindowWidth] = useState(window.innerWidth);
    // All logs are expanded by default and non-interactive

    const listRef = useRef<any>(null);
    const containerRef = useRef<HTMLDivElement | null>(null);
    const [listHeight, setListHeight] = useState<number>(300);
    const [hasMeasuredContainer, setHasMeasuredContainer] = useState<boolean>(false);
    const sizeMapRef = useRef<Record<string, number>>({});

    const setSizeForKey = useCallback((key: string, size: number, fromIndex: number) => {
        if (sizeMapRef.current[key] !== size) {
            sizeMapRef.current[key] = size;
            // Force the list to recompute sizes to avoid overlapping
            listRef.current?.resetAfterIndex(fromIndex, true);
        }
    }, []);

    useEffect(() => {
        const updateWidth = () => setWindowWidth(window.innerWidth);
        window.addEventListener("resize", updateWidth);

        let observer: ResizeObserver | null = null;
        if (containerRef.current) {
            observer = new ResizeObserver((entries) => {
                const entry = entries[0];
                if (!entry) return;
                const h = Math.max(0, Math.floor(entry.contentRect.height));
                setListHeight(h);
                setHasMeasuredContainer(true);
            });
            observer.observe(containerRef.current);
        }

        return () => {
            window.removeEventListener("resize", updateWidth);
            if (observer && containerRef.current) observer.disconnect();
        };
    }, []);

    const [isInitialLoading, setIsInitialLoading] = useState(logs.length === 0);

    useEffect(() => {
        let cancelled = false;
        const run = async () => {
            if (logs.length === 0) setIsInitialLoading(true);
            try {
                await loadInitial(jobId, 0, 2000);
            } finally {
                if (!cancelled) setIsInitialLoading(false);
            }
        };
        run();
        return () => {
            cancelled = true;
        };
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
        if (logs.length > 0 && listRef.current && !hasAutoScrolled && hasMeasuredContainer) {
            listRef.current.scrollToItem(logs.length, "end");
            setHasAutoScrolled(true);
        }
    }, [logs.length, hasAutoScrolled, hasMeasuredContainer]);

    // When job changes, clear cached sizes and auto-scroll state to avoid layout glitches
    useEffect(() => {
        sizeMapRef.current = {};
        listRef.current?.resetAfterIndex(0, true);
        setHasAutoScrolled(false);
    }, [jobId]);

    // (no expand/collapse state needed)

    const formatTime = (ts: number) => {
        const date = new Date(ts * 1000);
        return date.toLocaleTimeString("en-US", {
            hour: "numeric",
            minute: "2-digit",
            second: "2-digit",
            hour12: true,
        });
    };

    // (no interaction)

    const getItemSize = useCallback(
        (index: number) => {
            const row = items[index];
            if (!row) return 36;
            if (row.type === "divider") return 40;
            // All logs are expanded; use measured size or a small fallback until measured
            return sizeMapRef.current[row.id] ?? 36;
        },
        [items]
    );

    const handleFetchMorePreservePosition = async () => {};

    const totalItemCount = items.length;

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
        <div className="mt-4 mb-4 flex flex-col flex-1 min-h-0">
            <div className="flex items-center justify-between mb-3">
                <h2 className="text-lg font-semibold text-primary">Logs</h2>
            </div>

            <div className="flex-1 min-h-0 bg-white border border-gray-200 rounded-lg shadow-sm relative">
                {isInitialLoading ? (
                    <div
                        ref={containerRef}
                        className="h-full w-full flex items-center justify-center"
                    >
                        <div className="flex flex-col items-center text-gray-500">
                            <div
                                className="h-8 w-8 rounded-full border-2 border-gray-300 border-t-primary animate-spin"
                                role="status"
                                aria-label="Loading logs"
                            />
                            <div className="mt-2 text-sm">Loading logs…</div>
                        </div>
                    </div>
                ) : logs.length === 0 ? (
                    <div ref={containerRef} className="h-full w-full">
                        <ul className="font-mono text-xs text-gray-800 h-full flex items-center justify-center">
                            <li className="px-4 py-2 text-gray-400 text-sm text-center italic">
                                No logs
                            </li>
                        </ul>
                    </div>
                ) : (
                    <div ref={containerRef} className="font-mono text-xs text-gray-800 h-full">
                        <List
                            height={listHeight}
                            itemCount={totalItemCount}
                            itemSize={getItemSize}
                            width="100%"
                            ref={listRef}
                            itemKey={(index) => items[index]?.key ?? index}
                        >
                            {({ index, style }) => {
                                const row = items[index];
                                if (!row) return null;

                                if (row.type === "divider") {
                                    return (
                                        <div
                                            key={row.key}
                                            style={style}
                                            className="px-4 py-2"
                                            role="separator"
                                            aria-label={`Logs for ${row.label}`}
                                        >
                                            <div className="w-full flex items-center gap-3 select-none">
                                                <div
                                                    className="h-px w-full bg-gray-200 dark:bg-gray-800"
                                                    aria-hidden="true"
                                                />
                                                <span className="shrink-0 text-center text-xs sm:text-sm text-muted-foreground font-medium tracking-tight">
                                                    {row.label}
                                                </span>
                                                <div
                                                    className="h-px w-full bg-gray-200 dark:bg-gray-800"
                                                    aria-hidden="true"
                                                />
                                            </div>
                                        </div>
                                    );
                                }

                                const background = index % 2 === 0 ? "bg-gray-50" : "";

                                return (
                                    <div key={row.key} style={style} className="">
                                        <div
                                            ref={(el) => {
                                                if (!el) return;
                                                requestAnimationFrame(() => {
                                                    try {
                                                        if (!el || !el.isConnected) return;
                                                        const h = Math.ceil(el.offsetHeight);
                                                        const desired = h;
                                                        setSizeForKey(row.id, desired, index);
                                                    } catch {}
                                                });
                                            }}
                                            className={`grid grid-cols-[8rem,1fr] gap-2 px-4 py-2 border-t border-gray-200 transition ${background}`}
                                        >
                                            <div className="text-gray-500 text-left tabular-nums">
                                                {formatTime(row.createdAt)}
                                            </div>
                                            <div className={"whitespace-pre-wrap break-words"}>
                                                {row.message}
                                            </div>
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
