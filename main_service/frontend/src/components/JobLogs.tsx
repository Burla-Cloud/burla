// import { useEffect, useRef, useState, useCallback, useMemo } from "react";
// import { useLogsContext } from "@/contexts/LogsContext";
// import { VariableSizeList as List } from "react-window";

// interface JobLogsProps {
//     jobId: string;
//     jobStatus?: string;
// }

// const JobLogs = ({ jobId, jobStatus }: JobLogsProps) => {
//     const { logsByJobId, startLiveStream, loadInitial, closeLiveStream } = useLogsContext();
//     const rawLogs = logsByJobId[jobId] || [];
//     const logs = [...rawLogs];

//     type RowItem =
//         | { type: "divider"; key: string; label: string }
//         | { type: "log"; key: string; id: string; createdAt: number; message: string };

//     const formatDateLabel = (tsSeconds: number) => {
//         const d = new Date(tsSeconds * 1000);
//         return d.toLocaleDateString("en-US", {
//             weekday: "long",
//             month: "long",
//             day: "numeric",
//             year: "numeric",
//         });
//     };

//     const getDateKey = (tsSeconds: number) => {
//         const d = new Date(tsSeconds * 1000);
//         const y = d.getFullYear();
//         const m = String(d.getMonth() + 1).padStart(2, "0");
//         const da = String(d.getDate()).padStart(2, "0");
//         return `${y}-${m}-${da}`;
//     };

//     const items: RowItem[] = useMemo(() => {
//         const result: RowItem[] = [];
//         let lastDateKey: string | null = null;
//         for (const entry of logs) {
//             const dateKey = getDateKey(entry.created_at);
//             if (lastDateKey !== dateKey) {
//                 result.push({
//                     type: "divider",
//                     key: `divider-${dateKey}`,
//                     label: formatDateLabel(entry.created_at),
//                 });
//                 lastDateKey = dateKey;
//             }
//             const key = entry.id ?? `${entry.created_at}-${entry.message}`;
//             result.push({
//                 type: "log",
//                 key,
//                 id: key,
//                 createdAt: entry.created_at,
//                 message: entry.message || "No message",
//             });
//         }
//         return result;
//     }, [logs]);

//     const [hasAutoScrolled, setHasAutoScrolled] = useState(false);
//     const [windowWidth, setWindowWidth] = useState(window.innerWidth);
//     // All logs are expanded by default and non-interactive

//     const listRef = useRef<any>(null);
//     const containerRef = useRef<HTMLDivElement | null>(null);
//     const [listHeight, setListHeight] = useState<number>(300);
//     const [hasMeasuredContainer, setHasMeasuredContainer] = useState<boolean>(false);
//     const sizeMapRef = useRef<Record<string, number>>({});

//     const setSizeForKey = useCallback((key: string, size: number, fromIndex: number) => {
//         if (sizeMapRef.current[key] !== size) {
//             sizeMapRef.current[key] = size;
//             // Force the list to recompute sizes to avoid overlapping
//             listRef.current?.resetAfterIndex(fromIndex, true);
//         }
//     }, []);

//     useEffect(() => {
//         const updateWidth = () => setWindowWidth(window.innerWidth);
//         window.addEventListener("resize", updateWidth);

//         let observer: ResizeObserver | null = null;
//         if (containerRef.current) {
//             observer = new ResizeObserver((entries) => {
//                 const entry = entries[0];
//                 if (!entry) return;
//                 const h = Math.max(0, Math.floor(entry.contentRect.height));
//                 setListHeight(h);
//                 setHasMeasuredContainer(true);
//             });
//             observer.observe(containerRef.current);
//         }

//         return () => {
//             window.removeEventListener("resize", updateWidth);
//             if (observer && containerRef.current) observer.disconnect();
//         };
//     }, []);

//     const [isInitialLoading, setIsInitialLoading] = useState(logs.length === 0);

//     useEffect(() => {
//         let cancelled = false;
//         const run = async () => {
//             if (logs.length === 0) setIsInitialLoading(true);
//             try {
//                 await loadInitial(jobId, 0, 2000);
//             } finally {
//                 if (!cancelled) setIsInitialLoading(false);
//             }
//         };
//         run();
//         return () => {
//             cancelled = true;
//         };
//     }, [jobId, loadInitial]);

//     // Open/close SSE based on jobStatus
//     useEffect(() => {
//         if (jobStatus === "RUNNING") {
//             const stop = startLiveStream(jobId);
//             return () => stop();
//         }
//         // not running: ensure any open stream is closed
//         closeLiveStream(jobId);
//         return () => {};
//     }, [jobId, jobStatus, startLiveStream, closeLiveStream]);

//     useEffect(() => {
//         if (logs.length > 0 && listRef.current && !hasAutoScrolled && hasMeasuredContainer) {
//             listRef.current.scrollToItem(logs.length, "end");
//             setHasAutoScrolled(true);
//         }
//     }, [logs.length, hasAutoScrolled, hasMeasuredContainer]);

//     // When job changes, clear cached sizes and auto-scroll state to avoid layout glitches
//     useEffect(() => {
//         sizeMapRef.current = {};
//         listRef.current?.resetAfterIndex(0, true);
//         setHasAutoScrolled(false);
//     }, [jobId]);

//     // (no expand/collapse state needed)

//     const formatTime = (ts: number) => {
//         const date = new Date(ts * 1000);
//         return date.toLocaleTimeString("en-US", {
//             hour: "numeric",
//             minute: "2-digit",
//             second: "2-digit",
//             hour12: true,
//         });
//     };

//     // (no interaction)

//     const getItemSize = useCallback(
//         (index: number) => {
//             const row = items[index];
//             if (!row) return 36;
//             if (row.type === "divider") return 40;
//             // All logs are expanded; use measured size or a small fallback until measured
//             return sizeMapRef.current[row.id] ?? 36;
//         },
//         [items]
//     );

//     const handleFetchMorePreservePosition = async () => {};

//     const totalItemCount = items.length;

//     if (windowWidth <= 1000) {
//         return (
//             <div className="mt-4 mb-4 flex flex-col">
//                 <div className="flex items-center justify-between mb-3">
//                     <h2 className="text-lg font-semibold text-primary">Logs</h2>
//                 </div>
//                 <div className="text-gray-500 italic text-sm text-center p-4">
//                     Logs are hidden on small screens.
//                 </div>
//             </div>
//         );
//     }

//     return (
//         <div className="mt-4 mb-4 flex flex-col flex-1 min-h-0">
//             <div className="flex items-center justify-between mb-3">
//                 <h2 className="text-lg font-semibold text-primary">Logs</h2>
//             </div>

//             <div className="flex-1 min-h-0 bg-white border border-gray-200 rounded-lg shadow-sm relative">
//                 {isInitialLoading ? (
//                     <div
//                         ref={containerRef}
//                         className="h-full w-full flex items-center justify-center"
//                     >
//                         <div className="flex flex-col items-center text-gray-500">
//                             <div
//                                 className="h-8 w-8 rounded-full border-2 border-gray-300 border-t-primary animate-spin"
//                                 role="status"
//                                 aria-label="Loading logs"
//                             />
//                             <div className="mt-2 text-sm">Loading logs…</div>
//                         </div>
//                     </div>
//                 ) : logs.length === 0 ? (
//                     <div ref={containerRef} className="h-full w-full">
//                         <ul className="font-mono text-xs text-gray-800 h-full flex items-center justify-center">
//                             <li className="px-4 py-2 text-gray-400 text-sm text-center italic">
//                                 No logs
//                             </li>
//                         </ul>
//                     </div>
//                 ) : (
//                     <div ref={containerRef} className="font-mono text-xs text-gray-800 h-full">
//                         <List
//                             height={listHeight}
//                             itemCount={totalItemCount}
//                             itemSize={getItemSize}
//                             width="100%"
//                             ref={listRef}
//                             itemKey={(index) => items[index]?.key ?? index}
//                         >
//                             {({ index, style }) => {
//                                 const row = items[index];
//                                 if (!row) return null;

//                                 if (row.type === "divider") {
//                                     return (
//                                         <div
//                                             key={row.key}
//                                             style={style}
//                                             className="px-4 py-2"
//                                             role="separator"
//                                             aria-label={`Logs for ${row.label}`}
//                                         >
//                                             <div className="w-full flex items-center gap-3 select-none">
//                                                 <div
//                                                     className="h-px w-full bg-gray-200 dark:bg-gray-800"
//                                                     aria-hidden="true"
//                                                 />
//                                                 <span className="shrink-0 text-center text-xs sm:text-sm text-muted-foreground font-medium tracking-tight">
//                                                     {row.label}
//                                                 </span>
//                                                 <div
//                                                     className="h-px w-full bg-gray-200 dark:bg-gray-800"
//                                                     aria-hidden="true"
//                                                 />
//                                             </div>
//                                         </div>
//                                     );
//                                 }

//                                 const background = index % 2 === 0 ? "bg-gray-50" : "";

//                                 return (
//                                     <div key={row.key} style={style} className="">
//                                         <div
//                                             ref={(el) => {
//                                                 if (!el) return;
//                                                 requestAnimationFrame(() => {
//                                                     try {
//                                                         if (!el || !el.isConnected) return;
//                                                         const h = Math.ceil(el.offsetHeight);
//                                                         const desired = h;
//                                                         setSizeForKey(row.id, desired, index);
//                                                     } catch {}
//                                                 });
//                                             }}
//                                             className={`grid grid-cols-[8rem,1fr] gap-2 px-4 py-2 border-t border-gray-200 transition ${background}`}
//                                         >
//                                             <div className="text-gray-500 text-left tabular-nums">
//                                                 {formatTime(row.createdAt)}
//                                             </div>
//                                             <div className={"whitespace-pre-wrap break-words"}>
//                                                 {row.message}
//                                             </div>
//                                         </div>
//                                     </div>
//                                 );
//                             }}
//                         </List>
//                     </div>
//                 )}
//             </div>
//         </div>
//     );
// };

// export default JobLogs;

import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { useLogsContext } from "@/contexts/LogsContext";
import { VariableSizeList as List } from "react-window";
import { Switch } from "@/components/ui/switch";

interface JobLogsProps {
  jobId: string;
  jobStatus?: string;
  nInputs?: number;
}

type RowItem =
  | { type: "divider"; key: string; label: string }
  | { type: "empty"; key: string; label: string }
  | { type: "log"; key: string; id: string; createdAt: number; message: string };

const PAGE_SIZE = 100;
const LIMIT_PER_INDEX = 200;

const JobLogs = ({ jobId, jobStatus, nInputs }: JobLogsProps) => {
  const { getLogs, loadSummary, loadPage, evictToWindow, startLiveStream, closeLiveStream } =
    useLogsContext();

  const [selectedIndex, setSelectedIndex] = useState<number>(0);
  const [showFailedOnly, setShowFailedOnly] = useState(false);
  const [failedIndexes, setFailedIndexes] = useState<number[]>([]);
  const [isPageLoading, setIsPageLoading] = useState(true);

  const [hasAutoScrolled, setHasAutoScrolled] = useState(false);
  const [windowWidth, setWindowWidth] = useState(window.innerWidth);

  const listRef = useRef<List>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  const [listHeight, setListHeight] = useState<number>(300);
  const [hasMeasuredContainer, setHasMeasuredContainer] = useState<boolean>(false);

  const sizeMapRef = useRef<Record<string, number>>({});

  const setSizeForKey = useCallback((key: string, size: number, fromIndex: number) => {
    if (sizeMapRef.current[key] !== size) {
      sizeMapRef.current[key] = size;
      listRef.current?.resetAfterIndex(fromIndex, true);
    }
  }, []);

  // Load summary once per job
  useEffect(() => {
    let cancelled = false;
    (async () => {
      const s = await loadSummary(jobId);
      if (cancelled) return;
      setFailedIndexes(s?.failed_indexes || []);
    })();
    return () => {
      cancelled = true;
    };
  }, [jobId, loadSummary]);

  // Resize plumbing
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
      if (observer) observer.disconnect();
    };
  }, []);

  const totalInputs = useMemo(() => {
    if (typeof nInputs === "number" && nInputs > 0) return nInputs;
    return 0;
  }, [nInputs]);

  const activeIndexList = useMemo(() => {
    if (showFailedOnly) return failedIndexes;
    if (totalInputs > 0) return Array.from({ length: totalInputs }, (_, i) => i);
    return [];
  }, [showFailedOnly, failedIndexes, totalInputs]);

  // Keep selectedIndex valid when toggling failed-only
  useEffect(() => {
    if (activeIndexList.length === 0) {
      setSelectedIndex(0);
      return;
    }
    if (!activeIndexList.includes(selectedIndex)) setSelectedIndex(activeIndexList[0]);
  }, [activeIndexList, selectedIndex]);

  const displayIndex = useMemo(() => {
    if (activeIndexList.length === 0) return 1;
    const pos = activeIndexList.indexOf(selectedIndex);
    return pos === -1 ? 1 : pos + 1;
  }, [activeIndexList, selectedIndex]);

  const displayTotal = useMemo(() => {
    if (showFailedOnly) return failedIndexes.length;
    return totalInputs;
  }, [showFailedOnly, failedIndexes.length, totalInputs]);

  const maxKnownIndex = useMemo(() => {
    if (totalInputs > 0) return totalInputs - 1;
    if (failedIndexes.length > 0) return Math.max(...failedIndexes);
    return -1;
  }, [totalInputs, failedIndexes]);

  const pageStart = useMemo(
    () => Math.floor(Math.max(0, selectedIndex) / PAGE_SIZE) * PAGE_SIZE,
    [selectedIndex]
  );

  const pageEnd = useMemo(() => {
    if (maxKnownIndex < 0) return -1;
    return Math.min(pageStart + PAGE_SIZE - 1, maxKnownIndex);
  }, [pageStart, maxKnownIndex]);

  // Load current page (100 indexes at a time) and keep a small window cached
  useEffect(() => {
    if (pageEnd < pageStart || pageEnd < 0) {
      setIsPageLoading(false);
      return;
    }

    let cancelled = false;
    (async () => {
      setIsPageLoading(true);
      try {
        await loadPage(jobId, pageStart, pageEnd, LIMIT_PER_INDEX, true);

        // keep current page plus neighbors, but allow re-fetch later if evicted
        evictToWindow(jobId, pageStart, pageEnd, 3);
      } finally {
        if (!cancelled) setIsPageLoading(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [jobId, pageStart, pageEnd, loadPage, evictToWindow]);

  const goPrev = () => {
    if (isPageLoading) return;
    if (activeIndexList.length === 0) return;
    const pos = activeIndexList.indexOf(selectedIndex);
    const nextPos = pos <= 0 ? activeIndexList.length - 1 : pos - 1;
    setSelectedIndex(activeIndexList[nextPos]);
  };

  const goNext = () => {
    if (isPageLoading) return;
    if (activeIndexList.length === 0) return;
    const pos = activeIndexList.indexOf(selectedIndex);
    const nextPos = pos === -1 || pos === activeIndexList.length - 1 ? 0 : pos + 1;
    setSelectedIndex(activeIndexList[nextPos]);
  };

  // Stream logs for current index while RUNNING
  useEffect(() => {
    if (jobStatus === "RUNNING") {
      const stop = startLiveStream(jobId, selectedIndex, true);
      return () => stop();
    }

    closeLiveStream(jobId);
    return () => {};
  }, [jobId, jobStatus, selectedIndex, startLiveStream, closeLiveStream]);

  // Reset react-window sizing on index/toggle changes
  useEffect(() => {
    sizeMapRef.current = {};
    listRef.current?.resetAfterIndex(0, true);
    setHasAutoScrolled(false);
  }, [jobId, selectedIndex, showFailedOnly]);

  const logs = useMemo(() => getLogs(jobId, selectedIndex), [getLogs, jobId, selectedIndex]);

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
    if (logs.length === 0) {
      result.push({ type: "empty", key: "empty", label: `No logs for input ${selectedIndex + 1}` });
      return result;
    }

    let lastDateKey: string | null = null;

    for (const entry of logs) {
      const createdAt = entry.created_at ?? 0;
      const dateKey = getDateKey(createdAt);

      if (lastDateKey !== dateKey) {
        result.push({ type: "divider", key: `divider-${dateKey}`, label: formatDateLabel(createdAt) });
        lastDateKey = dateKey;
      }

      const id =
        entry.id ??
        `${createdAt}-${entry.index ?? "na"}-${entry.is_error ? 1 : 0}-${entry.message ?? ""}`;

      result.push({
        type: "log",
        key: `log-${id}`,
        id: String(id),
        createdAt,
        message: entry.message || "No message",
      });
    }

    return result;
  }, [logs, selectedIndex]);

  const formatTime = (ts: number) => {
    const date = new Date(ts * 1000);
    return date.toLocaleTimeString("en-US", {
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
      hour12: true,
    });
  };

  const getItemSize = useCallback(
    (index: number) => {
      const row = items[index];
      if (!row) return 36;
      if (row.type === "divider") return 40;
      if (row.type === "empty") return 60;
      return sizeMapRef.current[row.id] ?? 36;
    },
    [items]
  );

  useEffect(() => {
    if (!hasMeasuredContainer) return;
    if (items.length === 0) return;
    if (hasAutoScrolled) return;
    listRef.current?.scrollToItem(items.length - 1, "end");
    setHasAutoScrolled(true);
  }, [items.length, hasMeasuredContainer, hasAutoScrolled]);

  if (windowWidth <= 1000) {
    return (
      <div className="mt-4 mb-4 flex flex-col">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-lg font-semibold text-primary">Logs</h2>
        </div>
        <div className="text-gray-500 italic text-sm text-center p-4">Logs are hidden on small screens.</div>
      </div>
    );
  }

  const navBtnClass = (disabled: boolean) =>
    disabled
      ? "px-2 py-1 border border-gray-300 rounded opacity-50 cursor-default"
      : "px-2 py-1 border border-gray-300 rounded hover:bg-gray-50";

  return (
    <div className="mt-4 mb-4 flex flex-col flex-1 min-h-0">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-lg font-semibold text-primary">Logs</h2>

        {activeIndexList.length > 0 && (
          <div className="flex items-center gap-6">
            <div className="flex items-center gap-2 text-sm text-gray-700">
              <button type="button" onClick={goPrev} disabled={isPageLoading} className={navBtnClass(isPageLoading)}>
                {"<"}
              </button>
              <span className="tabular-nums">
                {displayIndex} / {displayTotal || 0}
              </span>
              <button type="button" onClick={goNext} disabled={isPageLoading} className={navBtnClass(isPageLoading)}>
                {">"}
              </button>
            </div>

            <label className="flex items-center gap-0.5 text-sm text-slate-600">
              <Switch
                checked={showFailedOnly}
                onCheckedChange={(checked) => {
                  setShowFailedOnly(checked);
                  if (checked && failedIndexes.length > 0) setSelectedIndex(failedIndexes[0]);
                  if (!checked) setSelectedIndex(0);
                }}
                disabled={failedIndexes.length === 0 || isPageLoading}
                className="scale-75 origin-left"
              />
              <span className="text-sm text-muted-foreground">Failed only</span>
            </label>
          </div>
        )}
      </div>

      <div className="flex-1 min-h-0 bg-white border border-gray-200 rounded-lg shadow-sm relative">
        {isPageLoading ? (
          <div ref={containerRef} className="h-full w-full flex items-center justify-center">
            <div className="flex flex-col items-center text-gray-500">
              <div
                className="h-8 w-8 rounded-full border-2 border-gray-300 border-t-primary animate-spin"
                role="status"
                aria-label="Loading logs"
              />
              <div className="mt-2 text-sm">Loading logs…</div>
            </div>
          </div>
        ) : (
          <div ref={containerRef} className="font-mono text-xs text-gray-800 h-full">
            <List
              height={listHeight}
              itemCount={items.length}
              itemSize={getItemSize}
              width="100%"
              ref={listRef as any}
              itemKey={(index) => items[index]?.key ?? index}
            >
              {({ index, style }) => {
                const row = items[index];
                if (!row) return null;

                if (row.type === "divider") {
                  return (
                    <div key={row.key} style={style} className="px-4 py-2" role="separator" aria-label={`Logs for ${row.label}`}>
                      <div className="w-full flex items-center gap-3 select-none">
                        <div className="h-px w-full bg-gray-200" aria-hidden="true" />
                        <span className="shrink-0 text-center text-xs sm:text-sm text-muted-foreground font-medium tracking-tight">
                          {row.label}
                        </span>
                        <div className="h-px w-full bg-gray-200" aria-hidden="true" />
                      </div>
                    </div>
                  );
                }

                if (row.type === "empty") {
                  return (
                    <div key={row.key} style={style} className="px-4 py-4 text-gray-400 italic">
                      {row.label}
                    </div>
                  );
                }

                const background = index % 2 === 0 ? "bg-gray-50" : "";

                return (
                  <div key={row.key} style={style}>
                    <div
                      ref={(el) => {
                        if (!el) return;
                        requestAnimationFrame(() => {
                          if (!el.isConnected) return;
                          const h = Math.ceil(el.offsetHeight);
                          setSizeForKey(row.id, h, index);
                        });
                      }}
                      className={`grid grid-cols-[8rem,1fr] gap-2 px-4 py-2 border-t border-gray-200 transition ${background}`}
                    >
                      <div className="text-gray-500 text-left tabular-nums">{formatTime(row.createdAt)}</div>
                      <div className="whitespace-pre-wrap break-words">{row.message}</div>
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
