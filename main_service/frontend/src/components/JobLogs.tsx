
import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { useLogsContext } from "@/contexts/LogsContext";
import { VariableSizeList as List } from "react-window";
import { Switch } from "@/components/ui/switch";
import { LogEntry } from "@/types/coreTypes";

interface JobLogsProps {
  jobId: string;
  nInputs?: number;
}

type RowItem =
  | { type: "divider"; key: string; label: string }
  | { type: "empty"; key: string; label: string }
  | { type: "log"; key: string; id: string; logTimestamp: number; message: string };

const getLogRowIdentifier = (logEntry: LogEntry) =>
  `${logEntry.log_timestamp}-${logEntry.is_error ? 1 : 0}-${logEntry.message ?? ""}`;

const JobLogs = ({ jobId, nInputs }: JobLogsProps) => {
  const {
    getLogs,
    getFailedInputsCount,
    getHasMoreOlderLogs,
    getOldestLoadedLogDocumentTimestamp,
    getNextFailedInputIndex,
    loadInputLogs,
    logsByJobId,
  } = useLogsContext();

  const [selectedIndex, setSelectedIndex] = useState<number>(0);
  const [showFailedOnly, setShowFailedOnly] = useState(false);

  const [isPageLoading, setIsPageLoading] = useState(true);
  const [isLoadingOlderLogs, setIsLoadingOlderLogs] = useState(false);

  const [hasAutoScrolled, setHasAutoScrolled] = useState(false);
  const [windowWidth, setWindowWidth] = useState(window.innerWidth);

  const listRef = useRef<List>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

  const [listHeight, setListHeight] = useState<number>(300);
  const [hasMeasuredContainer, setHasMeasuredContainer] = useState<boolean>(false);

  const sizeMapRef = useRef<Record<string, number>>({});
  const topAnchorLogIdRef = useRef<string | null>(null);

  const setSizeForKey = useCallback((key: string, size: number, fromIndex: number) => {
    if (sizeMapRef.current[key] !== size) {
      sizeMapRef.current[key] = size;
      listRef.current?.resetAfterIndex(fromIndex, true);
    }
  }, []);

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

  const availableIndexesFromLogs = useMemo(() => {
    const state = logsByJobId[jobId];
    if (!state) return [];
    return Object.keys(state.byIndex || {})
      .map((k) => Number(k))
      .filter((v) => Number.isFinite(v))
      .sort((a, b) => a - b);
  }, [jobId, logsByJobId]);

  const hasAnyKnownIndexes = availableIndexesFromLogs.length > 0;
  const failedInputsCount = getFailedInputsCount(jobId);

  const activeIndexList = useMemo(() => {
    if (totalInputs > 0) return Array.from({ length: totalInputs }, (_, i) => i);
    if (availableIndexesFromLogs.length > 0) return availableIndexesFromLogs;
    return [];
  }, [totalInputs, availableIndexesFromLogs]);

  // Keep selectedIndex valid when lists change
  useEffect(() => {
    if (activeIndexList.length === 0) {
      setSelectedIndex(0);
      return;
    }
    if (!activeIndexList.includes(selectedIndex)) setSelectedIndex(activeIndexList[0]);
  }, [activeIndexList, selectedIndex]);

  const maxKnownIndex = useMemo(() => {
    if (totalInputs > 0) return totalInputs - 1;
    if (activeIndexList.length > 0) return Math.max(...activeIndexList);
    return -1;
  }, [totalInputs, activeIndexList]);

  // Load only the currently viewed input index.
  useEffect(() => {
    if (selectedIndex < 0) {
      setIsPageLoading(false);
      return;
    }

    let cancelled = false;
    (async () => {
      setIsPageLoading(true);
      try {
        await loadInputLogs(jobId, selectedIndex);
      } finally {
        if (!cancelled) setIsPageLoading(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [jobId, selectedIndex, loadInputLogs]);

  // Logs for current index
  const logs = useMemo(() => getLogs(jobId, selectedIndex), [getLogs, jobId, selectedIndex]);
  const hasMoreOlderLogs = getHasMoreOlderLogs(jobId, selectedIndex);
  const oldestLoadedLogDocumentTimestamp = getOldestLoadedLogDocumentTimestamp(jobId, selectedIndex);

  const formatDateLabel = (logTimestamp: number) => {
    const d = new Date(logTimestamp * 1000);
    return d.toLocaleDateString("en-US", {
      weekday: "long",
      month: "long",
      day: "numeric",
      year: "numeric",
    });
  };

  const getDateKey = (logTimestamp: number) => {
    const d = new Date(logTimestamp * 1000);
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const da = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${da}`;
  };

  const items: RowItem[] = useMemo(() => {
    const result: RowItem[] = [];

    if (logs.length === 0) {
      const jobHasAnyPerInputLogs = hasAnyKnownIndexes;

      result.push({
        type: "empty",
        key: "empty",
        label: jobHasAnyPerInputLogs
          ? `No logs for input ${selectedIndex + 1}`
          : "This job produced no log output",
      });
      return result;
    }

    let lastDateKey: string | null = null;
    const occurrenceBySignature = new Map<string, number>();

    for (const entry of logs as LogEntry[]) {
      const logTimestamp = entry.log_timestamp;
      const dateKey = getDateKey(logTimestamp);

      if (lastDateKey !== dateKey) {
        result.push({
          type: "divider",
          key: `divider-${dateKey}`,
          label: formatDateLabel(logTimestamp),
        });
        lastDateKey = dateKey;
      }

      const signature = getLogRowIdentifier(entry);
      const nextOccurrence = (occurrenceBySignature.get(signature) ?? 0) + 1;
      occurrenceBySignature.set(signature, nextOccurrence);
      const id = `${signature}::${nextOccurrence}`;

      result.push({
        type: "log",
        key: `log-${id}`,
        id: String(id),
        logTimestamp,
        message: entry.message || "No message",
      });
    }

    return result;
  }, [logs, hasAnyKnownIndexes, selectedIndex]);

  const jobHasNoLogsAtAll = !hasAnyKnownIndexes;

  const stepperDisabled = isPageLoading || activeIndexList.length === 0 || jobHasNoLogsAtAll;

  const goPrev = () => {
    if (stepperDisabled) return;
    if (showFailedOnly) return;
    const pos = activeIndexList.indexOf(selectedIndex);
    const nextPos = pos <= 0 ? activeIndexList.length - 1 : pos - 1;
    setSelectedIndex(activeIndexList[nextPos]);
  };

  const selectNextFailedInput = useCallback(async () => {
    setIsPageLoading(true);
    const nextFailedInputIndex = await getNextFailedInputIndex(jobId, selectedIndex);
    if (nextFailedInputIndex === null || nextFailedInputIndex === selectedIndex) {
      setIsPageLoading(false);
      return;
    }
    setSelectedIndex(nextFailedInputIndex);
  }, [getNextFailedInputIndex, jobId, selectedIndex]);

  const goNext = async () => {
    if (stepperDisabled) return;
    if (showFailedOnly) {
      await selectNextFailedInput();
      return;
    }
    const pos = activeIndexList.indexOf(selectedIndex);
    const nextPos = pos === -1 || pos === activeIndexList.length - 1 ? 0 : pos + 1;
    setSelectedIndex(activeIndexList[nextPos]);
  };

  // Reset react-window sizing on index and filter changes
  useEffect(() => {
    sizeMapRef.current = {};
    listRef.current?.resetAfterIndex(0, true);
    setHasAutoScrolled(false);
  }, [jobId, selectedIndex, showFailedOnly]);

  const formatTime = (logTimestamp: number) => {
    const date = new Date(logTimestamp * 1000);
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
    if (isPageLoading) return;
    if (items.length === 0) return;
    if (hasAutoScrolled) return;

    const scrollToNewestLog = () => {
      listRef.current?.scrollToItem(items.length - 1, "end");
    };

    scrollToNewestLog();
    const delayedScrollTimer = window.setTimeout(scrollToNewestLog, 30);
    setHasAutoScrolled(true);

    return () => {
      window.clearTimeout(delayedScrollTimer);
    };
  }, [items.length, hasMeasuredContainer, hasAutoScrolled, isPageLoading]);

  useEffect(() => {
    if (isLoadingOlderLogs) return;
    const anchorLogId = topAnchorLogIdRef.current;
    if (!anchorLogId) return;

    const anchorRowIndex = items.findIndex(
      (row) => row.type === "log" && row.id === anchorLogId
    );
    if (anchorRowIndex >= 0) {
      listRef.current?.scrollToItem(anchorRowIndex, "start");
    }
    topAnchorLogIdRef.current = null;
  }, [items, isLoadingOlderLogs]);

  // Always show Input X of Y (even when failed-only is on)
  const actualInputLabel = useMemo(() => {
    const total = totalInputs || (maxKnownIndex >= 0 ? maxKnownIndex + 1 : 0);
    const actual = selectedIndex + 1;
    return total > 0 ? `Input ${actual} of ${total}` : `Input ${actual}`;
  }, [selectedIndex, totalInputs, maxKnownIndex]);

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

  const iconBtnClass = (disabled: boolean) =>
    disabled
      ? "h-8 w-8 grid place-items-center rounded-md border border-gray-200 bg-white opacity-50 cursor-default"
      : "h-8 w-8 grid place-items-center rounded-md border border-gray-200 bg-white hover:bg-gray-50 active:bg-gray-100";
  const failedCount = failedInputsCount;

  return (
    <div className="mt-4 mb-4 flex flex-col flex-1 min-h-0">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-lg font-semibold text-primary">Logs</h2>

        <div className="flex items-center">
          <div className="flex items-center gap-3 rounded-full border border-gray-200 bg-gray-50 px-3 py-2 shadow-sm">
            <span className="text-sm text-gray-800 tabular-nums whitespace-nowrap">
              {actualInputLabel}
            </span>

            <div className="flex items-center gap-2">
              <button
                type="button"
                onClick={goPrev}
                disabled={stepperDisabled}
                className={iconBtnClass(stepperDisabled)}
                aria-label="Previous input"
                title="Previous"
              >
                <span className="text-sm">{`<`}</span>
              </button>

              <button
                type="button"
                onClick={() => {
                  void goNext();
                }}
                disabled={stepperDisabled}
                className={iconBtnClass(stepperDisabled)}
                aria-label="Next input"
                title="Next"
              >
                <span className="text-sm">{`>`}</span>
              </button>
            </div>

            <div className="h-6 w-px bg-gray-200" aria-hidden="true" />

            <label className="flex items-center gap-2 text-sm text-gray-700 select-none">
              <Switch
                checked={showFailedOnly}
                onCheckedChange={(checked) => {
                  setShowFailedOnly(checked);
                  if (checked) {
                    void selectNextFailedInput();
                  }
                }}
                disabled={failedCount === 0}
                className="scale-75 origin-left disabled:cursor-default"
              />
              <span className="whitespace-nowrap text-muted-foreground">Failed only</span>
              <span className="ml-1 inline-flex items-center rounded-full border border-red-200 bg-red-50 px-2.5 py-1 text-xs tabular-nums text-red-700">
                {failedCount}
              </span>
            </label>

          </div>
        </div>
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
          <div ref={containerRef} className="font-mono text-xs text-gray-800 h-full relative">
            {isLoadingOlderLogs && (
              <div className="absolute top-0 left-0 right-0 z-10 flex items-center justify-center gap-2 border-b border-gray-200 bg-white/95 py-2">
                <div
                  className="h-4 w-4 rounded-full border-2 border-gray-300 border-t-primary animate-spin"
                  role="status"
                  aria-label="Loading older logs"
                />
                <span className="text-xs text-gray-600">Loading older logs…</span>
              </div>
            )}
            <List
              height={listHeight}
              itemCount={items.length}
              itemSize={getItemSize}
              width="100%"
              ref={listRef}
              itemKey={(index) => items[index]?.key ?? index}
              onScroll={({ scrollDirection, scrollOffset, scrollUpdateWasRequested }) => {
                if (scrollUpdateWasRequested) return;
                if (scrollDirection !== "backward") return;
                if (scrollOffset > 0) return;
                if (isPageLoading || isLoadingOlderLogs) return;
                if (!hasMoreOlderLogs) return;
                if (oldestLoadedLogDocumentTimestamp === undefined) return;

                const firstVisibleLogRow = items.find((row) => row.type === "log");
                if (!firstVisibleLogRow || firstVisibleLogRow.type !== "log") return;
                topAnchorLogIdRef.current = firstVisibleLogRow.id;
                setIsLoadingOlderLogs(true);
                void loadInputLogs(jobId, selectedIndex, oldestLoadedLogDocumentTimestamp).finally(() => {
                  setIsLoadingOlderLogs(false);
                });
              }}
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
                      <div className="text-gray-500 text-left tabular-nums">
                        {formatTime(row.logTimestamp)}
                      </div>
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
