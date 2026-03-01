import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { useLogsContext } from "@/contexts/LogsContext";
import { VariableSizeList as List } from "react-window";
import { Switch } from "@/components/ui/switch";
import { LogEntry } from "@/types/coreTypes";

interface JobLogsProps {
  jobId: string;
  jobStatus?: string | null;
  nInputs?: number;
  failedCount?: number;
  onFailedCountChange?: (failedCount: number) => void;
}

type RowItem =
  | { type: "divider"; key: string; label: string }
  | { type: "empty"; key: string; label: string }
  | { type: "log"; key: string; id: string; logTimestamp: number; message: string };

const getLogRowIdentifier = (logEntry: LogEntry) =>
  `${logEntry.log_timestamp}-${logEntry.is_error ? 1 : 0}-${logEntry.message ?? ""}`;

const JobLogs = ({ jobId, jobStatus, nInputs, failedCount, onFailedCountChange }: JobLogsProps) => {
  const {
    getLogs,
    getFailedInputsCount,
    getFailedInputIndexes,
    getIndexesWithLogs,
    getHasMoreOlderLogs,
    getOldestLoadedLogDocumentTimestamp,
    loadInputLogs,
    logsByJobId,
  } = useLogsContext();

  const [selectedIndex, setSelectedIndex] = useState<number>(0);
  const [showFailedOnly, setShowFailedOnly] = useState(false);
  const [showIndexesWithLogsOnly, setShowIndexesWithLogsOnly] = useState(false);
  const [isHasLogsSyncing, setIsHasLogsSyncing] = useState(false);
  const [failedIndexes, setFailedIndexes] = useState<number[]>([]);
  const [failedIndexesReady, setFailedIndexesReady] = useState(false);
  const [logsOnlyIndexes, setLogsOnlyIndexes] = useState<number[]>([]);

  const [isPageLoading, setIsPageLoading] = useState(true);
  const [isLoadingOlderLogs, setIsLoadingOlderLogs] = useState(false);

  const [hasAutoScrolled, setHasAutoScrolled] = useState(false);
  const [windowWidth, setWindowWidth] = useState(window.innerWidth);

  const listRef = useRef<List>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const outerListRef = useRef<HTMLDivElement | null>(null);

  const [listHeight, setListHeight] = useState<number>(300);
  const [hasMeasuredContainer, setHasMeasuredContainer] = useState<boolean>(false);

  const sizeMapRef = useRef<Record<string, number>>({});
  const topAnchorLogIdRef = useRef<string | null>(null);
  const failedToggleRequestRef = useRef(0);
  const logsOnlyToggleRequestRef = useRef(0);
  const logsOnlyRefreshRequestRef = useRef(0);
  const previousLogsLengthRef = useRef(0);
  const shouldFollowTailRef = useRef(true);
  const hasLogsSyncUnlockTimeoutRef = useRef<number | undefined>(undefined);

  const setSizeForKey = useCallback((key: string, size: number, fromIndex: number) => {
    if (sizeMapRef.current[key] !== size) {
      sizeMapRef.current[key] = size;
      listRef.current?.resetAfterIndex(fromIndex, true);
    }
  }, []);

  const updateShouldFollowTail = useCallback(() => {
    const outerEl = outerListRef.current;
    if (!outerEl) return;
    const distanceFromBottom = outerEl.scrollHeight - (outerEl.scrollTop + outerEl.clientHeight);
    shouldFollowTailRef.current = distanceFromBottom <= 24;
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

  const fetchedIndexesFromLogs = useMemo(() => {
    const state = logsByJobId[jobId];
    if (!state) return [];
    return Object.keys(state.byIndex || {})
      .map((k) => Number(k))
      .filter((v) => Number.isFinite(v))
      .sort((a, b) => a - b);
  }, [jobId, logsByJobId]);

  const indexesWithLogs = useMemo(() => {
    const state = logsByJobId[jobId];
    if (!state) return [];
    return Object.entries(state.byIndex || {})
      .filter(([, entries]) => (entries || []).length > 0)
      .map(([k]) => Number(k))
      .filter((v) => Number.isFinite(v))
      .sort((a, b) => a - b);
  }, [jobId, logsByJobId]);

  const hasAnyKnownIndexes = indexesWithLogs.length > 0;
  const failedInputsCount = getFailedInputsCount(jobId);
  const effectiveFailedCount = Math.max(
    0,
    typeof failedCount === "number" ? failedCount : failedInputsCount
  );
  const hasLoadedFailedCount =
    typeof failedCount === "number" || Boolean(logsByJobId[jobId]);

  useEffect(() => {
    if (!onFailedCountChange) return;
    onFailedCountChange(effectiveFailedCount);
  }, [effectiveFailedCount, onFailedCountChange]);

  const allIndexList = useMemo(() => {
    if (totalInputs > 0) return Array.from({ length: totalInputs }, (_, i) => i);
    if (fetchedIndexesFromLogs.length > 0) return fetchedIndexesFromLogs;
    return [];
  }, [totalInputs, fetchedIndexesFromLogs]);

  const activeFailedIndexes = useMemo(() => {
    return failedIndexes;
  }, [failedIndexes]);

  const navigationIndexList = useMemo(() => {
    if (showFailedOnly) return activeFailedIndexes;
    if (showIndexesWithLogsOnly) return logsOnlyIndexes;
    return allIndexList;
  }, [showFailedOnly, activeFailedIndexes, showIndexesWithLogsOnly, logsOnlyIndexes, allIndexList]);

  // Keep selectedIndex valid when lists change
  useEffect(() => {
    if (navigationIndexList.length === 0) {
      // In filtered modes, avoid force-resetting to 0 on transient list refreshes.
      if (!showIndexesWithLogsOnly && !showFailedOnly) {
        setSelectedIndex(0);
      }
      return;
    }
    if (!navigationIndexList.includes(selectedIndex)) {
      // Filter toggles explicitly choose an entry; don't auto-jump during live updates.
      if (!showIndexesWithLogsOnly && !showFailedOnly) {
        setSelectedIndex(navigationIndexList[0]);
      }
    }
  }, [navigationIndexList, selectedIndex, showIndexesWithLogsOnly, showFailedOnly]);

  const maxKnownIndex = useMemo(() => {
    if (totalInputs > 0) return totalInputs - 1;
    if (allIndexList.length > 0) return Math.max(...allIndexList);
    return -1;
  }, [totalInputs, allIndexList]);

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

  useEffect(() => {
    if (jobStatus !== "RUNNING" && jobStatus !== "PENDING") return;

    const intervalId = window.setInterval(() => {
      // While user is reading older logs, do not replace the current window.
      if (!shouldFollowTailRef.current) return;
      void loadInputLogs(jobId, selectedIndex);
    }, 2500);

    return () => {
      window.clearInterval(intervalId);
    };
  }, [jobId, selectedIndex, jobStatus, loadInputLogs]);

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
      result.push({
        type: "empty",
        key: "empty",
        label: `Index ${selectedIndex} doesn't have any logs.`,
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
  }, [logs, selectedIndex]);

  const stepperDisabled = isPageLoading || isHasLogsSyncing || navigationIndexList.length === 0;

  const goPrev = () => {
    if (stepperDisabled) return;
    const pos = navigationIndexList.indexOf(selectedIndex);
    const nextPos = pos <= 0 ? navigationIndexList.length - 1 : pos - 1;
    setSelectedIndex(navigationIndexList[nextPos]);
  };

  const jumpToFirstFailedInput = useCallback(async () => {
    const requestId = ++failedToggleRequestRef.current;
    setFailedIndexesReady(false);
    const nextFailedIndexes = await getFailedInputIndexes(jobId);
    if (requestId !== failedToggleRequestRef.current) return;

    setFailedIndexes(nextFailedIndexes);
    setFailedIndexesReady(true);

    if (nextFailedIndexes.length === 0) {
      setShowFailedOnly(false);
      return;
    }

    setSelectedIndex(nextFailedIndexes[0]);
  }, [getFailedInputIndexes, jobId]);

  const jumpToFirstIndexWithLogs = useCallback(async () => {
    const requestId = ++logsOnlyToggleRequestRef.current;
    setIsHasLogsSyncing(true);
    const nextIndexes = await getIndexesWithLogs(jobId);
    if (requestId !== logsOnlyToggleRequestRef.current) return;
    setLogsOnlyIndexes(nextIndexes);
    if (nextIndexes.length === 0) {
      setShowIndexesWithLogsOnly(false);
      setIsHasLogsSyncing(false);
      return;
    }
    setSelectedIndex(nextIndexes[0]);
    if (hasLogsSyncUnlockTimeoutRef.current) {
      window.clearTimeout(hasLogsSyncUnlockTimeoutRef.current);
    }
    hasLogsSyncUnlockTimeoutRef.current = window.setTimeout(() => {
      if (requestId !== logsOnlyToggleRequestRef.current) return;
      setIsHasLogsSyncing(false);
    }, 1000);
  }, [getIndexesWithLogs, jobId]); 
 
  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      const nextIndexes = await getIndexesWithLogs(jobId);
      if (cancelled) return;
      setLogsOnlyIndexes((previousIndexes) => {
        if (nextIndexes.length === 0 && previousIndexes.length > 0) return previousIndexes;
        return nextIndexes;
      });
    };
    load();
    return () => {
      cancelled = true;
      logsOnlyToggleRequestRef.current += 1;
    };
  }, [jobId, getIndexesWithLogs]);

  useEffect(() => {
    if (!showIndexesWithLogsOnly) return;
    if (jobStatus !== "RUNNING" && jobStatus !== "PENDING") return;

    let cancelled = false;

    const syncIndexesWithLogs = async () => {
      const requestId = ++logsOnlyRefreshRequestRef.current;
      const nextIndexes = await getIndexesWithLogs(jobId);
      if (cancelled || requestId !== logsOnlyRefreshRequestRef.current) return;
      setLogsOnlyIndexes((previousIndexes) => {
        if (nextIndexes.length === 0 && previousIndexes.length > 0) return previousIndexes;
        return nextIndexes;
      });
    };

    void syncIndexesWithLogs();

    const intervalId = window.setInterval(() => {
      void syncIndexesWithLogs();
    }, 2500);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
      logsOnlyRefreshRequestRef.current += 1;
    };
  }, [jobId, jobStatus, showIndexesWithLogsOnly, getIndexesWithLogs]);

  useEffect(() => {
    setFailedIndexes([]);
    setFailedIndexesReady(false);
    setShowFailedOnly(false);
    setIsHasLogsSyncing(false);
    if (hasLogsSyncUnlockTimeoutRef.current) {
      window.clearTimeout(hasLogsSyncUnlockTimeoutRef.current);
      hasLogsSyncUnlockTimeoutRef.current = undefined;
    }
  }, [jobId]);

  useEffect(() => {
    return () => {
      if (hasLogsSyncUnlockTimeoutRef.current) {
        window.clearTimeout(hasLogsSyncUnlockTimeoutRef.current);
      }
    };
  }, []);

  const goNext = () => {
    if (stepperDisabled) return;
    const pos = navigationIndexList.indexOf(selectedIndex);
    const nextPos = pos === -1 || pos === navigationIndexList.length - 1 ? 0 : pos + 1;
    setSelectedIndex(navigationIndexList[nextPos]);
  };

  // Reset react-window sizing on index and filter changes
  useEffect(() => {
    sizeMapRef.current = {};
    listRef.current?.resetAfterIndex(0, true);
    setHasAutoScrolled(false);
    shouldFollowTailRef.current = true;
    previousLogsLengthRef.current = 0;
  }, [jobId, selectedIndex, showFailedOnly, showIndexesWithLogsOnly]);

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
    shouldFollowTailRef.current = true;
    setHasAutoScrolled(true);

    return () => {
      window.clearTimeout(delayedScrollTimer);
    };
  }, [items.length, hasMeasuredContainer, hasAutoScrolled, isPageLoading]);

  useEffect(() => {
    if (!hasMeasuredContainer) return;
    if (isPageLoading || isLoadingOlderLogs) return;
    if (items.length === 0) return;
    if (!shouldFollowTailRef.current) return;

    const previousLength = previousLogsLengthRef.current;
    previousLogsLengthRef.current = logs.length;
    if (logs.length <= previousLength) return;

    listRef.current?.scrollToItem(items.length - 1, "end");
  }, [logs.length, items.length, hasMeasuredContainer, isPageLoading, isLoadingOlderLogs]);

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

  const totalLabel = useMemo(() => {
    const total = totalInputs || (maxKnownIndex >= 0 ? maxKnownIndex + 1 : 0);
    return total.toLocaleString();
  }, [totalInputs, maxKnownIndex]);

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
  const failedPosition = useMemo(() => {
    if (!showFailedOnly) return 0;
    const pos = activeFailedIndexes.indexOf(selectedIndex);
    return pos >= 0 ? pos : 0;
  }, [showFailedOnly, activeFailedIndexes, selectedIndex]);

  const failedProgressLabel = useMemo(() => {
    if (!hasLoadedFailedCount) return "…";
    if (effectiveFailedCount === 0) return "0";
    if (!showFailedOnly) return effectiveFailedCount.toLocaleString();
    if (!failedIndexesReady) return "…";
    if (activeFailedIndexes.length === 0) return "…";
    return `${(failedPosition + 1).toLocaleString()} of ${activeFailedIndexes.length.toLocaleString()}`;
  }, [
    hasLoadedFailedCount,
    effectiveFailedCount,
    showFailedOnly,
    failedIndexesReady,
    failedPosition,
    activeFailedIndexes.length,
  ]);
  const failedPillClass =
    !hasLoadedFailedCount
      ? "border-gray-200 bg-white text-gray-500"
      : effectiveFailedCount === 0
      ? "border-gray-200 bg-white text-gray-700"
      : stepperDisabled
      ? "border-red-200 bg-red-50 text-red-400 opacity-60"
      : "border-red-200 bg-red-50 text-red-700";

  return (
    <div className="mt-4 mb-4 flex flex-col flex-1 min-h-0">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-lg font-semibold text-primary">Logs</h2>

        <div className="flex items-center">
          <div className="flex items-center gap-2.5 rounded-full border border-gray-200 bg-gray-50 px-3 py-1.5 shadow-sm">
            <label className="text-sm text-gray-700 tabular-nums whitespace-nowrap flex items-center gap-1.5">
              <span className="text-gray-500 font-medium">Index</span>
              <span className="tabular-nums text-gray-900">{selectedIndex.toLocaleString()}</span>
              <span>of {totalLabel}</span>
            </label>

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
                checked={showIndexesWithLogsOnly}
                onCheckedChange={(checked) => {
                  if (showFailedOnly) return;
                  if (hasLogsSyncUnlockTimeoutRef.current) {
                    window.clearTimeout(hasLogsSyncUnlockTimeoutRef.current);
                    hasLogsSyncUnlockTimeoutRef.current = undefined;
                  }
                  setShowIndexesWithLogsOnly(checked);
                  if (checked) {
                    void jumpToFirstIndexWithLogs();
                  } else {
                    logsOnlyToggleRequestRef.current += 1;
                    setIsHasLogsSyncing(false);
                    setSelectedIndex(0);
                  }
                }}
                disabled={(logsOnlyIndexes.length === 0 && !showIndexesWithLogsOnly) || showFailedOnly}
                className="scale-75 origin-left disabled:cursor-default"
              />
              <span className="whitespace-nowrap text-muted-foreground">Has logs</span>
            </label>

            <div className="h-6 w-px bg-gray-200" aria-hidden="true" />

            <label className="flex items-center gap-2 text-sm text-gray-700 select-none">
              <Switch
                checked={showFailedOnly}
                onCheckedChange={(checked) => {
                  if (showIndexesWithLogsOnly) return;
                  failedToggleRequestRef.current += 1;
                  setShowFailedOnly(checked);
                  if (checked) {
                    void jumpToFirstFailedInput();
                  } else {
                    setFailedIndexesReady(false);
                    setSelectedIndex(0);
                  }
                }}
                disabled={!hasLoadedFailedCount || effectiveFailedCount === 0 || showIndexesWithLogsOnly}
                className="scale-75 origin-left disabled:cursor-default"
              />
              <span className="whitespace-nowrap text-muted-foreground">Failed only</span>
              <span
                className={`ml-1 inline-flex items-center rounded-full border px-2.5 py-1 text-xs tabular-nums ${failedPillClass}`}
              >
                {failedProgressLabel}
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
              outerRef={outerListRef}
              itemKey={(index) => items[index]?.key ?? index}
              onScroll={({ scrollDirection, scrollOffset, scrollUpdateWasRequested }) => {
                if (!scrollUpdateWasRequested) {
                  updateShouldFollowTail();
                }
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
