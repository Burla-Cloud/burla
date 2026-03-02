import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { useLogsContext } from "@/contexts/LogsContext";
import { VariableSizeList as List } from "react-window";
import { Switch } from "@/components/ui/switch";
import { Button } from "@/components/ui/button";
import { PowerOff } from "lucide-react";
import { LogEntry } from "@/types/coreTypes";

interface JobLogsProps {
  jobId: string;
  jobStatus?: string | null;
  nInputs?: number;
  failedCount?: number;
  onStopJob?: () => void;
  isStopDisabled?: boolean;
  onFailedCountChange?: (failedCount: number) => void;
}

type RowItem =
  | { type: "divider"; key: string; label: string }
  | { type: "empty"; key: string; label: string }
  | { type: "log"; key: string; id: string; logTimestamp: number; message: string };

const getLogRowIdentifier = (logEntry: LogEntry) =>
  `${logEntry.log_timestamp}-${logEntry.is_error ? 1 : 0}-${logEntry.message ?? ""}`;

const JobLogs = ({
  jobId,
  jobStatus,
  nInputs,
  failedCount,
  onStopJob,
  isStopDisabled,
  onFailedCountChange,
}: JobLogsProps) => {
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
  const [indexInputValue, setIndexInputValue] = useState("0");
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

  const goToCustomIndex = useCallback(() => {
    const parsedIndex = Number(indexInputValue.trim());
    if (!Number.isInteger(parsedIndex) || parsedIndex < 0) {
      setIndexInputValue(String(selectedIndex));
      return;
    }
    const boundedIndex =
      maxKnownIndex >= 0 ? Math.min(parsedIndex, maxKnownIndex) : parsedIndex;
    setSelectedIndex(boundedIndex);
    setIndexInputValue(String(boundedIndex));
  }, [indexInputValue, selectedIndex, maxKnownIndex]);

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

    if (!nextFailedIndexes.includes(selectedIndex)) {
      const closestFailedIndex = nextFailedIndexes.reduce((closestIndex, candidateIndex) => {
        const closestDistance = Math.abs(closestIndex - selectedIndex);
        const candidateDistance = Math.abs(candidateIndex - selectedIndex);
        if (candidateDistance < closestDistance) return candidateIndex;
        if (candidateDistance > closestDistance) return closestIndex;
        return candidateIndex < closestIndex ? candidateIndex : closestIndex;
      }, nextFailedIndexes[0]);
      setSelectedIndex(closestFailedIndex);
    }
  }, [getFailedInputIndexes, jobId, selectedIndex]);

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
    if (!nextIndexes.includes(selectedIndex)) {
      const closestIndexWithLogs = nextIndexes.reduce((closestIndex, candidateIndex) => {
        const closestDistance = Math.abs(closestIndex - selectedIndex);
        const candidateDistance = Math.abs(candidateIndex - selectedIndex);
        if (candidateDistance < closestDistance) return candidateIndex;
        if (candidateDistance > closestDistance) return closestIndex;
        return candidateIndex < closestIndex ? candidateIndex : closestIndex;
      }, nextIndexes[0]);
      setSelectedIndex(closestIndexWithLogs);
    }
    if (hasLogsSyncUnlockTimeoutRef.current) {
      window.clearTimeout(hasLogsSyncUnlockTimeoutRef.current);
    }
    hasLogsSyncUnlockTimeoutRef.current = window.setTimeout(() => {
      if (requestId !== logsOnlyToggleRequestRef.current) return;
      setIsHasLogsSyncing(false);
    }, 1000);
  }, [getIndexesWithLogs, jobId, selectedIndex]); 
 
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
  const totalIndexesCountDigits = useMemo(() => {
    const totalIndexesCount = Math.max(0, totalInputs || (maxKnownIndex >= 0 ? maxKnownIndex + 1 : 0));
    return String(totalIndexesCount).length;
  }, [totalInputs, maxKnownIndex]);

  if (windowWidth <= 1000) {
    return (
      <div className="mt-4 mb-4 flex flex-col">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-lg font-semibold text-primary">Logs</h2>
        </div>
        <div className="text-[14.5px] text-gray-800 text-center p-4">
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
      ? "border-gray-200 bg-white text-gray-800"
      : effectiveFailedCount === 0
      ? "border-gray-200 bg-white text-gray-800"
      : stepperDisabled
      ? "border-red-200 bg-red-50 text-gray-800"
      : "border-red-200 bg-red-50 text-gray-800";
  const isHasLogsLoading = showIndexesWithLogsOnly && stepperDisabled;

  useEffect(() => {
    setIndexInputValue(String(selectedIndex));
  }, [selectedIndex]);

  return (
    <div className="mt-0 mb-0 flex flex-col flex-1 min-h-0 text-[14.5px] font-normal text-gray-800">
      <div className="flex flex-1 min-h-0 flex-col rounded-lg border border-gray-200 bg-white shadow-sm overflow-hidden">
        <div className="flex items-center justify-between gap-3 border-b border-gray-200 bg-gray-50 px-3 py-1.5">
          <div className="flex items-center gap-2.5">
            <label className="text-[13px] tabular-nums whitespace-nowrap flex items-center gap-1.5">
              <span>Index</span>
              <input
                type="number"
                min={0}
                value={indexInputValue}
                onChange={(event) => {
                  setIndexInputValue(event.target.value);
                }}
                onBlur={() => {
                  goToCustomIndex();
                }}
                onKeyDown={(event) => {
                  if (event.key !== "Enter") return;
                  event.preventDefault();
                  goToCustomIndex();
                }}
                className="h-8 rounded-md border border-gray-200 bg-white px-1 text-[14.5px] font-normal tabular-nums text-gray-800 [appearance:textfield] [&::-webkit-inner-spin-button]:appearance-none [&::-webkit-outer-spin-button]:appearance-none focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
                style={{ width: `${Math.max(2, totalIndexesCountDigits) + 1}ch` }}
                aria-label="Current index"
                disabled={isHasLogsSyncing}
              />
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
                <span className="text-[14.5px]">{`<`}</span>
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
                <span className="text-[14.5px]">{`>`}</span>
              </button>
            </div>

            <div className="h-6 w-px bg-gray-200" aria-hidden="true" />

            <label className="flex items-center gap-2 text-[13px] select-none">
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
                    setIsHasLogsSyncing(true);
                    void jumpToFirstIndexWithLogs();
                  } else {
                    logsOnlyToggleRequestRef.current += 1;
                    setIsHasLogsSyncing(false);
                  }
                }}
                disabled={(logsOnlyIndexes.length === 0 && !showIndexesWithLogsOnly) || showFailedOnly}
                className="scale-75 origin-left disabled:cursor-default"
              />
              <span className="whitespace-nowrap">Has logs</span>
            </label>

            <div className="h-6 w-px bg-gray-200" aria-hidden="true" />

            <label className="flex items-center gap-2 text-[13px] select-none">
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
                  }
                }}
                disabled={!hasLoadedFailedCount || effectiveFailedCount === 0 || showIndexesWithLogsOnly}
                className="scale-75 origin-left disabled:cursor-default"
              />
              <span className="whitespace-nowrap">Failed only</span>
              <span
                className={`ml-1 inline-flex items-center rounded-full border px-2.5 py-1 text-[14.5px] tabular-nums ${failedPillClass}`}
              >
                {failedProgressLabel}
              </span>
            </label>

          </div>
        </div>
        {onStopJob && (
          <Button
            variant="destructive"
            size="lg"
            className="h-11 rounded-lg"
            onClick={onStopJob}
            disabled={Boolean(isStopDisabled)}
          >
            <PowerOff className="mr-2 h-4 w-4" />
            Stop
          </Button>
        )}

      <div className="flex-1 min-h-0 relative font-mono text-[13px] font-normal text-gray-800 bg-white">
        {isHasLogsLoading && !isPageLoading && (
          <div className="absolute inset-0 z-20 flex items-center justify-center bg-white/75">
            <div className="flex flex-col items-center text-gray-800">
              <div
                className="h-8 w-8 rounded-full border-2 border-gray-300 border-t-primary animate-spin"
                role="status"
                aria-label="Loading logs"
              />
              <div className="mt-2 text-[13px]">Loading logs…</div>
            </div>
          </div>
        )}
        {isPageLoading ? (
          <div ref={containerRef} className="h-full w-full flex items-center justify-center">
            <div className="flex flex-col items-center text-gray-800">
              <div
                className="h-8 w-8 rounded-full border-2 border-gray-300 border-t-primary animate-spin"
                role="status"
                aria-label="Loading logs"
              />
              <div className="mt-2 text-[13px]">Loading logs…</div>
            </div>
          </div>
        ) : (
          <div ref={containerRef} className="text-[13px] h-full relative">
            {isLoadingOlderLogs && (
              <div className="absolute top-0 left-0 right-0 z-10 flex items-center justify-center gap-2 border-b border-gray-200 bg-white/95 py-2">
                <div
                  className="h-4 w-4 rounded-full border-2 border-gray-300 border-t-primary animate-spin"
                  role="status"
                  aria-label="Loading older logs"
                />
                <span className="text-[13px] text-gray-800">Loading older logs…</span>
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
                        <span className="shrink-0 text-center text-[13px] font-normal tracking-tight text-gray-800">
                          {row.label}
                        </span>
                        <div className="h-px w-full bg-gray-200" aria-hidden="true" />
                      </div>
                    </div>
                  );
                }

                if (row.type === "empty") {
                  return (
                    <div key={row.key} style={style} className="px-4 py-4">
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
                      <div className="text-left tabular-nums">
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
    </div>
  );
};

export default JobLogs;
