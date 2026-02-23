import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { useLogsContext } from "@/contexts/LogsContext";
import { VariableSizeList as List } from "react-window";
import { Switch } from "@/components/ui/switch";

interface JobLogsProps {
  jobId: string;
  jobStatus?: string;
  nResults?: number;
  onFailedCountChange?: (failedCount: number) => void;
  initialSummary?: {
    failed_indexes: number[];
    seen_indexes?: number[];
    indexes_with_logs?: number[];
  } | null;
}

type RowItem =
  | { type: "divider"; key: string; label: string }
  | { type: "empty"; key: string; label: string }
  | { type: "log"; key: string; id: string; createdAt: number; message: string };

type LogGroup = {
  inputIndex: number;
};

const LIMIT_PER_INDEX = 200;
const SUMMARY_POLL_MS = 2500;

const JobLogs = ({
  jobId,
  jobStatus,
  nResults = 0,
  onFailedCountChange,
  initialSummary = null,
}: JobLogsProps) => {
  const { getLogs, loadSummary, clearSummaryCache, loadPage, evictToWindow, startLiveStream, closeLiveStream } =
    useLogsContext();

  const [showFailedOnly, setShowFailedOnly] = useState(false);
  const [failedIndexes, setFailedIndexes] = useState<number[]>([]);
  const [groups, setGroups] = useState<LogGroup[]>([]);
  const [selectedPos, setSelectedPos] = useState(0);
  const [isSummaryLoading, setIsSummaryLoading] = useState(true);

  const [isLoading, setIsLoading] = useState(true);

  const [hasAutoScrolled, setHasAutoScrolled] = useState(false);
  const [windowWidth, setWindowWidth] = useState(window.innerWidth);

  const [listHeight, setListHeight] = useState<number>(300);
  const [hasMeasuredContainer, setHasMeasuredContainer] = useState<boolean>(false);

  const listRef = useRef<List>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);
  const selectedInputIndexRef = useRef<number>(0);
  const showFailedOnlyRef = useRef<boolean>(false);

  const sizeMapRef = useRef<Record<string, number>>({});

  // hard reset on job change
  const lastJobIdRef = useRef<string>("");

  const setSizeForKey = useCallback((key: string, size: number, fromIndex: number) => {
    if (sizeMapRef.current[key] !== size) {
      sizeMapRef.current[key] = size;
      listRef.current?.resetAfterIndex(fromIndex, true);
    }
  }, []);

  const failedSet = useMemo(() => new Set(failedIndexes), [failedIndexes]);

  const activeGroups = useMemo(() => {
    if (!showFailedOnly) return groups;
    return groups.filter((g) => failedSet.has(g.inputIndex));
  }, [groups, showFailedOnly, failedSet]);

  const jobHasNoLogsAtAll = activeGroups.length === 0;

  // keep selectedPos valid
  useEffect(() => {
    if (activeGroups.length === 0) {
      setSelectedPos(0);
      return;
    }
    if (selectedPos >= activeGroups.length) setSelectedPos(0);
  }, [activeGroups.length, selectedPos]);

  const selectedInputIndex = useMemo(() => {
    if (activeGroups.length === 0) return 0;
    return activeGroups[Math.max(0, Math.min(selectedPos, activeGroups.length - 1))].inputIndex;
  }, [activeGroups, selectedPos]);

  useEffect(() => {
    selectedInputIndexRef.current = selectedInputIndex;
  }, [selectedInputIndex]);

  useEffect(() => {
    showFailedOnlyRef.current = showFailedOnly;
  }, [showFailedOnly]);

  const applySummaryToGroups = useCallback(
    (
      ordered: number[] | undefined,
      failed: number[] | undefined,
      opts?: { preserveSelection?: boolean }
    ) => {
    const safeOrdered = Array.from(new Set((ordered || []).map(Number).filter(Number.isFinite))).sort(
      (a, b) => a - b
    );
    const safeFailed = Array.from(new Set((failed || []).map(Number).filter(Number.isFinite))).sort(
      (a, b) => a - b
    );

    setFailedIndexes(safeFailed);
    if (onFailedCountChange) onFailedCountChange(safeFailed.length);

    // only include calls that actually have logs, ordered by call index ascending
    const nextGroups: LogGroup[] = safeOrdered.map((idx) => ({
      inputIndex: idx,
    }));

    setGroups(nextGroups);

    if (safeFailed.length === 0) setShowFailedOnly(false);

    if (opts?.preserveSelection) {
      const nextActive = showFailedOnlyRef.current
        ? nextGroups.filter((g) => safeFailed.includes(g.inputIndex))
        : nextGroups;
      const nextPos = nextActive.findIndex((g) => g.inputIndex === selectedInputIndexRef.current);
      setSelectedPos(nextPos >= 0 ? nextPos : 0);
      return;
    }

    // initial load: always jump to first call
    setSelectedPos(0);
  }, [onFailedCountChange]);

  // initial load summary (forced) + ALWAYS start at Call 1 for new job
  useEffect(() => {
    let cancelled = false;
    const summaryController = new AbortController();

    (async () => {
      const prevJobId = lastJobIdRef.current;
      const isNewJob = prevJobId !== jobId;
      lastJobIdRef.current = jobId;

      setIsLoading(true);
      setIsSummaryLoading(true);

      if (isNewJob && prevJobId) {
        closeLiveStream(prevJobId);
      }

      if (isNewJob) {
        setShowFailedOnly(false);
        setSelectedPos(0);
        sizeMapRef.current = {};
        listRef.current?.resetAfterIndex(0, true);
        setHasAutoScrolled(false);
      }

      clearSummaryCache(jobId);

      try {
        if (initialSummary) {
          applySummaryToGroups(initialSummary.indexes_with_logs, initialSummary.failed_indexes);
          return;
        }

        const s = await loadSummary(jobId, { force: true, signal: summaryController.signal });
        if (cancelled) return;
        applySummaryToGroups(s?.indexes_with_logs, s?.failed_indexes);
      } finally {
        if (!cancelled) {
          setIsSummaryLoading(false);
          setIsLoading(false);
        }
      }
    })();

    return () => {
      cancelled = true;
      summaryController.abort();
    };
  }, [jobId, loadSummary, clearSummaryCache, applySummaryToGroups, closeLiveStream, initialSummary]);

  // poll summary while RUNNING/FAILED and auto-apply updates
  useEffect(() => {
    let cancelled = false;
    let inFlight = false;
    let timeoutId: number | undefined;
    let currentController: AbortController | null = null;
    const shouldPoll = jobStatus === "RUNNING" || jobStatus === "FAILED";
    const pollMs = jobStatus === "RUNNING" ? SUMMARY_POLL_MS : 4000;

    const tick = async () => {
      if (cancelled || inFlight) return;
      inFlight = true;
      currentController = new AbortController();
      try {
        const s = await loadSummary(jobId, { force: true, signal: currentController.signal });
        if (cancelled) return;

        const incoming = (s?.indexes_with_logs ?? s?.seen_indexes ?? []).map(Number).filter(Number.isFinite);
        const incomingFailed = (s?.failed_indexes ?? []).map(Number).filter(Number.isFinite);

        const current = groups.map((g) => g.inputIndex);
        const currentSet = new Set(current);

        const hasNewIndexes = incoming.some((idx) => !currentSet.has(idx));

        const failedChanged =
          incomingFailed.length !== failedIndexes.length || incomingFailed.some((x) => !failedSet.has(x));

        if (hasNewIndexes || failedChanged) {
          applySummaryToGroups(incoming, incomingFailed, { preserveSelection: true });
        }
      } catch {
        // ignore
      } finally {
        currentController = null;
        inFlight = false;
        if (!cancelled && shouldPoll) timeoutId = window.setTimeout(tick, pollMs);
      }
    };

    // If initial summary is already provided by JobDetails, avoid issuing an
    // immediate duplicate summary scan; start the poll cycle after pollMs.
    if (initialSummary) {
      timeoutId = window.setTimeout(tick, pollMs);
    } else {
      tick();
    }

    return () => {
      cancelled = true;
      if (currentController) currentController.abort();
      if (timeoutId) window.clearTimeout(timeoutId);
    };
  }, [jobId, jobStatus, nResults, initialSummary, loadSummary, groups, failedIndexes, failedSet, applySummaryToGroups]);

  // resize plumbing
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

  // Load only the selected inputIndex page
  useEffect(() => {
    if (jobHasNoLogsAtAll) return;

    let cancelled = false;
    const pageController = new AbortController();
    (async () => {
      setIsLoading(true);
      try {
        await loadPage(
          jobId,
          selectedInputIndex,
          selectedInputIndex,
          LIMIT_PER_INDEX,
          true,
          pageController.signal
        );
        evictToWindow(jobId, selectedInputIndex, selectedInputIndex, 3);
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    })();

    return () => {
      cancelled = true;
      pageController.abort();
    };
  }, [jobId, selectedInputIndex, jobHasNoLogsAtAll, loadPage, evictToWindow]);

  const logs = useMemo(() => getLogs(jobId, selectedInputIndex), [getLogs, jobId, selectedInputIndex]);

  const indexedLogs = useMemo(
    () => logs.filter((e: any) => e?.input_index !== null && e?.input_index !== undefined),
    [logs]
  );
  const globalLogs = useMemo(
    () => logs.filter((e: any) => e?.input_index === null || e?.input_index === undefined),
    [logs]
  );

  const showOnlyGlobal = indexedLogs.length === 0 && globalLogs.length > 0;

  const formatDateLabel = (tsSeconds: number) => {
    const d = new Date(tsSeconds * 1000);
    return d.toLocaleDateString("en-US", { weekday: "long", month: "long", day: "numeric", year: "numeric" });
  };

  const getDateKey = (tsSeconds: number) => {
    const d = new Date(tsSeconds * 1000);
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const da = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${da}`;
  };

  const formatTime = (ts: number) => {
    const date = new Date(ts * 1000);
    return date.toLocaleTimeString("en-US", {
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
      hour12: true,
    });
  };

  const items: RowItem[] = useMemo(() => {
    const result: RowItem[] = [];

    if (jobHasNoLogsAtAll) {
      result.push({ type: "empty", key: "empty-job", label: "This job produced no log output" });
      return result;
    }

    const chosen = showOnlyGlobal ? globalLogs : indexedLogs;

    if (chosen.length === 0) {
      result.push({ type: "empty", key: "empty", label: isLoading ? "Loading logs…" : "No log output" });
      return result;
    }

    // inside a call, render oldest to newest (newest ends at bottom)
    const ordered = [...chosen].sort((a: any, b: any) => Number(a?.created_at ?? 0) - Number(b?.created_at ?? 0));

    let lastDateKey: string | null = null;

    for (const entry of ordered as any[]) {
      const createdAt = Number(entry.created_at ?? 0);
      const dateKey = getDateKey(createdAt);

      if (lastDateKey !== dateKey) {
        result.push({ type: "divider", key: `divider-${dateKey}`, label: formatDateLabel(createdAt) });
        lastDateKey = dateKey;
      }

      const id =
        entry.id ?? `${createdAt}-${entry.input_index ?? "g"}-${entry.is_error ? 1 : 0}-${entry.message ?? ""}`;

      result.push({
        type: "log",
        key: `log-${id}`,
        id: String(id),
        createdAt,
        message: entry.message || "No message",
      });
    }

    return result;
  }, [jobHasNoLogsAtAll, isLoading, indexedLogs, globalLogs, showOnlyGlobal]);

  const stepperDisabled = isSummaryLoading || jobHasNoLogsAtAll || showOnlyGlobal;

  const goPrev = () => {
    if (stepperDisabled) return;
    setSelectedPos((p) => (p <= 0 ? activeGroups.length - 1 : p - 1));
  };

  const goNext = () => {
    if (stepperDisabled) return;
    setSelectedPos((p) => (p >= activeGroups.length - 1 ? 0 : p + 1));
  };

  // Stream logs for current selected inputIndex while RUNNING
  useEffect(() => {
    if (jobStatus === "RUNNING" && !jobHasNoLogsAtAll) {
      const stop = startLiveStream(jobId, selectedInputIndex, true);
      return () => stop();
    }
    closeLiveStream(jobId);
    return () => {};
  }, [jobId, jobStatus, selectedInputIndex, jobHasNoLogsAtAll, startLiveStream, closeLiveStream]);

  // Reset react-window sizing when view changes
  useEffect(() => {
    sizeMapRef.current = {};
    listRef.current?.resetAfterIndex(0, true);
    setHasAutoScrolled(false);
  }, [jobId, selectedPos, showFailedOnly, showOnlyGlobal]);

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

  // Inside a call, newest should be at bottom. Scroll to bottom once per view.
  useEffect(() => {
    if (!hasMeasuredContainer) return;
    if (items.length === 0) return;
    if (hasAutoScrolled) return;
    listRef.current?.scrollToItem(items.length - 1, "end");
    setHasAutoScrolled(true);
  }, [items.length, hasMeasuredContainer, hasAutoScrolled]);

  if (windowWidth <= 1000) {
    return (
      <div className="mt-2 mb-0 flex flex-col">
        <div className="flex items-center justify-between mb-3">
          <h2 className="text-lg font-semibold text-primary">Logs</h2>
        </div>
        <div className="text-gray-500 italic text-sm text-center p-4">Logs are hidden on small screens.</div>
      </div>
    );
  }

  const iconBtnClass = (disabled: boolean) =>
    disabled
      ? "h-8 w-8 grid place-items-center rounded-md border border-gray-200 bg-white opacity-50 cursor-default"
      : "h-8 w-8 grid place-items-center rounded-md border border-gray-200 bg-white hover:bg-gray-50 active:bg-gray-100";

  const headerLabel = useMemo(() => {
    if (activeGroups.length === 0) return `Call 0 of ${nResults.toLocaleString()}`;
    const currentInputIndex = selectedInputIndex;
    return `Call ${currentInputIndex.toLocaleString()} of ${nResults.toLocaleString()}`;
  }, [activeGroups.length, selectedInputIndex, nResults]);

  const failedCount = failedIndexes.length;
  const failedProgressLabel = useMemo(() => {
    if (failedCount === 0) return "0";
    if (!showFailedOnly || activeGroups.length === 0) return failedCount.toLocaleString();
    return `${(selectedPos + 1).toLocaleString()} of ${failedCount.toLocaleString()}`;
  }, [failedCount, showFailedOnly, activeGroups.length, selectedPos]);

  const failedPillClass =
    failedCount === 0
      ? "border-gray-200 bg-white text-gray-700"
      : stepperDisabled
      ? "border-red-200 bg-red-50 text-red-400 opacity-60"
      : "border-red-200 bg-red-50 text-red-700";

  return (
    <div className="mt-0 mb-0 flex flex-col flex-1 min-h-0">
      <div className="mt-2 mb-4 flex items-center justify-between">
        <h2 className="text-lg font-semibold text-primary leading-none">Logs</h2>

        <div className="flex items-center gap-2 rounded-full border border-gray-200 bg-gray-50 px-3 py-1.5 shadow-sm">
          <div className="flex flex-col leading-tight">
            <span className="text-sm text-gray-800 tabular-nums whitespace-nowrap">{headerLabel}</span>
          </div>

          <div className="flex items-center gap-2">
            <button
              type="button"
              onClick={goPrev}
              disabled={stepperDisabled}
              className={iconBtnClass(stepperDisabled)}
              title="Previous"
            >
              <span className="text-sm">{`<`}</span>
            </button>

            <button
              type="button"
              onClick={goNext}
              disabled={stepperDisabled}
              className={iconBtnClass(stepperDisabled)}
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
                setSelectedPos(0);
              }}
              disabled={failedCount === 0 || stepperDisabled}
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

      <div className="mt-0 flex-1 min-h-0 h-full bg-white border border-gray-200 rounded-lg shadow-sm relative">
        {isSummaryLoading || isLoading ? (
          <div ref={containerRef} className="h-full w-full flex items-center justify-center">
            <div className="flex flex-col items-center text-gray-500">
              <div
                className="h-8 w-8 rounded-full border-2 border-gray-300 border-t-primary animate-spin"
                role="status"
                aria-label="Loading logs"
              />
              <div className="mt-2 text-sm">
                {isSummaryLoading ? "Loading stepper..." : "Loading logs..."}
              </div>
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
