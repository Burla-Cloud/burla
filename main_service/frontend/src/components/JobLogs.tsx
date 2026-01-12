import { useEffect, useRef, useState, useCallback, useMemo } from "react";
import { useLogsContext } from "@/contexts/LogsContext";
import { VariableSizeList as List } from "react-window";
import { Switch } from "@/components/ui/switch";

interface JobLogsProps {
  jobId: string;
  jobStatus?: string;
}

type RowItem =
  | { type: "divider"; key: string; label: string }
  | { type: "empty"; key: string; label: string }
  | { type: "log"; key: string; id: string; createdAt: number; message: string };

type LogGroup = {
  inputIndex: number;
  sortKey: number;
};

const LIMIT_PER_INDEX = 200;
const SUMMARY_POLL_MS = 4000;

const JobLogs = ({ jobId, jobStatus }: JobLogsProps) => {
  const { getLogs, loadSummary, clearSummaryCache, loadPage, evictToWindow, startLiveStream, closeLiveStream } =
    useLogsContext();

  const [showFailedOnly, setShowFailedOnly] = useState(false);
  const [failedIndexes, setFailedIndexes] = useState<number[]>([]);
  const [groups, setGroups] = useState<LogGroup[]>([]);
  const [selectedPos, setSelectedPos] = useState(0);

  const [isLoading, setIsLoading] = useState(true);

  const [hasAutoScrolled, setHasAutoScrolled] = useState(false);
  const [windowWidth, setWindowWidth] = useState(window.innerWidth);

  const [listHeight, setListHeight] = useState<number>(300);
  const [hasMeasuredContainer, setHasMeasuredContainer] = useState<boolean>(false);

  const [hasNewGroups, setHasNewGroups] = useState(false);
  const pendingOrderedIndexesRef = useRef<number[] | null>(null);
  const pendingFailedIndexesRef = useRef<number[] | null>(null);

  const listRef = useRef<List>(null);
  const containerRef = useRef<HTMLDivElement | null>(null);

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

  const applySummaryToGroups = useCallback((ordered: number[] | undefined, failed: number[] | undefined) => {
    const safeOrdered = (ordered || []).map(Number).filter(Number.isFinite);
    const safeFailed = (failed || []).map(Number).filter(Number.isFinite);

    setFailedIndexes(safeFailed);

    // only include calls that actually have logs, ordered newest-first
    const nextGroups: LogGroup[] = safeOrdered.map((idx, pos) => ({
      inputIndex: idx,
      sortKey: safeOrdered.length - pos,
    }));

    setGroups(nextGroups);

    if (safeFailed.length === 0) setShowFailedOnly(false);

    // always jump to Call 1
    setSelectedPos(0);
  }, []);

  // initial load summary (forced) + ALWAYS start at Call 1 for new job
  useEffect(() => {
    let cancelled = false;

    (async () => {
      const prevJobId = lastJobIdRef.current;
      const isNewJob = prevJobId !== jobId;
      lastJobIdRef.current = jobId;

      setIsLoading(true);
      setHasNewGroups(false);
      pendingOrderedIndexesRef.current = null;
      pendingFailedIndexesRef.current = null;

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
        const s = await loadSummary(jobId, { force: true });
        if (cancelled) return;
        applySummaryToGroups(s?.indexes_with_logs, s?.failed_indexes);
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [jobId, loadSummary, clearSummaryCache, applySummaryToGroups, closeLiveStream]);

  // poll summary while RUNNING/FAILED and show refresh button if new indexes appear OR failed indexes change
  useEffect(() => {
    const shouldPoll = jobStatus === "RUNNING" || jobStatus === "FAILED";
    if (!shouldPoll) return;

    let cancelled = false;

    const tick = async () => {
      try {
        const s = await loadSummary(jobId, { force: true });
        if (cancelled) return;

        const incoming = (s?.indexes_with_logs ?? s?.seen_indexes ?? []).map(Number).filter(Number.isFinite);
        const incomingFailed = (s?.failed_indexes ?? []).map(Number).filter(Number.isFinite);

        const current = groups.map((g) => g.inputIndex);
        const currentSet = new Set(current);

        const hasNewIndexes = incoming.some((idx) => !currentSet.has(idx));

        const failedChanged =
          incomingFailed.length !== failedIndexes.length || incomingFailed.some((x) => !failedSet.has(x));

        if (hasNewIndexes || failedChanged) {
          pendingOrderedIndexesRef.current = incoming;
          pendingFailedIndexesRef.current = incomingFailed;
          setHasNewGroups(true);
        }
      } catch {
        // ignore
      }
    };

    const id = window.setInterval(tick, SUMMARY_POLL_MS);
    tick();

    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [jobId, jobStatus, loadSummary, groups, failedIndexes, failedSet]);

  const onRefreshNewGroups = useCallback(async () => {
    setHasNewGroups(false);

    const pendingOrdered = pendingOrderedIndexesRef.current;
    const pendingFailed = pendingFailedIndexesRef.current;

    if (pendingOrdered && pendingOrdered.length > 0) {
      applySummaryToGroups(pendingOrdered, pendingFailed || []);
      pendingOrderedIndexesRef.current = null;
      pendingFailedIndexesRef.current = null;
      return;
    }

    const s = await loadSummary(jobId, { force: true });
    applySummaryToGroups(s?.indexes_with_logs, s?.failed_indexes);
  }, [jobId, loadSummary, applySummaryToGroups]);

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
    (async () => {
      setIsLoading(true);
      try {
        await loadPage(jobId, selectedInputIndex, selectedInputIndex, LIMIT_PER_INDEX, true);
        evictToWindow(jobId, selectedInputIndex, selectedInputIndex, 3);
      } finally {
        if (!cancelled) setIsLoading(false);
      }
    })();

    return () => {
      cancelled = true;
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

  const stepperDisabled = isLoading || jobHasNoLogsAtAll || showOnlyGlobal;

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
      <div className="mt-4 mb-4 flex flex-col">
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
    if (activeGroups.length === 0) return "Calls";
    return `Call ${selectedPos + 1} of ${activeGroups.length}`;
  }, [activeGroups.length, selectedPos]);

  const failedCount = failedIndexes.length;

  const failedPillClass =
    failedCount === 0
      ? "border-gray-200 bg-white text-gray-700"
      : stepperDisabled
      ? "border-red-200 bg-red-50 text-red-400 opacity-60"
      : "border-red-200 bg-red-50 text-red-700";

  const refreshPillClass = stepperDisabled
    ? "h-8 px-3 rounded-full border border-gray-200 bg-gray-50 text-xs text-gray-400 cursor-default"
    : "h-8 px-3 rounded-full border border-blue-200 bg-blue-50 text-xs text-blue-700 hover:bg-blue-100";

  return (
    <div className="mt-4 mb-4 flex flex-col flex-1 min-h-0">
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-lg font-semibold text-primary">Logs</h2>

        <div className="flex items-center gap-3 rounded-full border border-gray-200 bg-gray-50 px-3 py-2 shadow-sm">
          {hasNewGroups ? (
            <button
              type="button"
              onClick={onRefreshNewGroups}
              disabled={stepperDisabled}
              className={refreshPillClass}
              title="New logs exist. Click to refresh the call list."
            >
              Refresh: new logs
            </button>
          ) : null}

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
              {failedCount}
            </span>
          </label>
        </div>
      </div>

      <div className="flex-1 min-h-0 bg-white border border-gray-200 rounded-lg shadow-sm relative">
        {isLoading ? (
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
