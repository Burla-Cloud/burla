
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
  const [seenIndexes, setSeenIndexes] = useState<number[]>([]);

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

      const failed = (s?.failed_indexes || []).slice().sort((a, b) => a - b);
      const seen = (s?.seen_indexes || []).slice().sort((a, b) => a - b);

      setFailedIndexes(failed);
      setSeenIndexes(seen);

      if (failed.length === 0) setShowFailedOnly(false);
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
    // Failed-only affects stepping ONLY, not the displayed label.
    if (showFailedOnly) return failedIndexes;
    if (totalInputs > 0) return Array.from({ length: totalInputs }, (_, i) => i);
    if (seenIndexes.length > 0) return seenIndexes;
    return [];
  }, [showFailedOnly, failedIndexes, totalInputs, seenIndexes]);

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

  const pageStart = useMemo(
    () => Math.floor(Math.max(0, selectedIndex) / PAGE_SIZE) * PAGE_SIZE,
    [selectedIndex]
  );

  const pageEnd = useMemo(() => {
    if (maxKnownIndex < 0) return -1;
    return Math.min(pageStart + PAGE_SIZE - 1, maxKnownIndex);
  }, [pageStart, maxKnownIndex]);

  // Load current page and keep a small window cached
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
        evictToWindow(jobId, pageStart, pageEnd, 3);
      } finally {
        if (!cancelled) setIsPageLoading(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [jobId, pageStart, pageEnd, loadPage, evictToWindow]);

  // Logs for current index
  const logs = useMemo(() => getLogs(jobId, selectedIndex), [getLogs, jobId, selectedIndex]);

  const hasAnyIndexedLogs = useMemo(
    () => logs.some((e: any) => e?.index !== null && e?.index !== undefined),
    [logs]
  );

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

    // Logs exist but no per-input indexing (older format / global-only)
    if (logs.length > 0 && !hasAnyIndexedLogs) {
      result.push({
        type: "empty",
        key: "no-index",
        label: "Logs are not available",
      });
      return result;
    }

    if (logs.length === 0) {
      const jobHasAnyPerInputLogs = seenIndexes.length > 0 || failedIndexes.length > 0;

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

    for (const entry of logs as any[]) {
      const createdAt = entry.created_at ?? 0;
      const dateKey = getDateKey(createdAt);

      if (lastDateKey !== dateKey) {
        result.push({
          type: "divider",
          key: `divider-${dateKey}`,
          label: formatDateLabel(createdAt),
        });
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
  }, [logs, hasAnyIndexedLogs, selectedIndex, seenIndexes.length, failedIndexes.length]);

  const oldFormatNoPerInput =
    items.length === 1 && items[0]?.type === "empty" && logs.length > 0 && !hasAnyIndexedLogs;

  const jobHasNoLogsAtAll = seenIndexes.length === 0 && failedIndexes.length === 0;

  const stepperDisabled =
    isPageLoading || activeIndexList.length === 0 || oldFormatNoPerInput || jobHasNoLogsAtAll;

  const goPrev = () => {
    if (stepperDisabled) return;
    const pos = activeIndexList.indexOf(selectedIndex);
    const nextPos = pos <= 0 ? activeIndexList.length - 1 : pos - 1;
    setSelectedIndex(activeIndexList[nextPos]);
  };

  const goNext = () => {
    if (stepperDisabled) return;
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

  // Always show Input X of Y (even when failed-only is on)
  const actualInputLabel = useMemo(() => {
    const total = totalInputs || (maxKnownIndex >= 0 ? maxKnownIndex + 1 : 0);
    const actual = selectedIndex + 1;
    return total > 0 ? `Input ${actual} of ${total}` : `Input ${actual}`;
  }, [selectedIndex, totalInputs, maxKnownIndex]);

  const failedCount = failedIndexes.length;

  // FIX: keep red the same regardless of toggle, optionally fade when disabled
  const failedPillClass =
    failedCount === 0
      ? "border-gray-200 bg-white text-gray-700"
      : stepperDisabled
        ? "border-red-200 bg-red-50 text-red-400 opacity-60"
        : "border-red-200 bg-red-50 text-red-700";

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
                onClick={goNext}
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
                    if (failedIndexes.length > 0) setSelectedIndex(failedIndexes[0]);
                  } else {
                    // FIX: when turning off failed-only, go back to Input 1
                    setSelectedIndex(0);
                  }
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
                        {formatTime(row.createdAt)}
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
