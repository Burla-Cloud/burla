// import { useEffect, useRef, useState } from "react";
// import { useLogsContext } from "@/contexts/LogsContext";
// import { Loader2 } from "lucide-react";
// import { Button } from "@/components/ui/button";

// interface JobLogsProps {
//   jobId: string;
//   jobStatus?: string;
// }

// const useInterval = (callback: () => void, delay: number | null) => {
//   const savedCallback = useRef(callback);
//   useEffect(() => {
//     savedCallback.current = callback;
//   }, [callback]);

//   useEffect(() => {
//     if (delay === null) return;
//     const id = setInterval(() => savedCallback.current(), delay);
//     return () => clearInterval(id);
//   }, [delay]);
// };

// const JobLogs = ({ jobId, jobStatus }: JobLogsProps) => {
//   const { logsByJobId, hasMoreByJobId, fetchInitialLogs, fetchMoreLogs } = useLogsContext();
//   const rawLogs = logsByJobId[jobId] || [];
//   const logs = [...rawLogs].reverse(); // Oldest at top, newest at bottom
//   const hasMore = hasMoreByJobId[jobId] ?? false;

//   const containerRef = useRef<HTMLDivElement>(null);
//   const [initialLoading, setInitialLoading] = useState(true);
//   const [isFetchingMore, setIsFetchingMore] = useState(false);
//   const [initialScrollDone, setInitialScrollDone] = useState(false);
//   const [isAtTop, setIsAtTop] = useState(false);
//   const [isAtBottom, setIsAtBottom] = useState(true);

//   // Load initial logs
//   useEffect(() => {
//     const load = async () => {
//       setInitialLoading(true);
//       await fetchInitialLogs(jobId);
//       setInitialLoading(false);
//       scrollToBottom("auto");
//       setInitialScrollDone(true);
//     };
//     load();
//   }, [jobId]);

//   const scrollToBottom = (behavior: ScrollBehavior = "smooth") => {
//     requestAnimationFrame(() => {
//       setTimeout(() => {
//         const container = containerRef.current;
//         if (container) {
//           container.scrollTo({ top: container.scrollHeight, behavior });
//         }
//       }, 50); // allow DOM to settle
//     });
//   };

//   // Poll for logs
//   useInterval(() => {
//     const container = containerRef.current;
//     if (!container || isFetchingMore || initialLoading) return;

//     const nearTop = container.scrollTop < 200;
//     const nearBottom = container.scrollTop + container.clientHeight >= container.scrollHeight - 200;

//     if (nearTop && hasMore) {
//       fetchInitialLogs(jobId);
//     }

//     if (nearBottom) {
//       fetchInitialLogs(jobId).then(() => scrollToBottom("auto"));
//     }
//   }, 5000);

//   const handleScroll = async () => {
//     const container = containerRef.current;
//     if (!container) return;

//     const nearTop = container.scrollTop < 200;
//     const nearBottom = container.scrollTop + container.clientHeight >= container.scrollHeight - 200;

//     setIsAtTop(nearTop);
//     setIsAtBottom(nearBottom);

//     if (nearTop && hasMore && !isFetchingMore) {
//       setIsFetchingMore(true);
//       const prevScrollHeight = container.scrollHeight;
//       await fetchMoreLogs(jobId);
//       setIsFetchingMore(false);
//       const newScrollHeight = container.scrollHeight;
//       container.scrollTop = newScrollHeight - prevScrollHeight + container.scrollTop;
//     }
//   };

//   const handleManualRefresh = async () => {
//     setInitialLoading(true);
//     await fetchInitialLogs(jobId);
//     setInitialLoading(false);
//     scrollToBottom("smooth");
//   };

//   const formatTime = (ts: number) => {
//     const date = new Date(ts * 1000);
//     const ms = String(date.getMilliseconds()).padStart(3, "0");
//     const main = date.toLocaleString(undefined, {
//       year: "numeric",
//       month: "short",
//       day: "numeric",
//       hour: "2-digit",
//       minute: "2-digit",
//       second: "2-digit",
//       hour12: false,
//     });
//     return `${main}.${ms}`;
//   };

//   const refreshDisabled =
//     initialLoading || ((jobStatus === "COMPLETED" || jobStatus === "FAILED") && logs.length === 0);

//   return (
//     <div className="mt-4 flex flex-col max-h-[calc(100vh-210px)]">
//       <div className="flex items-center justify-between mb-3">
//         <h2 className="text-lg font-semibold text-[#3b5a64]">Logs</h2>
//         <Button onClick={handleManualRefresh} variant="outline" disabled={refreshDisabled}>
//           {initialLoading ? (
//             <>
//               <Loader2 className="w-4 h-4 animate-spin mr-2" />
//               Refreshing
//             </>
//           ) : (
//             "Refresh"
//           )}
//         </Button>
//       </div>

//       <div
//         ref={containerRef}
//         onScroll={handleScroll}
//         className="flex-1 bg-white border border-gray-200 rounded-lg shadow-sm overflow-y-auto relative"
//       >
//         {initialLoading ? (
//           <div className="flex h-full items-center justify-center py-10">
//             <Loader2 className="h-6 w-6 animate-spin text-[#3b5a64]" />
//           </div>
//         ) : (
//           <ul className="font-mono text-xs text-gray-800">
//             {logs.length === 0 ? (
//               <li className="px-4 py-2 text-gray-400 text-sm text-center italic min-h-[75px] flex items-center justify-center">
//                 No logs
//               </li>
//             ) : (
//               <>
//                 {isAtTop && (
//                   <>
//                     {isFetchingMore && hasMore && (
//                       <li className="px-4 py-2 flex gap-2 items-center text-gray-400 justify-center">
//                         <Loader2 className="h-4 w-4 animate-spin text-[#3b5a64]" />
//                         Loading more…
//                       </li>
//                     )}
//                     {!hasMore && (
//                       <li className="px-4 py-2 text-gray-400 text-sm text-center italic">
//                         No more logs
//                       </li>
//                     )}
//                   </>
//                 )}
//                 {logs.map((log) => (
//                   <li
//                     key={log.id ?? `${log.created_at}-${log.message}`}
//                     className="flex px-4 py-2 border-t border-gray-300 gap-5"
//                   >
//                     <span className="text-gray-600 min-w-[220px]">{formatTime(log.created_at)}</span>
//                     <span>{log.message || "No message"}</span>
//                   </li>
//                 ))}
//               </>
//             )}
//             {isFetchingMore && !isAtTop && (
//               <li className="px-4 py-2 flex gap-2 items-center text-gray-400 justify-center">
//                 <Loader2 className="h-4 w-4 animate-spin text-[#3b5a64]" />
//                 Loading more…
//               </li>
//             )}
//           </ul>
//         )}
//       </div>
//     </div>
//   );
// };

// export default JobLogs;





// import { useEffect, useRef, useState } from "react";
// import { useLogsContext } from "@/contexts/LogsContext";
// import { Loader2 } from "lucide-react";
// import { Button } from "@/components/ui/button";
// import { FixedSizeList as List } from "react-window";

// interface JobLogsProps {
//   jobId: string;
//   jobStatus?: string;
// }

// const JobLogs = ({ jobId, jobStatus }: JobLogsProps) => {
//   const { logsByJobId, hasMoreByJobId, fetchInitialLogs, fetchMoreLogs } = useLogsContext();
//   const rawLogs = logsByJobId[jobId] || [];
//   const logs = [...rawLogs];

//   const [initialLoading, setInitialLoading] = useState(true);
//   const [isFetchingMore, setIsFetchingMore] = useState(false);
//   const [listHeight, setListHeight] = useState(window.innerHeight - 210);

//   const listRef = useRef<any>(null);

//   useEffect(() => {
//     const updateHeight = () => setListHeight(window.innerHeight - 210);
//     window.addEventListener("resize", updateHeight);
//     return () => window.removeEventListener("resize", updateHeight);
//   }, []);

//   useEffect(() => {
//     const load = async () => {
//       setInitialLoading(true);
//       await fetchInitialLogs(jobId);
//       setInitialLoading(false);
//     };
//     load();
//   }, [jobId]);

//   useEffect(() => {
//     if (!initialLoading && logs.length > 0 && listRef.current) {
//       listRef.current.scrollToItem(logs.length - 1, "end");
//     }
//   }, [initialLoading, logs.length]);

//   const refreshDisabled =
//     initialLoading || ((jobStatus === "COMPLETED" || jobStatus === "FAILED") && logs.length === 0);

//   const handleManualRefresh = async () => {
//     setInitialLoading(true);
//     await fetchInitialLogs(jobId);
//     setInitialLoading(false);
//     if (listRef.current) {
//       listRef.current.scrollToItem(logs.length - 1, "end");
//     }
//   };

//   const formatTime = (ts: number) => {
//     const date = new Date(ts * 1000);
//     const ms = String(date.getMilliseconds()).padStart(3, "0");
//     const main = date.toLocaleString(undefined, {
//       year: "numeric",
//       month: "short",
//       day: "numeric",
//       hour: "2-digit",
//       minute: "2-digit",
//       second: "2-digit",
//       hour12: false,
//     });
//     return `${main}.${ms}`;
//   };

//   return (
//     <div className="mt-4 flex flex-col max-h-[calc(100vh-210px)]">
//       <div className="flex items-center justify-between mb-3">
//         <h2 className="text-lg font-semibold text-[#3b5a64]">Logs</h2>
//         <Button onClick={handleManualRefresh} variant="outline" disabled={refreshDisabled}>
//           {initialLoading ? (
//             <>
//               <Loader2 className="w-4 h-4 animate-spin mr-2" /> Refreshing
//             </>
//           ) : (
//             "Refresh"
//           )}
//         </Button>
//       </div>

//       <div className="flex-1 bg-white border border-gray-200 rounded-lg shadow-sm relative">
//         {initialLoading ? (
//           <div className="flex h-full items-center justify-center py-10">
//             <Loader2 className="h-6 w-6 animate-spin text-[#3b5a64]" />
//           </div>
//         ) : logs.length === 0 ? (
//           <ul className="font-mono text-xs text-gray-800">
//             <li className="px-4 py-2 text-gray-400 text-sm text-center italic min-h-[75px] flex items-center justify-center">
//               No logs
//             </li>
//           </ul>
//         ) : (
//           <div className="font-mono text-xs text-gray-800 h-full">
//             {isFetchingMore && (
//               <div className="px-4 py-2 flex gap-2 items-center text-gray-400 justify-center">
//                 <Loader2 className="h-4 w-4 animate-spin text-[#3b5a64]" /> Loading more…
//               </div>
//             )}

//             <List
//               height={listHeight}
//               itemCount={logs.length}
//               itemSize={36}
//               width="100%"
//               ref={listRef}
//               onScroll={({ scrollOffset }) => {
//                 if (scrollOffset < 200 && hasMoreByJobId[jobId] && !isFetchingMore) {
//                   setIsFetchingMore(true);
//                   fetchMoreLogs(jobId).finally(() => setIsFetchingMore(false));
//                 }
//               }}
//             >
//               {({ index, style }) => {
//                 const log = logs[index];
//                 return (
//                   <div
//                     key={log.id ?? `${log.created_at}-${log.message}`}
//                     style={style}
//                     className="flex px-4 py-2 border-t border-gray-300 gap-5"
//                   >
//                     <span className="text-gray-600 min-w-[220px]">
//                       {formatTime(log.created_at)}
//                     </span>
//                     <span>{log.message || "No message"}</span>
//                   </div>
//                 );
//               }}
//             </List>

//             {!hasMoreByJobId[jobId] && (
//               <div className="px-4 py-2 text-gray-400 text-sm text-center italic">
//                 No more logs
//               </div>
//             )}
//           </div>
//         )}
//       </div>
//     </div>
//   );
// };

// export default JobLogs;


import { useEffect, useRef, useState } from "react";
import { useLogsContext } from "@/contexts/LogsContext";
import { Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { FixedSizeList as List } from "react-window";

interface JobLogsProps {
  jobId: string;
  jobStatus?: string;
}

const JobLogs = ({ jobId, jobStatus }: JobLogsProps) => {
  const { logsByJobId, hasMoreByJobId, fetchInitialLogs, fetchMoreLogs } = useLogsContext();
  const rawLogs = logsByJobId[jobId] || [];
  const logs = [...rawLogs];

  const [initialLoading, setInitialLoading] = useState(true);
  const [isFetchingMore, setIsFetchingMore] = useState(false);
  const [listHeight, setListHeight] = useState(window.innerHeight - 210);
  const [hasAutoScrolled, setHasAutoScrolled] = useState(false);

  const listRef = useRef<any>(null);

  useEffect(() => {
    const updateHeight = () => setListHeight(window.innerHeight - 210);
    window.addEventListener("resize", updateHeight);
    return () => window.removeEventListener("resize", updateHeight);
  }, []);

  useEffect(() => {
    const load = async () => {
      setInitialLoading(true);
      await fetchInitialLogs(jobId);
      setInitialLoading(false);
    };
    load();
  }, [jobId]);

  useEffect(() => {
    if (!initialLoading && logs.length > 0 && listRef.current && !hasAutoScrolled) {
      listRef.current.scrollToItem(logs.length, "end");
      setHasAutoScrolled(true);
    }
  }, [initialLoading, logs.length, hasAutoScrolled]);

  const refreshDisabled =
    initialLoading || ((jobStatus === "COMPLETED" || jobStatus === "FAILED") && logs.length === 0);

  const handleManualRefresh = async () => {
    setInitialLoading(true);
    await fetchInitialLogs(jobId);
    setInitialLoading(false);
    setHasAutoScrolled(false);
  };

  const formatTime = (ts: number) => {
    const date = new Date(ts * 1000);
    const ms = String(date.getMilliseconds()).padStart(3, "0");
    const main = date.toLocaleString(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    });
    return `${main}.${ms}`;
  };

  const handleFetchMorePreservePosition = async () => {
    if (!listRef.current) return;
    const scrollEl = listRef.current._outerRef;
    const prevScrollHeight = scrollEl.scrollHeight;
    const prevScrollTop = scrollEl.scrollTop;

    await fetchMoreLogs(jobId);

    requestAnimationFrame(() => {
      const newScrollHeight = scrollEl.scrollHeight;
      scrollEl.scrollTop = newScrollHeight - prevScrollHeight + prevScrollTop;
      listRef.current?.scrollTo(scrollEl.scrollTop + 1);
    });
  };

  const hasExtraRow = logs.length >= 1000;
  const totalItemCount = logs.length + (hasExtraRow ? 1 : 0);
  const listMaxHeight = window.innerHeight - 260;

  return (
    <div
      className="mt-4 mb-8 flex flex-col"
      style={{ maxHeight: "calc(100vh - 140px)", overflow: "hidden" }}
    >
      <div className="flex items-center justify-between mb-3">
        <h2 className="text-lg font-semibold text-[#3b5a64]">Logs</h2>
        <Button onClick={handleManualRefresh} variant="outline" disabled={refreshDisabled}>
          {initialLoading ? (
            <>
              <Loader2 className="w-4 h-4 animate-spin mr-2" /> Refreshing
            </>
          ) : (
            "Refresh"
          )}
        </Button>
      </div>

      <div className="flex-1 bg-white border border-gray-200 rounded-lg shadow-sm relative">
        {initialLoading ? (
          <div className="flex h-full items-center justify-center py-10">
            <Loader2 className="h-6 w-6 animate-spin text-[#3b5a64]" />
          </div>
        ) : logs.length === 0 ? (
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
              itemSize={36}
              width="100%"
              ref={listRef}
              onScroll={({ scrollOffset }) => {
                const isNearTop = scrollOffset < 200;
                if (isNearTop && hasMoreByJobId[jobId] && !isFetchingMore) {
                  setIsFetchingMore(true);
                  handleFetchMorePreservePosition().finally(() => setIsFetchingMore(false));
                }
              }}
            >
              {({ index, style }) => {
                if (hasExtraRow && index === 0) {
                  return (
                    <div style={style} className="text-center px-4 py-2 text-gray-400">
                      {!hasMoreByJobId[jobId]
                        ? "No more logs"
                        : isFetchingMore
                        ? (
                            <div className="flex justify-center items-center gap-2">
                              <Loader2 className="h-4 w-4 animate-spin text-[#3b5a64]" /> Loading more…
                            </div>
                          )
                        : null}
                    </div>
                  );
                }

                const log = logs[index - (hasExtraRow ? 1 : 0)];
                return (
                  <div
                    key={log.id ?? `${log.created_at}-${log.message}`}
                    style={style}
                    className="flex px-4 py-2 border-t border-gray-300 gap-5"
                  >
                    <span className="text-gray-600 min-w-[220px]">
                      {formatTime(log.created_at)}
                    </span>
                    <span>{log.message || "No message"}</span>
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
